//! Sled-backed persistence (replaces ActiveMQ's KahaDB).

use super::destination::Destination;
use super::message::Message;
use super::scheduler::Job;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io;
use std::path::Path;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DurableRecord {
    pub client_id: String,
    pub name: String,
    pub destination: Destination,
    pub selector: Option<String>,
    pub no_local: bool,
}

/// Persistent image of a prepared XA transaction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PreparedRecord {
    pub xid: XidRecord,
    pub sends: Vec<Message>,
    /// (store key, sequence) of messages acknowledged by the transaction.
    pub acks: Vec<(String, u64)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct XidRecord {
    pub format_id: i32,
    pub global_id: Vec<u8>,
    pub branch_id: Vec<u8>,
}

#[derive(Default)]
pub struct Batch {
    puts: Vec<(Vec<u8>, Vec<u8>)>,
    removes: Vec<Vec<u8>>,
}

impl Batch {
    pub fn is_empty(&self) -> bool {
        self.puts.is_empty() && self.removes.is_empty()
    }

    pub fn put_message(
        &mut self,
        store_key: &str,
        sequence: u64,
        message: &Message,
    ) -> io::Result<()> {
        self.puts
            .push((message_key(store_key, sequence), encode(message)?));
        Ok(())
    }

    pub fn remove_message(&mut self, store_key: &str, sequence: u64) {
        self.removes.push(message_key(store_key, sequence));
    }
}

pub struct Store {
    database: sled::Db,
    messages: sled::Tree,
    destinations: sled::Tree,
    durables: sled::Tree,
    jobs: sled::Tree,
    prepared: sled::Tree,
    retained: sled::Tree,
}

pub type RecoveredMessages = BTreeMap<String, Vec<(u64, Message)>>;

impl Store {
    pub fn open(path: &Path) -> io::Result<Self> {
        // Another handle (e.g. a broker that is still shutting down) may hold
        // the sled file lock for a moment; retry briefly before failing.
        let mut attempts = 0;
        let database = loop {
            match sled::open(path) {
                Ok(database) => break database,
                Err(sled::Error::Io(error))
                    if attempts < 100 && error.to_string().contains("could not acquire lock") =>
                {
                    attempts += 1;
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
                Err(error) => return Err(storage_error(error)),
            }
        };
        Ok(Self {
            messages: database.open_tree("messages").map_err(storage_error)?,
            destinations: database.open_tree("destinations").map_err(storage_error)?,
            durables: database.open_tree("durables").map_err(storage_error)?,
            jobs: database
                .open_tree("scheduled-jobs")
                .map_err(storage_error)?,
            prepared: database
                .open_tree("prepared-transactions")
                .map_err(storage_error)?,
            retained: database.open_tree("retained").map_err(storage_error)?,
            database,
        })
    }

    pub fn apply(&self, batch: Batch) -> io::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let mut sled_batch = sled::Batch::default();
        for (key, value) in batch.puts {
            sled_batch.insert(key, value);
        }
        for key in batch.removes {
            sled_batch.remove(key);
        }
        self.messages
            .apply_batch(sled_batch)
            .map_err(storage_error)?;
        self.flush()
    }

    pub fn flush(&self) -> io::Result<()> {
        self.database.flush().map(|_| ()).map_err(storage_error)
    }

    pub fn load_messages(&self) -> io::Result<(RecoveredMessages, u64)> {
        let mut recovered = RecoveredMessages::new();
        let mut max_sequence = 0;
        for entry in self.messages.iter() {
            let (key, value) = entry.map_err(storage_error)?;
            let (store_key, sequence) = parse_message_key(&key)?;
            max_sequence = max_sequence.max(sequence);
            recovered
                .entry(store_key)
                .or_default()
                .push((sequence, decode(&value)?));
        }
        Ok((recovered, max_sequence))
    }

    pub fn purge(&self, store_key: &str) -> io::Result<usize> {
        let mut prefix = store_key.as_bytes().to_vec();
        prefix.push(0);
        let mut batch = sled::Batch::default();
        let mut removed = 0;
        for entry in self.messages.scan_prefix(&prefix) {
            let (key, _) = entry.map_err(storage_error)?;
            batch.remove(key);
            removed += 1;
        }
        self.messages.apply_batch(batch).map_err(storage_error)?;
        self.flush()?;
        Ok(removed)
    }

    pub fn put_destination(&self, destination: &Destination) -> io::Result<()> {
        self.destinations
            .insert(destination.to_uri(), encode(destination)?)
            .map_err(storage_error)?;
        self.flush()
    }

    pub fn remove_destination(&self, destination: &Destination) -> io::Result<()> {
        self.destinations
            .remove(destination.to_uri())
            .map_err(storage_error)?;
        self.flush()
    }

    pub fn load_destinations(&self) -> io::Result<Vec<Destination>> {
        load_all(&self.destinations)
    }

    pub fn put_durable(&self, record: &DurableRecord) -> io::Result<()> {
        self.durables
            .insert(
                durable_key(&record.client_id, &record.name),
                encode(record)?,
            )
            .map_err(storage_error)?;
        self.flush()
    }

    pub fn remove_durable(&self, client_id: &str, name: &str) -> io::Result<()> {
        self.durables
            .remove(durable_key(client_id, name))
            .map_err(storage_error)?;
        self.flush()
    }

    pub fn load_durables(&self) -> io::Result<Vec<DurableRecord>> {
        load_all(&self.durables)
    }

    pub fn put_job(&self, job: &Job) -> io::Result<()> {
        self.jobs
            .insert(job.id.as_bytes(), encode(job)?)
            .map_err(storage_error)?;
        self.flush()
    }

    pub fn remove_job(&self, id: &str) -> io::Result<()> {
        self.jobs.remove(id.as_bytes()).map_err(storage_error)?;
        self.flush()
    }

    pub fn load_jobs(&self) -> io::Result<Vec<Job>> {
        load_all(&self.jobs)
    }

    pub fn put_prepared(&self, record: &PreparedRecord) -> io::Result<()> {
        self.prepared
            .insert(encode(&record.xid)?, encode(record)?)
            .map_err(storage_error)?;
        self.flush()
    }

    pub fn remove_prepared(&self, xid: &XidRecord) -> io::Result<()> {
        self.prepared.remove(encode(xid)?).map_err(storage_error)?;
        self.flush()
    }

    pub fn load_prepared(&self) -> io::Result<Vec<PreparedRecord>> {
        load_all(&self.prepared)
    }

    pub fn put_retained(&self, topic: &str, message: &Message) -> io::Result<()> {
        self.retained
            .insert(topic.as_bytes(), encode(message)?)
            .map_err(storage_error)?;
        self.flush()
    }

    pub fn remove_retained(&self, topic: &str) -> io::Result<()> {
        self.retained
            .remove(topic.as_bytes())
            .map_err(storage_error)?;
        self.flush()
    }

    pub fn load_retained(&self) -> io::Result<Vec<Message>> {
        load_all(&self.retained)
    }

    pub fn size_on_disk(&self) -> u64 {
        self.database.size_on_disk().unwrap_or_default()
    }
}

fn load_all<T: DeserializeOwned>(tree: &sled::Tree) -> io::Result<Vec<T>> {
    tree.iter()
        .map(|entry| {
            let (_, value) = entry.map_err(storage_error)?;
            decode(&value)
        })
        .collect()
}

fn durable_key(client_id: &str, name: &str) -> Vec<u8> {
    let mut key = client_id.as_bytes().to_vec();
    key.push(0);
    key.extend_from_slice(name.as_bytes());
    key
}

fn message_key(store_key: &str, sequence: u64) -> Vec<u8> {
    let mut key = store_key.as_bytes().to_vec();
    key.push(0);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn parse_message_key(key: &[u8]) -> io::Result<(String, u64)> {
    if key.len() < 9 || key[key.len() - 9] != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid sled message key",
        ));
    }
    let split = key.len() - 9;
    let store_key = String::from_utf8(key[..split].to_vec())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid store key"))?;
    let sequence = u64::from_be_bytes(key[split + 1..].try_into().expect("eight bytes"));
    Ok((store_key, sequence))
}

fn encode<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    bincode::serialize(value).map_err(storage_error)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> io::Result<T> {
    bincode::deserialize(bytes).map_err(storage_error)
}

fn storage_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("sled store: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::message::Value;

    pub(crate) fn temp_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "activemq-rs-{name}-{}-{}",
            std::process::id(),
            crate::core::message::now_millis()
        ))
    }

    #[test]
    fn messages_round_trip_in_sequence_order() {
        let path = temp_path("store");
        {
            let store = Store::open(&path).unwrap();
            let mut batch = Batch::default();
            let message =
                Message::text(Destination::queue("q"), "one").with_property("n", Value::Long(1));
            batch.put_message("queue:q", 2, &message).unwrap();
            batch
                .put_message(
                    "queue:q",
                    1,
                    &Message::text(Destination::queue("q"), "zero"),
                )
                .unwrap();
            batch
                .put_message(
                    "queue:q.sub",
                    3,
                    &Message::text(Destination::queue("q.sub"), "x"),
                )
                .unwrap();
            store.apply(batch).unwrap();
            store.put_destination(&Destination::queue("q")).unwrap();
        }
        let store = Store::open(&path).unwrap();
        let (messages, max) = store.load_messages().unwrap();
        assert_eq!(max, 3);
        let queue = &messages["queue:q"];
        assert_eq!(queue[0].1.text_body(), Some("zero"));
        assert_eq!(queue[1].1.properties["n"], Value::Long(1));
        assert_eq!(messages["queue:q.sub"].len(), 1);
        assert_eq!(store.purge("queue:q").unwrap(), 2);
        assert_eq!(store.load_messages().unwrap().0.len(), 1);
        assert_eq!(
            store.load_destinations().unwrap(),
            vec![Destination::queue("q")]
        );
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
}
