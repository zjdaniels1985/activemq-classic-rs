use crate::openwire_commands::IncomingMessage;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const MESSAGE_PREFIX: &[u8] = b"message:";
const INDEX_PREFIX: &[u8] = b"id:";

pub struct SledStore {
    tree: sled::Tree,
    next_key: AtomicU64,
}

impl SledStore {
    pub fn open(path: &Path) -> io::Result<Self> {
        let database = sled::open(path).map_err(storage_error)?;
        let tree = database
            .open_tree("queue-messages")
            .map_err(storage_error)?;
        let mut recovered = Vec::new();
        let mut max_key = 0_u64;
        for result in tree.scan_prefix(MESSAGE_PREFIX) {
            let (key, value) = result.map_err(storage_error)?;
            let sequence = parse_message_sequence(&key)?;
            max_key = max_key.max(sequence);
            let message: IncomingMessage = serde_json::from_slice(&value).map_err(storage_error)?;
            recovered.push((sequence, message));
        }
        for result in tree.scan_prefix(INDEX_PREFIX) {
            let (key, _) = result.map_err(storage_error)?;
            tree.remove(key).map_err(storage_error)?;
        }
        for (sequence, message) in &recovered {
            tree.insert(index_key(message), sequence.to_be_bytes().to_vec())
                .map_err(storage_error)?;
        }
        tree.flush().map_err(storage_error)?;
        Ok(Self {
            tree,
            next_key: AtomicU64::new(max_key.saturating_add(1)),
        })
    }

    pub fn recover(&self) -> io::Result<Vec<IncomingMessage>> {
        let mut recovered = Vec::new();
        for result in self.tree.scan_prefix(MESSAGE_PREFIX) {
            let (_, value) = result.map_err(storage_error)?;
            recovered.push(serde_json::from_slice(&value).map_err(storage_error)?);
        }
        Ok(recovered)
    }

    pub fn save(&self, message: &IncomingMessage) -> io::Result<()> {
        if !message.persistent {
            return Ok(());
        }
        let sequence = self.next_key.fetch_add(1, Ordering::Relaxed);
        let key = message_key(sequence);
        let value = serde_json::to_vec(message).map_err(storage_error)?;
        self.tree
            .insert(key.clone(), value)
            .map_err(storage_error)?;
        self.tree
            .insert(index_key(message), sequence.to_be_bytes().to_vec())
            .map_err(storage_error)?;
        self.tree.flush().map_err(storage_error)?;
        Ok(())
    }

    pub fn acknowledge(&self, message: &IncomingMessage) -> io::Result<()> {
        if !message.persistent {
            return Ok(());
        }
        let index = index_key(message);
        if let Some(sequence) = self.tree.get(&index).map_err(storage_error)? {
            if sequence.len() == 8 {
                let sequence: [u8; 8] = sequence.as_ref().try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid sled message index")
                })?;
                self.tree
                    .remove(message_key(u64::from_be_bytes(sequence)))
                    .map_err(storage_error)?;
            }
            self.tree.remove(index).map_err(storage_error)?;
            self.tree.flush().map_err(storage_error)?;
        }
        Ok(())
    }

    pub fn purge_queue(&self, destination: &str) -> io::Result<usize> {
        let mut removed = Vec::new();
        for result in self.tree.scan_prefix(MESSAGE_PREFIX) {
            let (key, value) = result.map_err(storage_error)?;
            let message: IncomingMessage = serde_json::from_slice(&value).map_err(storage_error)?;
            if message.destination == destination {
                removed.push((key, message));
            }
        }
        for (key, message) in &removed {
            self.tree.remove(key).map_err(storage_error)?;
            self.tree
                .remove(index_key(message))
                .map_err(storage_error)?;
        }
        if !removed.is_empty() {
            self.tree.flush().map_err(storage_error)?;
        }
        Ok(removed.len())
    }
}

fn message_key(sequence: u64) -> Vec<u8> {
    let mut key = MESSAGE_PREFIX.to_vec();
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn index_key(message: &IncomingMessage) -> Vec<u8> {
    let mut key = INDEX_PREFIX.to_vec();
    let id = &message.producer_id;
    key.extend_from_slice(&(id.connection_id.len() as u32).to_be_bytes());
    key.extend_from_slice(id.connection_id.as_bytes());
    key.extend_from_slice(&id.session_id.to_be_bytes());
    key.extend_from_slice(&id.value.to_be_bytes());
    key.extend_from_slice(&message.producer_sequence.to_be_bytes());
    key
}

fn parse_message_sequence(key: &[u8]) -> io::Result<u64> {
    if key.len() != MESSAGE_PREFIX.len() + 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid sled message key",
        ));
    }
    Ok(u64::from_be_bytes(
        key[MESSAGE_PREFIX.len()..].try_into().unwrap(),
    ))
}

fn storage_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "activemq-sled-store-{}-{}.sled",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn message(destination: &str, persistent: bool, sequence: i64) -> IncomingMessage {
        IncomingMessage {
            command_id: 0,
            response_required: false,
            destination: destination.to_owned(),
            producer_id: crate::openwire_commands::ProducerId {
                connection_id: "client".to_owned(),
                session_id: 1,
                value: 2,
            },
            producer_sequence: sequence,
            timestamp: 0,
            persistent,
            priority: 4,
            text_message: true,
            content: b"body".to_vec(),
        }
    }

    #[test]
    fn persistent_messages_recover_in_sequence_and_acknowledgements_remove_them() {
        let path = test_path();
        {
            let store = SledStore::open(&path).unwrap();
            let first = message("orders", true, 1);
            let second = message("orders", true, 2);
            store.save(&first).unwrap();
            store.save(&second).unwrap();
            assert_eq!(store.recover().unwrap().len(), 2);
        }
        {
            let store = SledStore::open(&path).unwrap();
            let recovered = store.recover().unwrap();
            assert_eq!(recovered.len(), 2);
            assert_eq!(recovered[0].producer_sequence, 1);
            store.acknowledge(&recovered[0]).unwrap();
            assert_eq!(store.recover().unwrap().len(), 1);
            assert_eq!(store.purge_queue("orders").unwrap(), 1);
            assert!(store.recover().unwrap().is_empty());
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn non_persistent_messages_are_not_stored() {
        let path = test_path();
        let store = SledStore::open(&path).unwrap();
        store.save(&message("orders", false, 1)).unwrap();
        assert!(store.recover().unwrap().is_empty());
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
}
