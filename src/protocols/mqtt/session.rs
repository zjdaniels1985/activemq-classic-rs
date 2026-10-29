//! MQTT packet state, shared across reconnects. Durable message queues live in core.

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::codec::{Publish, invalid};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Sent {
    pub id: u16,
    pub qos: u8,
    pub released: bool,
}

#[derive(Default, Serialize, Deserialize)]
pub(super) struct Session {
    pub persistent: bool,
    #[serde(skip)]
    pub owner: u64,
    pub subscriptions: BTreeMap<String, u8>,
    pub incoming: BTreeMap<u16, Publish>,
    pub outgoing: BTreeMap<String, Sent>,
    pub next_id: u16,
}

impl Session {
    pub fn packet_id(&mut self) -> io::Result<u16> {
        for _ in 0..65535 {
            self.next_id = self.next_id.wrapping_add(1).max(1);
            if !self.outgoing.values().any(|sent| sent.id == self.next_id) {
                return Ok(self.next_id);
            }
        }
        Err(invalid("all MQTT packet identifiers are in use"))
    }
}

type Sessions = BTreeMap<String, Arc<Mutex<Session>>>;

pub(super) struct Store {
    pub handshake: tokio::sync::Mutex<()>,
    sessions: Mutex<Sessions>,
    directory: Option<PathBuf>,
}

impl Store {
    pub fn new(directory: Option<&str>) -> io::Result<Self> {
        let directory = directory.map(PathBuf::from);
        if let Some(directory) = &directory {
            std::fs::create_dir_all(directory)?;
        }
        Ok(Self {
            handshake: tokio::sync::Mutex::new(()),
            sessions: Mutex::new(BTreeMap::new()),
            directory,
        })
    }

    fn path(&self, client: &str) -> Option<PathBuf> {
        self.directory.as_ref().map(|directory| {
            directory.join(format!(
                "{}.json",
                hex::encode(Sha256::digest(client.as_bytes()))
            ))
        })
    }

    pub fn open(
        &self,
        client: &str,
        clean: bool,
        owner: u64,
    ) -> io::Result<(Arc<Mutex<Session>>, bool)> {
        let mut sessions = self.sessions.lock().unwrap();
        if clean
            || sessions
                .get(client)
                .is_some_and(|s| !s.lock().unwrap().persistent)
        {
            sessions.remove(client);
            self.remove_file(client)?;
        }
        let mut present = sessions.contains_key(client);
        let session = if let Some(session) = sessions.get(client) {
            Arc::clone(session)
        } else {
            let data = if let Some(path) = self.path(client).filter(|path| path.exists()) {
                present = true;
                serde_json::from_slice(&std::fs::read(path)?).map_err(io::Error::other)?
            } else {
                Session::default()
            };
            let session = Arc::new(Mutex::new(data));
            sessions.insert(client.to_owned(), Arc::clone(&session));
            session
        };
        {
            let mut state = session.lock().unwrap();
            state.owner = owner;
            state.persistent = !clean;
        }
        Ok((session, present))
    }

    pub fn save(&self, client: &str, session: &Session, clean: bool) -> io::Result<()> {
        if !clean && let Some(path) = self.path(client) {
            let bytes = serde_json::to_vec(session).map_err(io::Error::other)?;
            let staging = path.with_extension("new");
            let mut file = std::fs::File::create(&staging)?;
            use std::io::Write;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::rename(staging, path)?;
        }
        Ok(())
    }

    pub fn remove(&self, client: &str, owner: u64) -> io::Result<()> {
        let mut sessions = self.sessions.lock().unwrap();
        if sessions
            .get(client)
            .is_some_and(|s| s.lock().unwrap().owner == owner)
        {
            sessions.remove(client);
            self.remove_file(client)?;
        }
        Ok(())
    }

    fn remove_file(&self, client: &str) -> io::Result<()> {
        if let Some(path) = self.path(client) {
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}
