//! Sled-backed persistence (replaces ActiveMQ's KahaDB).

use super::destination::Destination;
use super::message::Message;
use super::scheduler::Job;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use sled::transaction::Transactional;

use super::types::Event;

pub(crate) type PendingEvent = (tokio::sync::mpsc::UnboundedSender<Event>, Event);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StoreOptions {
    pub queue_capacity: usize,
    pub max_batch_operations: usize,
    pub max_batch_bytes: usize,
    pub max_pending_bytes: usize,
    pub max_delay_us: u64,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            queue_capacity: 64,
            max_batch_operations: 64,
            max_batch_bytes: 4 * 1024 * 1024,
            max_pending_bytes: 64 * 1024 * 1024,
            max_delay_us: 0,
        }
    }
}

impl StoreOptions {
    pub fn validate(&self) -> io::Result<()> {
        if self.queue_capacity == 0
            || self.max_batch_operations == 0
            || self.max_batch_bytes == 0
            || self.max_pending_bytes == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "group commit queue_capacity, max_batch_operations, max_batch_bytes and max_pending_bytes must be positive",
            ));
        }
        if self.max_delay_us > 1_000_000 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "group commit max_delay_us must be between 0 and 1000000",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Tree {
    Messages,
    Destinations,
    Durables,
    Jobs,
    Prepared,
    Retained,
}

struct Mutation {
    tree: Tree,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

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
    operations: Vec<Mutation>,
    bytes: usize,
    error: Option<String>,
}

impl Batch {
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty() && self.error.is_none()
    }

    pub fn put_message(
        &mut self,
        store_key: &str,
        sequence: u64,
        message: &Message,
    ) -> io::Result<()> {
        self.put(Tree::Messages, message_key(store_key, sequence), message)
    }

    pub fn remove_message(&mut self, store_key: &str, sequence: u64) {
        self.remove(Tree::Messages, message_key(store_key, sequence));
    }

    fn put<T: Serialize>(&mut self, tree: Tree, key: Vec<u8>, value: &T) -> io::Result<()> {
        let value = encode(value).inspect_err(|error| {
            self.error = Some(error.to_string());
        })?;
        self.bytes = self
            .bytes
            .saturating_add(key.len())
            .saturating_add(value.len());
        self.operations.push(Mutation {
            tree,
            key,
            value: Some(value),
        });
        Ok(())
    }

    fn remove(&mut self, tree: Tree, key: Vec<u8>) {
        self.bytes = self.bytes.saturating_add(key.len());
        self.operations.push(Mutation {
            tree,
            key,
            value: None,
        });
    }

    pub fn put_destination(&mut self, destination: &Destination) -> io::Result<()> {
        self.put(
            Tree::Destinations,
            destination.to_uri().into_bytes(),
            destination,
        )
    }

    pub fn remove_destination(&mut self, destination: &Destination) {
        self.remove(Tree::Destinations, destination.to_uri().into_bytes());
    }

    pub fn put_durable(&mut self, record: &DurableRecord) -> io::Result<()> {
        self.put(
            Tree::Durables,
            durable_key(&record.client_id, &record.name),
            record,
        )
    }

    pub fn remove_durable(&mut self, client_id: &str, name: &str) {
        self.remove(Tree::Durables, durable_key(client_id, name));
    }

    pub fn put_job(&mut self, job: &Job) -> io::Result<()> {
        self.put(Tree::Jobs, job.id.as_bytes().to_vec(), job)
    }

    pub fn remove_job(&mut self, id: &str) {
        self.remove(Tree::Jobs, id.as_bytes().to_vec());
    }

    pub fn put_prepared(&mut self, record: &PreparedRecord) -> io::Result<()> {
        self.put(Tree::Prepared, encode(&record.xid)?, record)
    }

    pub fn remove_prepared(&mut self, xid: &XidRecord) -> io::Result<()> {
        self.remove(Tree::Prepared, encode(xid)?);
        Ok(())
    }

    pub fn put_retained(&mut self, topic: &str, message: &Message) -> io::Result<()> {
        self.put(Tree::Retained, topic.as_bytes().to_vec(), message)
    }

    pub fn remove_retained(&mut self, topic: &str) {
        self.remove(Tree::Retained, topic.as_bytes().to_vec());
    }
}

#[derive(Default)]
struct CommitMetrics {
    requests: AtomicU64,
    flushes: AtomicU64,
    bytes: AtomicU64,
    flush_nanos: AtomicU64,
    max_group: AtomicU64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct StoreMetrics {
    pub commit_requests: u64,
    pub flushes: u64,
    pub written_bytes: u64,
    pub flush_nanos: u64,
    pub max_group_size: u64,
    pub failed: bool,
    pub pending_bytes: usize,
}

struct CommitRequest {
    batch: Batch,
    events: Vec<PendingEvent>,
    complete: mpsc::SyncSender<Result<(), String>>,
    _budget: BytePermit,
}

struct ByteBudget {
    used: Mutex<usize>,
    available: Condvar,
    limit: usize,
}

struct BytePermit {
    budget: Arc<ByteBudget>,
    bytes: usize,
}

impl ByteBudget {
    fn reserve(self: &Arc<Self>, bytes: usize) -> BytePermit {
        let mut used = self.used.lock().unwrap_or_else(|e| e.into_inner());
        // An oversized atomic operation may occupy the budget alone; splitting
        // it or rejecting it after state mutation would violate atomicity.
        while *used > 0 && used.saturating_add(bytes) > self.limit {
            used = blocking(|| self.available.wait(used).unwrap_or_else(|e| e.into_inner()));
        }
        *used = used.saturating_add(bytes);
        BytePermit {
            budget: Arc::clone(self),
            bytes,
        }
    }
}

impl Drop for BytePermit {
    fn drop(&mut self) {
        let mut used = self.budget.used.lock().unwrap_or_else(|e| e.into_inner());
        *used = used.saturating_sub(self.bytes);
        self.budget.available.notify_all();
    }
}

pub(crate) struct CommitReceipt(mpsc::Receiver<Result<(), String>>);

impl CommitReceipt {
    pub(crate) fn wait(self) -> io::Result<()> {
        blocking(|| self.0.recv().map_err(storage_error)?.map_err(storage_error))
    }
}

// Preserve the synchronous native API without pinning a multi-threaded Tokio
// worker while waiting on disk, a full commit queue, or a contended state lock.
pub(crate) fn blocking<T>(wait: impl FnOnce() -> T) -> T {
    if tokio::runtime::Handle::try_current()
        .is_ok_and(|handle| handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
    {
        tokio::task::block_in_place(wait)
    } else {
        wait()
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
    commits: Option<mpsc::SyncSender<CommitRequest>>,
    worker: Option<thread::JoinHandle<()>>,
    failure: Arc<Mutex<Option<String>>>,
    metrics: Arc<CommitMetrics>,
    budget: Arc<ByteBudget>,
    #[cfg(test)]
    control: Arc<TestCommitControl>,
}

#[cfg(test)]
type FlushGate = (mpsc::SyncSender<()>, mpsc::Receiver<()>);

#[cfg(test)]
#[derive(Default)]
struct TestCommitControl {
    gate: Mutex<Option<FlushGate>>,
    fail_next: std::sync::atomic::AtomicBool,
}

pub type RecoveredMessages = BTreeMap<String, Vec<(u64, Message)>>;

impl Store {
    pub fn open(path: &Path) -> io::Result<Self> {
        Self::open_with_options(path, StoreOptions::default())
    }

    pub fn open_with_options(path: &Path, options: StoreOptions) -> io::Result<Self> {
        options.validate()?;
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
        let mut store = Self {
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
            commits: None,
            worker: None,
            failure: Arc::new(Mutex::new(None)),
            metrics: Arc::new(CommitMetrics::default()),
            budget: Arc::new(ByteBudget {
                used: Mutex::new(0),
                available: Condvar::new(),
                limit: options.max_pending_bytes,
            }),
            #[cfg(test)]
            control: Arc::new(TestCommitControl::default()),
        };
        let (sender, receiver) = mpsc::sync_channel(options.queue_capacity);
        let trees = [
            store.messages.clone(),
            store.destinations.clone(),
            store.durables.clone(),
            store.jobs.clone(),
            store.prepared.clone(),
            store.retained.clone(),
        ];
        let database = store.database.clone();
        let failure = Arc::clone(&store.failure);
        let metrics = Arc::clone(&store.metrics);
        #[cfg(test)]
        let control = Arc::clone(&store.control);
        store.worker = Some(
            thread::Builder::new()
                .name("sled-group-commit".into())
                .spawn(move || {
                    commit_loop(
                        database,
                        trees,
                        receiver,
                        failure,
                        metrics,
                        options,
                        #[cfg(test)]
                        control,
                    )
                })?,
        );
        store.commits = Some(sender);
        Ok(store)
    }

    pub fn apply(&self, batch: Batch) -> io::Result<()> {
        if batch.is_empty() {
            return self.check_health();
        }
        self.submit(batch, Vec::new())?.wait()
    }

    pub fn flush(&self) -> io::Result<()> {
        self.submit(Batch::default(), Vec::new())?.wait()
    }

    pub(crate) fn check_health(&self) -> io::Result<()> {
        match self
            .failure
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            Some(error) => Err(storage_error(error)),
            None => Ok(()),
        }
    }

    pub(crate) fn submit(
        &self,
        batch: Batch,
        events: Vec<PendingEvent>,
    ) -> io::Result<CommitReceipt> {
        self.check_health()?;
        let (complete, receipt) = mpsc::sync_channel(1);
        let budget = self.budget.reserve(batch.bytes);
        let sender = self
            .commits
            .as_ref()
            .ok_or_else(|| storage_error("store is closed"))?;
        let request = CommitRequest {
            batch,
            events,
            complete,
            _budget: budget,
        };
        match sender.try_send(request) {
            Ok(()) => {}
            Err(mpsc::TrySendError::Full(request)) => {
                blocking(|| sender.send(request)).map_err(storage_error)?
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                return Err(storage_error("commit worker is closed"));
            }
        }
        Ok(CommitReceipt(receipt))
    }

    pub fn metrics(&self) -> StoreMetrics {
        StoreMetrics {
            commit_requests: self.metrics.requests.load(Ordering::Relaxed),
            flushes: self.metrics.flushes.load(Ordering::Relaxed),
            written_bytes: self.metrics.bytes.load(Ordering::Relaxed),
            flush_nanos: self.metrics.flush_nanos.load(Ordering::Relaxed),
            max_group_size: self.metrics.max_group.load(Ordering::Relaxed),
            failed: self.check_health().is_err(),
            pending_bytes: *self.budget.used.lock().unwrap_or_else(|e| e.into_inner()),
        }
    }

    #[cfg(test)]
    pub(crate) fn pause_next_flush(&self) -> (mpsc::Receiver<()>, mpsc::SyncSender<()>) {
        let (entered, waiting) = mpsc::sync_channel(1);
        let (resume, gate) = mpsc::sync_channel(1);
        *self.control.gate.lock().unwrap() = Some((entered, gate));
        (waiting, resume)
    }

    #[cfg(test)]
    pub(crate) fn fail_next_flush(&self) {
        self.control.fail_next.store(true, Ordering::SeqCst);
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
        let mut batch = Batch::default();
        let mut removed = 0;
        for entry in self.messages.scan_prefix(&prefix) {
            let (key, _) = entry.map_err(storage_error)?;
            batch.remove(Tree::Messages, key.to_vec());
            removed += 1;
        }
        self.apply(batch)?;
        Ok(removed)
    }

    pub fn put_destination(&self, destination: &Destination) -> io::Result<()> {
        let mut batch = Batch::default();
        batch.put_destination(destination)?;
        self.apply(batch)
    }

    pub fn remove_destination(&self, destination: &Destination) -> io::Result<()> {
        let mut batch = Batch::default();
        batch.remove_destination(destination);
        self.apply(batch)
    }

    pub fn load_destinations(&self) -> io::Result<Vec<Destination>> {
        load_all(&self.destinations)
    }

    pub fn put_durable(&self, record: &DurableRecord) -> io::Result<()> {
        let mut batch = Batch::default();
        batch.put_durable(record)?;
        self.apply(batch)
    }

    pub fn remove_durable(&self, client_id: &str, name: &str) -> io::Result<()> {
        let mut batch = Batch::default();
        batch.remove_durable(client_id, name);
        self.apply(batch)
    }

    pub fn load_durables(&self) -> io::Result<Vec<DurableRecord>> {
        load_all(&self.durables)
    }

    pub fn put_job(&self, job: &Job) -> io::Result<()> {
        let mut batch = Batch::default();
        batch.put_job(job)?;
        self.apply(batch)
    }

    pub fn remove_job(&self, id: &str) -> io::Result<()> {
        let mut batch = Batch::default();
        batch.remove_job(id);
        self.apply(batch)
    }

    pub fn load_jobs(&self) -> io::Result<Vec<Job>> {
        load_all(&self.jobs)
    }

    pub fn put_prepared(&self, record: &PreparedRecord) -> io::Result<()> {
        let mut batch = Batch::default();
        batch.put_prepared(record)?;
        self.apply(batch)
    }

    pub fn remove_prepared(&self, xid: &XidRecord) -> io::Result<()> {
        let mut batch = Batch::default();
        batch.remove_prepared(xid)?;
        self.apply(batch)
    }

    pub fn load_prepared(&self) -> io::Result<Vec<PreparedRecord>> {
        load_all(&self.prepared)
    }

    pub fn put_retained(&self, topic: &str, message: &Message) -> io::Result<()> {
        let mut batch = Batch::default();
        batch.put_retained(topic, message)?;
        self.apply(batch)
    }

    pub fn remove_retained(&self, topic: &str) -> io::Result<()> {
        let mut batch = Batch::default();
        batch.remove_retained(topic);
        self.apply(batch)
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        self.commits.take();
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            tracing::error!("sled commit worker panicked during shutdown");
        }
    }
}

fn commit_loop(
    database: sled::Db,
    trees: [sled::Tree; 6],
    receiver: mpsc::Receiver<CommitRequest>,
    failure: Arc<Mutex<Option<String>>>,
    metrics: Arc<CommitMetrics>,
    options: StoreOptions,
    #[cfg(test)] control: Arc<TestCommitControl>,
) {
    while let Ok(first) = receiver.recv() {
        let deadline = Instant::now() + Duration::from_micros(options.max_delay_us);
        let mut bytes = first.batch.bytes;
        let mut group = vec![first];
        // Event-only operations need ordering, but should not pay the batching delay.
        while bytes > 0
            && bytes < options.max_batch_bytes
            && group.len() < options.max_batch_operations
        {
            // Even with zero delay, coalesce requests that are already waiting.
            let next = match receiver.try_recv() {
                Ok(request) => Ok(request),
                Err(mpsc::TryRecvError::Disconnected) => break,
                Err(mpsc::TryRecvError::Empty) => {
                    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                        break;
                    };
                    receiver.recv_timeout(remaining)
                }
            };
            match next {
                Ok(request) => {
                    bytes = bytes.saturating_add(request.batch.bytes);
                    group.push(request);
                }
                Err(_) => break,
            }
        }
        metrics
            .requests
            .fetch_add(group.len() as u64, Ordering::Relaxed);
        let previous_failure = failure.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let result = if let Some(error) = previous_failure {
            Err(error)
        } else {
            apply_group(
                &database,
                &trees,
                &group,
                &metrics,
                #[cfg(test)]
                &control,
            )
        };
        if let Err(error) = &result {
            tracing::error!(%error, "persistence failed; broker writes are fenced until restart");
            *failure.lock().unwrap_or_else(|e| e.into_inner()) = Some(error.clone());
        }
        for request in group {
            if result.is_ok() {
                for (sender, event) in request.events {
                    let _ = sender.send(event);
                }
            }
            let _ = request.complete.send(result.clone());
        }
    }
}

fn apply_group(
    database: &sled::Db,
    trees: &[sled::Tree; 6],
    group: &[CommitRequest],
    metrics: &CommitMetrics,
    #[cfg(test)] control: &TestCommitControl,
) -> Result<(), String> {
    let mut batches: [sled::Batch; 6] = std::array::from_fn(|_| sled::Batch::default());
    let mut touched = [false; 6];
    let mut bytes = 0u64;
    for request in group {
        if let Some(error) = &request.batch.error {
            return Err(error.clone());
        }
        bytes = bytes.saturating_add(request.batch.bytes as u64);
        for operation in &request.batch.operations {
            let index = match operation.tree {
                Tree::Messages => 0,
                Tree::Destinations => 1,
                Tree::Durables => 2,
                Tree::Jobs => 3,
                Tree::Prepared => 4,
                Tree::Retained => 5,
            };
            touched[index] = true;
            match &operation.value {
                Some(value) => batches[index].insert(operation.key.clone(), value.clone()),
                None => batches[index].remove(operation.key.clone()),
            }
        }
    }
    if bytes == 0 {
        return Ok(());
    }
    if touched.iter().filter(|used| **used).count() == 1 {
        // A tree's native batch is already atomic. Reserve cross-tree
        // transactions for operations that span metadata/message trees.
        let index = touched.iter().position(|used| *used).expect("one tree");
        trees[index]
            .apply_batch(std::mem::take(&mut batches[index]))
            .map_err(|error| error.to_string())?;
    } else {
        (
            &trees[0], &trees[1], &trees[2], &trees[3], &trees[4], &trees[5],
        )
            .transaction(
                |(messages, destinations, durables, jobs, prepared, retained)| {
                    messages.apply_batch(&batches[0])?;
                    destinations.apply_batch(&batches[1])?;
                    durables.apply_batch(&batches[2])?;
                    jobs.apply_batch(&batches[3])?;
                    prepared.apply_batch(&batches[4])?;
                    retained.apply_batch(&batches[5])?;
                    Ok::<_, sled::transaction::ConflictableTransactionError<()>>(())
                },
            )
            .map_err(|error| format!("{error:?}"))?;
    }
    let start = Instant::now();
    #[cfg(test)]
    {
        if let Some((entered, resume)) = control.gate.lock().unwrap().take() {
            entered.send(()).map_err(|error| error.to_string())?;
            resume.recv().map_err(|error| error.to_string())?;
        }
        if control.fail_next.swap(false, Ordering::SeqCst) {
            return Err("injected disk flush failure".into());
        }
    }
    database.flush().map_err(|error| error.to_string())?;
    metrics.flush_nanos.fetch_add(
        start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
        Ordering::Relaxed,
    );
    metrics.flushes.fetch_add(1, Ordering::Relaxed);
    metrics.bytes.fetch_add(bytes, Ordering::Relaxed);
    metrics
        .max_group
        .fetch_max(group.len() as u64, Ordering::Relaxed);
    Ok(())
}

impl Store {
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
pub(crate) mod tests {
    use super::*;
    use crate::core::message::Value;

    #[test]
    fn group_commit_preserves_operation_order_and_bounds_group_size() {
        let path = temp_path("grouped-store");
        let store = Store::open_with_options(
            &path,
            StoreOptions {
                max_batch_operations: 4,
                max_delay_us: 10_000,
                ..StoreOptions::default()
            },
        )
        .unwrap();
        let message = Message::text(Destination::queue("q"), "value");
        let mut receipts = Vec::new();
        for index in 0..12 {
            let mut batch = Batch::default();
            batch.put_message("queue:q", index, &message).unwrap();
            // Same-key updates in a group must be applied in enqueue order.
            if index > 0 {
                batch.remove_message("queue:q", index - 1);
            }
            receipts.push(store.submit(batch, Vec::new()).unwrap());
        }
        for receipt in receipts {
            receipt.wait().unwrap();
        }
        let (messages, _) = store.load_messages().unwrap();
        assert_eq!(messages["queue:q"].len(), 1);
        assert_eq!(messages["queue:q"][0].0, 11);
        let metrics = store.metrics();
        assert!(metrics.max_group_size > 1, "{metrics:?}");
        assert!(metrics.max_group_size <= 4, "{metrics:?}");
        assert!(metrics.flushes < 12, "{metrics:?}");
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn group_commit_byte_target_does_not_split_an_operation() {
        let path = temp_path("group-bytes");
        let store = Store::open_with_options(
            &path,
            StoreOptions {
                max_batch_bytes: 1,
                ..StoreOptions::default()
            },
        )
        .unwrap();
        let mut receipts = Vec::new();
        for index in 0..3 {
            let mut batch = Batch::default();
            batch
                .put_message(
                    "queue:q",
                    index,
                    &Message::text(Destination::queue("q"), "v"),
                )
                .unwrap();
            batch.put_destination(&Destination::queue("q")).unwrap();
            receipts.push(store.submit(batch, Vec::new()).unwrap());
        }
        for receipt in receipts {
            receipt.wait().unwrap();
        }
        assert_eq!(store.metrics().max_group_size, 1);
        assert_eq!(store.load_messages().unwrap().0["queue:q"].len(), 3);
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn pending_byte_budget_blocks_until_previous_commit_finishes() {
        let path = temp_path("byte-backpressure");
        let store = Arc::new(
            Store::open_with_options(
                &path,
                StoreOptions {
                    max_pending_bytes: 1,
                    ..StoreOptions::default()
                },
            )
            .unwrap(),
        );
        let (entered, resume) = store.pause_next_flush();
        let mut first = Batch::default();
        first
            .put_message(
                "queue:q",
                1,
                &Message::text(Destination::queue("q"), "first"),
            )
            .unwrap();
        let first = store.submit(first, Vec::new()).unwrap();
        entered.recv_timeout(Duration::from_secs(3)).unwrap();
        let copy = Arc::clone(&store);
        let (submitted, received) = mpsc::sync_channel(1);
        let second = thread::spawn(move || {
            let mut batch = Batch::default();
            batch
                .put_message(
                    "queue:q",
                    2,
                    &Message::text(Destination::queue("q"), "second"),
                )
                .unwrap();
            let receipt = copy.submit(batch, Vec::new()).unwrap();
            submitted.send(()).unwrap();
            receipt.wait().unwrap();
        });
        let premature = received.recv_timeout(Duration::from_millis(30)).is_ok();
        resume.send(()).unwrap();
        first.wait().unwrap();
        second.join().unwrap();
        store.flush().unwrap();
        assert!(
            !premature,
            "oversized operations must occupy the budget alone"
        );
        assert_eq!(store.metrics().pending_bytes, 0);
        assert_eq!(store.load_messages().unwrap().0["queue:q"].len(), 2);
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

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
