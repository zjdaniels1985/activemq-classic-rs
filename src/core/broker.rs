//! The protocol-neutral broker: state, recovery and shared helpers.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::thread;
use std::time::Duration;

use tokio::sync::mpsc::UnboundedSender;

use super::destination::{Destination, DestinationKind};
use super::message::{AdvisoryData, Message, now_millis};
use super::queue::{Entry, MessageQueue, QueueKind, SubscriptionFilter, durable_key, queue_key};
use super::scheduler::Job;
use super::security::{Permission, Principal, SecurityManager};
use super::selector::Selector;
use super::store::{Batch, DurableRecord, Store};
use super::types::{
    ConnectInfo, ConnectionId, ConsumerId, ConsumerOptions, CoreError, Event, Policy, ProducerId,
    TxId,
};

pub(crate) struct Connection {
    pub info: ConnectInfo,
    pub principal: Principal,
    pub sender: UnboundedSender<Event>,
    pub consumers: BTreeSet<ConsumerId>,
    pub producers: BTreeSet<ProducerId>,
    pub temps: BTreeSet<Destination>,
    pub connected_at: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct InFlight {
    pub queue: String,
    pub seq: u64,
    pub message: Arc<Message>,
    pub persisted: bool,
    pub window_freed: bool,
    pub tx: Option<TxKey>,
}

pub(crate) struct Consumer {
    pub connection: ConnectionId,
    pub options: ConsumerOptions,
    pub selector: Option<Selector>,
    /// Store keys of the queues this consumer is attached to.
    pub queues: Vec<String>,
    pub in_flight: BTreeMap<u64, InFlight>,
    pub window_used: u32,
    pub credit: u32,
    /// Pull deadline in epoch millis (`i64::MAX` waits forever).
    pub pull_deadline: Option<i64>,
    pub durable: Option<(String, String)>,
}

impl Consumer {
    pub fn available(&self) -> u32 {
        if self.options.browser {
            0
        } else if self.options.credit_mode {
            self.credit
        } else if self.options.prefetch == 0 {
            u32::from(self.pull_deadline.is_some() && self.window_used == 0)
        } else {
            self.options.prefetch.saturating_sub(self.window_used)
        }
    }
}

/// Local transactions are scoped to their connection; XA ones are global.
pub(crate) type TxKey = (Option<ConnectionId>, TxId);

#[derive(Clone, Debug)]
pub(crate) struct Detached {
    pub queue: String,
    pub seq: u64,
    pub message: Arc<Message>,
    pub persisted: bool,
}

pub(crate) struct Transaction {
    pub connection: Option<ConnectionId>,
    pub sends: Vec<Message>,
    pub acks: Vec<(ConsumerId, u64)>,
    pub detached: Vec<Detached>,
    pub prepared: bool,
}

#[derive(Default)]
pub(crate) struct TopicStats {
    pub enqueued: u64,
}

pub(crate) struct Inner {
    pub next_id: u64,
    pub next_seq: u64,
    pub connections: HashMap<ConnectionId, Connection>,
    pub consumers: HashMap<ConsumerId, Consumer>,
    pub producers: HashMap<ProducerId, (ConnectionId, Option<Destination>)>,
    pub queues: BTreeMap<String, MessageQueue>,
    pub topics: BTreeMap<String, TopicStats>,
    pub temps: BTreeSet<Destination>,
    pub durables: BTreeMap<(String, String), DurableRecord>,
    pub transactions: HashMap<TxKey, Transaction>,
    pub jobs: BTreeMap<String, Job>,
    pub retained: BTreeMap<String, Message>,
    pub client_ids: HashMap<String, ConnectionId>,
    pub batch: Batch,
    pub last_expiry_check: i64,
}

impl Inner {
    pub fn next_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    pub fn emit(&self, connection: ConnectionId, event: Event) {
        if let Some(connection) = self.connections.get(&connection) {
            let _ = connection.sender.send(event);
        }
    }

    pub fn principal(&self, connection: ConnectionId) -> Result<Principal, CoreError> {
        self.connections
            .get(&connection)
            .map(|connection| connection.principal.clone())
            .ok_or_else(|| CoreError::IllegalState("connection is closed".to_owned()))
    }

    pub fn destination_exists(&self, destination: &Destination) -> bool {
        match destination.kind {
            DestinationKind::Queue => self.queues.contains_key(&queue_key(destination)),
            DestinationKind::Topic => {
                destination.is_advisory() || self.topics.contains_key(&destination.name)
            }
            _ => self.temps.contains(destination),
        }
    }
}

/// Protocol-neutral ActiveMQ Classic broker core.
pub struct BrokerCore {
    pub(crate) inner: Mutex<Inner>,
    pub(crate) store: Option<Store>,
    pub(crate) security: SecurityManager,
    pub(crate) policy: Policy,
    broker_name: String,
    broker_id: String,
    shutdown: AtomicBool,
}

impl BrokerCore {
    /// Opens (and recovers) a broker. `data_dir` of `None` keeps everything in memory.
    pub fn open(
        broker_name: impl Into<String>,
        data_dir: Option<&Path>,
        policy: Policy,
        security: SecurityManager,
    ) -> Result<Arc<Self>, CoreError> {
        let store = data_dir.map(Store::open).transpose()?;
        let broker_name = broker_name.into();
        let broker_id = format!("ID:{}-{}-{}", broker_name, std::process::id(), now_millis());
        let core = Arc::new(Self {
            inner: Mutex::new(Inner {
                next_id: 0,
                next_seq: 1,
                connections: HashMap::new(),
                consumers: HashMap::new(),
                producers: HashMap::new(),
                queues: BTreeMap::new(),
                topics: BTreeMap::new(),
                temps: BTreeSet::new(),
                durables: BTreeMap::new(),
                transactions: HashMap::new(),
                jobs: BTreeMap::new(),
                retained: BTreeMap::new(),
                client_ids: HashMap::new(),
                batch: Batch::default(),
                last_expiry_check: 0,
            }),
            store,
            security,
            policy,
            broker_name,
            broker_id,
            shutdown: AtomicBool::new(false),
        });
        core.recover_state()?;
        let weak = Arc::downgrade(&core);
        thread::Builder::new()
            .name("broker-timer".to_owned())
            .spawn(move || timer_loop(weak))
            .map_err(CoreError::from)?;
        Ok(core)
    }

    /// In-memory broker with default policy and security disabled.
    pub fn in_memory() -> Arc<Self> {
        Self::open(
            "localhost",
            None,
            Policy::default(),
            SecurityManager::disabled(),
        )
        .expect("in-memory broker")
    }

    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    pub fn broker_id(&self) -> &str {
        &self.broker_id
    }

    pub fn policy(&self) -> &Policy {
        &self.policy
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(store) = &self.store {
            let _ = store.flush();
        }
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Writes the batch accumulated during an operation.
    pub(crate) fn flush_batch(&self, inner: &mut Inner) -> Result<(), CoreError> {
        let batch = std::mem::take(&mut inner.batch);
        if let Some(store) = &self.store {
            store.apply(batch)?;
        }
        Ok(())
    }

    fn recover_state(&self) -> Result<(), CoreError> {
        let Some(store) = &self.store else {
            return Ok(());
        };
        let mut inner = self.lock();
        for destination in store.load_destinations()? {
            match destination.kind {
                DestinationKind::Queue => {
                    let key = queue_key(&destination);
                    inner
                        .queues
                        .entry(key.clone())
                        .or_insert_with(|| MessageQueue::new(key, destination, QueueKind::Queue));
                }
                DestinationKind::Topic => {
                    inner.topics.entry(destination.name).or_default();
                }
                _ => {}
            }
        }
        for record in store.load_durables()? {
            let key = durable_key(&record.client_id, &record.name);
            let mut queue =
                MessageQueue::new(key.clone(), record.destination.clone(), QueueKind::Durable);
            queue.filter = Some(SubscriptionFilter {
                pattern: record.destination.clone(),
                selector: record
                    .selector
                    .as_deref()
                    .and_then(|source| Selector::parse(source).ok()),
                no_local_connection: None,
            });
            inner.queues.insert(key, queue);
            inner
                .durables
                .insert((record.client_id.clone(), record.name.clone()), record);
        }
        let (messages, max_sequence) = store.load_messages()?;
        inner.next_seq = max_sequence + 1;
        let mut orphaned = Batch::default();
        for (key, entries) in messages {
            if !inner.queues.contains_key(&key)
                && let Some(name) = key.strip_prefix("queue:")
            {
                let destination = Destination::queue(name);
                inner.queues.insert(
                    key.clone(),
                    MessageQueue::new(key.clone(), destination, QueueKind::Queue),
                );
            }
            match inner.queues.get_mut(&key) {
                Some(queue) => {
                    for (seq, message) in entries {
                        queue.push(Entry {
                            seq,
                            message: Arc::new(message),
                            redelivered: false,
                            persisted: true,
                        });
                    }
                }
                None => {
                    for (seq, _) in entries {
                        orphaned.remove_message(&key, seq);
                    }
                }
            }
        }
        store.apply(orphaned)?;
        for job in store.load_jobs()? {
            inner.jobs.insert(job.id.clone(), job);
        }
        for message in store.load_retained()? {
            inner
                .retained
                .insert(message.destination.name.clone(), message);
        }
        for record in store.load_prepared()? {
            let mut detached = Vec::new();
            for (key, seq) in &record.acks {
                if let Some(entry) = inner
                    .queues
                    .get_mut(key)
                    .and_then(|queue| queue.remove_seq(*seq))
                {
                    detached.push(Detached {
                        queue: key.clone(),
                        seq: entry.seq,
                        message: entry.message,
                        persisted: entry.persisted,
                    });
                }
            }
            inner.transactions.insert(
                (None, TxId::Xa(record.xid.clone())),
                Transaction {
                    connection: None,
                    sends: record.sends,
                    acks: Vec::new(),
                    detached,
                    prepared: true,
                },
            );
        }
        tracing::info!(
            queues = inner.queues.len(),
            topics = inner.topics.len(),
            durables = inner.durables.len(),
            jobs = inner.jobs.len(),
            prepared = inner.transactions.len(),
            "recovered broker state from sled"
        );
        Ok(())
    }

    /// Creates the queue if needed and returns its store key.
    pub(crate) fn ensure_queue(&self, inner: &mut Inner, destination: &Destination) -> String {
        let key = queue_key(destination);
        if inner.queues.contains_key(&key) {
            return key;
        }
        inner.queues.insert(
            key.clone(),
            MessageQueue::new(key.clone(), destination.clone(), QueueKind::Queue),
        );
        if !destination.is_temporary() {
            if let Some(store) = &self.store
                && let Err(error) = store.put_destination(destination)
            {
                tracing::warn!(%error, "failed to persist destination");
            }
            self.advise(
                inner,
                destination.destination_advisory_topic(),
                AdvisoryData::DestinationAdded(destination.clone()),
            );
        }
        // Attach existing wildcard / composite consumers.
        let matching = inner
            .consumers
            .iter()
            .filter(|(_, consumer)| {
                !consumer.options.browser
                    && consumer.options.destination.is_queue()
                    && (consumer.options.destination.is_wildcard()
                        || consumer.options.destination.is_composite())
                    && consumer.options.destination.matches(destination)
            })
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for id in matching {
            if let Some(consumer) = inner.consumers.get_mut(&id) {
                consumer.queues.push(key.clone());
            }
            if let Some(queue) = inner.queues.get_mut(&key) {
                queue.consumers.push(id);
            }
        }
        key
    }

    pub(crate) fn ensure_topic(&self, inner: &mut Inner, destination: &Destination) {
        if destination.is_advisory()
            || destination.is_temporary()
            || inner.topics.contains_key(&destination.name)
        {
            return;
        }
        inner
            .topics
            .insert(destination.name.clone(), TopicStats::default());
        if let Some(store) = &self.store
            && let Err(error) = store.put_destination(destination)
        {
            tracing::warn!(%error, "failed to persist destination");
        }
        self.advise(
            inner,
            destination.destination_advisory_topic(),
            AdvisoryData::DestinationAdded(destination.clone()),
        );
    }

    /// Publishes an advisory message (non persistent) when advisories are enabled.
    pub(crate) fn advise(&self, inner: &mut Inner, topic: Destination, data: AdvisoryData) {
        if !self.policy.advisory_support {
            return;
        }
        let message = self.advisory_message(topic, data);
        self.route_topic(inner, Arc::new(message));
    }

    /// Checks permission, additionally requiring admin rights to implicitly
    /// create a destination that does not exist yet (ActiveMQ semantics).
    pub(crate) fn check_access(
        &self,
        inner: &Inner,
        principal: &Principal,
        destination: &Destination,
        permission: Permission,
    ) -> Result<(), CoreError> {
        for component in destination.components() {
            self.security.authorize(principal, &component, permission)?;
            if !component.is_temporary()
                && !component.is_wildcard()
                && !inner.destination_exists(&component)
            {
                if !self.policy.auto_create_destinations && !principal.system {
                    return Err(CoreError::InvalidDestination(format!(
                        "Destination {component} does not exist"
                    )));
                }
                self.security
                    .authorize(principal, &component, Permission::Admin)?;
            }
        }
        Ok(())
    }

    pub(crate) fn dead_letter_destination(&self, message: &Message) -> Destination {
        if self.policy.individual_dead_letter {
            let origin = message
                .original_destination
                .as_ref()
                .unwrap_or(&message.destination);
            Destination::queue(format!(
                "{}{}",
                self.policy.individual_dead_letter_prefix, origin.name
            ))
        } else {
            Destination::queue(self.policy.dead_letter_queue.clone())
        }
    }

    pub(crate) fn is_dead_letter_queue(&self, destination: &Destination) -> bool {
        destination.is_queue()
            && (destination.name == self.policy.dead_letter_queue
                || (self.policy.individual_dead_letter
                    && destination
                        .name
                        .starts_with(&self.policy.individual_dead_letter_prefix)))
    }

    /// Sends a message to the dead letter queue (or drops non-persistent ones).
    pub(crate) fn dead_letter(&self, inner: &mut Inner, message: &Message, cause: &str) {
        if !message.persistent || self.is_dead_letter_queue(&message.destination) {
            tracing::debug!(id = %message.message_id, cause, "discarding dead message");
            return;
        }
        let mut dead = message.clone();
        dead.original_destination
            .get_or_insert_with(|| message.destination.clone());
        dead.destination = self.dead_letter_destination(message);
        dead.expiration = 0;
        dead.properties.insert(
            "dlqDeliveryFailureCause".to_owned(),
            cause.to_owned().into(),
        );
        tracing::info!(id = %message.message_id, dlq = %dead.destination, cause, "message moved to DLQ");
        let key = self.ensure_queue(inner, &dead.destination.clone());
        let _ = self.enqueue(inner, &key, Arc::new(dead));
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }
}

fn timer_loop(core: Weak<BrokerCore>) {
    loop {
        thread::sleep(Duration::from_millis(50));
        let Some(core) = core.upgrade() else {
            return;
        };
        if core.is_shutdown() {
            return;
        }
        core.tick();
    }
}
