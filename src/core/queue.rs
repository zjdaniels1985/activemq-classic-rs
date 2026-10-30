//! Per-destination (or per-subscription) pending message storage.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use super::destination::Destination;
use super::message::Message;
use super::selector::Selector;
use super::types::{ConnectionId, ConsumerId};

#[derive(Clone, Debug)]
pub(crate) struct Entry {
    pub seq: u64,
    pub message: Arc<Message>,
    pub redelivered: bool,
    pub persisted: bool,
}

/// Topic subscription attributes applied when a published message is copied
/// into a subscription queue.
pub(crate) struct SubscriptionFilter {
    pub pattern: Destination,
    pub selector: Option<Selector>,
    pub no_local_connection: Option<ConnectionId>,
}

impl SubscriptionFilter {
    pub fn accepts(&self, message: &Message) -> bool {
        if !self.pattern.matches(&message.destination) {
            return false;
        }
        if let Some(connection) = self.no_local_connection
            && message.producer_connection == Some(connection)
        {
            return false;
        }
        self.selector
            .as_ref()
            .is_none_or(|selector| selector.matches(message))
    }
}

pub(crate) enum QueueKind {
    Queue,
    /// Non-durable topic subscription owned by a single consumer.
    TopicSubscription,
    /// Durable topic subscription.
    Durable,
}

pub(crate) struct MessageQueue {
    pub key: String,
    pub destination: Destination,
    pub kind: QueueKind,
    pub filter: Option<SubscriptionFilter>,
    /// Pending messages ordered by priority (high first) then arrival.
    pub pending: BTreeMap<(Reverse<u8>, u64), Entry>,
    pub consumers: Vec<ConsumerId>,
    pub round_robin: usize,
    pub groups: HashMap<String, ConsumerId>,
    pub enqueued: u64,
    pub dequeued: u64,
    pub dispatched: u64,
    pub expired: u64,
    pub in_flight: usize,
}

impl MessageQueue {
    pub fn new(key: String, destination: Destination, kind: QueueKind) -> Self {
        Self {
            key,
            destination,
            kind,
            filter: None,
            pending: BTreeMap::new(),
            consumers: Vec::new(),
            round_robin: 0,
            groups: HashMap::new(),
            enqueued: 0,
            dequeued: 0,
            dispatched: 0,
            expired: 0,
            in_flight: 0,
        }
    }

    /// Whether persistent messages are written to the store for this queue.
    pub fn durable_storage(&self) -> bool {
        match self.kind {
            QueueKind::Queue => !self.destination.is_temporary(),
            QueueKind::TopicSubscription => false,
            QueueKind::Durable => true,
        }
    }

    pub fn push(&mut self, entry: Entry) {
        let key = (Reverse(entry.message.priority.min(9)), entry.seq);
        self.pending.insert(key, entry);
    }

    pub fn remove_seq(&mut self, seq: u64) -> Option<Entry> {
        let key = self
            .pending
            .iter()
            .find(|(_, entry)| entry.seq == seq)
            .map(|(key, _)| *key)?;
        self.pending.remove(&key)
    }

    pub fn remove_message_id(&mut self, id: &str) -> Option<Entry> {
        let key = self
            .pending
            .iter()
            .find(|(_, entry)| entry.message.message_id == id)
            .map(|(key, _)| *key)?;
        self.pending.remove(&key)
    }
}

pub(crate) fn queue_key(destination: &Destination) -> String {
    if destination.is_temporary() {
        format!("temp-queue:{}", destination.name)
    } else {
        format!("queue:{}", destination.name)
    }
}

pub(crate) fn durable_key(client_id: &str, name: &str) -> String {
    format!("durable:{}\u{1}{}", client_id, name)
}

pub(crate) fn subscription_key(consumer: ConsumerId) -> String {
    format!("sub:{consumer}")
}
