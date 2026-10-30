//! Public types of the protocol-neutral broker API used by every adapter.

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::destination::Destination;
use super::message::Message;
use super::store::XidRecord;

pub type ConnectionId = u64;
pub type ConsumerId = u64;
pub type ProducerId = u64;

/// Events pushed by the core to a connection's adapter.
#[derive(Debug)]
pub enum Event {
    Deliver(Delivery),
    /// A browser consumer has received every message of the snapshot.
    BrowseDone {
        consumer: ConsumerId,
    },
    /// A pull request (prefetch 0) timed out without a message.
    PullEmpty {
        consumer: ConsumerId,
    },
    /// A drain request completed; `credit` is the credit that was discarded.
    Drained {
        consumer: ConsumerId,
        credit: u32,
    },
    /// The broker closed the consumer (for example the destination was deleted).
    ConsumerClosed {
        consumer: ConsumerId,
        reason: String,
    },
    /// The broker closed the connection (admin action or client-id takeover).
    ConnectionClosed {
        reason: String,
    },
}

#[derive(Clone, Debug)]
pub struct Delivery {
    pub consumer: ConsumerId,
    pub delivery_id: u64,
    pub message: Arc<Message>,
    pub redelivered: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ConnectInfo {
    pub client_id: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub protocol: String,
    pub remote_address: String,
    /// When true, rolled back transactional acks are redelivered by the
    /// broker (STOMP/AMQP). OpenWire clients redeliver locally instead.
    pub redeliver_on_rollback: bool,
    /// When true, an existing connection with the same client id is closed
    /// instead of rejecting this one (MQTT semantics).
    pub takeover: bool,
}

#[derive(Clone, Debug)]
pub struct ConsumerOptions {
    pub destination: Destination,
    pub selector: Option<String>,
    /// Prefetch window. 0 means pull mode (see [`super::BrokerCore::pull`]).
    pub prefetch: u32,
    pub no_local: bool,
    pub durable_name: Option<String>,
    pub browser: bool,
    pub exclusive: bool,
    pub retroactive: bool,
    pub priority: i8,
    /// True for network bridge consumers.
    pub network: bool,
    /// Broker ids the subscription has travelled through (network loop prevention).
    pub broker_path: Vec<String>,
    /// Maximum number of brokers a message may traverse when forwarded to
    /// this (network) consumer.
    pub network_ttl: Option<u32>,
    /// Protocol specific identifier, reported in advisories and the console.
    pub external_id: Option<String>,
    /// AMQP style explicit credit instead of a prefetch window.
    pub credit_mode: bool,
}

impl ConsumerOptions {
    pub fn new(destination: Destination) -> Self {
        Self {
            destination,
            selector: None,
            prefetch: 1000,
            no_local: false,
            durable_name: None,
            browser: false,
            exclusive: false,
            retroactive: false,
            priority: 0,
            network: false,
            broker_path: Vec::new(),
            network_ttl: None,
            external_id: None,
            credit_mode: false,
        }
    }
}

/// Which deliveries an acknowledgement refers to.
#[derive(Clone, Debug)]
pub enum Acks {
    Deliveries(Vec<u64>),
    /// Every delivery of the consumer up to and including this delivery id.
    UpTo(u64),
    /// A message id, optionally including every earlier delivery.
    MessageId {
        id: String,
        up_to: bool,
    },
    /// Every message in flight for the consumer.
    All,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckOp {
    /// The message was consumed and can be removed.
    Consumed,
    /// The message was delivered to the application; frees prefetch window only.
    Delivered,
    /// Give the message back for redelivery.
    Release { increment: bool },
    /// The message is poison: send it to the dead letter queue.
    Reject,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TxId {
    Local(String),
    Xa(XidRecord),
}

impl fmt::Display for TxId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TxId::Local(id) => write!(f, "{id}"),
            TxId::Xa(xid) => write!(
                f,
                "XID:[{},{},{}]",
                xid.format_id,
                hex::encode(&xid.global_id),
                hex::encode(&xid.branch_id)
            ),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CoreError {
    Security(String),
    InvalidClientId(String),
    InvalidDestination(String),
    InvalidSelector(String),
    NotFound(String),
    Transaction(String),
    ResourceAllocation(String),
    Store(String),
    IllegalState(String),
}

impl CoreError {
    /// The Java exception class an OpenWire client expects for this error.
    pub fn java_class(&self) -> &'static str {
        match self {
            CoreError::Security(_) => "java.lang.SecurityException",
            CoreError::InvalidClientId(_) => "javax.jms.InvalidClientIDException",
            CoreError::InvalidDestination(_) => "javax.jms.InvalidDestinationException",
            CoreError::InvalidSelector(_) => "javax.jms.InvalidSelectorException",
            CoreError::NotFound(_) => "javax.jms.JMSException",
            CoreError::Transaction(_) => "javax.transaction.xa.XAException",
            CoreError::ResourceAllocation(_) => "javax.jms.ResourceAllocationException",
            CoreError::Store(_) => "java.io.IOException",
            CoreError::IllegalState(_) => "javax.jms.IllegalStateException",
        }
    }

    pub fn message(&self) -> &str {
        match self {
            CoreError::Security(m)
            | CoreError::InvalidClientId(m)
            | CoreError::InvalidDestination(m)
            | CoreError::InvalidSelector(m)
            | CoreError::NotFound(m)
            | CoreError::Transaction(m)
            | CoreError::ResourceAllocation(m)
            | CoreError::Store(m)
            | CoreError::IllegalState(m) => m,
        }
    }
}

impl fmt::Display for CoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for CoreError {}

impl From<std::io::Error> for CoreError {
    fn from(error: std::io::Error) -> Self {
        CoreError::Store(error.to_string())
    }
}

impl From<super::security::SecurityError> for CoreError {
    fn from(error: super::security::SecurityError) -> Self {
        CoreError::Security(error.0)
    }
}

/// Broker wide behaviour, mirroring ActiveMQ Classic policy entries.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Policy {
    /// Deliveries after which a released message goes to the DLQ.
    pub max_redeliveries: u32,
    pub dead_letter_queue: String,
    /// Use `DLQ.<destination>` per destination instead of one shared DLQ.
    pub individual_dead_letter: bool,
    pub individual_dead_letter_prefix: String,
    /// Send expired persistent messages to the DLQ.
    pub dead_letter_expired: bool,
    /// Pending message limit per queue (0 = unlimited).
    pub max_queue_messages: usize,
    pub advisory_support: bool,
    pub scheduler_support: bool,
    pub virtual_topics: bool,
    pub virtual_topic_prefix: String,
    pub virtual_topic_consumer_prefix: String,
    /// Whether producers may create destinations implicitly.
    pub auto_create_destinations: bool,
    pub expire_check_interval_ms: u64,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            max_redeliveries: 6,
            dead_letter_queue: "ActiveMQ.DLQ".to_owned(),
            individual_dead_letter: false,
            individual_dead_letter_prefix: "DLQ.".to_owned(),
            dead_letter_expired: true,
            max_queue_messages: 0,
            advisory_support: true,
            scheduler_support: true,
            virtual_topics: true,
            virtual_topic_prefix: "VirtualTopic.".to_owned(),
            virtual_topic_consumer_prefix: "Consumer.".to_owned(),
            auto_create_destinations: true,
            expire_check_interval_ms: 1000,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct DestinationStats {
    pub destination: Destination,
    pub uri: String,
    pub pending: usize,
    pub in_flight: usize,
    pub consumers: usize,
    pub producers: usize,
    pub enqueued: u64,
    pub dequeued: u64,
    pub dispatched: u64,
    pub expired: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ConnectionStats {
    pub id: ConnectionId,
    pub client_id: Option<String>,
    pub username: String,
    pub protocol: String,
    pub remote_address: String,
    pub connected_at: i64,
    pub consumers: usize,
    pub producers: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct SubscriptionStats {
    pub client_id: String,
    pub name: String,
    pub destination: String,
    pub selector: Option<String>,
    pub active: bool,
    pub pending: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct ConsumerStats {
    pub id: ConsumerId,
    pub connection: ConnectionId,
    pub destination: String,
    pub selector: Option<String>,
    pub prefetch: u32,
    pub in_flight: usize,
    pub durable_name: Option<String>,
    pub network: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct JobStats {
    pub id: String,
    pub destination: String,
    pub next_time: i64,
    pub delay: i64,
    pub period: i64,
    pub repeat: i32,
    pub cron: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct MessageView {
    pub message_id: String,
    pub destination: String,
    pub persistent: bool,
    pub priority: u8,
    pub timestamp: i64,
    pub expiration: i64,
    pub redelivery_counter: u32,
    pub correlation_id: Option<String>,
    pub message_type: Option<String>,
    pub properties: std::collections::BTreeMap<String, String>,
    pub body: String,
}

impl MessageView {
    pub fn from_message(message: &Message) -> Self {
        let body = match message.body.as_ref() {
            super::message::Body::Text(text) => text.clone(),
            super::message::Body::Empty => String::new(),
            other => format!("<{} bytes>", other.len()),
        };
        Self {
            message_id: message.message_id.clone(),
            destination: message.destination.to_uri(),
            persistent: message.persistent,
            priority: message.priority,
            timestamp: message.timestamp,
            expiration: message.expiration,
            redelivery_counter: message.redelivery_counter,
            correlation_id: message.correlation_id.clone(),
            message_type: message.message_type.clone(),
            properties: message
                .properties
                .iter()
                .map(|(k, v)| (k.clone(), v.to_display_string()))
                .collect(),
            body,
        }
    }
}
