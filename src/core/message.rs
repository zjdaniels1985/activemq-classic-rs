use super::destination::Destination;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Typed property / map values compatible with JMS and the ActiveMQ wire formats.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Byte(i8),
    Short(i16),
    Char(u16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(String),
    Bytes(Vec<u8>),
    Map(BTreeMap<String, Value>),
    List(Vec<Value>),
}

impl Value {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Byte(value) => Some(i64::from(*value)),
            Self::Short(value) => Some(i64::from(*value)),
            Self::Int(value) => Some(i64::from(*value)),
            Self::Long(value) => Some(*value),
            Self::String(value) => value.trim().parse().ok(),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            Self::String(value) => match value.to_ascii_lowercase().as_str() {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            },
            _ => None,
        }
    }

    /// Human readable rendering used by text based protocols and the console.
    pub fn to_display_string(&self) -> String {
        match self {
            Self::Null => String::new(),
            Self::Bool(value) => value.to_string(),
            Self::Byte(value) => value.to_string(),
            Self::Short(value) => value.to_string(),
            Self::Char(value) => char::from_u32(u32::from(*value))
                .map(String::from)
                .unwrap_or_default(),
            Self::Int(value) => value.to_string(),
            Self::Long(value) => value.to_string(),
            Self::Float(value) => value.to_string(),
            Self::Double(value) => value.to_string(),
            Self::String(value) => value.clone(),
            Self::Bytes(value) => String::from_utf8_lossy(value).into_owned(),
            Self::Map(value) => format!("{value:?}"),
            Self::List(value) => format!("{value:?}"),
        }
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self::Int(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Long(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

/// Message body, mirroring the JMS message types.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub enum Body {
    #[default]
    Empty,
    Text(String),
    Bytes(Vec<u8>),
    Map(BTreeMap<String, Value>),
    Stream(Vec<Value>),
    /// Serialized Java object (opaque to the broker).
    Object(Vec<u8>),
}

impl Body {
    pub fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Text(text) => text.len(),
            Self::Bytes(bytes) | Self::Object(bytes) => bytes.len(),
            Self::Map(map) => map.len(),
            Self::Stream(values) => values.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Best-effort byte representation for protocols without typed bodies.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Empty => Vec::new(),
            Self::Text(text) => text.as_bytes().to_vec(),
            Self::Bytes(bytes) | Self::Object(bytes) => bytes.clone(),
            Self::Map(map) => format!("{map:?}").into_bytes(),
            Self::Stream(values) => format!("{values:?}").into_bytes(),
        }
    }
}

/// Protocol-neutral metadata attached to advisory messages.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AdvisoryData {
    ConsumerAdded {
        connection: u64,
        consumer: u64,
        external_id: Option<String>,
        destination: Destination,
        selector: Option<String>,
        durable_name: Option<String>,
        client_id: Option<String>,
        network: bool,
        broker_path: Vec<String>,
        prefetch: u32,
    },
    ConsumerRemoved {
        connection: u64,
        consumer: u64,
        external_id: Option<String>,
        destination: Destination,
    },
    ProducerAdded {
        connection: u64,
        producer: u64,
        destination: Option<Destination>,
    },
    ProducerRemoved {
        connection: u64,
        producer: u64,
    },
    ConnectionAdded {
        connection: u64,
        client_id: Option<String>,
        username: Option<String>,
        remote_address: String,
    },
    ConnectionRemoved {
        connection: u64,
        client_id: Option<String>,
    },
    DestinationAdded(Destination),
    DestinationRemoved(Destination),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub message_id: String,
    pub destination: Destination,
    #[serde(default)]
    pub original_destination: Option<Destination>,
    #[serde(default)]
    pub reply_to: Option<Destination>,
    #[serde(default)]
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub message_type: Option<String>,
    #[serde(default)]
    pub group_id: Option<String>,
    #[serde(default)]
    pub group_sequence: i32,
    pub persistent: bool,
    pub priority: u8,
    /// Absolute expiry time in milliseconds since the epoch; 0 means never.
    pub expiration: i64,
    pub timestamp: i64,
    #[serde(default)]
    pub redelivery_counter: u32,
    #[serde(default)]
    pub user_id: Option<String>,
    /// Connection that produced the message (used for `noLocal`).
    #[serde(default)]
    pub producer_connection: Option<u64>,
    /// Brokers this message has passed through (network loop prevention).
    #[serde(default)]
    pub broker_path: Vec<String>,
    #[serde(default)]
    pub broker_in_time: i64,
    #[serde(default)]
    pub properties: BTreeMap<String, Value>,
    #[serde(default)]
    pub body: Body,
    #[serde(default)]
    pub advisory: Option<AdvisoryData>,
}

pub const DEFAULT_PRIORITY: u8 = 4;

impl Message {
    pub fn new(destination: Destination, body: Body) -> Self {
        Self {
            message_id: generate_message_id(),
            destination,
            original_destination: None,
            reply_to: None,
            correlation_id: None,
            message_type: None,
            group_id: None,
            group_sequence: 0,
            persistent: true,
            priority: DEFAULT_PRIORITY,
            expiration: 0,
            timestamp: now_millis(),
            redelivery_counter: 0,
            user_id: None,
            producer_connection: None,
            broker_path: Vec::new(),
            broker_in_time: 0,
            properties: BTreeMap::new(),
            body,
            advisory: None,
        }
    }

    pub fn text(destination: Destination, text: impl Into<String>) -> Self {
        Self::new(destination, Body::Text(text.into()))
    }

    pub fn bytes(destination: Destination, bytes: impl Into<Vec<u8>>) -> Self {
        Self::new(destination, Body::Bytes(bytes.into()))
    }

    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    pub fn property(&self, name: &str) -> Option<&Value> {
        self.properties.get(name)
    }

    pub fn is_expired(&self, now: i64) -> bool {
        self.expiration > 0 && self.expiration <= now
    }

    pub fn text_body(&self) -> Option<&str> {
        match &self.body {
            Body::Text(text) => Some(text),
            _ => None,
        }
    }

    /// Approximate in-memory size, used for statistics.
    pub fn approximate_size(&self) -> usize {
        256 + self.body.len()
            + self
                .properties
                .iter()
                .map(|(key, value)| key.len() + value.to_display_string().len())
                .sum::<usize>()
    }
}

static MESSAGE_COUNTER: AtomicU64 = AtomicU64::new(1);
static ID_PREFIX: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// Generates a unique, ActiveMQ-style message ID.
pub fn generate_message_id() -> String {
    let prefix =
        ID_PREFIX.get_or_init(|| format!("ID:rust-{}-{}", std::process::id(), now_millis()));
    format!(
        "{prefix}-1:1:1:1:{}",
        MESSAGE_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

pub fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}
