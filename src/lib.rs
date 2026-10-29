//! Rust-native broker core and protocol primitives for ActiveMQ Classic compatibility.

pub mod broker;
pub mod openwire;
pub mod server;

pub use broker::{Broker, BrokerError, Message};
