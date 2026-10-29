//! Rust-native broker core and protocol primitives for ActiveMQ Classic compatibility.

pub mod broker;
pub mod config;
pub mod core;
pub mod openwire;
pub mod openwire_commands;
pub mod server;
pub mod sled_store;
pub mod web_console;

pub use broker::{Broker, BrokerError, Message};
