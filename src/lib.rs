//! ActiveMQ Classic compatible message broker written in Rust.
//!
//! * [`core`] – protocol-neutral broker engine (queues, topics, durable
//!   subscriptions, transactions, selectors, security, scheduler, Sled store).
//! * [`protocols`] – OpenWire, STOMP, MQTT and AMQP 1.0 adapters.
//! * [`network`] – network-of-brokers bridges.
//! * [`web_console`] – administrative web console and REST API.

pub mod config;
pub mod core;
pub mod logging;
pub mod network;
pub mod protocols;
pub mod runtime;
pub mod web_console;
