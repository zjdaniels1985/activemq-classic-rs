//! Protocol-neutral broker core shared by every wire-protocol adapter.

mod admin;
mod broker;
mod connection;
mod consumer;
pub mod destination;
pub mod message;
mod queue;
pub mod scheduler;
pub mod security;
pub mod selector;
pub mod store;
mod timer;
mod tx;
pub mod types;

#[cfg(test)]
mod tests;

pub use admin::BrokerInfo;
pub use broker::BrokerCore;
pub use connection::{RETAIN_PROPERTY, RETAINED_PROPERTY};
pub use destination::{Destination, DestinationKind};
pub use message::{AdvisoryData, Body, Message, Value};
pub use types::*;
