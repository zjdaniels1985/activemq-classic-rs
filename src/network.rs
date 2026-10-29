//! Network of brokers: static OpenWire bridges (not yet implemented).

use std::sync::Arc;

use crate::config::NetworkConnectorConfig;
use crate::core::BrokerCore;

pub fn start(_core: Arc<BrokerCore>, connectors: Vec<NetworkConnectorConfig>) {
    for connector in connectors {
        tracing::warn!(name = %connector.name, "network connectors are not implemented yet");
    }
}
