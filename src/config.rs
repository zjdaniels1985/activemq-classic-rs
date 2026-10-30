//! TOML configuration (`activemq-rust.toml`).
//!
//! Every section is optional. Unknown protocol sections under `[protocols]`
//! are accepted and reported so new transports can be added without breaking
//! existing configuration files.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::core::Policy;
use crate::core::security::SecurityConfig;
use crate::core::store::StoreOptions;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AppConfig {
    pub broker: BrokerConfig,
    pub policy: Policy,
    pub security: SecurityConfig,
    pub protocols: ProtocolsConfig,
    pub persistence: PersistenceConfig,
    pub web_console: WebConsoleConfig,
    pub logging: LoggingConfig,
    pub network_connectors: Vec<NetworkConnectorConfig>,
}

impl AppConfig {
    pub fn from_file(path: &Path) -> io::Result<Self> {
        let contents = fs::read_to_string(path)?;
        Self::parse(&contents)
    }

    pub fn parse(contents: &str) -> io::Result<Self> {
        let config: Self = toml::from_str(contents)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> io::Result<()> {
        if self.persistence.backend != "sled" && self.persistence.backend != "memory" {
            return Err(invalid(format!(
                "unsupported persistence backend {:?} (use \"sled\" or \"memory\")",
                self.persistence.backend
            )));
        }
        self.persistence.group_commit.validate()?;
        if self.broker.max_frame_size == 0 || self.broker.max_frame_size > i32::MAX as usize {
            return Err(invalid(
                "broker.max_frame_size must be between 1 and i32::MAX",
            ));
        }
        for (name, listener) in self.protocols.listeners() {
            if listener.enabled {
                listener
                    .bind
                    .parse::<SocketAddr>()
                    .map_err(|error| invalid(format!("protocols.{name}.bind: {error}")))?;
            }
        }
        if let Some(name) = self.protocols.extra.keys().next() {
            return Err(invalid(format!(
                "protocol {name:?} is not supported by this broker version"
            )));
        }
        for connector in &self.network_connectors {
            if connector.uris.is_empty() {
                return Err(invalid(format!(
                    "network connector {:?} needs at least one uri",
                    connector.name
                )));
            }
        }
        self.web_console.validate()?;
        Ok(())
    }

    /// The Sled directory, or `None` for an in-memory broker.
    pub fn data_dir(&self) -> Option<PathBuf> {
        (self.persistence.backend == "sled").then(|| PathBuf::from(&self.persistence.path))
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BrokerConfig {
    pub name: String,
    pub max_frame_size: usize,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            name: "activemq-classic-rs".to_owned(),
            max_frame_size: 100 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ProtocolsConfig {
    pub openwire: ListenerConfig,
    pub stomp: ListenerConfig,
    pub amqp: ListenerConfig,
    pub mqtt: ListenerConfig,
    /// Sections for protocols this version does not know about.
    #[serde(flatten)]
    pub extra: BTreeMap<String, toml::Value>,
}

impl ProtocolsConfig {
    pub fn listeners(&self) -> [(&'static str, &ListenerConfig); 4] {
        [
            ("openwire", &self.openwire),
            ("stomp", &self.stomp),
            ("amqp", &self.amqp),
            ("mqtt", &self.mqtt),
        ]
    }
}

impl Default for ProtocolsConfig {
    fn default() -> Self {
        Self {
            openwire: ListenerConfig::new(true, "127.0.0.1:61616"),
            stomp: ListenerConfig::new(true, "127.0.0.1:61613"),
            amqp: ListenerConfig::new(true, "127.0.0.1:5672"),
            mqtt: ListenerConfig::new(true, "127.0.0.1:1883"),
            extra: BTreeMap::new(),
        }
    }
}

/// A protocol listener. `options` holds protocol specific settings, for
/// example `options.max_inactivity_ms` or `options.prefetch`.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ListenerConfig {
    pub enabled: bool,
    pub bind: String,
    /// Maximum concurrent connections on this listener (0 = unlimited).
    pub max_connections: usize,
    pub options: BTreeMap<String, toml::Value>,
}

impl ListenerConfig {
    pub fn new(enabled: bool, bind: &str) -> Self {
        Self {
            enabled,
            bind: bind.to_owned(),
            ..Self::default()
        }
    }

    pub fn option_i64(&self, name: &str) -> Option<i64> {
        self.options.get(name).and_then(toml::Value::as_integer)
    }

    pub fn option_bool(&self, name: &str) -> Option<bool> {
        self.options.get(name).and_then(toml::Value::as_bool)
    }

    pub fn option_str(&self, name: &str) -> Option<&str> {
        self.options.get(name).and_then(toml::Value::as_str)
    }
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: "127.0.0.1:0".to_owned(),
            max_connections: 0,
            options: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PersistenceConfig {
    /// `sled` (default) or `memory`.
    pub backend: String,
    pub path: String,
    pub group_commit: StoreOptions,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            backend: "sled".to_owned(),
            path: "./data/sled".to_owned(),
            group_commit: StoreOptions::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebConsoleConfig {
    pub enabled: bool,
    pub bind: String,
    pub admin_token: Option<String>,
}

impl Default for WebConsoleConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind: "127.0.0.1:8161".to_owned(),
            admin_token: None,
        }
    }
}

impl WebConsoleConfig {
    pub fn validate(&self) -> io::Result<SocketAddr> {
        let address: SocketAddr = self
            .bind
            .parse()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        if self.enabled
            && !address.ip().is_loopback()
            && self
                .admin_token
                .as_deref()
                .is_none_or(|token| token.trim().is_empty())
        {
            return Err(invalid(
                "web console on a non-loopback address requires web_console.admin_token",
            ));
        }
        Ok(address)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
    /// Default filter (`RUST_LOG` takes precedence), e.g. `info,activemq_classic_rs=debug`.
    pub level: String,
    /// `text` or `json`.
    pub format: String,
    /// Optional log file; logs go to stdout when unset.
    pub file: Option<String>,
    pub ansi: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_owned(),
            format: "text".to_owned(),
            file: None,
            ansi: true,
        }
    }
}

/// Static network connector (ActiveMQ `networkConnector`), bridging this
/// broker to remote brokers over OpenWire.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NetworkConnectorConfig {
    pub name: String,
    /// `tcp://host:port` addresses; the first reachable one is used.
    pub uris: Vec<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    /// Also forward messages from the remote broker to this one.
    pub duplex: bool,
    /// Maximum number of brokers a message/subscription may traverse.
    pub network_ttl: u32,
    /// Destination patterns (URI form, e.g. `queue://>`) to forward on demand.
    pub dynamically_included_destinations: Vec<String>,
    pub excluded_destinations: Vec<String>,
    /// Destinations always forwarded, regardless of remote demand.
    pub static_included_destinations: Vec<String>,
    pub conduit_subscriptions: bool,
    pub reconnect_delay_ms: u64,
    pub prefetch: u32,
}

impl Default for NetworkConnectorConfig {
    fn default() -> Self {
        Self {
            name: "bridge".to_owned(),
            uris: Vec::new(),
            username: None,
            password: None,
            duplex: false,
            network_ttl: 1,
            dynamically_included_destinations: Vec::new(),
            excluded_destinations: Vec::new(),
            static_included_destinations: Vec::new(),
            conduit_subscriptions: true,
            reconnect_delay_ms: 1000,
            prefetch: 1000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omitted_fields_receive_safe_defaults() {
        let config = AppConfig::parse("").unwrap();
        assert_eq!(config.persistence.backend, "sled");
        assert!(config.web_console.enabled);
        assert!(config.protocols.stomp.enabled);
        assert_eq!(config.policy.max_redeliveries, 6);
    }

    #[test]
    fn rejects_remote_console_without_an_admin_token() {
        let config = WebConsoleConfig {
            enabled: true,
            bind: "0.0.0.0:8161".to_owned(),
            admin_token: None,
        };
        assert_eq!(
            config.validate().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn rejects_empty_remote_console_tokens() {
        let config = WebConsoleConfig {
            enabled: true,
            bind: "0.0.0.0:8161".to_owned(),
            admin_token: Some("  ".to_owned()),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_unknown_protocols_and_backends() {
        let error = AppConfig::parse("[protocols.ws]\nenabled = true\n").unwrap_err();
        assert!(error.to_string().contains("ws"));
        assert!(AppConfig::parse("[persistence]\nbackend = \"kahadb\"\n").is_err());
    }

    #[test]
    fn parses_shipped_configuration() {
        let config = AppConfig::parse(include_str!("../activemq-rust.toml")).unwrap();
        assert!(config.protocols.openwire.enabled);
    }

    #[test]
    fn validates_group_commit_limits_and_zero_delay() {
        for field in [
            "queue_capacity",
            "max_batch_operations",
            "max_batch_bytes",
            "max_pending_bytes",
        ] {
            assert!(
                AppConfig::parse(&format!("[persistence.group_commit]\n{field} = 0\n")).is_err()
            );
        }
        assert!(AppConfig::parse("[persistence.group_commit]\nmax_delay_us = 1000001\n").is_err());
        let config = AppConfig::parse("[persistence.group_commit]\nmax_delay_us = 0\n").unwrap();
        assert_eq!(config.persistence.group_commit.max_delay_us, 0);
    }

    #[test]
    fn parses_security_and_network_sections() {
        let config = AppConfig::parse(
            r#"
            [security]
            enabled = true
            [[security.users]]
            username = "admin"
            password = "admin"
            groups = ["admins"]
            [[security.authorization]]
            destination = "queue://>"
            read = ["admins"]
            write = ["admins"]
            admin = ["admins"]
            [[network_connectors]]
            name = "east"
            uris = ["tcp://10.0.0.2:61616"]
            duplex = true
            "#,
        )
        .unwrap();
        assert!(config.security.enabled);
        assert_eq!(config.network_connectors[0].name, "east");
    }
}
