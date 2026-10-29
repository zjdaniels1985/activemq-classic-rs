use serde::Deserialize;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub broker: BrokerConfig,
    pub protocols: ProtocolsConfig,
    pub persistence: PersistenceConfig,
    pub web_console: WebConsoleConfig,
}

impl AppConfig {
    pub fn from_file(path: &Path) -> io::Result<Self> {
        let contents = fs::read_to_string(path)?;
        toml::from_str(&contents).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }
}

impl TryFrom<AppConfig> for crate::server::ServerConfig {
    type Error = io::Error;

    fn try_from(config: AppConfig) -> Result<Self, Self::Error> {
        if !config.protocols.openwire.enabled {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the OpenWire listener must be enabled",
            ));
        }
        for (name, listener) in [
            ("STOMP", &config.protocols.stomp),
            ("AMQP", &config.protocols.amqp),
            ("MQTT", &config.protocols.mqtt),
        ] {
            if listener.enabled {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{name} is configured but not implemented yet"),
                ));
            }
        }
        if config.persistence.backend != "sled" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the only supported persistence backend is sled",
            ));
        }
        config.web_console.validate()?;
        Ok(Self {
            bind_address: config.protocols.openwire.bind,
            max_frame_size: config.broker.max_frame_size,
            broker_name: config.broker.name,
            store_path: PathBuf::from(config.persistence.path),
            web_console: config.web_console,
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
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
}

impl Default for ProtocolsConfig {
    fn default() -> Self {
        Self {
            openwire: ListenerConfig {
                enabled: true,
                bind: "127.0.0.1:61616".to_owned(),
            },
            stomp: ListenerConfig {
                bind: "127.0.0.1:61613".to_owned(),
                ..ListenerConfig::default()
            },
            amqp: ListenerConfig {
                bind: "127.0.0.1:5672".to_owned(),
                ..ListenerConfig::default()
            },
            mqtt: ListenerConfig {
                bind: "127.0.0.1:1883".to_owned(),
                ..ListenerConfig::default()
            },
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ListenerConfig {
    pub enabled: bool,
    pub bind: String,
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: "127.0.0.1:0".to_owned(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct PersistenceConfig {
    pub backend: String,
    pub path: String,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            backend: "sled".to_owned(),
            path: "./data/sled".to_owned(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "web console on a non-loopback address requires web_console.admin_token",
            ));
        }
        Ok(address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omitted_fields_receive_safe_defaults() {
        let config: AppConfig = toml::from_str("").unwrap();
        assert_eq!(config.persistence.backend, "sled");
        assert!(config.web_console.enabled);
        assert!(!config.protocols.stomp.enabled);
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
    fn refuses_enabled_protocols_without_adapters() {
        let mut config = AppConfig::default();
        config.protocols.stomp.enabled = true;
        let error = crate::server::ServerConfig::try_from(config).unwrap_err();
        assert!(error.to_string().contains("STOMP"));
    }

    #[test]
    fn rejects_empty_remote_console_tokens() {
        let config = WebConsoleConfig {
            enabled: true,
            bind: "0.0.0.0:8161".to_owned(),
            admin_token: Some("  ".to_owned()),
        };
        assert_eq!(
            config.validate().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
