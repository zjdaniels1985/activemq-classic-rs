//! Logging setup based on `tracing`.

use std::fs::OpenOptions;
use std::io;
use std::sync::Mutex;

use tracing_subscriber::EnvFilter;

use crate::config::LoggingConfig;

/// Installs the global subscriber. `RUST_LOG` overrides `config.level`.
pub fn init(config: &LoggingConfig) -> io::Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&config.level))
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let json = match config.format.as_str() {
        "text" => false,
        "json" => true,
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("logging.format must be \"text\" or \"json\", not {other:?}"),
            ));
        }
    };
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true);
    let result = match &config.file {
        Some(path) => {
            if let Some(parent) = std::path::Path::new(path).parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent)?;
            }
            let file = OpenOptions::new().create(true).append(true).open(path)?;
            let builder = builder.with_ansi(false).with_writer(Mutex::new(file));
            if json {
                builder.json().try_init()
            } else {
                builder.try_init()
            }
        }
        None => {
            let builder = builder.with_ansi(config.ansi);
            if json {
                builder.json().try_init()
            } else {
                builder.try_init()
            }
        }
    };
    result.map_err(io::Error::other)
}
