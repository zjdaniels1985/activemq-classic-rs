//! Starts every configured component of the broker.

use std::io;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::task::JoinSet;

use crate::config::AppConfig;
use crate::core::BrokerCore;
use crate::core::security::SecurityManager;
use crate::protocols::{self, ProtocolContext};

/// A running broker with the bound listener addresses.
pub struct RunningBroker {
    pub core: Arc<BrokerCore>,
    pub addresses: Vec<(&'static str, std::net::SocketAddr)>,
    tasks: JoinSet<io::Result<()>>,
    network_tasks: JoinSet<()>,
}

impl RunningBroker {
    pub fn address(&self, protocol: &str) -> Option<std::net::SocketAddr> {
        self.addresses
            .iter()
            .find(|(name, _)| *name == protocol)
            .map(|(_, address)| *address)
    }

    /// Waits until a listener fails (they normally run forever).
    pub async fn wait(&mut self) -> io::Result<()> {
        match self.tasks.join_next().await {
            Some(Ok(result)) => result,
            Some(Err(error)) => Err(io::Error::other(error)),
            None => std::future::pending().await,
        }
    }

    pub fn stop(mut self) {
        self.tasks.abort_all();
        self.network_tasks.abort_all();
        self.core.shutdown();
    }
}

/// Opens the store, binds all enabled listeners and starts the console and
/// network connectors. Must be called inside a tokio runtime.
pub async fn start(config: AppConfig) -> io::Result<RunningBroker> {
    config.validate()?;
    let security = SecurityManager::new(config.security.clone())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
    let data_dir = config.data_dir();
    let core = BrokerCore::open_with_store_options(
        config.broker.name.clone(),
        data_dir.as_deref(),
        config.policy.clone(),
        security,
        config.persistence.group_commit.clone(),
    )
    .map_err(|error| io::Error::other(error.message().to_owned()))?;
    tracing::info!(
        broker = %config.broker.name,
        persistence = %config.persistence.backend,
        path = ?data_dir,
        "broker core started"
    );

    let mut tasks = JoinSet::new();
    let mut addresses = Vec::new();
    for (name, listener_config) in config.protocols.listeners() {
        if !listener_config.enabled {
            continue;
        }
        let listener = TcpListener::bind(&listener_config.bind)
            .await
            .map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("{name} bind {}: {error}", listener_config.bind),
                )
            })?;
        addresses.push((name, listener.local_addr()?));
        let context = ProtocolContext::new(
            Arc::clone(&core),
            listener_config.clone(),
            config.broker.max_frame_size,
        );
        match name {
            "openwire" => tasks.spawn(protocols::openwire::serve(context, listener)),
            "stomp" => tasks.spawn(protocols::stomp::serve(context, listener)),
            "amqp" => tasks.spawn(protocols::amqp::serve(context, listener)),
            "mqtt" => tasks.spawn(protocols::mqtt::serve(context, listener)),
            _ => unreachable!("unknown protocol {name}"),
        };
    }

    if config.web_console.enabled {
        let address = config.web_console.validate()?;
        let listener = TcpListener::bind(address).await?;
        addresses.push(("web_console", listener.local_addr()?));
        tasks.spawn(crate::web_console::serve(
            Arc::clone(&core),
            listener,
            config.web_console.admin_token.clone(),
        ));
    }

    let network_tasks = crate::network::start(Arc::clone(&core), config.network_connectors.clone());

    Ok(RunningBroker {
        core,
        addresses,
        tasks,
        network_tasks,
    })
}
