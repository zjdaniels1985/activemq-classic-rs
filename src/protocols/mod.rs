//! Wire-protocol adapters. Each adapter translates its protocol to the
//! protocol-neutral [`crate::core::BrokerCore`] API.

pub mod amqp;
pub mod mqtt;
pub mod openwire;
pub mod stomp;

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

use crate::config::ListenerConfig;
use crate::core::BrokerCore;

/// Shared state handed to every connection of a listener.
#[derive(Clone)]
pub struct ProtocolContext {
    pub core: Arc<BrokerCore>,
    pub listener: ListenerConfig,
    pub max_frame_size: usize,
}

impl ProtocolContext {
    pub fn new(core: Arc<BrokerCore>, listener: ListenerConfig, max_frame_size: usize) -> Self {
        Self {
            core,
            listener,
            max_frame_size,
        }
    }
}

/// Accepts connections forever, enforcing `max_connections`, and runs
/// `handler` for each one on its own task.
pub async fn accept_loop<F, Fut>(
    protocol: &'static str,
    context: ProtocolContext,
    listener: TcpListener,
    handler: F,
) -> io::Result<()>
where
    F: Fn(ProtocolContext, TcpStream, SocketAddr) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = io::Result<()>> + Send + 'static,
{
    tracing::info!(protocol, address = %listener.local_addr()?, "listener ready");
    let limit = match context.listener.max_connections {
        0 => Semaphore::MAX_PERMITS,
        n => n,
    };
    let permits = Arc::new(Semaphore::new(limit));
    loop {
        let (stream, remote) = listener.accept().await?;
        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
            tracing::warn!(protocol, %remote, "connection limit reached; rejecting");
            drop(stream);
            continue;
        };
        let _ = stream.set_nodelay(true);
        let future = handler(context.clone(), stream, remote);
        tokio::spawn(async move {
            if let Err(error) = future.await {
                tracing::debug!(protocol, %remote, %error, "connection ended with error");
            }
            drop(permit);
        });
    }
}
