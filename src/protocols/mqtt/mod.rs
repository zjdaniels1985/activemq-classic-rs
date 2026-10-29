//! mqtt adapter (not yet implemented).

use std::io;

use tokio::net::TcpListener;

use super::ProtocolContext;

pub async fn serve(context: ProtocolContext, listener: TcpListener) -> io::Result<()> {
    super::accept_loop(
        "mqtt",
        context,
        listener,
        |_context, _stream, _remote| async { Err(io::Error::other("mqtt is not implemented yet")) },
    )
    .await
}
