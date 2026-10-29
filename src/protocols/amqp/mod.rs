//! amqp adapter (not yet implemented).

use std::io;

use tokio::net::TcpListener;

use super::ProtocolContext;

pub async fn serve(context: ProtocolContext, listener: TcpListener) -> io::Result<()> {
    super::accept_loop(
        "amqp",
        context,
        listener,
        |_context, _stream, _remote| async { Err(io::Error::other("amqp is not implemented yet")) },
    )
    .await
}
