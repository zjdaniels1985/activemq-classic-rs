//! Minimal bidirectional OpenWire client for tests and network bridge reuse.
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use flate2::read::ZlibDecoder;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::time;

use super::codec::{self, OpenWireFrame, WireDestination, WireId};
use crate::core::{Body, Destination, DestinationKind, Message};

static NEXT_CLIENT_INSTANCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct OpenWireError(pub String);
impl std::fmt::Display for OpenWireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for OpenWireError {}
impl From<io::Error> for OpenWireError {
    fn from(error: io::Error) -> Self {
        Self(error.to_string())
    }
}

pub struct OpenWireClient {
    reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
    max_frame_size: usize,
    next_command_id: i32,
    next_sequence: i64,
    remote_broker_id: Option<String>,
    connection_id: Option<WireId>,
    session_id: Option<WireId>,
    consumer_destinations: HashMap<WireId, WireDestination>,
    pending: VecDeque<OpenWireFrame>,
    keepalive: time::Interval,
}

impl OpenWireClient {
    /// Connects and negotiates loose encoding with caching disabled.
    pub async fn connect(
        address: impl ToSocketAddrs,
        max_frame_size: usize,
    ) -> Result<Self, OpenWireError> {
        let mut stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;
        let hello = codec::encode_wire_format_info("activemq-rust-client", max_frame_size, 12);
        write_frame(&mut stream, &hello, max_frame_size).await?;
        let response = read_frame(&mut stream, max_frame_size).await?;
        if response.command_type != 1 {
            return Err(OpenWireError(
                "broker did not reply with WireFormatInfo".into(),
            ));
        }
        let broker = read_frame(&mut stream, max_frame_size).await?;
        if broker.command_type != 2 {
            return Err(OpenWireError("broker did not send BrokerInfo".into()));
        }
        let remote_broker_id = codec::decode_broker_info(&broker)?;
        let (reader, writer) = stream.into_split();
        Ok(Self {
            reader,
            writer,
            max_frame_size,
            next_command_id: 1,
            next_sequence: 1,
            remote_broker_id,
            connection_id: None,
            session_id: None,
            consumer_destinations: HashMap::new(),
            pending: VecDeque::new(),
            keepalive: time::interval_at(
                time::Instant::now() + Duration::from_secs(10),
                Duration::from_secs(10),
            ),
        })
    }

    pub fn remote_broker_id(&self) -> Option<&str> {
        self.remote_broker_id.as_deref()
    }

    /// Negotiates a connection and creates its default session.
    pub async fn connect_client(
        address: impl ToSocketAddrs,
        max_frame_size: usize,
        client_id: Option<&str>,
        username: Option<&str>,
        password: Option<&str>,
    ) -> Result<Self, OpenWireError> {
        let mut client = Self::connect(address, max_frame_size).await?;
        client.start_client(client_id, username, password).await?;
        Ok(client)
    }

    async fn start_client(
        &mut self,
        client_id: Option<&str>,
        username: Option<&str>,
        password: Option<&str>,
    ) -> Result<(), OpenWireError> {
        let connection = WireId {
            connection: format!(
                "ID:rust-{}-{}",
                std::process::id(),
                NEXT_CLIENT_INSTANCE.fetch_add(1, Ordering::Relaxed)
            ),
            session: 0,
            value: 0,
        };
        let id = self.next_command_id();
        let client_id = client_id.unwrap_or(&connection.connection);
        self.send_and_wait(
            codec::client_connection_info(id, &connection, Some(client_id), username, password),
            id,
        )
        .await?;
        let session = WireId {
            connection: connection.connection.clone(),
            session: 1,
            value: 0,
        };
        let id = self.next_command_id();
        self.send_and_wait(codec::client_session_info(id, &session), id)
            .await?;
        self.connection_id = Some(connection);
        self.session_id = Some(session);
        Ok(())
    }

    pub async fn create_consumer(
        &mut self,
        destination: &Destination,
        prefetch: u32,
        selector: Option<&str>,
    ) -> Result<WireId, OpenWireError> {
        self.create_consumer_options(destination, prefetch, selector, false, &[])
            .await
    }

    pub async fn create_network_consumer(
        &mut self,
        destination: &Destination,
        prefetch: u32,
        selector: Option<&str>,
        broker_path: &[String],
    ) -> Result<WireId, OpenWireError> {
        self.create_consumer_options(destination, prefetch, selector, true, broker_path)
            .await
    }

    async fn create_consumer_options(
        &mut self,
        destination: &Destination,
        prefetch: u32,
        selector: Option<&str>,
        network: bool,
        broker_path: &[String],
    ) -> Result<WireId, OpenWireError> {
        let session = self
            .session_id
            .as_ref()
            .ok_or_else(|| OpenWireError("client connection is not started".into()))?;
        let id = WireId {
            connection: session.connection.clone(),
            session: session.session,
            value: self.next_sequence,
        };
        self.next_sequence += 1;
        let cmd = self.next_command_id();
        let wire_destination = codec::wire_destination(destination);
        self.send_and_wait(
            codec::client_consumer_info_options(
                cmd,
                &id,
                &wire_destination,
                prefetch,
                selector,
                network,
                broker_path,
            ),
            cmd,
        )
        .await?;
        self.consumer_destinations
            .insert(id.clone(), wire_destination);
        Ok(id)
    }

    pub async fn add_temporary_destination(
        &mut self,
        kind: DestinationKind,
    ) -> Result<Destination, OpenWireError> {
        let connection = self
            .connection_id
            .as_ref()
            .cloned()
            .ok_or_else(|| OpenWireError("client connection is not started".into()))?;
        let destination = Destination::new(
            kind,
            format!("temp-rust-{}-{}", std::process::id(), self.next_sequence),
        );
        self.next_sequence += 1;
        let id = self.next_command_id();
        let wire_destination = codec::wire_destination(&destination);
        self.send_and_wait(
            codec::client_destination_info(id, &connection, &wire_destination, 0),
            id,
        )
        .await?;
        Ok(destination)
    }

    pub async fn add_destination(
        &mut self,
        destination: &Destination,
    ) -> Result<(), OpenWireError> {
        let connection = self
            .connection_id
            .as_ref()
            .cloned()
            .ok_or_else(|| OpenWireError("client connection is not started".into()))?;
        let id = self.next_command_id();
        self.send_and_wait(
            codec::client_destination_info(
                id,
                &connection,
                &codec::wire_destination(destination),
                0,
            ),
            id,
        )
        .await
    }

    pub async fn remove_destination(
        &mut self,
        destination: &Destination,
    ) -> Result<(), OpenWireError> {
        let connection = self
            .connection_id
            .as_ref()
            .cloned()
            .ok_or_else(|| OpenWireError("client connection is not started".into()))?;
        let id = self.next_command_id();
        self.send_and_wait(
            codec::client_destination_info(
                id,
                &connection,
                &codec::wire_destination(destination),
                1,
            ),
            id,
        )
        .await
    }

    pub async fn remove_consumer(&mut self, consumer: &WireId) -> Result<(), OpenWireError> {
        let id = self.next_command_id();
        self.send_and_wait(codec::client_remove_info(id, consumer), id)
            .await?;
        self.consumer_destinations.remove(consumer);
        Ok(())
    }

    pub async fn create_producer(
        &mut self,
        destination: Option<&Destination>,
    ) -> Result<WireId, OpenWireError> {
        let session = self
            .session_id
            .as_ref()
            .ok_or_else(|| OpenWireError("client connection is not started".into()))?;
        let id = WireId {
            connection: session.connection.clone(),
            session: session.session,
            value: self.next_sequence,
        };
        self.next_sequence += 1;
        let cmd = self.next_command_id();
        let destination = destination.map(codec::wire_destination);
        self.send_and_wait(
            codec::client_producer_info(cmd, &id, destination.as_ref()),
            cmd,
        )
        .await?;
        Ok(id)
    }

    pub async fn send(
        &mut self,
        producer: &WireId,
        message: &Message,
    ) -> Result<(), OpenWireError> {
        let id = self.next_command_id();
        let frame = codec::client_message(id, producer, message);
        self.send_command(&frame).await
    }

    pub async fn receive(&mut self) -> Result<OpenWireFrame, OpenWireError> {
        loop {
            let frame = if let Some(frame) = self.pending.pop_front() {
                frame
            } else {
                tokio::select! {
                    result = read_frame(&mut self.reader, self.max_frame_size) => result?,
                    _ = self.keepalive.tick() => {
                        let keepalive = OpenWireFrame {
                            command_type: 10,
                            payload: codec::base(0, false),
                        };
                        write_frame(&mut self.writer, &keepalive, self.max_frame_size).await?;
                        continue;
                    }
                }
            };
            if frame.command_type == 10 {
                continue;
            }
            return Ok(frame);
        }
    }

    pub async fn receive_message(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<(WireId, Message, u32)>, OpenWireError> {
        let deadline = time::Instant::now() + timeout;
        let frame = loop {
            let remaining = deadline.saturating_duration_since(time::Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            let frame = match time::timeout(remaining, self.receive()).await {
                Ok(frame) => frame?,
                Err(_) => return Ok(None),
            };
            if frame.command_type == 31 {
                return Err(OpenWireError(decode_exception(&frame)));
            }
            if frame.command_type == 21 {
                break frame;
            }
        };
        let (consumer, _destination, wire, redelivery) =
            codec::decode_dispatch(&frame, self.max_frame_size)?;
        let mut content = wire.body;
        if wire.compressed {
            let mut decoder = ZlibDecoder::new(content.as_slice());
            let mut bytes = Vec::new();
            io::Read::read_to_end(&mut decoder, &mut bytes)?;
            content = bytes;
        }
        let body = match wire.command_type {
            28 => Body::Text(codec::decode_text_body(&content)?),
            24 => Body::Bytes(content),
            25 => Body::Map(codec::unmarshal_properties(&content)?),
            26 => Body::Object(content),
            27 => Body::Stream(codec::decode_stream(&content)?),
            _ => Body::Empty,
        };
        let dest = wire
            .destination
            .as_ref()
            .map(codec::destination_kind)
            .map(|kind| {
                Destination::new(
                    kind,
                    wire.destination
                        .as_ref()
                        .map(|d| d.name.clone())
                        .unwrap_or_default(),
                )
            })
            .ok_or_else(|| OpenWireError("received message has no destination".into()))?;
        let mut message = Message::new(dest, body);
        if !wire.message_id.is_empty() {
            message.message_id = wire.message_id
        }
        message.persistent = wire.persistent;
        message.expiration = wire.expiration;
        message.timestamp = wire.timestamp;
        message.priority = wire.priority;
        message.correlation_id = wire.correlation_id;
        message.reply_to = wire
            .reply_to
            .map(|d| Destination::new(codec::destination_kind(&d), d.name));
        message.group_id = wire.group_id;
        message.group_sequence = wire.group_sequence;
        message.message_type = wire.message_type;
        message.broker_path = wire.broker_path;
        message.advisory = wire.advisory;
        message.properties = codec::unmarshal_properties(&wire.properties)?;
        Ok(Some((consumer, message, redelivery)))
    }

    pub async fn acknowledge(
        &mut self,
        consumer: &WireId,
        message_id: &str,
    ) -> Result<(), OpenWireError> {
        let destination = self
            .consumer_destinations
            .get(consumer)
            .cloned()
            .ok_or_else(|| OpenWireError("unknown consumer ID".into()))?;
        let command = self.next_command_id();
        let frame = codec::client_ack(command, consumer, &destination, message_id);
        self.send_command(&frame).await
    }

    pub async fn send_command(&mut self, frame: &OpenWireFrame) -> Result<(), OpenWireError> {
        write_frame(&mut self.writer, frame, self.max_frame_size).await?;
        Ok(())
    }
    pub fn next_command_id(&mut self) -> i32 {
        let id = self.next_command_id;
        self.next_command_id = self.next_command_id.wrapping_add(1);
        id
    }
    pub fn into_parts(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        (self.reader, self.writer)
    }

    async fn send_and_wait(
        &mut self,
        frame: OpenWireFrame,
        correlation: i32,
    ) -> Result<(), OpenWireError> {
        self.send_command(&frame).await?;
        loop {
            let frame = read_frame(&mut self.reader, self.max_frame_size).await?;
            if frame.command_type == 10 {
                continue;
            }
            match frame.command_type {
                30 => {
                    if frame.payload.len() >= 9
                        && i32::from_be_bytes(frame.payload[5..9].try_into().unwrap())
                            == correlation
                    {
                        return Ok(());
                    }
                    self.pending.push_back(frame)
                }
                31 => return Err(OpenWireError(decode_exception(&frame))),
                _ => self.pending.push_back(frame),
            }
        }
    }
}

fn decode_exception(frame: &OpenWireFrame) -> String {
    let mut d = codec::Decoder::new(&frame.payload);
    let _ = d.header();
    let _ = d.i32();
    if d.bool().unwrap_or(false) {
        format!(
            "{}: {}",
            d.string().ok().flatten().unwrap_or_default(),
            d.string().ok().flatten().unwrap_or_default()
        )
    } else {
        "OpenWire broker exception".into()
    }
}
async fn write_frame(
    stream: &mut (impl AsyncWrite + Unpin),
    frame: &OpenWireFrame,
    max: usize,
) -> io::Result<()> {
    let mut b = Vec::new();
    frame.write_to(&mut b, max)?;
    stream.write_all(&b).await
}
async fn read_frame(
    stream: &mut (impl AsyncRead + Unpin),
    max: usize,
) -> io::Result<OpenWireFrame> {
    let n = stream.read_u32().await? as usize;
    if n == 0 || n > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid frame length",
        ));
    }
    let mut b = vec![0; n];
    stream.read_exact(&mut b).await?;
    Ok(OpenWireFrame {
        command_type: b[0],
        payload: b[1..].to_vec(),
    })
}
