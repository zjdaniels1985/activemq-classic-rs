//! Minimal bidirectional OpenWire client for tests and network bridge reuse.
use std::collections::{HashMap, VecDeque};
use std::io;
use std::time::Duration;

use flate2::read::ZlibDecoder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::time;

use super::codec::{self, OpenWireFrame, WireDestination, WireId};
use crate::core::{Body, Destination, Message};

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
    stream: TcpStream,
    max_frame_size: usize,
    next_command_id: i32,
    next_sequence: i64,
    connection_id: Option<WireId>,
    session_id: Option<WireId>,
    consumer_destinations: HashMap<WireId, WireDestination>,
    pending: VecDeque<OpenWireFrame>,
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
        Ok(Self {
            stream,
            max_frame_size,
            next_command_id: 1,
            next_sequence: 1,
            connection_id: None,
            session_id: None,
            consumer_destinations: HashMap::new(),
            pending: VecDeque::new(),
        })
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
        let connection = WireId {
            connection: format!("ID:rust-{}", std::process::id()),
            session: 0,
            value: 0,
        };
        let id = client.next_command_id();
        client
            .send_and_wait(
                codec::client_connection_info(id, &connection, client_id, username, password),
                id,
            )
            .await?;
        let session = WireId {
            connection: connection.connection.clone(),
            session: 1,
            value: 0,
        };
        let id = client.next_command_id();
        client
            .send_and_wait(codec::client_session_info(id, &session), id)
            .await?;
        client.connection_id = Some(connection);
        client.session_id = Some(session);
        Ok(client)
    }

    pub async fn create_consumer(
        &mut self,
        destination: &Destination,
        prefetch: u32,
        selector: Option<&str>,
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
            codec::client_consumer_info(cmd, &id, &wire_destination, prefetch, selector),
            cmd,
        )
        .await?;
        self.consumer_destinations
            .insert(id.clone(), wire_destination);
        Ok(id)
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
                read_frame(&mut self.stream, self.max_frame_size).await?
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
        write_frame(&mut self.stream, frame, self.max_frame_size).await?;
        Ok(())
    }
    pub fn next_command_id(&mut self) -> i32 {
        let id = self.next_command_id;
        self.next_command_id = self.next_command_id.wrapping_add(1);
        id
    }
    pub fn into_stream(self) -> TcpStream {
        self.stream
    }

    async fn send_and_wait(
        &mut self,
        frame: OpenWireFrame,
        correlation: i32,
    ) -> Result<(), OpenWireError> {
        self.send_command(&frame).await?;
        loop {
            let frame = read_frame(&mut self.stream, self.max_frame_size).await?;
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
async fn write_frame(stream: &mut TcpStream, frame: &OpenWireFrame, max: usize) -> io::Result<()> {
    let mut b = Vec::new();
    frame.write_to(&mut b, max)?;
    stream.write_all(&b).await
}
async fn read_frame(stream: &mut TcpStream, max: usize) -> io::Result<OpenWireFrame> {
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
