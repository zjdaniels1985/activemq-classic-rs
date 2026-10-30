//! ActiveMQ OpenWire v1–v12 TCP adapter using loose encoding and disabled caches.
mod client;
mod codec;

pub use client::{OpenWireClient, OpenWireError};
pub use codec::{OpenWireFrame, WireDestination, WireId};

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use flate2::read::ZlibDecoder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{self, Duration, Instant};

use super::ProtocolContext;
use crate::core::{
    AckOp, Acks, Body, BrokerCore, ConnectInfo, ConsumerOptions, CoreError, Destination, Event,
    Message, Value,
};
use codec::{Decoder, IncomingMessage};

const MAX_WIRE_VERSION: i32 = 12;
const MIN_WIRE_VERSION: i32 = 1;

struct DispatchedDelivery {
    delivery_id: u64,
    message_id: String,
}

pub fn encode_client_message(
    producer: &WireId,
    message: &Message,
    command_id: i32,
) -> OpenWireFrame {
    codec::encode_client_message(command_id, producer, message)
}

pub async fn serve(context: ProtocolContext, listener: TcpListener) -> io::Result<()> {
    super::accept_loop("openwire", context, listener, handle_connection).await
}

async fn handle_connection(
    context: ProtocolContext,
    mut stream: TcpStream,
    remote: SocketAddr,
) -> io::Result<()> {
    let max_size = context.max_frame_size;
    let mut reader = FrameReader::default();
    let first = reader.read_frame(&mut stream, max_size).await?;
    let (client_version, client_idle_timeout) = codec::validate_wire_format(&first)?;
    if client_version < MIN_WIRE_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OpenWire version must be at least 1",
        ));
    }
    let version = client_version.min(MAX_WIRE_VERSION);
    write_frame(
        &mut stream,
        &codec::encode_wire_format_info(&context.core.info().name, max_size, version),
        max_size,
    )
    .await?;
    write_frame(
        &mut stream,
        &codec::encode_broker_info(&context.core.info().id, &context.core.info().name, version),
        max_size,
    )
    .await?;

    let mut core_connection = None;
    let cleanup = ConnectionCleanup::new(context.core.clone());
    let mut events: Option<UnboundedReceiver<Event>> = None;
    let mut consumers: HashMap<WireId, u64> = HashMap::new();
    let mut consumer_wire: HashMap<u64, WireId> = HashMap::new();
    let mut consumer_destinations: HashMap<u64, WireDestination> = HashMap::new();
    let mut producers: HashMap<WireId, u64> = HashMap::new();
    let mut producer_windows: HashMap<WireId, i32> = HashMap::new();
    let mut delivery_ids: HashMap<(WireId, String), DispatchedDelivery> = HashMap::new();
    let mut command_id = 1_i64;
    let mut last_seen = Instant::now();
    let mut heartbeat = time::interval(Duration::from_secs(10));
    heartbeat.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            result = reader.read_frame(&mut stream, max_size) => {
                let frame = match result { Ok(frame) => frame, Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break, Err(error) => return Err(error) };
                last_seen = Instant::now();
                process_command(&context, &remote, frame, &mut stream, max_size, version, &cleanup, &mut core_connection, &mut events, &mut consumers, &mut consumer_wire, &mut consumer_destinations, &mut producers, &mut producer_windows, &mut delivery_ids).await?;
            }
            event = async {
                match events.as_mut() {
                    Some(receiver) => receiver.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                match event {
                    Some(Event::Deliver(delivery)) => {
                        if let Some(wire) = consumer_wire.get(&delivery.consumer) {
                            let message = delivery.message.as_ref();
                            tracing::debug!(message_id=%message.message_id, destination=%message.destination, advisory=?message.advisory, body=?message.body, "dispatching OpenWire message");
                            let destination = codec::wire_destination(&message.destination);
                            let frame = codec::encode_dispatch_version(wire, &destination, message, command_id, u32::from(delivery.redelivered).saturating_mul(message.redelivery_counter), version);
                            command_id = command_id.wrapping_add(1);
                            delivery_ids.insert(
                                (wire.clone(), codec::dispatched_message_id(&message.message_id, version)),
                                DispatchedDelivery {
                                    delivery_id: delivery.delivery_id,
                                    message_id: message.message_id.clone(),
                                },
                            );
                            write_frame(&mut stream, &frame, max_size).await?;
                        }
                    }
                    Some(Event::BrowseDone { consumer }) => {
                        if let Some(wire) = consumer_wire.get(&consumer) {
                            let frame = encode_browse_done(wire, consumer_destinations.get(&consumer).cloned());
                            write_frame(&mut stream, &frame, max_size).await?;
                        }
                    }
                    Some(Event::ConsumerClosed { consumer, .. }) => {
                        if let Some(wire) = consumer_wire.remove(&consumer) {
                            consumers.remove(&wire);
                            consumer_destinations.remove(&consumer);
                            delivery_ids.retain(|(consumer, _), _| *consumer != wire);
                            write_frame(&mut stream, &encode_remove_info(&wire, version), max_size).await?;
                        }
                    }
                    Some(Event::ConnectionClosed { .. }) | None => break,
                    Some(Event::PullEmpty { consumer }) => {
                        if let Some(wire) = consumer_wire.get(&consumer) {
                            write_frame(&mut stream, &encode_pull_empty(wire, consumer_destinations.get(&consumer).cloned()), max_size).await?;
                        }
                    }
                    Some(Event::Drained { .. }) => {}
                }
            }
            _ = heartbeat.tick() => {
                if client_idle_timeout.is_some_and(|timeout| last_seen.elapsed() > Duration::from_millis(timeout)) { break; }
                let payload = codec::base(0, false);
                write_frame(&mut stream, &OpenWireFrame { command_type: 10, payload }, max_size).await?;
            }
        }
    }
    Ok(())
}

struct ConnectionCleanup {
    core: Arc<BrokerCore>,
    connection: AtomicU64,
}

impl ConnectionCleanup {
    fn new(core: Arc<BrokerCore>) -> Self {
        Self {
            core,
            connection: AtomicU64::new(0),
        }
    }

    fn set(&self, connection: Option<u64>) {
        self.connection
            .store(connection.unwrap_or_default(), Ordering::Release);
    }
}

impl Drop for ConnectionCleanup {
    fn drop(&mut self) {
        let connection = self.connection.swap(0, Ordering::AcqRel);
        if connection != 0 {
            self.core.disconnect(connection);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_command(
    context: &ProtocolContext,
    remote: &SocketAddr,
    frame: OpenWireFrame,
    stream: &mut TcpStream,
    max_size: usize,
    wire_version: i32,
    cleanup: &ConnectionCleanup,
    connection: &mut Option<u64>,
    events: &mut Option<UnboundedReceiver<Event>>,
    consumers: &mut HashMap<WireId, u64>,
    consumer_wire: &mut HashMap<u64, WireId>,
    consumer_destinations: &mut HashMap<u64, WireDestination>,
    producers: &mut HashMap<WireId, u64>,
    producer_windows: &mut HashMap<WireId, i32>,
    delivery_ids: &mut HashMap<(WireId, String), DispatchedDelivery>,
) -> io::Result<bool> {
    tracing::debug!(
        command_type = frame.command_type,
        size = frame.payload.len(),
        "received OpenWire command"
    );
    match frame.command_type {
        1 => Ok(false),
        3 => {
            let (header, client_id, username, password) =
                codec::decode_connection_info_version(&frame, wire_version)?;
            let result = context.core.connect(ConnectInfo {
                client_id,
                username,
                password,
                protocol: "openwire".to_owned(),
                remote_address: remote.to_string(),
                redeliver_on_rollback: false,
                ..ConnectInfo::default()
            });
            match result {
                Ok((id, receiver)) => {
                    *connection = Some(id);
                    cleanup.set(Some(id));
                    *events = Some(receiver);
                    send_response(stream, &header, max_size).await?;
                }
                Err(error) => send_error(stream, &header, &error, max_size).await?,
            }
            Ok(true)
        }
        4 => {
            let header = codec::Decoder::new(&frame.payload).header()?;
            if header.response_required {
                send_response(stream, &header, max_size).await?;
            }
            Ok(true)
        }
        5 => {
            let info = codec::decode_consumer_version(&frame, wire_version)?;
            tracing::debug!(selector=?info.selector, destination=%info.destination.name, "decoded ConsumerInfo");
            let Some(connection) = *connection else {
                return send_protocol_error(
                    stream,
                    &info.header,
                    "connection is not established",
                    max_size,
                )
                .await;
            };
            let destination = core_destination(&info.destination);
            let mut options = ConsumerOptions::new(destination);
            options.selector = info.selector.clone();
            options.prefetch = info.prefetch.max(0) as u32;
            options.no_local = info.no_local;
            options.durable_name = info.subscription_name.clone();
            options.browser = info.browser;
            options.exclusive = info.exclusive;
            options.retroactive = info.retroactive;
            options.priority = info.priority;
            options.network = info.network;
            options.broker_path = info.broker_path.clone();
            options.external_id = Some(info.id.display());
            match context.core.subscribe(connection, options) {
                Ok(id) => {
                    consumers.insert(info.id.clone(), id);
                    consumer_wire.insert(id, info.id.clone());
                    consumer_destinations.insert(id, info.destination.clone());
                    if info.header.response_required {
                        send_response(stream, &info.header, max_size).await?;
                    }
                }
                Err(error) => send_error(stream, &info.header, &error, max_size).await?,
            }
            Ok(true)
        }
        6 => {
            let info = codec::decode_producer_version(&frame, wire_version)?;
            let Some(connection) = *connection else {
                return send_protocol_error(
                    stream,
                    &info.header,
                    "connection is not established",
                    max_size,
                )
                .await;
            };
            let destination = info.destination.as_ref().map(core_destination);
            tracing::debug!(
                producer = %info.id.display(),
                window_size = info.window_size,
                "registering OpenWire producer"
            );
            match context.core.register_producer(connection, destination) {
                Ok(id) => {
                    producer_windows.insert(info.id.clone(), info.window_size);
                    producers.insert(info.id, id);
                    if info.header.response_required {
                        send_response(stream, &info.header, max_size).await?;
                    }
                }
                Err(error) => send_error(stream, &info.header, &error, max_size).await?,
            }
            Ok(true)
        }
        8 => {
            let (header, destination, operation) =
                codec::decode_destination_info(&frame, wire_version)?;
            let destination = destination.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "DestinationInfo has no destination",
                )
            })?;
            let Some(connection) = *connection else {
                return send_protocol_error(
                    stream,
                    &header,
                    "connection is not established",
                    max_size,
                )
                .await;
            };
            let dest = core_destination(&destination);
            let result = match operation {
                0 => context.core.add_destination(connection, &dest),
                1 => context.core.remove_destination(connection, &dest),
                _ => Err(CoreError::IllegalState(format!(
                    "unsupported destination operation {operation}"
                ))),
            };
            match result {
                Ok(()) => {
                    if header.response_required {
                        send_response(stream, &header, max_size).await?;
                    }
                }
                Err(error) => send_error(stream, &header, &error, max_size).await?,
            }
            Ok(true)
        }
        9 => {
            let mut d = Decoder::new(&frame.payload);
            let header = d.header()?;
            let _client_connection = d.id(120)?;
            let name = d.string()?.unwrap_or_default();
            let _client_id = d.string()?;
            if let Some(conn) = *connection {
                match context.core.remove_durable(conn, &name) {
                    Ok(()) => {
                        if header.response_required {
                            send_response(stream, &header, max_size).await?;
                        }
                    }
                    Err(e) => send_error(stream, &header, &e, max_size).await?,
                }
            }
            Ok(true)
        }
        10 | 15 | 14 | 18 => {
            let mut d = Decoder::new(&frame.payload);
            let h = d.header()?;
            if frame.command_type == 10 && h.response_required {
                send_response(stream, &h, max_size).await?;
            }
            Ok(true)
        }
        12 => {
            let (header, id) = codec::decode_remove_version(&frame, wire_version)?;
            if let (Some(conn), Some((kind, wire))) = (*connection, id) {
                match kind {
                    122 => {
                        if let Some(consumer) = consumers.remove(&wire) {
                            context.core.close_consumer(consumer);
                            consumer_wire.remove(&consumer);
                            consumer_destinations.remove(&consumer);
                        }
                        delivery_ids.retain(|(consumer, _), _| *consumer != wire);
                    }
                    123 => {
                        if let Some(producer) = producers.remove(&wire) {
                            context.core.unregister_producer(producer);
                        }
                        producer_windows.remove(&wire);
                    }
                    121 => {
                        let children = consumers
                            .keys()
                            .filter(|id| {
                                id.connection == wire.connection && id.session == wire.session
                            })
                            .cloned()
                            .collect::<Vec<_>>();
                        for child in children {
                            if let Some(consumer) = consumers.remove(&child) {
                                context.core.close_consumer(consumer);
                                consumer_wire.remove(&consumer);
                                consumer_destinations.remove(&consumer);
                            }
                            delivery_ids.retain(|(consumer, _), _| *consumer != child);
                        }
                        let children = producers
                            .keys()
                            .filter(|id| {
                                id.connection == wire.connection && id.session == wire.session
                            })
                            .cloned()
                            .collect::<Vec<_>>();
                        for child in children {
                            if let Some(producer) = producers.remove(&child) {
                                context.core.unregister_producer(producer);
                            }
                            producer_windows.remove(&child);
                        }
                    }
                    120 => {
                        context.core.disconnect(conn);
                        *connection = None;
                        cleanup.set(None);
                        *events = None;
                        consumers.clear();
                        consumer_wire.clear();
                        consumer_destinations.clear();
                        producers.clear();
                        producer_windows.clear();
                        delivery_ids.clear();
                    }
                    _ => {}
                }
            }
            if header.response_required {
                send_response(stream, &header, max_size).await?;
            }
            Ok(true)
        }
        20 => {
            let (_, wire, timeout) = codec::decode_pull(&frame)?;
            if let Some(id) = consumers.get(&wire) {
                context.core.pull(*id, timeout);
            }
            Ok(true)
        }
        22 => {
            let ack = codec::decode_ack_version(&frame, wire_version)?;
            let Some(conn) = *connection else {
                return send_protocol_error(
                    stream,
                    &ack.header,
                    "connection is not established",
                    max_size,
                )
                .await;
            };
            if let Some(consumer) = consumers.get(&ack.consumer) {
                let operation = match ack.ack_type {
                    0 => AckOp::Delivered,
                    1 => AckOp::Reject,
                    3 => AckOp::Release { increment: true },
                    5 => AckOp::Release { increment: false },
                    _ => AckOp::Consumed,
                };
                let target = ack
                    .message_id
                    .as_ref()
                    .and_then(|mid| delivery_ids.get(&(ack.consumer.clone(), mid.clone())));
                let last_delivery = target.map(|delivery| delivery.delivery_id);
                let cumulative = ack.ack_type == 2 || ack.count > 1;
                let acknowledgements = if let Some(mid) = ack.message_id.as_ref() {
                    if cumulative {
                        Acks::MessageId {
                            id: target.map_or_else(
                                || mid.clone(),
                                |delivery| delivery.message_id.clone(),
                            ),
                            up_to: true,
                        }
                    } else {
                        target
                            .map(|delivery| Acks::Deliveries(vec![delivery.delivery_id]))
                            .unwrap_or_else(|| Acks::MessageId {
                                id: mid.clone(),
                                up_to: false,
                            })
                    }
                } else {
                    Acks::All
                };
                if let Err(error) = context.core.ack(
                    conn,
                    *consumer,
                    acknowledgements,
                    operation,
                    ack.tx.as_ref(),
                ) {
                    send_error(stream, &ack.header, &error, max_size).await?;
                    return Ok(true);
                }
                if operation != AckOp::Delivered && ack.tx.is_none() {
                    delivery_ids.retain(|(consumer, _), delivery| {
                        *consumer != ack.consumer
                            || match last_delivery {
                                Some(last) if cumulative => delivery.delivery_id > last,
                                Some(last) => delivery.delivery_id != last,
                                None => ack.message_id.is_some(),
                            }
                    });
                }
            }
            if ack.header.response_required {
                send_response(stream, &ack.header, max_size).await?;
            }
            Ok(true)
        }
        7 => process_transaction(context, connection, &frame, stream, max_size).await,
        23..=29 => {
            let incoming =
                codec::decode_message_version(&frame, max_size, wire_version).map_err(|error| {
                    io::Error::new(
                        error.kind(),
                        format!("OpenWire v{wire_version} Message decode failed: {error}"),
                    )
                })?;
            let incoming_tx = incoming.tx.clone();
            let producer_id = incoming.producer_id.clone();
            // Producer windows count Classic's message memory estimate, not
            // the number of encoded bytes on the socket.
            let message_size = i32::try_from(
                1024usize
                    .saturating_add(incoming.properties.len())
                    .saturating_add(incoming.body.len()),
            )
            .unwrap_or(i32::MAX);
            tracing::debug!(properties_len=incoming.properties.len(), properties=?incoming.properties, "decoded OpenWire property bytes");
            let Some(conn) = *connection else {
                return send_protocol_error(
                    stream,
                    &incoming.header,
                    "connection is not established",
                    max_size,
                )
                .await;
            };
            match convert_message(incoming, max_size) {
                Ok(mut message) => {
                    if message.user_id.is_none()
                        && let Some(user) = context
                            .core
                            .connections()
                            .into_iter()
                            .find(|c| c.id == conn)
                            .map(|c| c.username)
                            .filter(|user| user != "anonymous")
                    {
                        message.user_id = Some(user.clone());
                        message
                            .properties
                            .insert("JMSXUserID".to_owned(), Value::String(user));
                    }
                    tracing::debug!(destination=%message.destination, properties=?message.properties, body=?message.body, "decoded OpenWire message");
                    match context.core.send(conn, message, incoming_tx.as_ref()) {
                        Ok(()) => {
                            if let Some(producer) = producer_id
                                && producer_windows
                                    .get(&producer)
                                    .is_some_and(|window| *window > 0)
                            {
                                tracing::debug!(
                                    producer = %producer.display(),
                                    size = message_size,
                                    "sending OpenWire producer flow-control acknowledgement"
                                );
                                write_frame(
                                    stream,
                                    &codec::producer_ack(&producer, message_size),
                                    max_size,
                                )
                                .await?;
                            }
                            if let Some(h) = command_header(&frame)? {
                                send_response(stream, &h, max_size).await?;
                            }
                        }
                        Err(error) => {
                            if let Some(h) = command_header(&frame)? {
                                send_error(stream, &h, &error, max_size).await?;
                            }
                        }
                    }
                }
                Err(error) => {
                    if let Some(h) = command_header(&frame)? {
                        send_protocol_error(stream, &h, &error, max_size).await?;
                    }
                }
            }
            Ok(true)
        }
        11 => {
            let h = command_header(&frame)?;
            if let (Some(conn), Some(h)) = (*connection, h) {
                if h.response_required {
                    send_response(stream, &h, max_size).await?;
                }
                context.core.disconnect(conn);
                cleanup.set(None);
            }
            *connection = None;
            Ok(true)
        }
        17 | 19 | 31 => {
            let header = command_header(&frame)?;
            if let Some(h) = header {
                if frame.command_type == 31 {
                    return Ok(false);
                }
                if h.response_required {
                    send_response(stream, &h, max_size).await?;
                }
            }
            Ok(true)
        }
        _ => {
            if let Some(header) = command_header(&frame)? {
                return send_protocol_error(
                    stream,
                    &header,
                    &format!("unsupported OpenWire command {}", frame.command_type),
                    max_size,
                )
                .await;
            }
            Ok(false)
        }
    }
}

async fn process_transaction(
    context: &ProtocolContext,
    connection: &Option<u64>,
    frame: &OpenWireFrame,
    stream: &mut TcpStream,
    max: usize,
) -> io::Result<bool> {
    let (header, kind, tx) = codec::decode_transaction(frame)?;
    let Some(conn) = *connection else {
        return send_protocol_error(stream, &header, "connection is not established", max).await;
    };
    if kind == 5 {
        let xids = context.core.recover();
        write_frame(stream, &codec::xid_array_response(&header, &xids), max).await?;
        return Ok(true);
    }
    let Some(tx) = tx else {
        return send_protocol_error(stream, &header, "transaction identifier is missing", max)
            .await;
    };
    match kind {
        0 => match context.core.begin(conn, tx) {
            Ok(()) => {
                if header.response_required {
                    send_response(stream, &header, max).await?;
                }
            }
            Err(error) => send_error(stream, &header, &error, max).await?,
        },
        1 => match context.core.prepare(conn, &tx) {
            Ok(read_only) => {
                write_frame(
                    stream,
                    &codec::integer_response(&header, if read_only { 3 } else { 0 }),
                    max,
                )
                .await?
            }
            Err(error) => send_error(stream, &header, &error, max).await?,
        },
        2 => match context.core.commit(conn, &tx, true) {
            Ok(()) => {
                if header.response_required {
                    send_response(stream, &header, max).await?;
                }
            }
            Err(error) => send_error(stream, &header, &error, max).await?,
        },
        3 => match context.core.commit(conn, &tx, false) {
            Ok(()) => {
                if header.response_required {
                    send_response(stream, &header, max).await?;
                }
            }
            Err(error) => send_error(stream, &header, &error, max).await?,
        },
        4 => match context.core.rollback(conn, &tx) {
            Ok(()) => {
                if header.response_required {
                    send_response(stream, &header, max).await?;
                }
            }
            Err(error) => send_error(stream, &header, &error, max).await?,
        },
        6 => match context.core.forget(conn, &tx) {
            Ok(()) => {
                if header.response_required {
                    send_response(stream, &header, max).await?;
                }
            }
            Err(error) => send_error(stream, &header, &error, max).await?,
        },
        7 => {
            if header.response_required {
                send_response(stream, &header, max).await?;
            }
        }
        _ => {
            return send_protocol_error(
                stream,
                &header,
                &format!("unsupported transaction operation {kind}"),
                max,
            )
            .await;
        }
    }
    Ok(true)
}

fn convert_message(mut wire: IncomingMessage, max: usize) -> Result<Message, String> {
    let destination = wire
        .destination
        .take()
        .ok_or_else(|| "message destination missing".to_owned())?;
    let dest = core_destination(&destination);
    let mut content = wire.body;
    if wire.compressed {
        let mut decoder = ZlibDecoder::new(content.as_slice());
        let mut decompressed = Vec::new();
        io::Read::read_to_end(&mut decoder, &mut decompressed)
            .map_err(|e| format!("invalid compressed message body: {e}"))?;
        if decompressed.len() > max {
            return Err("decompressed message exceeds maximum frame size".into());
        }
        content = decompressed;
    }
    let body = match wire.command_type {
        28 => Body::Text(codec::decode_text_body(&content).map_err(|e| e.to_string())?),
        24 => Body::Bytes(content),
        26 => Body::Object(content),
        25 => Body::Map(codec::unmarshal_properties(&content).map_err(|e| e.to_string())?),
        27 => Body::Stream(codec::decode_stream(&content).map_err(|e| e.to_string())?),
        _ => Body::Empty,
    };
    let mut message = Message::new(dest, body);
    if !wire.message_id.is_empty() {
        message.message_id = wire.message_id
    }
    message.persistent = wire.persistent;
    message.expiration = wire.expiration;
    message.timestamp = wire.timestamp;
    message.priority = wire.priority;
    message.correlation_id = wire.correlation_id;
    message.reply_to = wire.reply_to.map(|d| core_destination(&d));
    message.message_type = wire.message_type;
    message.group_id = wire.group_id;
    message.group_sequence = wire.group_sequence;
    message.broker_path = wire.broker_path;
    message.advisory = wire.advisory;
    message.properties =
        codec::unmarshal_properties(&wire.properties).map_err(|e| e.to_string())?;
    if message.group_id.is_none() {
        message.group_id = message
            .properties
            .get("JMSXGroupID")
            .and_then(Value::as_str)
            .map(str::to_owned);
    }
    if message.group_sequence == 0 {
        message.group_sequence = message
            .properties
            .get("JMSXGroupSeq")
            .and_then(Value::as_i64)
            .and_then(|n| i32::try_from(n).ok())
            .unwrap_or_default();
    }
    if let Some(Value::String(user)) = message.properties.remove("JMSXUserID") {
        message.user_id = Some(user)
    }
    Ok(message)
}

fn core_destination(d: &WireDestination) -> Destination {
    Destination::new(codec::destination_kind(d), d.name.clone())
}
fn command_header(frame: &OpenWireFrame) -> io::Result<Option<codec::CommandHeader>> {
    if frame.payload.len() < 5 {
        return Ok(None);
    };
    Ok(Some(Decoder::new(&frame.payload).header()?))
}
async fn send_response(
    stream: &mut TcpStream,
    header: &codec::CommandHeader,
    max: usize,
) -> io::Result<()> {
    write_frame(stream, &codec::response(header), max).await
}
async fn send_error(
    stream: &mut TcpStream,
    header: &codec::CommandHeader,
    error: &CoreError,
    max: usize,
) -> io::Result<()> {
    tracing::warn!(
        class = error.java_class(),
        message = error.message(),
        "OpenWire command failed"
    );
    write_frame(
        stream,
        &codec::exception(header, error.java_class(), error.message()),
        max,
    )
    .await
}
async fn send_protocol_error(
    stream: &mut TcpStream,
    header: &codec::CommandHeader,
    error: &str,
    max: usize,
) -> io::Result<bool> {
    tracing::warn!(%error, "OpenWire protocol command rejected");
    send_error(
        stream,
        header,
        &CoreError::IllegalState(error.to_owned()),
        max,
    )
    .await?;
    Ok(true)
}
#[derive(Default)]
struct FrameReader {
    header: [u8; 4],
    header_read: usize,
    body: Vec<u8>,
    body_read: usize,
}

impl FrameReader {
    // All partial progress survives select! cancellation when a dispatch or
    // heartbeat wins. read_exact/read_u32 would discard that progress.
    async fn read_frame(
        &mut self,
        stream: &mut TcpStream,
        max: usize,
    ) -> io::Result<OpenWireFrame> {
        while self.header_read < self.header.len() {
            let count = stream.read(&mut self.header[self.header_read..]).await?;
            if count == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            self.header_read += count;
        }
        let size = u32::from_be_bytes(self.header) as usize;
        if size == 0 || size > max {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid or oversized OpenWire frame",
            ));
        }
        if self.body.is_empty() {
            self.body.resize(size, 0);
        }
        while self.body_read < size {
            let count = stream.read(&mut self.body[self.body_read..]).await?;
            if count == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            self.body_read += count;
        }
        let frame = OpenWireFrame {
            command_type: self.body[0],
            payload: self.body[1..].to_vec(),
        };
        self.header_read = 0;
        self.body.clear();
        self.body_read = 0;
        Ok(frame)
    }
}

async fn write_frame(stream: &mut TcpStream, frame: &OpenWireFrame, max: usize) -> io::Result<()> {
    let mut b = Vec::new();
    frame.write_to(&mut b, max)?;
    stream.write_all(&b).await
}

fn encode_browse_done(wire: &WireId, dest: Option<WireDestination>) -> OpenWireFrame {
    let mut p = codec::base(0, false);
    codec::nested_id(&mut p, 122, wire);
    if let Some(d) = dest {
        codec::nested_destination(&mut p, &d)
    } else {
        p.push(0)
    }
    p.push(0);
    p.extend_from_slice(&0i32.to_be_bytes());
    OpenWireFrame {
        command_type: 21,
        payload: p,
    }
}
fn encode_pull_empty(wire: &WireId, destination: Option<WireDestination>) -> OpenWireFrame {
    let mut p = codec::base(0, false);
    codec::nested_id(&mut p, 122, wire);
    if let Some(destination) = destination {
        codec::nested_destination(&mut p, &destination);
    } else {
        p.push(0);
    }
    p.push(0);
    p.extend_from_slice(&0i32.to_be_bytes());
    OpenWireFrame {
        command_type: 21,
        payload: p,
    }
}
fn encode_remove_info(wire: &WireId, version: i32) -> OpenWireFrame {
    let mut p = codec::base(0, false);
    codec::nested_id(&mut p, 122, wire);
    if version >= 5 {
        p.extend_from_slice(&0i64.to_be_bytes());
    }
    OpenWireFrame {
        command_type: 12,
        payload: p,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_remove_info_has_the_negotiated_version_layout() {
        let wire = WireId {
            connection: "ID:client".into(),
            session: 2,
            value: 3,
        };
        for version in 1..=12 {
            let frame = encode_remove_info(&wire, version);
            let mut decoder = Decoder::new(&frame.payload);
            decoder.header().unwrap();
            assert_eq!(decoder.id(122).unwrap(), Some(wire.clone()));
            if version >= 5 {
                assert_eq!(decoder.i64().unwrap(), 0);
            }
            assert_eq!(decoder.position() as usize, frame.payload.len());
        }
    }

    #[tokio::test]
    async fn frame_reader_preserves_fragmented_progress_when_cancelled() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut writer = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut reader = FrameReader::default();
        let header = 6u32.to_be_bytes();
        writer.write_all(&header[..2]).await.unwrap();
        assert!(
            time::timeout(
                Duration::from_millis(20),
                reader.read_frame(&mut stream, 1024)
            )
            .await
            .is_err()
        );
        assert_eq!(reader.header_read, 2);
        writer.write_all(&header[2..]).await.unwrap();
        writer.write_all(&[10, 0]).await.unwrap();
        assert!(
            time::timeout(
                Duration::from_millis(20),
                reader.read_frame(&mut stream, 1024)
            )
            .await
            .is_err()
        );
        assert_eq!(reader.body_read, 2);
        writer.write_all(&[0, 0, 1, 0]).await.unwrap();
        let frame = reader.read_frame(&mut stream, 1024).await.unwrap();
        assert_eq!(frame.command_type, 10);
        assert_eq!(frame.payload, [0, 0, 0, 1, 0]);
        writer.write_all(&[0, 0, 0, 1, 15]).await.unwrap();
        let next = reader.read_frame(&mut stream, 1024).await.unwrap();
        assert_eq!(next.command_type, 15);
        assert!(next.payload.is_empty());
    }

    #[tokio::test]
    async fn frame_reader_rejects_oversized_frame_before_allocating() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut writer = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut reader = FrameReader::default();
        writer.write_all(&4096u32.to_be_bytes()).await.unwrap();
        assert_eq!(
            reader
                .read_frame(&mut stream, 1024)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert!(reader.body.is_empty());
    }
}
