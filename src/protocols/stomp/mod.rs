//! STOMP 1.0, 1.1 and 1.2 transport adapter.

mod map_json;

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior};

use super::ProtocolContext;
use crate::core::{
    AckOp, Acks, Body, ConnectInfo, ConsumerId, ConsumerOptions, CoreError, Delivery, Destination,
    DestinationKind, Event, Message, TxId, Value,
};

const SUPPORTED: [&str; 3] = ["1.0", "1.1", "1.2"];
const MAX_HEADERS: usize = 100;
const MAX_HEADER_LINE: usize = 8192;

pub async fn serve(context: ProtocolContext, listener: TcpListener) -> io::Result<()> {
    super::accept_loop("stomp", context, listener, handle_connection).await
}

#[derive(Debug, Clone)]
struct Frame {
    command: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Frame {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

enum Input {
    Frame(Frame),
    Heartbeat,
    Closed,
    Error(String),
}

async fn read_line<R: AsyncRead + Unpin>(
    reader: &mut R,
    limit: usize,
) -> Result<Option<Vec<u8>>, String> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        match reader.read(&mut byte).await {
            Ok(0) if line.is_empty() => return Ok(None),
            Ok(0) => return Err("unexpected end of stream in frame".into()),
            Ok(_) if byte[0] == b'\n' => {
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                return Ok(Some(line));
            }
            Ok(_) => {
                if line.len() >= limit {
                    return Err("frame header line is too large".into());
                }
                line.push(byte[0]);
            }
            Err(error) => return Err(error.to_string()),
        }
    }
}

async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R, max_size: usize) -> Input {
    let command = match read_line(reader, MAX_HEADER_LINE).await {
        Ok(None) => return Input::Closed,
        Ok(Some(line)) if line.is_empty() => return Input::Heartbeat,
        Ok(Some(line)) => match String::from_utf8(line) {
            Ok(command) => command,
            Err(_) => return Input::Error("command is not valid UTF-8".into()),
        },
        Err(error) => return Input::Error(error),
    };
    if command.len() > max_size {
        return Input::Error("frame exceeds maximum size".into());
    }
    let mut headers = Vec::new();
    let mut header_bytes = command.len();
    loop {
        match read_line(reader, MAX_HEADER_LINE).await {
            Ok(Some(line)) if line.is_empty() => break,
            Ok(Some(line)) => {
                header_bytes = header_bytes.saturating_add(line.len() + 1);
                if headers.len() >= MAX_HEADERS {
                    return Input::Error("too many headers".into());
                }
                if header_bytes > max_size {
                    return Input::Error("frame exceeds maximum size".into());
                }
                let Ok(line) = String::from_utf8(line) else {
                    return Input::Error("header is not valid UTF-8".into());
                };
                let Some((key, value)) = line.split_once(':') else {
                    return Input::Error("malformed header".into());
                };
                headers.push((key.to_owned(), value.to_owned()));
            }
            Ok(None) => return Input::Error("unexpected end of stream in headers".into()),
            Err(error) => return Input::Error(error),
        }
    }
    let content_length = match headers.iter().find(|(key, _)| key == "content-length") {
        Some((_, value)) => match value.parse::<usize>() {
            Ok(length) => Some(length),
            Err(_) => return Input::Error("invalid content-length header".into()),
        },
        None => None,
    };
    let mut body = Vec::new();
    if let Some(length) = content_length {
        if length > max_size || header_bytes.saturating_add(length) > max_size {
            return Input::Error("frame exceeds maximum size".into());
        }
        body.resize(length, 0);
        if let Err(error) = reader.read_exact(&mut body).await {
            return Input::Error(error.to_string());
        }
        let mut terminator = [0];
        if let Err(error) = reader.read_exact(&mut terminator).await {
            return Input::Error(error.to_string());
        }
        if terminator[0] != 0 {
            return Input::Error("content-length body is not NUL terminated".into());
        }
    } else {
        loop {
            let mut byte = [0];
            match reader.read(&mut byte).await {
                Ok(0) => return Input::Error("unexpected end of stream in body".into()),
                Ok(_) if byte[0] == 0 => break,
                Ok(_) => {
                    if body.len().saturating_add(header_bytes) >= max_size {
                        return Input::Error("frame exceeds maximum size".into());
                    }
                    body.push(byte[0]);
                }
                Err(error) => return Input::Error(error.to_string()),
            }
        }
    }
    Input::Frame(Frame {
        command,
        headers,
        body,
    })
}

async fn reader_task<R: AsyncRead + Unpin>(
    mut reader: R,
    max_size: usize,
    tx: mpsc::UnboundedSender<Input>,
) {
    loop {
        let input = read_frame(&mut reader, max_size).await;
        let terminal = matches!(input, Input::Closed | Input::Error(_));
        if tx.send(input).is_err() || terminal {
            return;
        }
    }
}

async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    command: &str,
    headers: &[(String, String)],
    body: &[u8],
    version: &str,
) -> io::Result<()> {
    writer.write_all(command.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    let escape =
        version != "1.0" && command != "CONNECTED" && command != "CONNECT" && command != "STOMP";
    for (key, value) in headers {
        let key = if escape {
            escape_header(key)
        } else {
            key.clone()
        };
        writer.write_all(key.as_bytes()).await?;
        writer.write_all(b":").await?;
        let value = if escape {
            escape_header(value)
        } else {
            value.clone()
        };
        writer.write_all(value.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }
    writer.write_all(b"\n").await?;
    writer.write_all(body).await?;
    writer.write_all(&[0]).await?;
    writer.flush().await
}

fn escape_header(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            ':' => out.push_str("\\c"),
            _ => out.push(ch),
        }
    }
    out
}

fn unescape_header(value: &str) -> Result<String, String> {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('\\') => out.push('\\'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('c') => out.push(':'),
            _ => return Err("invalid header escape".into()),
        }
    }
    Ok(out)
}

fn header(frame: &Frame, key: &str, version: &str) -> Result<Option<String>, String> {
    let raw = frame.headers.iter().find(|(header_key, _)| {
        if version == "1.0" || matches!(frame.command.as_str(), "CONNECT" | "STOMP" | "CONNECTED") {
            header_key == key
        } else {
            unescape_header(header_key).is_ok_and(|decoded| decoded == key)
        }
    });
    let Some((_, value)) = raw else {
        return Ok(None);
    };
    if version == "1.0" || matches!(frame.command.as_str(), "CONNECT" | "STOMP" | "CONNECTED") {
        Ok(Some(value.clone()))
    } else {
        unescape_header(value).map(Some)
    }
}

fn response_headers(
    entries: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
) -> Vec<(String, String)> {
    entries
        .into_iter()
        .map(|(key, value)| (key.into(), value.into()))
        .collect()
}

async fn send_error<W: AsyncWrite + Unpin>(
    writer: &mut W,
    version: &str,
    message: &str,
    receipt: Option<&str>,
) -> io::Result<()> {
    let mut headers = vec![("message".into(), message.to_owned())];
    if let Some(id) = receipt {
        headers.push(("receipt-id".into(), id.to_owned()));
    }
    write_frame(writer, "ERROR", &headers, message.as_bytes(), version).await
}

async fn handle_connection(
    context: ProtocolContext,
    stream: TcpStream,
    remote: SocketAddr,
) -> io::Result<()> {
    let (read_half, mut writer) = tokio::io::split(stream);
    let (input_tx, mut input_rx) = mpsc::unbounded_channel();
    let reader_handle = tokio::spawn(reader_task(
        BufReader::new(read_half),
        context.max_frame_size,
        input_tx,
    ));
    let mut conn = None;
    let mut events: Option<tokio::sync::mpsc::UnboundedReceiver<Event>> = None;
    let mut version = "1.0".to_owned();
    let mut heartbeat_send = 0u64;
    let mut heartbeat_receive = 0u64;
    let mut last_input = Instant::now();
    let mut last_output = Instant::now();
    let mut heartbeat = tokio::time::interval(Duration::from_millis(100));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut subscriptions: HashMap<String, Subscription> = HashMap::new();
    let mut consumer_names: HashMap<ConsumerId, String> = HashMap::new();
    let mut destinations: HashMap<String, Destination> = HashMap::new();
    let mut deliveries: HashMap<String, DeliveryRef> = HashMap::new();
    let mut seq = 0u64;
    let mut transactions: HashMap<String, TxId> = HashMap::new();
    let mut connected = false;
    let mut close = false;

    while !close {
        tokio::select! {
            input = input_rx.recv() => match input {
                Some(Input::Heartbeat) => { last_input = Instant::now(); }
                Some(Input::Frame(frame)) => {
                    last_input = Instant::now();
                    if !connected {
                        if !matches!(frame.command.as_str(), "CONNECT" | "STOMP") {
                            send_error(&mut writer, "1.0", "expected CONNECT or STOMP", frame.header("receipt")).await?;
                            break;
                        }
                        match connect_frame(&frame, &context, remote).await {
                            Ok((id, rx, negotiated, send_ms, receive_ms)) => {
                                conn = Some(id); events = Some(rx); version = negotiated.clone();
                                heartbeat_send = send_ms; heartbeat_receive = receive_ms;
                                let hb = format!("{heartbeat_send},{heartbeat_receive}");
                                let response = response_headers([("version", negotiated.as_str()), ("session", &id.to_string()), ("server", "ActiveMQ/5.x (activemq-classic-rs)"), ("heart-beat", hb.as_str())]);
                                write_frame(&mut writer, "CONNECTED", &response, &[], &version).await?;
                                connected = true;
                            }
                            Err(error) => {
                                send_error(&mut writer, "1.0", &error, frame.header("receipt")).await?;
                                break;
                            }
                        }
                    } else {
                        let id = conn.expect("connected connection");
                        match process_frame(&frame, &mut writer, &context, id, &version, &mut subscriptions, &mut consumer_names, &mut destinations, &mut deliveries, &mut transactions).await {
                            Ok(should_close) => close = should_close,
                            Err(error) => {
                                send_error(&mut writer, &version, &error, frame.header("receipt")).await?;
                                if matches!(error.as_str(), "unknown command" | "invalid header escape" | "malformed destination") { close = true; }
                            }
                        }
                    }
                }
                Some(Input::Error(error)) => { if connected { send_error(&mut writer, &version, &error, None).await?; } break; }
                Some(Input::Closed) | None => break,
            },
            event = async { events.as_mut().expect("connected event receiver").recv().await }, if connected => match event {
                Some(Event::Deliver(delivery)) => {
                    if write_delivery(&mut writer, &version, delivery, &subscriptions, &consumer_names, &mut deliveries, &mut seq, &context.core, conn.expect("connected connection")).await.is_err() { break; }
                }
                Some(Event::BrowseDone { consumer }) => {
                    if let Some(subscription) = subscriptions.values().find(|sub| sub.consumer == consumer) {
                        let headers = response_headers([("subscription", subscription.id.as_str()), ("destination", subscription.destination.as_str()), ("browser:end", "true")]);
                        if write_frame(&mut writer, "MESSAGE", &headers, &[], &version).await.is_err() { break; }
                    }
                }
                Some(Event::ConsumerClosed { consumer, reason }) => {
                    let sub = consumer_names.get(&consumer).cloned().unwrap_or_default();
                    let mut headers = vec![("message".into(), reason.clone())];
                    if !sub.is_empty() { headers.push(("subscription".into(), sub)); }
                    write_frame(&mut writer, "ERROR", &headers, reason.as_bytes(), &version).await?;
                }
                Some(Event::ConnectionClosed { reason }) => { send_error(&mut writer, &version, &reason, None).await?; break; }
                Some(Event::PullEmpty { .. } | Event::Drained { .. }) => {}
                None => break,
            },
            _ = heartbeat.tick(), if connected => {
                let now = Instant::now();
                if heartbeat_receive > 0 && now.duration_since(last_input) > Duration::from_millis(heartbeat_receive.saturating_mul(2)) {
                    send_error(&mut writer, &version, "client heartbeat timeout", None).await?;
                    break;
                }
                if heartbeat_send > 0 && now.duration_since(last_output) >= Duration::from_millis(heartbeat_send) {
                    writer.write_all(b"\n").await?;
                    writer.flush().await?;
                    last_output = now;
                }
            }
        }
    }
    reader_handle.abort();
    if let Some(id) = conn {
        context.core.disconnect(id);
    }
    Ok(())
}

async fn connect_frame(
    frame: &Frame,
    context: &ProtocolContext,
    remote: SocketAddr,
) -> Result<
    (
        u64,
        tokio::sync::mpsc::UnboundedReceiver<Event>,
        String,
        u64,
        u64,
    ),
    String,
> {
    let accepted = frame.header("accept-version").unwrap_or("1.0");
    let requested = accepted.split(',').map(str::trim).collect::<Vec<_>>();
    let version = SUPPORTED
        .iter()
        .rev()
        .find(|v| requested.contains(v))
        .copied()
        .ok_or_else(|| "unsupported STOMP version".to_owned())?;
    if version != "1.0" && frame.header("host").is_none() {
        return Err("host header is required".into());
    }
    let client_id = frame.header("client-id").map(str::to_owned);
    let info = ConnectInfo {
        client_id,
        username: frame.header("login").map(str::to_owned),
        password: frame.header("passcode").map(str::to_owned),
        protocol: "stomp".into(),
        remote_address: remote.to_string(),
        redeliver_on_rollback: true,
        ..ConnectInfo::default()
    };
    let (id, rx) = context
        .core
        .connect(info)
        .map_err(|error| error.message().to_owned())?;
    let (client_send, client_receive) =
        parse_heartbeats(frame.header("heart-beat").unwrap_or("0,0"));
    let (server_send, server_receive) = if version == "1.0" {
        (0, 0)
    } else {
        (
            negotiate(
                client_receive,
                context
                    .listener
                    .option_i64("heart-beat-send")
                    .unwrap_or(10000)
                    .max(0) as u64,
            ),
            negotiate(
                client_send,
                context
                    .listener
                    .option_i64("heart-beat-receive")
                    .unwrap_or(10000)
                    .max(0) as u64,
            ),
        )
    };
    Ok((id, rx, version.to_owned(), server_send, server_receive))
}

fn parse_heartbeats(value: &str) -> (u64, u64) {
    let mut pair = value.split(',');
    (
        pair.next().and_then(|n| n.parse().ok()).unwrap_or(0),
        pair.next().and_then(|n| n.parse().ok()).unwrap_or(0),
    )
}
fn negotiate(client: u64, server: u64) -> u64 {
    if client == 0 || server == 0 {
        0
    } else {
        client.max(server)
    }
}

struct Subscription {
    id: String,
    destination: String,
    durable_name: Option<String>,
    consumer: ConsumerId,
    ack: AckMode,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum AckMode {
    Auto,
    Client,
    Individual,
}
#[derive(Clone, Copy)]
struct DeliveryRef {
    consumer: ConsumerId,
    core_id: u64,
}

fn destination_for(
    path: &str,
    core: &crate::core::BrokerCore,
    conn: u64,
    map: &mut HashMap<String, Destination>,
    allow_temp: bool,
) -> Result<Destination, String> {
    if let Some(destination) = map.get(path) {
        return Ok(destination.clone());
    }
    let mut components = path.split(',');
    let (kind, first_name) = parse_destination_component(components.next().unwrap_or_default())?;
    let mut names = vec![first_name];
    for component in components {
        let (component_kind, name) = parse_destination_component(component)?;
        names.push(if component_kind == kind {
            name
        } else {
            format!("{}://{name}", component_kind.scheme())
        });
    }
    let destination = Destination::new(kind, names.join(","));
    if kind.is_temporary()
        && allow_temp
        && path.starts_with("/temp-")
        && let Err(error) = core.create_temp(conn, destination.clone())
    {
        return Err(error.message().to_owned());
    }
    map.insert(path.to_owned(), destination.clone());
    Ok(destination)
}

fn parse_destination_component(path: &str) -> Result<(DestinationKind, String), String> {
    let prefixes = [
        ("/queue/", DestinationKind::Queue),
        ("/topic/", DestinationKind::Topic),
        ("/temp-queue/", DestinationKind::TempQueue),
        ("/temp-topic/", DestinationKind::TempTopic),
        ("/remote-temp-queue/", DestinationKind::TempQueue),
        ("/remote-temp-topic/", DestinationKind::TempTopic),
    ];
    let (prefix, kind) = prefixes
        .iter()
        .find(|(prefix, _)| path.starts_with(prefix))
        .ok_or_else(|| "malformed destination".to_owned())?;
    let name = path.strip_prefix(prefix).unwrap_or_default();
    if name.is_empty() {
        return Err("malformed destination".into());
    }
    Ok((*kind, name.to_owned()))
}

fn stom_destination(destination: &Destination) -> String {
    let prefix = match destination.kind {
        DestinationKind::Queue => "/queue/",
        DestinationKind::Topic => "/topic/",
        DestinationKind::TempQueue => "/remote-temp-queue/",
        DestinationKind::TempTopic => "/remote-temp-topic/",
    };
    format!("{prefix}{}", destination.name)
}

#[allow(clippy::too_many_arguments)]
async fn process_frame<W: AsyncWrite + Unpin>(
    frame: &Frame,
    writer: &mut W,
    context: &ProtocolContext,
    conn: u64,
    version: &str,
    subscriptions: &mut HashMap<String, Subscription>,
    consumer_names: &mut HashMap<ConsumerId, String>,
    destinations: &mut HashMap<String, Destination>,
    deliveries: &mut HashMap<String, DeliveryRef>,
    transactions: &mut HashMap<String, TxId>,
) -> Result<bool, String> {
    match frame.command.as_str() {
        "SEND" => {
            let path =
                header(frame, "destination", version)?.ok_or("destination header is required")?;
            let destination = destination_for(&path, &context.core, conn, destinations, true)?;
            let content_type = header(frame, "content-type", version)?;
            let as_bytes = frame.header("content-length").is_some()
                || frame.header("amq-msg-type") == Some("bytes");
            let transformation = header(frame, "transformation", version)?;
            let body = if as_bytes {
                Body::Bytes(frame.body.clone())
            } else if transformation.as_deref() == Some("jms-map-json") {
                Body::Map(map_json::decode(&frame.body)?)
            } else {
                Body::Text(String::from_utf8_lossy(&frame.body).into_owned())
            };
            let mut message = Message::new(destination, body);
            if let Some(v) = header(frame, "persistent", version)? {
                message.persistent = v != "false" && v != "0";
            }
            if let Some(v) = header(frame, "priority", version)? {
                message.priority = v.parse::<u8>().unwrap_or(4).min(9);
            }
            if let Some(v) = header(frame, "expires", version)? {
                message.expiration = v.parse().unwrap_or(0);
            }
            if let Some(v) = header(frame, "correlation-id", version)? {
                message.correlation_id = Some(v);
            }
            if let Some(v) = header(frame, "type", version)? {
                message.message_type = Some(v);
            }
            if let Some(v) = header(frame, "JMSXGroupID", version)? {
                message.group_id = Some(v);
            }
            if let Some(v) = header(frame, "JMSXGroupSeq", version)? {
                message.group_sequence = v.parse().unwrap_or(0);
            }
            if let Some(v) = header(frame, "reply-to", version)? {
                message.reply_to = Some(destination_for(
                    &v,
                    &context.core,
                    conn,
                    destinations,
                    v.starts_with("/temp-"),
                )?);
            }
            if let Some(v) = content_type {
                message
                    .properties
                    .insert("content-type".into(), Value::String(v));
            }
            for (key, value) in &frame.headers {
                if matches!(
                    key.as_str(),
                    "destination"
                        | "content-length"
                        | "content-type"
                        | "persistent"
                        | "priority"
                        | "expires"
                        | "correlation-id"
                        | "reply-to"
                        | "type"
                        | "JMSXGroupID"
                        | "JMSXGroupSeq"
                        | "receipt"
                        | "transaction"
                        | "amq-msg-type"
                ) {
                    continue;
                }
                let property_key = if version == "1.0" {
                    key.clone()
                } else {
                    unescape_header(key)?
                };
                let val = if version == "1.0" {
                    value.clone()
                } else {
                    unescape_header(value)?
                };
                let typed = match property_key.as_str() {
                    "AMQ_SCHEDULED_DELAY" | "AMQ_SCHEDULED_PERIOD" => val
                        .parse::<i64>()
                        .map(Value::Long)
                        .unwrap_or_else(|_| Value::String(val.clone())),
                    "AMQ_SCHEDULED_REPEAT" => val
                        .parse::<i32>()
                        .map(Value::Int)
                        .unwrap_or_else(|_| Value::String(val.clone())),
                    _ => Value::String(val),
                };
                message.properties.insert(property_key, typed);
            }
            let tx = frame
                .header("transaction")
                .map(|id| {
                    transactions
                        .get(id)
                        .cloned()
                        .ok_or_else(|| format!("unknown transaction {id}"))
                })
                .transpose()?;
            context
                .core
                .send(conn, message, tx.as_ref())
                .map_err(core_error)?;
        }
        "SUBSCRIBE" => {
            let id = header(frame, "id", version)?;
            if version != "1.0" && id.is_none() {
                return Err("id is required for STOMP 1.1+ subscriptions".into());
            }
            let id = id.unwrap_or_else(|| format!("sub-{}", subscriptions.len() + 1));
            let path =
                header(frame, "destination", version)?.ok_or("destination header is required")?;
            let destination = destination_for(&path, &context.core, conn, destinations, true)?;
            let ack = match frame.header("ack").unwrap_or("auto") {
                "auto" => AckMode::Auto,
                "client" => AckMode::Client,
                "client-individual" => AckMode::Individual,
                _ => return Err("invalid ack mode".into()),
            };
            let durable_name = header(frame, "activemq.subscriptionName", version)?;
            let prefetch = header(frame, "activemq.prefetchSize", version)?
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000);
            let options = ConsumerOptions {
                destination,
                selector: header(frame, "selector", version)?,
                prefetch,
                no_local: header(frame, "activemq.noLocal", version)?.is_some_and(|v| v == "true"),
                durable_name: durable_name.clone(),
                browser: header(frame, "browser", version)?.is_some_and(|v| v == "true"),
                exclusive: header(frame, "activemq.exclusive", version)?
                    .is_some_and(|v| v == "true"),
                priority: header(frame, "activemq.priority", version)?
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0),
                retroactive: header(frame, "activemq.retroactive", version)?
                    .is_some_and(|v| v == "true"),
                external_id: Some(id.clone()),
                ..ConsumerOptions::new(Destination::queue("unused"))
            };
            let consumer = context.core.subscribe(conn, options).map_err(core_error)?;
            subscriptions.insert(
                id.clone(),
                Subscription {
                    id: id.clone(),
                    destination: path,
                    durable_name,
                    consumer,
                    ack,
                },
            );
            consumer_names.insert(consumer, id);
        }
        "UNSUBSCRIBE" => {
            if let Some(name) = header(frame, "activemq.subscriptionName", version)? {
                if let Some((id, subscription)) = subscriptions
                    .iter()
                    .find(|(_, sub)| sub.durable_name.as_deref() == Some(&name))
                    .map(|(id, sub)| (id.clone(), sub.consumer))
                {
                    subscriptions.remove(&id);
                    consumer_names.remove(&subscription);
                    context.core.close_consumer(subscription);
                }
                context
                    .core
                    .remove_durable(conn, &name)
                    .map_err(core_error)?;
            } else {
                let id = header(frame, "id", version)?
                    .or(header(frame, "destination", version)?)
                    .ok_or("subscription id is required")?;
                let subscription = subscriptions
                    .remove(&id)
                    .ok_or_else(|| format!("subscription {id} not found"))?;
                consumer_names.remove(&subscription.consumer);
                context.core.close_consumer(subscription.consumer);
            }
        }
        "ACK" | "NACK" => {
            let id = if version == "1.2" {
                header(frame, "id", version)?.ok_or("acknowledgement id is required")?
            } else {
                let message_id =
                    header(frame, "message-id", version)?.ok_or("message-id is required")?;
                let subscription =
                    header(frame, "subscription", version)?.ok_or("subscription is required")?;
                format!("{subscription}\0{message_id}")
            };
            let record = *deliveries
                .get(&id)
                .ok_or_else(|| format!("unknown acknowledgement id {id}"))?;
            let mode = subscriptions
                .values()
                .find(|sub| sub.consumer == record.consumer)
                .ok_or("subscription not found")?
                .ack;
            let ack_ids = if mode == AckMode::Client {
                Acks::UpTo(record.core_id)
            } else {
                Acks::Deliveries(vec![record.core_id])
            };
            let tx = header(frame, "transaction", version)?
                .map(|name| {
                    transactions
                        .get(&name)
                        .cloned()
                        .ok_or_else(|| format!("unknown transaction {name}"))
                })
                .transpose()?;
            let op = if frame.command == "ACK" {
                AckOp::Consumed
            } else {
                AckOp::Release { increment: true }
            };
            context
                .core
                .ack(conn, record.consumer, ack_ids, op, tx.as_ref())
                .map_err(core_error)?;
            if tx.is_none() {
                if mode == AckMode::Client {
                    deliveries.retain(|_, delivered| {
                        delivered.consumer != record.consumer || delivered.core_id > record.core_id
                    });
                } else {
                    deliveries.retain(|_, delivered| {
                        delivered.consumer != record.consumer || delivered.core_id != record.core_id
                    });
                }
            }
        }
        "BEGIN" => {
            let name =
                header(frame, "transaction", version)?.ok_or("transaction header is required")?;
            let tx = TxId::Local(name.clone());
            context.core.begin(conn, tx.clone()).map_err(core_error)?;
            transactions.insert(name, tx);
        }
        "COMMIT" | "ABORT" => {
            let name =
                header(frame, "transaction", version)?.ok_or("transaction header is required")?;
            let tx = transactions
                .remove(&name)
                .ok_or_else(|| format!("unknown transaction {name}"))?;
            if frame.command == "COMMIT" {
                context.core.commit(conn, &tx, true).map_err(core_error)?;
            } else {
                context.core.rollback(conn, &tx).map_err(core_error)?;
            }
        }
        "DISCONNECT" => {
            context.core.disconnect(conn);
            receipt(frame, writer, version)
                .await
                .map_err(|error| error.to_string())?;
            return Ok(true);
        }
        _ => return Err("unknown command".into()),
    }
    receipt(frame, writer, version)
        .await
        .map_err(|error| error.to_string())?;
    Ok(false)
}

fn core_error(error: CoreError) -> String {
    error.message().to_owned()
}
async fn receipt<W: AsyncWrite + Unpin>(
    frame: &Frame,
    writer: &mut W,
    version: &str,
) -> io::Result<()> {
    if let Some(id) = frame.header("receipt") {
        write_frame(
            writer,
            "RECEIPT",
            &[(
                "receipt-id".into(),
                if version == "1.0" {
                    id.to_owned()
                } else {
                    unescape_header(id).unwrap_or_else(|_| id.to_owned())
                },
            )],
            &[],
            version,
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn write_delivery<W: AsyncWrite + Unpin>(
    writer: &mut W,
    version: &str,
    delivery: Delivery,
    subscriptions: &HashMap<String, Subscription>,
    consumer_names: &HashMap<ConsumerId, String>,
    deliveries: &mut HashMap<String, DeliveryRef>,
    seq: &mut u64,
    core: &crate::core::BrokerCore,
    conn: u64,
) -> io::Result<()> {
    let Some(subscription) = consumer_names.get(&delivery.consumer) else {
        return Ok(());
    };
    let mode = subscriptions
        .get(subscription)
        .map(|sub| sub.ack)
        .unwrap_or(AckMode::Auto);
    *seq += 1;
    let ack_token = format!("ack-{seq}");
    let message = &delivery.message;
    let mut headers = vec![
        ("subscription".into(), subscription.clone()),
        ("message-id".into(), message.message_id.clone()),
        ("destination".into(), stom_destination(&message.destination)),
        ("timestamp".into(), message.timestamp.to_string()),
        ("priority".into(), message.priority.to_string()),
        ("persistent".into(), message.persistent.to_string()),
    ];
    if version == "1.2" && mode != AckMode::Auto {
        headers.push(("ack".into(), ack_token.clone()));
    }
    if version != "1.2" && mode != AckMode::Auto {
        headers.push(("ack".into(), message.message_id.clone()));
    }
    if delivery.redelivered || message.redelivery_counter > 0 {
        headers.push(("redelivered".into(), "true".into()));
    }
    if message.expiration > 0 {
        headers.push(("expires".into(), message.expiration.to_string()));
    }
    if let Some(value) = &message.correlation_id {
        headers.push(("correlation-id".into(), value.clone()));
    }
    if let Some(value) = &message.reply_to {
        headers.push(("reply-to".into(), stom_destination(value)));
    }
    if let Some(value) = &message.message_type {
        headers.push(("type".into(), value.clone()));
    }
    for (key, value) in &message.properties {
        headers.push((key.clone(), value.to_display_string()));
    }
    let bytes = matches!(message.body.as_ref(), Body::Bytes(_) | Body::Object(_));
    if bytes {
        headers.push(("content-length".into(), message.body.len().to_string()));
    }
    let body = match message.body.as_ref() {
        Body::Text(text) => text.as_bytes().to_vec(),
        Body::Map(map)
            if message
                .properties
                .get("transformation")
                .and_then(Value::as_str)
                == Some("jms-map-json") =>
        {
            let json = map
                .iter()
                .map(|(key, value)| (key.clone(), json_value(value)))
                .collect::<serde_json::Map<_, _>>();
            serde_json::to_vec(&serde_json::Value::Object(json)).unwrap_or_default()
        }
        other => other.to_bytes(),
    };
    if mode == AckMode::Auto {
        core.ack(
            conn,
            delivery.consumer,
            Acks::Deliveries(vec![delivery.delivery_id]),
            AckOp::Consumed,
            None,
        )
        .map_err(|error| io::Error::other(error.message().to_owned()))?;
    } else {
        let reference = DeliveryRef {
            consumer: delivery.consumer,
            core_id: delivery.delivery_id,
        };
        deliveries.insert(ack_token, reference);
        deliveries.insert(format!("{subscription}\0{}", message.message_id), reference);
    }
    write_frame(writer, "MESSAGE", &headers, &body, version).await?;
    Ok(())
}

fn json_value(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(v) => (*v).into(),
        Value::Byte(v) => (*v).into(),
        Value::Short(v) => (*v).into(),
        Value::Int(v) => (*v).into(),
        Value::Long(v) => (*v).into(),
        Value::Float(v) => serde_json::Number::from_f64(f64::from(*v))
            .map(Into::into)
            .unwrap_or(serde_json::Value::Null),
        Value::Double(v) => serde_json::Number::from_f64(*v)
            .map(Into::into)
            .unwrap_or(serde_json::Value::Null),
        Value::String(v) => v.clone().into(),
        Value::Char(v) => char::from_u32(u32::from(*v))
            .unwrap_or_default()
            .to_string()
            .into(),
        Value::Bytes(v) => String::from_utf8_lossy(v).into_owned().into(),
        Value::Map(v) => {
            serde_json::Value::Object(v.iter().map(|(k, v)| (k.clone(), json_value(v))).collect())
        }
        Value::List(v) => serde_json::Value::Array(v.iter().map(json_value).collect()),
    }
}
