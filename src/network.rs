//! OpenWire network connectors.
//!
//! Static destinations are always subscribed locally; dynamic destinations
//! are subscribed only while the remote broker reports demand.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::config::NetworkConnectorConfig;
use crate::core::{
    AckOp, Acks, Body, BrokerCore, ConsumerOptions, Destination, Event, Message, Value,
};
use crate::protocols::openwire::{OpenWireClient, OpenWireFrame};

const DEFAULT_MAX_FRAME_SIZE: usize = 100 * 1024 * 1024;
const CONSUMER_ADVISORY: &str = "ActiveMQ.Advisory.Consumer.>";

type AdvisoryAck = Box<
    dyn for<'a> Fn(
            &'a mut OpenWireClient,
            &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>
        + Send
        + Sync,
>;

pub fn start(
    core: Arc<BrokerCore>,
    connectors: Vec<NetworkConnectorConfig>,
) -> tokio::task::JoinSet<()> {
    let mut tasks = tokio::task::JoinSet::new();
    for connector in connectors {
        if connector.duplex {
            tracing::error!(
                name = %connector.name,
                "duplex network connectors are unsupported until OpenWire network-consumer metadata can be sent"
            );
            continue;
        }
        if connector.uris.is_empty() {
            tracing::error!(name = %connector.name, "network connector has no remote URIs");
            continue;
        }
        if connector.network_ttl == 0 {
            tracing::error!(name = %connector.name, "network_ttl must be greater than zero");
            continue;
        }
        let invalid_patterns = connector
            .dynamically_included_destinations
            .iter()
            .chain(&connector.excluded_destinations)
            .filter(|pattern| Destination::parse_uri(pattern).is_none())
            .collect::<Vec<_>>();
        if !invalid_patterns.is_empty() {
            for pattern in invalid_patterns {
                tracing::error!(
                    name = %connector.name,
                    pattern,
                    "invalid network destination pattern; expected queue:// or topic:// URI"
                );
            }
            continue;
        }
        let static_temporary_destinations = connector
            .static_included_destinations
            .iter()
            .filter_map(|pattern| Destination::parse_uri(pattern))
            .filter(Destination::is_temporary)
            .collect::<Vec<_>>();
        if !static_temporary_destinations.is_empty() {
            tracing::error!(
                name = %connector.name,
                destinations = ?static_temporary_destinations,
                "static temporary destinations are unsupported; temporary forwarding requires live remote demand"
            );
            continue;
        }
        let advisory_probe = Destination::topic("ActiveMQ.Advisory.Consumer.Queue.network-probe");
        let advisory_patterns = connector
            .static_included_destinations
            .iter()
            .chain(&connector.dynamically_included_destinations)
            .filter_map(|pattern| Destination::parse_uri(pattern))
            .filter(|pattern| pattern.matches(&advisory_probe))
            .collect::<Vec<_>>();
        if !advisory_patterns.is_empty() {
            tracing::error!(
                name = %connector.name,
                patterns = ?advisory_patterns,
                "advisory forwarding is unsupported until OpenWire network messages encode advisory DataStructures"
            );
            continue;
        }
        let core = Arc::clone(&core);
        tasks.spawn(async move {
            connector_loop(core, connector).await;
        });
    }
    tasks
}

struct BridgeConnection {
    core: Arc<BrokerCore>,
    connection: u64,
}

impl Drop for BridgeConnection {
    fn drop(&mut self) {
        self.core.disconnect(self.connection);
    }
}

fn decode_modified_utf8(bytes: &[u8]) -> Result<String, String> {
    let mut units = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        let first = bytes[index];
        if first & 0x80 == 0 {
            units.push(u16::from(first));
            index += 1;
        } else if first & 0xe0 == 0xc0 {
            if index + 1 >= bytes.len() {
                return Err("truncated modified UTF-8 string".into());
            }
            units.push((u16::from(first & 0x1f) << 6) | u16::from(bytes[index + 1] & 0x3f));
            index += 2;
        } else if first & 0xf0 == 0xe0 {
            if index + 2 >= bytes.len() {
                return Err("truncated modified UTF-8 string".into());
            }
            units.push(
                (u16::from(first & 0x0f) << 12)
                    | (u16::from(bytes[index + 1] & 0x3f) << 6)
                    | u16::from(bytes[index + 2] & 0x3f),
            );
            index += 3;
        } else {
            return Err("invalid modified UTF-8 string".into());
        }
    }
    String::from_utf16(&units).map_err(|_| "invalid modified UTF-8 surrogate sequence".into())
}

async fn connector_loop(core: Arc<BrokerCore>, config: NetworkConnectorConfig) {
    let delay = Duration::from_millis(config.reconnect_delay_ms.max(100));
    loop {
        match connect_and_run(Arc::clone(&core), &config).await {
            Ok(()) => tracing::warn!(name = %config.name, "network connector stopped"),
            Err(error) => {
                tracing::warn!(name = %config.name, %error, "network connector disconnected")
            }
        }
        tokio::time::sleep(delay).await;
    }
}

async fn connect_and_run(
    core: Arc<BrokerCore>,
    config: &NetworkConnectorConfig,
) -> Result<(), String> {
    let (address, mut advisory, acknowledge_advisory) = connect_remote(config).await?;

    let broker_info = core.info();
    let local_broker_id = broker_info.id;
    let (connection, mut events) = core.connect_internal(&format!("network:{}", config.name));
    let _connection_guard = BridgeConnection {
        core: Arc::clone(&core),
        connection,
    };
    let mut local_consumers = BTreeMap::<Destination, Vec<u64>>::new();
    let mut remote_demands = HashMap::<String, (Destination, Option<u64>)>::new();
    let mut temp_proxies = HashSet::<Destination>::new();
    let mut observer_options = ConsumerOptions::new(Destination::topic(CONSUMER_ADVISORY));
    observer_options.prefetch = 256;
    let observer = core
        .subscribe(connection, observer_options)
        .map_err(|error| error.to_string())?;

    for item in &config.static_included_destinations {
        match parse_destination(item) {
            Ok(destination) if allowed(config, &destination) => {
                if let Err(error) = subscribe_local(
                    &core,
                    connection,
                    config,
                    &local_broker_id,
                    &destination,
                    &mut local_consumers,
                    false,
                ) {
                    core.disconnect(connection);
                    return Err(error);
                }
            }
            Ok(_) => tracing::info!(
                name = %config.name,
                destination = item,
                "static network destination excluded by connector policy"
            ),
            Err(error) => tracing::error!(
                name = %config.name,
                destination = item,
                %error,
                "invalid static network destination"
            ),
        }
    }

    tracing::info!(
        name = %config.name,
        remote = %address,
        "OpenWire network connector connected"
    );

    let result = loop {
        tokio::select! {
            frame = advisory.receive() => {
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(error) => break Err(error.to_string()),
                };
                if frame.command_type == 31 {
                    break Err("remote broker returned an advisory connection exception".into());
                }
                if frame.command_type != 21 {
                    continue;
                }
                match decode_consumer_advisory(&frame) {
                    Ok(Some(AdvisoryAction::Added {
                        external_id,
                        destination,
                        message_id,
                        network,
                        broker_path,
                    })) => {
                        if let Err(error) = acknowledge_advisory(&mut advisory, &message_id).await {
                            break Err(error);
                        }
                        if network || broker_path.contains(&local_broker_id) {
                            continue;
                        }
                        if !destination.is_advisory()
                            && allowed(config, &destination)
                            && dynamically_included(config, &destination)
                        {
                            if destination.is_temporary()
                                && let Err(error) = ensure_temp_proxy(
                                    &core,
                                    connection,
                                    &destination,
                                    &mut temp_proxies,
                                )
                            {
                                tracing::warn!(name = %config.name, %destination, %error, "refusing unsafe temporary destination proxy");
                                break Err(format!(
                                    "temporary destination proxy conflict for {destination}: {error}"
                                ));
                            }
                            let local_consumer = if config.conduit_subscriptions {
                                None
                            } else if remote_demands.contains_key(&external_id) {
                                continue;
                            } else {
                                match subscribe_local(
                                    &core,
                                    connection,
                                    config,
                                    &local_broker_id,
                                    &destination,
                                    &mut local_consumers,
                                    true,
                                ) {
                                    Ok(consumer) => Some(consumer),
                                    Err(error) => break Err(error),
                                }
                            };
                            remote_demands.insert(external_id, (destination.clone(), local_consumer));
                            if config.conduit_subscriptions && let Err(error) = subscribe_local(
                                &core,
                                connection,
                                config,
                                &local_broker_id,
                                &destination,
                                &mut local_consumers,
                                false,
                            ) {
                                break Err(error);
                            }
                        }
                    }
                    Ok(Some(AdvisoryAction::Removed { external_id, message_id })) => {
                        if let Err(error) = acknowledge_advisory(&mut advisory, &message_id).await {
                            break Err(error);
                        }
                        if let Some((destination, consumer)) = remote_demands.remove(&external_id) {
                            if let Some(consumer) = consumer {
                                close_local_consumer(&core, &destination, consumer, &mut local_consumers);
                            }
                            let still_demanded = remote_demands.values().any(|(d, _)| d == &destination);
                            if config.conduit_subscriptions && !still_demanded
                                && !config.static_included_destinations.iter().filter_map(|s| Destination::parse_uri(s)).any(|d| d == destination)
                            {
                                close_local(&core, destination.clone(), &mut local_consumers);
                            }
                            maybe_delete_temp_proxy(&core, connection, &destination, &remote_demands, &mut temp_proxies);
                        }
                    }
                    Ok(None) => {
                        if let Ok(dispatch) = parse_dispatch_prefix(&frame)
                            && let Err(error) = acknowledge_advisory(&mut advisory, &dispatch.message_id).await
                        {
                            break Err(error);
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            name = %config.name,
                            %error,
                            "could not decode remote consumer advisory"
                        );
                        if let Ok(dispatch) = parse_dispatch_prefix(&frame)
                            && let Err(error) = acknowledge_advisory(&mut advisory, &dispatch.message_id).await
                        {
                            break Err(error);
                        }
                    }
                }
            }
            event = events.recv() => {
                match event {
                    Some(Event::Deliver(delivery)) => {
                        let mut message = (*delivery.message).clone();
                        if delivery.consumer == observer {
                            if let Some(crate::core::AdvisoryData::ConsumerRemoved {
                                destination,
                                ..
                            }) = message.advisory.as_ref()
                            {
                                maybe_delete_temp_proxy(
                                    &core,
                                    connection,
                                    destination,
                                    &remote_demands,
                                    &mut temp_proxies,
                                );
                            }
                            if let Err(error) = core.ack(
                                connection,
                                delivery.consumer,
                                Acks::Deliveries(vec![delivery.delivery_id]),
                                AckOp::Consumed,
                                None,
                            ) {
                                break Err(error.to_string());
                            }
                            continue;
                        }
                        if message.advisory.is_some() {
                            break Err(format!(
                                "cannot forward advisory {}: OpenWire message encoding has no advisory DataStructure encoder",
                                message.destination
                            ));
                        }
                        if message.broker_path.contains(&local_broker_id) {
                            if let Err(error) = core.ack(connection, delivery.consumer, Acks::Deliveries(vec![delivery.delivery_id]), AckOp::Consumed, None) {
                                break Err(error.to_string());
                            }
                            continue;
                        }
                        message.broker_path.push(local_broker_id.clone());
                        match send_and_confirm(&address, config, &message).await {
                            Ok(()) => {
                                if let Err(error) = core.ack(connection, delivery.consumer, Acks::Deliveries(vec![delivery.delivery_id]), AckOp::Consumed, None) {
                                    break Err(error.to_string());
                                }
                            }
                            Err(error) => break Err(format!("remote send failed: {error}")),
                        }
                    }
                    Some(Event::ConnectionClosed { reason }) => break Err(reason),
                    Some(Event::ConsumerClosed { consumer, .. }) => {
                        local_consumers.retain(|_, ids| {
                            ids.retain(|id| *id != consumer);
                            !ids.is_empty()
                        });
                    }
                    Some(Event::BrowseDone { .. } | Event::PullEmpty { .. } | Event::Drained { .. }) => {}
                    None => break Err("local broker connection closed".into()),
                }
            }
        }
    };

    core.disconnect(connection);
    result
}

#[derive(Clone)]
struct ProducerId {
    connection: String,
    session: i64,
    value: i64,
}

async fn send_and_confirm(
    address: &str,
    config: &NetworkConnectorConfig,
    message: &Message,
) -> Result<(), String> {
    let mut client = OpenWireClient::connect_client(
        address,
        DEFAULT_MAX_FRAME_SIZE,
        None,
        config.username.as_deref(),
        config.password.as_deref(),
    )
    .await
    .map_err(|error| error.to_string())?;
    let producer = client
        .create_producer(None)
        .await
        .map_err(|error| error.to_string())?;
    let producer = ProducerId {
        connection: producer.connection.clone(),
        session: producer.session,
        value: producer.value,
    };
    let command_id = client.next_command_id();
    let frame = encode_message(command_id, &producer, message)?;
    client
        .send_command(&frame)
        .await
        .map_err(|error| error.to_string())?;
    loop {
        let response = tokio::time::timeout(Duration::from_secs(30), client.receive())
            .await
            .map_err(|_| "timed out waiting for remote send response".to_owned())?
            .map_err(|error| error.to_string())?;
        match response.command_type {
            30 if response.payload.get(5..9) == Some(command_id.to_be_bytes().as_slice()) => {
                return Ok(());
            }
            31 if response.payload.get(5..9) == Some(command_id.to_be_bytes().as_slice()) => {
                return Err(decode_remote_exception(&response.payload));
            }
            _ => tracing::debug!(
                command_type = response.command_type,
                "ignoring unrelated OpenWire response on bridge sender"
            ),
        }
    }
}

fn encode_message(
    command_id: i32,
    producer: &ProducerId,
    message: &Message,
) -> Result<OpenWireFrame, String> {
    let mut payload = command_id.to_be_bytes().to_vec();
    payload.push(1);
    append_id(&mut payload, 123, producer)?;
    append_destination(&mut payload, &message.destination)?;
    payload.push(0); // transaction
    append_optional_destination(&mut payload, message.original_destination.as_ref())?;
    payload.push(1);
    payload.push(110); // MessageId
    append_string(&mut payload, Some(&message.message_id))?;
    append_id(&mut payload, 123, producer)?;
    payload.extend_from_slice(&0i64.to_be_bytes());
    payload.extend_from_slice(&0i64.to_be_bytes());
    payload.push(0);
    append_string(&mut payload, message.group_id.as_deref())?;
    payload.extend_from_slice(&message.group_sequence.to_be_bytes());
    append_string(&mut payload, message.correlation_id.as_deref())?;
    payload.push(u8::from(message.persistent));
    payload.extend_from_slice(&message.expiration.to_be_bytes());
    payload.push(message.priority);
    append_optional_destination(&mut payload, message.reply_to.as_ref())?;
    payload.extend_from_slice(&message.timestamp.to_be_bytes());
    append_string(&mut payload, message.message_type.as_deref())?;

    let body = encode_body(&message.body)?;
    write_bytes(&mut payload, Some(&body))?;
    let properties = encode_properties(&message.properties, 0)?;
    write_bytes(
        &mut payload,
        (!message.properties.is_empty()).then_some(properties.as_slice()),
    )?;
    payload.extend_from_slice(&[0, 0, 0]); // advisory, compression, and optional property flags
    payload.extend_from_slice(&0i32.to_be_bytes());
    if message.broker_path.is_empty() {
        payload.push(0);
    } else {
        payload.push(1);
        let count = u16::try_from(message.broker_path.len())
            .map_err(|_| "broker path exceeds OpenWire limit")?;
        payload.extend_from_slice(&count.to_be_bytes());
        for broker in &message.broker_path {
            append_nested_string(&mut payload, 124, Some(broker))?;
        }
    }
    payload.extend_from_slice(&0i64.to_be_bytes());
    append_string(&mut payload, message.user_id.as_deref())?;
    payload.extend_from_slice(&[0, 0, 0]);
    payload.extend_from_slice(&0i64.to_be_bytes());
    payload.extend_from_slice(&message.broker_in_time.to_be_bytes());
    payload.push(0);
    let command_type = match message.body {
        Body::Empty => 23,
        Body::Bytes(_) => 24,
        Body::Map(_) => 25,
        Body::Object(_) => 26,
        Body::Stream(_) => 27,
        Body::Text(_) => 28,
    };
    Ok(OpenWireFrame {
        command_type,
        payload,
    })
}

fn append_id(out: &mut Vec<u8>, kind: u8, id: &ProducerId) -> Result<(), String> {
    out.extend_from_slice(&[1, kind]);
    append_string(out, Some(&id.connection))?;
    if kind == 123 {
        out.extend_from_slice(&id.value.to_be_bytes());
        out.extend_from_slice(&id.session.to_be_bytes());
    }
    Ok(())
}

fn append_destination(out: &mut Vec<u8>, destination: &Destination) -> Result<(), String> {
    let kind = match destination.kind {
        crate::core::DestinationKind::Queue => 100,
        crate::core::DestinationKind::Topic => 101,
        crate::core::DestinationKind::TempQueue => 102,
        crate::core::DestinationKind::TempTopic => 103,
    };
    out.extend_from_slice(&[1, kind]);
    append_string(out, Some(&destination.name))
}

fn append_optional_destination(
    out: &mut Vec<u8>,
    destination: Option<&Destination>,
) -> Result<(), String> {
    if let Some(destination) = destination {
        append_destination(out, destination)
    } else {
        out.push(0);
        Ok(())
    }
}

fn append_nested_string(out: &mut Vec<u8>, kind: u8, value: Option<&str>) -> Result<(), String> {
    out.extend_from_slice(&[1, kind]);
    append_string(out, value)
}

fn append_string(out: &mut Vec<u8>, value: Option<&str>) -> Result<(), String> {
    if let Some(value) = value {
        out.push(1);
        append_java_utf(out, value)
    } else {
        out.push(0);
        Ok(())
    }
}

fn append_java_utf(out: &mut Vec<u8>, value: &str) -> Result<(), String> {
    let bytes = encode_modified_utf8(value);
    let len = u16::try_from(bytes.len()).map_err(|_| "OpenWire string exceeds 65535 bytes")?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&bytes);
    Ok(())
}

fn encode_modified_utf8(value: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    for unit in value.encode_utf16() {
        match unit {
            1..=0x7f => bytes.push(unit as u8),
            0..=0x7ff => {
                bytes.push((0xc0 | ((unit >> 6) & 0x1f)) as u8);
                bytes.push((0x80 | (unit & 0x3f)) as u8);
            }
            _ => {
                bytes.push((0xe0 | ((unit >> 12) & 0x0f)) as u8);
                bytes.push((0x80 | ((unit >> 6) & 0x3f)) as u8);
                bytes.push((0x80 | (unit & 0x3f)) as u8);
            }
        }
    }
    bytes
}

fn write_bytes(out: &mut Vec<u8>, bytes: Option<&[u8]>) -> Result<(), String> {
    if let Some(bytes) = bytes {
        let len = i32::try_from(bytes.len()).map_err(|_| "OpenWire byte sequence too large")?;
        out.push(1);
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(bytes);
    } else {
        out.push(0);
    }
    Ok(())
}

fn encode_body(body: &Body) -> Result<Vec<u8>, String> {
    match body {
        Body::Empty => Ok(Vec::new()),
        Body::Text(text) => {
            let bytes = encode_modified_utf8(text);
            let len = i32::try_from(bytes.len()).map_err(|_| "text body too large")?;
            let mut out = len.to_be_bytes().to_vec();
            out.extend_from_slice(&bytes);
            Ok(out)
        }
        Body::Bytes(bytes) | Body::Object(bytes) => Ok(bytes.clone()),
        Body::Map(map) => encode_properties(map, 0),
        Body::Stream(values) => {
            let mut out = Vec::new();
            for value in values {
                encode_value(&mut out, value, 0)?;
            }
            Ok(out)
        }
    }
}

fn encode_properties(
    properties: &std::collections::BTreeMap<String, Value>,
    depth: usize,
) -> Result<Vec<u8>, String> {
    if depth > 16 {
        return Err("OpenWire property nesting exceeds limit".into());
    }
    let count = i32::try_from(properties.len()).map_err(|_| "too many OpenWire properties")?;
    let mut out = count.to_be_bytes().to_vec();
    for (key, value) in properties {
        append_java_utf(&mut out, key)?;
        encode_value(&mut out, value, depth + 1)?;
    }
    Ok(out)
}

fn encode_value(out: &mut Vec<u8>, value: &Value, depth: usize) -> Result<(), String> {
    if depth > 16 {
        return Err("OpenWire property nesting exceeds limit".into());
    }
    match value {
        Value::Null => out.push(0),
        Value::Bool(value) => out.extend_from_slice(&[1, u8::from(*value)]),
        Value::Byte(value) => out.extend_from_slice(&[2, *value as u8]),
        Value::Char(value) => {
            out.push(3);
            out.extend_from_slice(&value.to_be_bytes());
        }
        Value::Short(value) => {
            out.push(4);
            out.extend_from_slice(&value.to_be_bytes());
        }
        Value::Int(value) => {
            out.push(5);
            out.extend_from_slice(&value.to_be_bytes());
        }
        Value::Long(value) => {
            out.push(6);
            out.extend_from_slice(&value.to_be_bytes());
        }
        Value::Double(value) => {
            out.push(7);
            out.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        Value::Float(value) => {
            out.push(8);
            out.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        Value::String(value) => {
            out.push(9);
            append_java_utf(out, value)?;
        }
        Value::Bytes(value) => {
            out.push(10);
            let len = i32::try_from(value.len()).map_err(|_| "OpenWire byte property too large")?;
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(value);
        }
        Value::Map(value) => {
            out.push(11);
            out.extend_from_slice(&encode_properties(value, depth + 1)?);
        }
        Value::List(values) => {
            out.push(12);
            let count = i32::try_from(values.len()).map_err(|_| "OpenWire list too large")?;
            out.extend_from_slice(&count.to_be_bytes());
            for value in values {
                encode_value(out, value, depth + 1)?;
            }
        }
    }
    Ok(())
}

fn decode_remote_exception(payload: &[u8]) -> String {
    let mut reader = WireReader::new(payload);
    if reader.skip(9).is_err() || !reader.bool().unwrap_or(false) {
        return "remote broker rejected the message".into();
    }
    let class = reader.string_option().ok().flatten().unwrap_or_default();
    let message = reader.string_option().ok().flatten().unwrap_or_default();
    format!("{class}: {message}")
}

async fn connect_remote(
    config: &NetworkConnectorConfig,
) -> Result<(String, OpenWireClient, AdvisoryAck), String> {
    let mut failures = Vec::new();
    for uri in &config.uris {
        let Some(address) = uri.strip_prefix("tcp://") else {
            failures.push(format!("{uri}: only tcp:// OpenWire URIs are supported"));
            continue;
        };
        let mut advisory = match OpenWireClient::connect_client(
            address,
            DEFAULT_MAX_FRAME_SIZE,
            None,
            config.username.as_deref(),
            config.password.as_deref(),
        )
        .await
        {
            Ok(client) => client,
            Err(error) => {
                failures.push(format!("{uri}: {error}"));
                continue;
            }
        };
        let consumer = match advisory
            .create_consumer(&Destination::topic(CONSUMER_ADVISORY), 256, None)
            .await
        {
            Ok(consumer) => consumer,
            Err(error) => {
                failures.push(format!("{uri}: {error}"));
                continue;
            }
        };
        let acknowledge: AdvisoryAck = Box::new(move |client, message_id| {
            let consumer = consumer.clone();
            Box::pin(async move {
                client
                    .acknowledge(&consumer, message_id)
                    .await
                    .map_err(|error| error.to_string())
            })
        });
        return Ok((address.to_owned(), advisory, acknowledge));
    }
    Err(format!(
        "no configured remote broker could be reached: {}",
        failures.join("; ")
    ))
}

fn subscribe_local(
    core: &BrokerCore,
    connection: u64,
    config: &NetworkConnectorConfig,
    local_broker_id: &str,
    destination: &Destination,
    consumers: &mut BTreeMap<Destination, Vec<u64>>,
    force_new: bool,
) -> Result<u64, String> {
    if !force_new && let Some(consumer) = consumers.get(destination).and_then(|ids| ids.first()) {
        return Ok(*consumer);
    }
    let mut options = ConsumerOptions::new(destination.clone());
    options.prefetch = config.prefetch.max(1);
    options.network = true;
    options.network_ttl = Some(config.network_ttl);
    options.broker_path = vec![local_broker_id.to_owned()];
    options.external_id = Some(format!("network:{}", config.name));
    let consumer = core
        .subscribe(connection, options)
        .map_err(|error| error.to_string())?;
    consumers
        .entry(destination.clone())
        .or_default()
        .push(consumer);
    Ok(consumer)
}

fn close_local(
    core: &BrokerCore,
    destination: Destination,
    consumers: &mut BTreeMap<Destination, Vec<u64>>,
) {
    if let Some(ids) = consumers.remove(&destination) {
        for consumer in ids {
            core.close_consumer(consumer);
        }
    }
}

fn close_local_consumer(
    core: &BrokerCore,
    destination: &Destination,
    consumer: u64,
    consumers: &mut BTreeMap<Destination, Vec<u64>>,
) {
    core.close_consumer(consumer);
    if let Some(ids) = consumers.get_mut(destination) {
        ids.retain(|id| *id != consumer);
        if ids.is_empty() {
            consumers.remove(destination);
        }
    }
}

fn ensure_temp_proxy(
    core: &BrokerCore,
    connection: u64,
    destination: &Destination,
    proxies: &mut HashSet<Destination>,
) -> Result<(), String> {
    if proxies.contains(destination) {
        return Ok(());
    }
    if core
        .destinations()
        .iter()
        .any(|existing| existing.destination == *destination)
    {
        return Err(format!(
            "temporary destination {destination} already exists locally and is not owned by this connector"
        ));
    }
    core.create_temp(connection, destination.clone())
        .map_err(|error| error.to_string())?;
    proxies.insert(destination.clone());
    Ok(())
}

fn maybe_delete_temp_proxy(
    core: &BrokerCore,
    connection: u64,
    destination: &Destination,
    remote_demands: &HashMap<String, (Destination, Option<u64>)>,
    proxies: &mut HashSet<Destination>,
) {
    if destination.is_temporary()
        && proxies.contains(destination)
        && !remote_demands.values().any(|(d, _)| d == destination)
    {
        match core.delete_temp(connection, destination) {
            Ok(()) => {
                proxies.remove(destination);
            }
            Err(error) => {
                tracing::warn!(%destination, %error, "temporary destination proxy cleanup deferred until disconnect")
            }
        }
    }
}

fn parse_destination(value: &str) -> Result<Destination, String> {
    Destination::parse_uri(value)
        .ok_or_else(|| format!("expected queue:// or topic:// destination URI, got {value:?}"))
}

fn allowed(config: &NetworkConnectorConfig, destination: &Destination) -> bool {
    !config.excluded_destinations.iter().any(|pattern| {
        Destination::parse_uri(pattern).is_some_and(|pattern| pattern.matches(destination))
    })
}

fn dynamically_included(config: &NetworkConnectorConfig, destination: &Destination) -> bool {
    config.dynamically_included_destinations.is_empty()
        || config
            .dynamically_included_destinations
            .iter()
            .any(|pattern| {
                Destination::parse_uri(pattern).is_some_and(|pattern| pattern.matches(destination))
            })
}

enum AdvisoryAction {
    Added {
        external_id: String,
        destination: Destination,
        message_id: String,
        network: bool,
        broker_path: Vec<String>,
    },
    Removed {
        external_id: String,
        message_id: String,
    },
}

struct DecodedAdvisoryData {
    kind: u8,
    destination: Destination,
    external_id: String,
    network: bool,
    broker_path: Vec<String>,
}

struct DispatchPrefix {
    message_id: String,
}

fn decode_consumer_advisory(frame: &OpenWireFrame) -> Result<Option<AdvisoryAction>, String> {
    let dispatch = parse_dispatch_prefix(frame)?;
    let Some(data) = parse_advisory_data(frame)? else {
        return Ok(None);
    };
    match data.kind {
        5 => Ok(Some(AdvisoryAction::Added {
            external_id: data.external_id,
            destination: data.destination,
            message_id: dispatch.message_id,
            network: data.network,
            broker_path: data.broker_path,
        })),
        12 => Ok(Some(AdvisoryAction::Removed {
            external_id: data.external_id,
            message_id: dispatch.message_id,
        })),
        _ => Ok(None),
    }
}

fn parse_dispatch_prefix(frame: &OpenWireFrame) -> Result<DispatchPrefix, String> {
    let mut reader = WireReader::new(&frame.payload);
    reader.skip(5)?;
    reader.nested_id(122)?;
    reader.nested_destination()?;
    if !reader.bool()? {
        return Err("dispatch has no message".into());
    }
    let _message_type = reader.byte()?;
    reader.skip(5)?; // Message command header
    reader.nested_id(123)?;
    reader.nested_destination_option()?;
    reader.nested_any()?;
    reader.nested_destination_option()?;
    let message_id = reader.nested_message_id()?;
    Ok(DispatchPrefix { message_id })
}

fn parse_advisory_data(frame: &OpenWireFrame) -> Result<Option<DecodedAdvisoryData>, String> {
    let mut reader = WireReader::new(&frame.payload);
    reader.skip(5)?;
    reader.nested_id(122)?;
    let advisory_topic = reader.nested_destination()?;
    if !reader.bool()? {
        return Ok(None);
    }
    reader.byte()?;
    reader.skip(5)?;
    reader.nested_id(123)?;
    reader.nested_destination_option()?;
    reader.nested_any()?;
    reader.nested_destination_option()?;
    reader.nested_message_id()?;
    reader.nested_any()?;
    reader.string_option()?;
    reader.skip(4)?;
    reader.string_option()?;
    reader.bool()?;
    reader.skip(8 + 1)?;
    reader.nested_destination_option()?;
    reader.skip(8)?;
    reader.string_option()?;
    reader.byte_sequence()?;
    reader.byte_sequence()?;
    if !reader.bool()? {
        return Ok(None);
    }
    let structure_type = reader.byte()?;
    if structure_type == 5 {
        reader.skip(5)?; // nested ConsumerInfo header
        let id = reader.nested_id(122)?;
        reader.bool()?;
        let destination = reader.nested_destination()?;
        reader.skip(8)?;
        reader.bool()?;
        reader.string_option()?;
        reader.string_option()?;
        reader.string_option()?;
        reader.skip(4)?;
        let broker_path = reader.string_array()?;
        reader.nested_any()?;
        let network = reader.bool()?;
        Ok(Some(DecodedAdvisoryData {
            kind: structure_type,
            destination,
            external_id: id.display(),
            network,
            broker_path,
        }))
    } else if structure_type == 12 {
        reader.skip(5)?;
        let id = reader.nested_id(122)?;
        Ok(Some(DecodedAdvisoryData {
            kind: structure_type,
            destination: advisory_topic,
            external_id: id.display(),
            network: false,
            broker_path: Vec::new(),
        }))
    } else {
        Ok(None)
    }
}

struct WireReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

struct WireIdentifier {
    connection: String,
    session: i64,
    value: i64,
}

impl WireIdentifier {
    fn display(&self) -> String {
        format!("{}:{}:{}", self.connection, self.session, self.value)
    }
}

impl<'a> WireReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn skip(&mut self, n: usize) -> Result<(), String> {
        self.take(n).map(|_| ())
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self.offset.checked_add(n).ok_or("wire offset overflow")?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or("truncated advisory frame")?;
        self.offset = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    fn bool(&mut self) -> Result<bool, String> {
        Ok(self.byte()? != 0)
    }

    fn string_option(&mut self) -> Result<Option<String>, String> {
        if !self.bool()? {
            return Ok(None);
        }
        let n = usize::from(u16::from_be_bytes(self.take(2)?.try_into().unwrap()));
        Ok(Some(decode_modified_utf8(self.take(n)?)?))
    }

    fn nested_any(&mut self) -> Result<(), String> {
        if !self.bool()? {
            return Ok(());
        }
        let kind = self.byte()?;
        match kind {
            120..=123 => {
                self.read_id_payload()?;
            }
            124 => {
                self.string_option()?;
            }
            110 => {
                self.string_option()?;
                self.nested_id(123)?;
                self.skip(16)?;
            }
            _ => return Err(format!("unsupported nested OpenWire type {kind}")),
        }
        Ok(())
    }

    fn nested_id(&mut self, expected: u8) -> Result<WireIdentifier, String> {
        if !self.bool()? {
            return Err("missing OpenWire identifier".into());
        }
        let kind = self.byte()?;
        if kind != expected {
            return Err(format!("expected OpenWire id {expected}, found {kind}"));
        }
        self.read_id_payload()
    }

    fn read_id_payload(&mut self) -> Result<WireIdentifier, String> {
        let connection = self.string_option()?.ok_or("null OpenWire id")?;
        let session = i64::from_be_bytes(self.take(8)?.try_into().unwrap());
        let value = i64::from_be_bytes(self.take(8)?.try_into().unwrap());
        Ok(WireIdentifier {
            connection,
            session,
            value,
        })
    }

    fn nested_destination(&mut self) -> Result<Destination, String> {
        let Some(destination) = self.nested_destination_option()? else {
            return Err("missing OpenWire destination".into());
        };
        Ok(destination)
    }

    fn nested_destination_option(&mut self) -> Result<Option<Destination>, String> {
        if !self.bool()? {
            return Ok(None);
        }
        let kind = self.byte()?;
        let name = self.string_option()?.ok_or("null destination name")?;
        let destination = match kind {
            100 => Destination::queue(name),
            101 => Destination::topic(name),
            102 => Destination::temp_queue(name),
            103 => Destination::temp_topic(name),
            _ => return Err(format!("unknown OpenWire destination type {kind}")),
        };
        Ok(Some(destination))
    }

    fn nested_message_id(&mut self) -> Result<String, String> {
        if !self.bool()? {
            return Ok(String::new());
        }
        let kind = self.byte()?;
        if kind != 110 {
            return Err(format!("expected MessageId, found {kind}"));
        }
        let id = self.string_option()?.unwrap_or_default();
        self.nested_id(123)?;
        self.skip(16)?;
        Ok(id)
    }

    fn string_array(&mut self) -> Result<Vec<String>, String> {
        if !self.bool()? {
            return Ok(Vec::new());
        }
        let count = usize::from(u16::from_be_bytes(self.take(2)?.try_into().unwrap()));
        if count > 1024 {
            return Err("OpenWire broker path is too large".into());
        }
        (0..count)
            .map(|_| {
                if !self.bool()? {
                    return Err("null broker path entry".into());
                }
                let kind = self.byte()?;
                if kind != 124 {
                    return Err(format!("expected broker id string, found {kind}"));
                }
                self.string_option()?
                    .ok_or_else(|| "null broker path entry".into())
            })
            .collect()
    }

    fn byte_sequence(&mut self) -> Result<(), String> {
        if self.bool()? {
            let n = i32::from_be_bytes(self.take(4)?.try_into().unwrap());
            if n < 0 {
                return Err("negative OpenWire byte sequence length".into());
            }
            self.skip(n as usize)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn destination_filters_honor_wildcards_and_exclusions() {
        let config = NetworkConnectorConfig {
            dynamically_included_destinations: vec!["queue://orders.>".into()],
            excluded_destinations: vec!["queue://orders.private.>".into()],
            ..NetworkConnectorConfig::default()
        };
        assert!(dynamically_included(
            &config,
            &Destination::queue("orders.eu")
        ));
        assert!(!dynamically_included(
            &config,
            &Destination::topic("orders.eu")
        ));
        assert!(!allowed(
            &config,
            &Destination::queue("orders.private.internal")
        ));
    }

    #[test]
    fn static_destination_filter_uses_uri_schemes() {
        assert_eq!(
            parse_destination("topic://news"),
            Ok(Destination::topic("news"))
        );
        assert!(parse_destination("news").is_err());
    }
}
