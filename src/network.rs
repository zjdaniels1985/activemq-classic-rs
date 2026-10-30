//! OpenWire network connectors.
//!
//! Static destinations are always subscribed locally; dynamic destinations
//! are subscribed only while the remote broker reports demand. Duplex
//! connectors additionally mirror local consumers as OpenWire network
//! consumers on the remote broker. Forwarding is at-least-once: a message is
//! acknowledged only after the receiving broker accepted it, so a crash in
//! between can produce a duplicate.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, oneshot};

use crate::config::NetworkConnectorConfig;
use crate::core::{
    AckOp, Acks, AdvisoryData, BrokerCore, ConsumerOptions, Destination, Event, Message,
};
use crate::protocols::openwire::{self, OpenWireClient, OpenWireFrame};

const DEFAULT_MAX_FRAME_SIZE: usize = 100 * 1024 * 1024;
const CONSUMER_ADVISORY: &str = "ActiveMQ.Advisory.Consumer.>";
/// Long idle waits end with a reconnect because a timed-out OpenWire read
/// cannot be resumed safely.
const DUPLEX_IDLE_RECONNECT: Duration = Duration::from_secs(24 * 60 * 60);

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
        if let Err(error) = validate_advisory_patterns(&connector) {
            tracing::error!(name = %connector.name, %error, "invalid advisory forwarding configuration");
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
    let (address, advisory, acknowledge_advisory) = connect_remote(config).await?;
    let remote_broker_id = advisory.remote_broker_id().map(str::to_owned);
    // The OpenWire client read path is not cancellation safe, so every remote
    // reader runs in its own task and is only ever stopped by dropping its
    // connection.
    let (advisory_tx, mut remote_advisories) = mpsc::channel(256);
    let _advisory_reader = AbortOnDrop(tokio::spawn(read_remote_advisories(
        advisory,
        acknowledge_advisory,
        advisory_tx,
    )));
    let mut sender = Forwarder::connect(&address, config).await?;

    let broker_info = core.info();
    let local_broker_id = broker_info.id;
    // Forwarding consumers never take messages that already passed through
    // either end of this bridge, so duplex-received messages are not echoed.
    let mut forward_path = vec![local_broker_id.clone()];
    forward_path.extend(remote_broker_id.clone());
    let (connection, mut events) = core.connect_internal(&format!("network:{}", config.name));
    let _connection_guard = BridgeConnection {
        core: Arc::clone(&core),
        connection,
    };
    let mut local_consumers = BTreeMap::<Destination, Vec<u64>>::new();
    let mut remote_demands = HashMap::<String, (Destination, Option<u64>)>::new();
    let mut temp_proxies = HashSet::<Destination>::new();
    // Internal demand observer: local consumer advisories drive temporary
    // proxy cleanup and duplex demand. It is never forwarded.
    let mut observer_options = ConsumerOptions::new(Destination::topic(CONSUMER_ADVISORY));
    observer_options.prefetch = 256;
    let observer = core
        .subscribe(connection, observer_options)
        .map_err(|error| error.to_string())?;
    let (duplex_failures_tx, mut duplex_failures) = mpsc::unbounded_channel();
    let mut duplex = config.duplex.then(|| DuplexBridges {
        core: Arc::clone(&core),
        connection,
        address: address.clone(),
        config: config.clone(),
        local_broker_id: local_broker_id.clone(),
        remote_broker_id: remote_broker_id.clone(),
        failures: duplex_failures_tx,
        consumers: HashMap::new(),
        bridges: HashMap::new(),
    });

    for item in &config.static_included_destinations {
        match parse_destination(item) {
            Ok(destination) if allowed(config, &destination) => {
                subscribe_local(
                    &core,
                    connection,
                    config,
                    &forward_path,
                    &destination,
                    &mut local_consumers,
                    false,
                )?;
                if let Some(duplex) = duplex.as_mut()
                    && !destination.is_advisory()
                {
                    duplex.add_static(&destination);
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

    if let Some(duplex) = duplex.as_mut() {
        for consumer in core.consumers() {
            if consumer.network || consumer.connection == connection {
                continue;
            }
            if let Some(destination) = Destination::parse_uri(&consumer.destination) {
                duplex.consumer_added(
                    consumer.id,
                    &destination,
                    consumer.selector.as_deref(),
                    false,
                    &[],
                );
            }
        }
    }

    tracing::info!(
        name = %config.name,
        remote = %address,
        duplex = config.duplex,
        "OpenWire network connector connected"
    );

    loop {
        tokio::select! {
            failure = sender.failures.recv() => {
                return Err(failure.unwrap_or_else(|| "remote forwarding worker stopped".into()));
            }
            action = remote_advisories.recv() => {
                let action = match action {
                    Some(Ok(action)) => action,
                    Some(Err(error)) => return Err(error),
                    None => return Err("remote advisory reader stopped".into()),
                };
                match action {
                    AdvisoryAction::Added {
                        external_id,
                        destination,
                        network,
                        broker_path,
                        ..
                    } => {
                        if network || broker_path.contains(&local_broker_id) {
                            continue;
                        }
                        if destination.is_advisory()
                            || !allowed(config, &destination)
                            || !dynamically_included(config, &destination)
                        {
                            continue;
                        }
                        if destination.is_temporary()
                            && let Err(error) =
                                ensure_temp_proxy(&core, connection, &destination, &mut temp_proxies)
                        {
                            tracing::warn!(name = %config.name, %destination, %error, "refusing unsafe temporary destination proxy");
                            return Err(format!(
                                "temporary destination proxy conflict for {destination}: {error}"
                            ));
                        }
                        let local_consumer = if config.conduit_subscriptions {
                            None
                        } else if remote_demands.contains_key(&external_id) {
                            continue;
                        } else {
                            Some(subscribe_local(
                                &core,
                                connection,
                                config,
                                &forward_path,
                                &destination,
                                &mut local_consumers,
                                true,
                            )?)
                        };
                        remote_demands.insert(external_id, (destination.clone(), local_consumer));
                        if config.conduit_subscriptions {
                            subscribe_local(
                                &core,
                                connection,
                                config,
                                &forward_path,
                                &destination,
                                &mut local_consumers,
                                false,
                            )?;
                        }
                    }
                    AdvisoryAction::Removed { external_id, .. } => {
                        if let Some((destination, consumer)) = remote_demands.remove(&external_id) {
                            if let Some(consumer) = consumer {
                                close_local_consumer(&core, &destination, consumer, &mut local_consumers);
                            }
                            let still_demanded = remote_demands.values().any(|(d, _)| d == &destination);
                            if config.conduit_subscriptions
                                && !still_demanded
                                && !config
                                    .static_included_destinations
                                    .iter()
                                    .filter_map(|s| Destination::parse_uri(s))
                                    .any(|d| d == destination)
                            {
                                close_local(&core, destination.clone(), &mut local_consumers);
                            }
                            maybe_delete_temp_proxy(&core, connection, &destination, &remote_demands, &mut temp_proxies);
                        }
                    }
                }
            }
            Some(error) = duplex_failures.recv() => return Err(error),
            event = events.recv() => {
                match event {
                    Some(Event::Deliver(delivery)) => {
                        let ack = || {
                            let core = Arc::clone(&core);
                            let consumer = delivery.consumer;
                            let delivery_id = delivery.delivery_id;
                            async move {
                                tokio::task::spawn_blocking(move || {
                                    core.ack(
                                        connection,
                                        consumer,
                                        Acks::Deliveries(vec![delivery_id]),
                                        AckOp::Consumed,
                                        None,
                                    )
                                    .map_err(|error| error.to_string())
                                })
                                .await
                                .map_err(|error| error.to_string())?
                            }
                        };
                        if delivery.consumer == observer {
                            match delivery.message.advisory.as_ref() {
                                Some(AdvisoryData::ConsumerAdded {
                                    consumer,
                                    destination,
                                    selector,
                                    network,
                                    broker_path,
                                    ..
                                }) => {
                                    if let Some(duplex) = duplex.as_mut() {
                                        duplex.consumer_added(
                                            *consumer,
                                            destination,
                                            selector.as_deref(),
                                            *network || temp_proxies.contains(destination),
                                            broker_path,
                                        );
                                    }
                                }
                                Some(AdvisoryData::ConsumerRemoved {
                                    consumer,
                                    destination,
                                    ..
                                }) => {
                                    maybe_delete_temp_proxy(
                                        &core,
                                        connection,
                                        destination,
                                        &remote_demands,
                                        &mut temp_proxies,
                                    );
                                    if let Some(duplex) = duplex.as_mut() {
                                        duplex.consumer_removed(*consumer);
                                    }
                                }
                                _ => {}
                            }
                            ack().await?;
                            continue;
                        }
                        let mut message = (*delivery.message).clone();
                        if (message.advisory.is_some() || message.destination.is_advisory())
                            && !advisory_forwarding_allowed(config, &message.destination)
                        {
                            tracing::debug!(
                                name = %config.name,
                                destination = %message.destination,
                                "advisory message not forwarded; advisory topics must be statically included by name"
                            );
                            ack().await?;
                            continue;
                        }
                        if message.broker_path.contains(&local_broker_id) {
                            ack().await?;
                            continue;
                        }
                        message.broker_path.push(local_broker_id.clone());
                        sender.send_and_confirm(message)
                            .await
                            .map_err(|error| format!("remote send failed: {error}"))?;
                        ack().await?;
                    }
                    Some(Event::ConnectionClosed { reason }) => return Err(reason),
                    Some(Event::ConsumerClosed { consumer, .. }) => {
                        local_consumers.retain(|_, ids| {
                            ids.retain(|id| *id != consumer);
                            !ids.is_empty()
                        });
                    }
                    Some(Event::BrowseDone { .. } | Event::PullEmpty { .. } | Event::Drained { .. }) => {}
                    None => return Err("local broker connection closed".into()),
                }
            }
        }
    }
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn read_remote_advisories(
    mut advisory: OpenWireClient,
    acknowledge: AdvisoryAck,
    actions: mpsc::Sender<Result<AdvisoryAction, String>>,
) {
    let error = loop {
        let frame = match advisory.receive().await {
            Ok(frame) => frame,
            Err(error) => break error.to_string(),
        };
        if frame.command_type == 31 {
            break "remote broker returned an advisory connection exception".into();
        }
        if frame.command_type != 21 {
            continue;
        }
        let decoded = decode_consumer_advisory(&frame);
        // Consumer advisories are acknowledged once decoded; current demand is
        // replayed by the remote broker whenever the bridge reconnects.
        if let Ok(dispatch) = parse_dispatch_prefix(&frame)
            && let Err(error) = acknowledge(&mut advisory, &dispatch.message_id).await
        {
            break error;
        }
        match decoded {
            Ok(Some(action)) => {
                if actions.send(Ok(action)).await.is_err() {
                    return;
                }
            }
            Ok(None) => {}
            Err(error) => tracing::warn!(%error, "could not decode remote consumer advisory"),
        }
    };
    let _ = actions.send(Err(error)).await;
}

/// Duplex demand: local consumers are mirrored as OpenWire network consumers
/// on the remote broker so a stock peer pushes matching messages back over
/// connections opened by this broker.
struct DuplexBridges {
    core: Arc<BrokerCore>,
    connection: u64,
    address: String,
    config: NetworkConnectorConfig,
    local_broker_id: String,
    remote_broker_id: Option<String>,
    failures: mpsc::UnboundedSender<String>,
    consumers: HashMap<u64, String>,
    bridges: HashMap<String, (AbortOnDrop, usize)>,
}

impl DuplexBridges {
    fn add_static(&mut self, destination: &Destination) {
        if destination.is_temporary() {
            return;
        }
        self.retain(format!("static:{destination}"), destination, None);
    }

    /// `bridged` marks demand that must not be mirrored back: network
    /// consumers and consumers of local proxies for remote-owned temporaries.
    fn consumer_added(
        &mut self,
        consumer: u64,
        destination: &Destination,
        selector: Option<&str>,
        bridged: bool,
        broker_path: &[String],
    ) {
        if bridged
            || broker_path.contains(&self.local_broker_id)
            || destination.is_advisory()
            || self.consumers.contains_key(&consumer)
            || !allowed(&self.config, destination)
            || !dynamically_included(&self.config, destination)
        {
            return;
        }
        // A temporary destination gets one remote proxy whatever the conduit
        // setting: the proxy is owned by its bridge connection, so a second
        // co-owning connection would delete it while still in use.
        let (key, selector) = if destination.is_temporary() {
            (format!("temporary:{destination}"), None)
        } else if self.config.conduit_subscriptions {
            (format!("destination:{destination}"), None)
        } else {
            (format!("consumer:{consumer}"), selector)
        };
        self.consumers.insert(consumer, key.clone());
        self.retain(key, destination, selector);
    }

    fn consumer_removed(&mut self, consumer: u64) {
        let Some(key) = self.consumers.remove(&consumer) else {
            return;
        };
        if let Some((_, references)) = self.bridges.get_mut(&key) {
            *references -= 1;
            if *references == 0 {
                // Closing the dedicated connection removes the remote network
                // consumer; unacknowledged dispatches are redelivered remotely.
                self.bridges.remove(&key);
            }
        }
    }

    fn retain(&mut self, key: String, destination: &Destination, selector: Option<&str>) {
        if let Some((_, references)) = self.bridges.get_mut(&key) {
            *references += 1;
            return;
        }
        let task = tokio::spawn(run_duplex_consumer(
            Arc::clone(&self.core),
            self.connection,
            self.address.clone(),
            self.config.clone(),
            self.local_broker_id.clone(),
            self.remote_broker_id.clone(),
            destination.clone(),
            selector.map(str::to_owned),
            self.failures.clone(),
        ));
        self.bridges.insert(key, (AbortOnDrop(task), 1));
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_duplex_consumer(
    core: Arc<BrokerCore>,
    connection: u64,
    address: String,
    config: NetworkConnectorConfig,
    local_broker_id: String,
    remote_broker_id: Option<String>,
    destination: Destination,
    selector: Option<String>,
    failures: mpsc::UnboundedSender<String>,
) {
    let result = async {
        let mut client = OpenWireClient::connect_client(
            address.as_str(),
            DEFAULT_MAX_FRAME_SIZE,
            None,
            config.username.as_deref(),
            config.password.as_deref(),
        )
        .await
        .map_err(|error| error.to_string())?;
        if destination.is_temporary() {
            // Equivalent to Classic bridgeTempDestinations: the remote proxy is
            // owned by this dedicated connection and disappears with it, i.e.
            // when the last local consumer of the temporary destination goes.
            if let Err(error) = client.add_destination(&destination).await {
                tracing::warn!(
                    name = %config.name,
                    %destination,
                    %error,
                    "remote broker refused temporary destination proxy; duplex demand not forwarded"
                );
                return Ok(());
            }
        }
        client
            .create_network_consumer(
                &destination,
                config.prefetch.max(1),
                selector.as_deref(),
                std::slice::from_ref(&local_broker_id),
            )
            .await
            .map_err(|error| error.to_string())?;
        tracing::debug!(name = %config.name, %destination, "duplex network consumer registered remotely");
        loop {
            let Some((consumer, mut message, _)) = client
                .receive_message(DUPLEX_IDLE_RECONNECT)
                .await
                .map_err(|error| error.to_string())?
            else {
                return Err::<(), String>("duplex consumer idle; reconnecting".into());
            };
            let message_id = message.message_id.clone();
            if !message.broker_path.contains(&local_broker_id) {
                // Like Classic, record the broker the message was bridged
                // from; without a BrokerInfo id fall back to the local id.
                message
                    .broker_path
                    .push(remote_broker_id.clone().unwrap_or_else(|| local_broker_id.clone()));
                // At-least-once: the remote dispatch is acknowledged only
                // after the local broker accepted the message.
                let core = Arc::clone(&core);
                tokio::task::spawn_blocking(move || core.send(connection, message, None))
                    .await
                    .map_err(|error| error.to_string())?
                    .map_err(|error| format!("local send failed: {error}"))?;
            }
            client
                .acknowledge(&consumer, &message_id)
                .await
                .map_err(|error| error.to_string())?;
        }
    }
    .await;
    if let Err(error) = result {
        let _ = failures.send(format!("duplex demand for {destination} failed: {error}"));
    }
}

struct ForwardRequest {
    message: Message,
    confirmed: oneshot::Sender<Result<(), String>>,
}

/// A bounded handoff keeps the remote socket driver independent from local
/// durable acknowledgements. No detached retry can acknowledge a failed send.
struct Forwarder {
    requests: mpsc::Sender<ForwardRequest>,
    failures: mpsc::Receiver<String>,
    _worker: AbortOnDrop,
}

impl Forwarder {
    async fn connect(address: &str, config: &NetworkConnectorConfig) -> Result<Self, String> {
        let mut sender = ForwardSender::connect(address, config).await?;
        let (requests, mut pending) = mpsc::channel::<ForwardRequest>(1);
        let (failures, receiver) = mpsc::channel(1);
        let worker = AbortOnDrop(tokio::spawn(async move {
            let error = loop {
                tokio::select! {
                    request = pending.recv() => {
                        let Some(request) = request else { return };
                        let result = sender.send_and_confirm(&request.message).await;
                        let error = result.as_ref().err().cloned();
                        let _ = request.confirmed.send(result);
                        if let Some(error) = error {
                            break error;
                        }
                    }
                    response = sender.responses.recv() => {
                        match response {
                            Some(Ok(_)) => {}
                            Some(Err(error)) => break error,
                            None => break "remote forwarding reader stopped".into(),
                        }
                    }
                    _ = sender.heartbeat.tick() => {
                        if let Err(error) = sender.send_heartbeat().await {
                            break error;
                        }
                    }
                }
            };
            let _ = failures.send(error).await;
        }));
        Ok(Self {
            requests,
            failures: receiver,
            _worker: worker,
        })
    }

    async fn send_and_confirm(&self, message: Message) -> Result<(), String> {
        let (confirmed, result) = oneshot::channel();
        self.requests
            .send(ForwardRequest { message, confirmed })
            .await
            .map_err(|_| "remote forwarding worker stopped".to_owned())?;
        result
            .await
            .map_err(|_| "remote forwarding worker stopped before confirmation".to_owned())?
    }
}

/// One anonymous producer and one outstanding send per connector. Awaiting
/// confirmation before consuming the next local delivery preserves source
/// prefetch backpressure and never acknowledges an unconfirmed remote send.
struct ForwardSender {
    writer: OwnedWriteHalf,
    producer: openwire::WireId,
    next_command_id: i32,
    responses: mpsc::Receiver<Result<OpenWireFrame, String>>,
    heartbeat: tokio::time::Interval,
    _reader: AbortOnDrop,
}

impl ForwardSender {
    async fn connect(address: &str, config: &NetworkConnectorConfig) -> Result<Self, String> {
        let (mut client, producer) = tokio::time::timeout(Duration::from_secs(30), async {
            let mut client = OpenWireClient::connect_client(
                address,
                DEFAULT_MAX_FRAME_SIZE,
                None,
                config.username.as_deref(),
                config.password.as_deref(),
            )
            .await?;
            let producer = client.create_producer(None).await?;
            Ok::<_, openwire::OpenWireError>((client, producer))
        })
        .await
        .map_err(|_| "timed out establishing remote forwarding connection".to_owned())?
        .map_err(|error| error.to_string())?;
        let next_command_id = client.next_command_id();
        let (reader, writer) = client.into_parts();
        let (responses, receiver) = mpsc::channel(16);
        let reader = AbortOnDrop(tokio::spawn(read_forward_responses(reader, responses)));
        let mut heartbeat = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(10),
            Duration::from_secs(10),
        );
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Ok(Self {
            writer,
            producer,
            next_command_id,
            responses: receiver,
            heartbeat,
            _reader: reader,
        })
    }

    async fn write(&mut self, frame: &OpenWireFrame) -> Result<(), String> {
        let mut bytes = Vec::new();
        frame
            .write_to(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
            .map_err(|error| error.to_string())?;
        tokio::time::timeout(Duration::from_secs(30), self.writer.write_all(&bytes))
            .await
            .map_err(|_| "timed out writing to remote forwarding connection".to_owned())?
            .map_err(|error| error.to_string())
    }

    async fn send_heartbeat(&mut self) -> Result<(), String> {
        self.write(&OpenWireFrame {
            command_type: 10,
            payload: vec![0; 5],
        })
        .await
    }

    async fn send_and_confirm(&mut self, message: &Message) -> Result<(), String> {
        let command_id = self.next_command_id;
        self.next_command_id = self.next_command_id.wrapping_add(1);
        let mut frame = openwire::encode_client_message(&self.producer, message, command_id);
        // Loose-encoded command header: CommandId (i32) then responseRequired.
        frame.payload[4] = 1;
        tokio::time::timeout(Duration::from_secs(30), async {
            self.write(&frame).await?;
            loop {
                tokio::select! {
                    response = self.responses.recv() => {
                        let response = response
                            .ok_or_else(|| "remote forwarding reader stopped".to_owned())??;
                        if response.payload.get(5..9)
                            != Some(command_id.to_be_bytes().as_slice())
                        {
                            continue;
                        }
                        match response.command_type {
                            30 => return Ok(()),
                            31 => return Err(decode_remote_exception(&response.payload)),
                            _ => {}
                        }
                    }
                    _ = self.heartbeat.tick() => self.send_heartbeat().await?,
                }
            }
        })
        .await
        .map_err(|_| "timed out waiting for remote send response".to_owned())?
    }
}

async fn read_forward_responses(
    mut reader: OwnedReadHalf,
    responses: mpsc::Sender<Result<OpenWireFrame, String>>,
) {
    // Never race a socket read with timers or outbound work: a partial frame
    // must finish before the next read begins. Aborting this task also drops
    // the connection; a cancelled read is never resumed on a reused socket.
    let result = async {
        loop {
            let size = reader.read_u32().await.map_err(|error| error.to_string())? as usize;
            if size == 0 || size > DEFAULT_MAX_FRAME_SIZE {
                return Err("invalid remote forwarding frame length".to_owned());
            }
            let mut bytes = vec![0; size];
            reader
                .read_exact(&mut bytes)
                .await
                .map_err(|error| error.to_string())?;
            match bytes[0] {
                30 | 31 => {
                    if responses
                        .send(Ok(OpenWireFrame {
                            command_type: bytes[0],
                            payload: bytes[1..].to_vec(),
                        }))
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                }
                11 | 16 => return Err("remote forwarding connection closed".to_owned()),
                _ => {}
            }
        }
    }
    .await;
    if let Err(error) = result {
        let _ = responses.send(Err(error)).await;
    }
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
    broker_path: &[String],
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
    options.broker_path = broker_path.to_vec();
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

/// Advisory topics are bridged only when named by an explicit static
/// `topic://ActiveMQ.Advisory...` include, mirroring Classic's default of not
/// forwarding advisories through generic wildcards.
fn advisory_forwarding_allowed(config: &NetworkConnectorConfig, destination: &Destination) -> bool {
    destination.is_advisory()
        && allowed(config, destination)
        && config
            .static_included_destinations
            .iter()
            .filter_map(|pattern| Destination::parse_uri(pattern))
            .any(|pattern| pattern.is_advisory() && pattern.matches(destination))
}

fn validate_advisory_patterns(config: &NetworkConnectorConfig) -> Result<(), String> {
    let demand_probes = [
        Destination::queue("network-probe").consumer_advisory_topic(),
        Destination::topic("network-probe").consumer_advisory_topic(),
        Destination::temp_queue("network-probe").consumer_advisory_topic(),
        Destination::temp_topic("network-probe").consumer_advisory_topic(),
    ];
    for pattern in &config.dynamically_included_destinations {
        if Destination::parse_uri(pattern).is_some_and(|pattern| pattern.is_advisory()) {
            return Err(format!(
                "{pattern}: advisory consumers emit no consumer advisories, so dynamic advisory demand cannot be observed; use static_included_destinations"
            ));
        }
    }
    for pattern in &config.static_included_destinations {
        let Some(destination) = Destination::parse_uri(pattern) else {
            continue;
        };
        if destination.is_advisory() && !destination.is_topic() {
            return Err(format!("{pattern}: advisory destinations are topics"));
        }
        if destination.is_advisory() && demand_probes.iter().any(|probe| destination.matches(probe))
        {
            return Err(format!(
                "{pattern}: consumer advisories are the network demand channel; forwarding them would create phantom demand on peers"
            ));
        }
    }
    Ok(())
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
        network: bool,
        broker_path: Vec<String>,
    },
    Removed {
        external_id: String,
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
    parse_dispatch_prefix(frame)?;
    let Some(data) = parse_advisory_data(frame)? else {
        return Ok(None);
    };
    match data.kind {
        5 => Ok(Some(AdvisoryAction::Added {
            external_id: data.external_id,
            destination: data.destination,
            network: data.network,
            broker_path: data.broker_path,
        })),
        12 => Ok(Some(AdvisoryAction::Removed {
            external_id: data.external_id,
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

    #[tokio::test]
    async fn reusable_sender_correlates_fragmented_responses_across_heartbeats() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (client, accepted) = tokio::join!(
            tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept(),
        );
        let client = client.unwrap();
        client.set_nodelay(true).unwrap();
        let (mut peer, _) = accepted.unwrap();
        peer.set_nodelay(true).unwrap();
        let (reader, writer) = client.into_split();
        let (responses, receiver) = mpsc::channel(16);
        let mut sender = ForwardSender {
            writer,
            producer: openwire::WireId {
                connection: "test-forwarder".into(),
                session: 1,
                value: 1,
            },
            next_command_id: 7,
            responses: receiver,
            heartbeat: tokio::time::interval(Duration::from_millis(1)),
            _reader: AbortOnDrop(tokio::spawn(read_forward_responses(reader, responses))),
        };
        let peer_task = tokio::spawn(async move {
            let mut heartbeat_count = 0;
            for command_id in [7i32, 8] {
                loop {
                    let size = peer.read_u32().await.unwrap();
                    let mut bytes = vec![0; size as usize];
                    peer.read_exact(&mut bytes).await.unwrap();
                    if bytes[0] == 10 {
                        heartbeat_count += 1;
                        continue;
                    }
                    assert_eq!(&bytes[1..5], &command_id.to_be_bytes());
                    assert_eq!(bytes[5], 1, "each message requires a broker response");
                    break;
                }
                if command_id == 7 {
                    let mut unrelated = vec![0; 5];
                    unrelated.extend_from_slice(&8i32.to_be_bytes());
                    unrelated.push(0);
                    let mut bytes = Vec::new();
                    OpenWireFrame {
                        command_type: 31,
                        payload: unrelated,
                    }
                    .write_to(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
                    .unwrap();
                    peer.write_all(&bytes).await.unwrap();
                }
                let mut payload = vec![0; 5];
                payload.extend_from_slice(&command_id.to_be_bytes());
                if command_id == 8 {
                    payload.push(0);
                }
                let mut bytes = Vec::new();
                OpenWireFrame {
                    command_type: if command_id == 7 { 30 } else { 31 },
                    payload,
                }
                .write_to(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
                .unwrap();
                // Both the length prefix and frame body cross heartbeat ticks.
                for chunk in bytes.chunks(2) {
                    peer.write_all(chunk).await.unwrap();
                    tokio::time::sleep(Duration::from_millis(3)).await;
                }
            }
            assert!(
                heartbeat_count > 0,
                "sender must heartbeat while awaiting a response"
            );
        });
        tokio::time::timeout(Duration::from_secs(3), async {
            let message = Message::text(Destination::queue("test"), "body");
            sender.send_and_confirm(&message).await.unwrap();
            let error = sender.send_and_confirm(&message).await.unwrap_err();
            assert!(error.contains("rejected"), "{error}");
            peer_task.await.unwrap();
        })
        .await
        .expect("fragmented responses or heartbeat writes stalled");
    }

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
