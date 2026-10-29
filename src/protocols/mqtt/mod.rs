//! MQTT 3.1 / 3.1.1 transport, using ActiveMQ's default topic subscription strategy.

mod codec;
mod session;

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::AsyncWrite;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout};

use super::ProtocolContext;
use crate::core::{
    AckOp, Acks, BrokerCore, ConnectInfo, ConnectionId, ConsumerId, ConsumerOptions, CoreError,
    Delivery, Destination, Event, Message, RETAIN_PROPERTY, RETAINED_PROPERTY, Value,
};
use codec::{Connect, Cursor, Packet, Publish, invalid};
use session::{Sent, Session, Store};

const QOS_PROPERTY: &str = "ActiveMQ.MQTT.QoS";
const QOS_NAMES: [&str; 3] = ["AT_MOST_ONCE", "AT_LEAST_ONCE", "EXACTLY_ONCE"];

pub async fn serve(context: ProtocolContext, listener: TcpListener) -> io::Result<()> {
    let store = Arc::new(Store::new(
        context.listener.option_str("session_state_dir"),
    )?);
    super::accept_loop("mqtt", context, listener, move |context, stream, remote| {
        let store = Arc::clone(&store);
        async move {
            let result = connection(context, stream, remote, store).await;
            if let Err(error) = &result
                && matches!(
                    error.kind(),
                    io::ErrorKind::InvalidData | io::ErrorKind::TimedOut
                )
            {
                tracing::warn!(%remote, %error, "MQTT protocol error");
            }
            result
        }
    })
    .await
}

/// Even an aborted connection task must release its core consumers and deliveries.
struct ConnectionGuard {
    core: Arc<BrokerCore>,
    id: ConnectionId,
}
impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.core.disconnect(self.id);
    }
}

struct ReaderGuard(tokio::task::JoinHandle<()>);
impl Drop for ReaderGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Clone)]
struct Subscription {
    consumer: ConsumerId,
    qos: u8,
    durable: Option<String>,
}

struct Client {
    context: ProtocolContext,
    id: ConnectionId,
    connect: Connect,
    store: Arc<Store>,
    session: Arc<Mutex<Session>>,
    subscriptions: BTreeMap<String, Subscription>,
    pending: BTreeMap<u16, (String, Delivery)>,
    graceful: bool,
}

async fn connection(
    context: ProtocolContext,
    mut stream: TcpStream,
    remote: SocketAddr,
    store: Arc<Store>,
) -> io::Result<()> {
    let max = context.max_frame_size;
    let connect_timeout = context
        .listener
        .option_i64("connect_timeout_ms")
        .unwrap_or(10_000)
        .max(1) as u64;
    let packet = timeout(
        Duration::from_millis(connect_timeout),
        codec::read(&mut stream, max),
    )
    .await
    .map_err(|_| invalid("MQTT CONNECT timeout"))??;
    if packet.header != 0x10 {
        return Err(invalid("CONNECT must be the first MQTT packet"));
    }
    let mut cursor = Cursor::new(&packet.body);
    let name = cursor.string()?;
    let version = cursor.byte()?;
    if !matches!((name.as_str(), version), ("MQIsdp", 3) | ("MQTT", 4)) {
        codec::write(&mut stream, 0x20, &[0, 1], max).await?;
        return Ok(());
    }
    let mut connect = Connect::decode(&packet.body)?;
    if connect.client_id.is_empty() {
        if !connect.clean {
            codec::write(&mut stream, 0x20, &[0, 2], max).await?;
            return Ok(());
        }
        connect.client_id = format!("mqtt-{}", crate::core::message::generate_message_id());
    }
    let info = ConnectInfo {
        client_id: Some(connect.client_id.clone()),
        username: connect.username.clone(),
        password: connect.password.clone(),
        protocol: "mqtt".to_owned(),
        remote_address: remote.to_string(),
        takeover: true,
        redeliver_on_rollback: true,
    };
    // Serialize ownership transfer with session restoration; concurrent takeovers
    // must not install an older connection as the owner of the newer session.
    let handshake_store = Arc::clone(&store);
    let handshake = handshake_store.handshake.lock().await;
    let (id, mut events) = match context.core.connect(info) {
        Ok(connection) => connection,
        Err(error) => {
            let code = match error {
                CoreError::Security(_) if connect.username.is_some() => 4,
                CoreError::Security(_) => 5,
                CoreError::InvalidClientId(_) => 2,
                _ => 3,
            };
            codec::write(&mut stream, 0x20, &[0, code], max).await?;
            return Ok(());
        }
    };
    let _guard = ConnectionGuard {
        core: Arc::clone(&context.core),
        id,
    };
    let durables: Vec<_> = context
        .core
        .durable_subscriptions()
        .into_iter()
        .filter(|sub| sub.client_id == connect.client_id)
        .collect();
    let (session, mut present) = store.open(&connect.client_id, connect.clean, id)?;
    present |= !durables.is_empty();
    if connect.clean {
        for sub in &durables {
            context
                .core
                .remove_durable(id, &sub.name)
                .map_err(io::Error::other)?;
        }
    } else {
        let mut state = session.lock().unwrap();
        for sub in &durables {
            if let Some((topic, qos)) = decode_durable(&sub.name) {
                state.subscriptions.insert(topic.to_owned(), qos);
            }
        }
    }
    let keep_alive = connect.keep_alive;
    let mut client = Client {
        context,
        id,
        connect,
        store,
        session,
        subscriptions: BTreeMap::new(),
        pending: BTreeMap::new(),
        graceful: false,
    };
    let result = async {
        let restore = client.session.lock().unwrap().subscriptions.clone();
        for (topic, qos) in restore {
            client.subscribe(&topic, qos)?;
        }
        client.save()?;
        drop(handshake);
        codec::write(
            &mut stream,
            0x20,
            &[
                u8::from(client.connect.version == 4 && !client.connect.clean && present),
                0,
            ],
            max,
        )
        .await?;
        let (mut reader, mut writer) = stream.into_split();
        let (sender, mut packets) = mpsc::channel(16);
        let _reader = ReaderGuard(tokio::spawn(async move {
            loop {
                let packet = codec::read(&mut reader, max).await;
                let failed = packet.is_err();
                if sender.send(packet).await.is_err() || failed {
                    break;
                }
            }
        }));
        let grace = if keep_alive == 0 {
            Duration::from_secs(86400 * 365)
        } else {
            Duration::from_millis(u64::from(keep_alive) * 1500)
        };
        let mut deadline = Instant::now() + grace;
        loop {
            tokio::select! {
                biased;
                event = events.recv() => match event {
                    Some(Event::Deliver(delivery)) => client.deliver(delivery, &mut writer).await?,
                    Some(Event::ConnectionClosed { reason }) => {
                        tracing::debug!(connection = id, %reason, "MQTT connection closed by core");
                        break;
                    }
                    Some(Event::ConsumerClosed { consumer, .. }) => {
                        client.subscriptions.retain(|_, sub| sub.consumer != consumer);
                    }
                    Some(_) => {},
                    None => break,
                },
                _ = tokio::time::sleep_until(deadline), if keep_alive != 0 => {
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "MQTT keep alive expired"));
                }
                packet = packets.recv() => {
                    let packet = packet.ok_or_else(|| invalid("MQTT reader stopped"))??;
                    deadline = Instant::now() + grace;
                    if client.session.lock().unwrap().owner != id || client.context.core.principal(id).is_none() { break; }
                    if !client.packet(packet, &mut writer).await? { break; }
                }
            }
        }
        Ok(())
    }
    .await;
    if !client.graceful
        && let Some(will) = &client.connect.will
    {
        // Takeover removes the original core connection before its close event is read.
        if client.publish(will).is_err() {
            let info = ConnectInfo {
                username: client.connect.username.clone(),
                password: client.connect.password.clone(),
                protocol: "mqtt-will".to_owned(),
                ..ConnectInfo::default()
            };
            if let Ok((will_id, _)) = client.context.core.connect(info) {
                let _ = client.context.core.send(will_id, message(will), None);
                client.context.core.disconnect(will_id);
            }
        }
    }
    if client.connect.clean {
        client.store.remove(&client.connect.client_id, id)?;
    }
    result
}

fn decode_durable(name: &str) -> Option<(&str, u8)> {
    let (qos, topic) = name.split_once(':')?;
    Some((topic, QOS_NAMES.iter().position(|name| *name == qos)? as u8))
}

fn message(publish: &Publish) -> Message {
    let mut message = Message::bytes(
        Destination::topic(codec::to_activemq(&publish.topic)),
        publish.payload.clone(),
    );
    message.persistent = publish.qos > 0;
    message
        .properties
        .insert(QOS_PROPERTY.to_owned(), Value::Int(i32::from(publish.qos)));
    if publish.retain {
        message
            .properties
            .insert(RETAIN_PROPERTY.to_owned(), Value::Bool(true));
    }
    message
}

impl Client {
    fn save(&self) -> io::Result<()> {
        self.store.save(
            &self.connect.client_id,
            &self.session.lock().unwrap(),
            self.connect.clean,
        )
    }

    fn publish(&self, publish: &Publish) -> io::Result<()> {
        self.context
            .core
            .send(self.id, message(publish), None)
            .map_err(io::Error::other)
    }

    fn subscribe(&mut self, topic: &str, qos: u8) -> io::Result<()> {
        codec::validate_topic(topic, true)?;
        if qos > 2 {
            return Err(invalid("unsupported MQTT subscription QoS"));
        }
        if let Some(existing) = self.subscriptions.get(topic) {
            if existing.qos == qos {
                return self
                    .context
                    .core
                    .replay_retained(self.id, existing.consumer)
                    .map_err(io::Error::other);
            }
            self.unsubscribe(topic)?;
        }
        let mut options = ConsumerOptions::new(Destination::topic(codec::to_activemq(topic)));
        options.prefetch = self
            .context
            .listener
            .option_i64("prefetch")
            .unwrap_or(1000)
            .clamp(1, 65535) as u32;
        options.retroactive = true;
        options.external_id = Some(topic.to_owned());
        if !self.connect.clean && qos > 0 {
            options.durable_name = Some(format!("{}:{topic}", QOS_NAMES[usize::from(qos)]));
        }
        let durable = options.durable_name.clone();
        let consumer = self
            .context
            .core
            .subscribe(self.id, options)
            .map_err(io::Error::other)?;
        self.subscriptions.insert(
            topic.to_owned(),
            Subscription {
                consumer,
                qos,
                durable,
            },
        );
        self.session
            .lock()
            .unwrap()
            .subscriptions
            .insert(topic.to_owned(), qos);
        self.save()
    }

    fn unsubscribe(&mut self, topic: &str) -> io::Result<()> {
        if let Some(sub) = self.subscriptions.remove(topic) {
            self.context.core.close_consumer(sub.consumer);
            if let Some(name) = sub.durable {
                self.context
                    .core
                    .remove_durable(self.id, &name)
                    .map_err(io::Error::other)?;
            }
        }
        self.pending
            .retain(|_, (key, _)| !key.starts_with(&format!("{topic}\0")));
        {
            let mut state = self.session.lock().unwrap();
            state.subscriptions.remove(topic);
            state
                .outgoing
                .retain(|key, _| !key.starts_with(&format!("{topic}\0")));
        }
        self.save()
    }

    fn ack(&self, delivery: &Delivery) -> io::Result<()> {
        self.context
            .core
            .ack(
                self.id,
                delivery.consumer,
                Acks::Deliveries(vec![delivery.delivery_id]),
                AckOp::Consumed,
                None,
            )
            .map_err(io::Error::other)
    }

    async fn deliver<W: AsyncWrite + Unpin>(
        &mut self,
        delivery: Delivery,
        writer: &mut W,
    ) -> io::Result<()> {
        if self.session.lock().unwrap().owner != self.id
            || self.context.core.principal(self.id).is_none()
        {
            return Err(invalid("MQTT session taken over"));
        }
        let Some((filter, subscription)) = self
            .subscriptions
            .iter()
            .find(|(_, sub)| sub.consumer == delivery.consumer)
        else {
            return Ok(());
        };
        let topic = codec::to_mqtt(&delivery.message.destination.name);
        // MQTT root wildcards must not expose system topics. Core uses JMS matching.
        if topic.starts_with('$') && !filter.starts_with('$') || delivery.message.advisory.is_some()
        {
            return self.ack(&delivery);
        }
        let qos = delivery
            .message
            .property(QOS_PROPERTY)
            .and_then(Value::as_i64)
            .unwrap_or(if delivery.message.persistent { 0 } else { 1 })
            .clamp(0, 2) as u8;
        let qos = qos.min(subscription.qos);
        let retain = delivery
            .message
            .property(RETAINED_PROPERTY)
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let key = format!("{filter}\0{}", delivery.message.message_id);
        let (id, released, duplicate) = if qos == 0 {
            (0, false, false)
        } else {
            let mut state = self.session.lock().unwrap();
            let duplicate = state.outgoing.contains_key(&key) || delivery.redelivered;
            let sent = if let Some(sent) = state.outgoing.get(&key) {
                sent.clone()
            } else {
                let sent = Sent {
                    id: state.packet_id()?,
                    qos,
                    released: false,
                };
                state.outgoing.insert(key.clone(), sent.clone());
                sent
            };
            self.store
                .save(&self.connect.client_id, &state, self.connect.clean)?;
            (sent.id, sent.released, duplicate)
        };
        if released {
            codec::write(writer, 0x62, &id.to_be_bytes(), self.context.max_frame_size).await?;
        } else {
            let mut body = Vec::new();
            codec::string(&mut body, &topic)?;
            if qos > 0 {
                body.extend_from_slice(&id.to_be_bytes());
            }
            body.extend_from_slice(&delivery.message.body.to_bytes());
            let header = 0x30 | (qos << 1) | u8::from(retain) | if duplicate { 8 } else { 0 };
            codec::write(writer, header, &body, self.context.max_frame_size).await?;
        }
        if qos == 0 {
            self.ack(&delivery)?;
        } else {
            self.pending.insert(id, (key, delivery));
        }
        Ok(())
    }

    async fn packet<W: AsyncWrite + Unpin>(
        &mut self,
        packet: Packet,
        writer: &mut W,
    ) -> io::Result<bool> {
        tracing::debug!(
            connection = self.id,
            packet_type = packet.header >> 4,
            "MQTT packet"
        );
        let max = self.context.max_frame_size;
        let mut cursor = Cursor::new(&packet.body);
        match packet.header >> 4 {
            3 => {
                let (publish, id) = Publish::decode(&packet)?;
                match publish.qos {
                    0 => self.publish(&publish)?,
                    1 => {
                        self.publish(&publish)?;
                        codec::write(writer, 0x40, &id.to_be_bytes(), max).await?;
                    }
                    2 => {
                        self.session
                            .lock()
                            .unwrap()
                            .incoming
                            .entry(id)
                            .or_insert(publish);
                        self.save()?;
                        codec::write(writer, 0x50, &id.to_be_bytes(), max).await?;
                    }
                    _ => unreachable!(),
                }
            }
            4 | 5 | 7 => {
                let id = cursor.id()?;
                cursor.end()?;
                if let Some((key, delivery)) = self.pending.get(&id) {
                    let sent = self.session.lock().unwrap().outgoing.get(key).cloned();
                    if let Some(sent) = sent {
                        match packet.header >> 4 {
                            5 if sent.qos == 2 => {
                                self.session
                                    .lock()
                                    .unwrap()
                                    .outgoing
                                    .get_mut(key)
                                    .unwrap()
                                    .released = true;
                                self.save()?;
                                codec::write(writer, 0x62, &id.to_be_bytes(), max).await?;
                            }
                            4 if sent.qos == 1 => {
                                self.ack(delivery)?;
                                self.finish(id)?;
                            }
                            7 if sent.qos == 2 && sent.released => {
                                self.ack(delivery)?;
                                self.finish(id)?;
                            }
                            _ => {}
                        }
                    }
                }
            }
            6 => {
                let id = cursor.id()?;
                cursor.end()?;
                let publish = self.session.lock().unwrap().incoming.get(&id).cloned();
                if let Some(publish) = publish {
                    self.publish(&publish)?;
                    self.session.lock().unwrap().incoming.remove(&id);
                    self.save()?;
                }
                codec::write(writer, 0x70, &id.to_be_bytes(), max).await?;
            }
            8 => {
                let id = cursor.id()?;
                let mut granted = id.to_be_bytes().to_vec();
                if cursor.empty() {
                    return Err(invalid("empty MQTT SUBSCRIBE"));
                }
                while !cursor.empty() {
                    let topic = cursor.string()?;
                    let qos = cursor.byte()?;
                    let granted_qos = if self.subscribe(&topic, qos).is_ok() {
                        qos
                    } else {
                        0x80
                    };
                    granted.push(granted_qos);
                }
                codec::write(writer, 0x90, &granted, max).await?;
            }
            10 => {
                let id = cursor.id()?;
                if cursor.empty() {
                    return Err(invalid("empty MQTT UNSUBSCRIBE"));
                }
                while !cursor.empty() {
                    let topic = cursor.string()?;
                    codec::validate_topic(&topic, true)?;
                    self.unsubscribe(&topic)?;
                }
                codec::write(writer, 0xb0, &id.to_be_bytes(), max).await?;
            }
            12 => {
                cursor.end()?;
                codec::write(writer, 0xd0, &[], max).await?;
            }
            14 => {
                cursor.end()?;
                self.graceful = true;
                return Ok(false);
            }
            _ => return Err(invalid("unexpected MQTT packet type")),
        }
        Ok(true)
    }

    fn finish(&mut self, id: u16) -> io::Result<()> {
        if let Some((key, _)) = self.pending.remove(&id) {
            self.session.lock().unwrap().outgoing.remove(&key);
        }
        self.save()
    }
}
