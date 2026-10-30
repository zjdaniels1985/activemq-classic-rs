//! Connections, producers, temporary destinations and message routing.

use std::sync::Arc;

use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

use super::broker::{BrokerCore, Connection, Inner};
use super::destination::{
    Destination, DestinationKind, SCHEDULER_MANAGEMENT_TOPIC, wildcard_match,
};
use super::message::{AdvisoryData, Message, Value, now_millis};
use super::queue::{Entry, QueueKind, queue_key};
use super::scheduler::is_scheduled;
use super::security::{Permission, Principal};
use super::types::{ConnectInfo, ConnectionId, CoreError, Event, ProducerId, TxId};

pub const RETAIN_PROPERTY: &str = "ActiveMQ.Retain";
pub const RETAINED_PROPERTY: &str = "ActiveMQ.Retained";
const CONNECTION_ADVISORY: &str = "ActiveMQ.Advisory.Connection";

impl BrokerCore {
    /// Authenticates and registers a client connection.
    pub fn connect(
        &self,
        info: ConnectInfo,
    ) -> Result<(ConnectionId, UnboundedReceiver<Event>), CoreError> {
        let principal = self
            .security
            .authenticate(info.username.as_deref(), info.password.as_deref())?;
        self.connect_with(info, principal)
    }

    /// Registers a broker-internal connection (console, network bridge) that
    /// bypasses authorization.
    pub fn connect_internal(&self, name: &str) -> (ConnectionId, UnboundedReceiver<Event>) {
        let info = ConnectInfo {
            protocol: "internal".to_owned(),
            remote_address: name.to_owned(),
            redeliver_on_rollback: true,
            ..ConnectInfo::default()
        };
        self.connect_with(info, Principal::system(name))
            .expect("internal connections never fail")
    }

    fn connect_with(
        &self,
        mut info: ConnectInfo,
        principal: Principal,
    ) -> Result<(ConnectionId, UnboundedReceiver<Event>), CoreError> {
        info.password = None;
        let mut inner = self.lock();
        if let Some(client_id) = info.client_id.clone().filter(|id| !id.is_empty()) {
            if let Some(existing) = inner.client_ids.get(&client_id).copied() {
                if !info.takeover {
                    return Err(CoreError::InvalidClientId(format!(
                        "Broker: {} - Client: {} already connected from {}",
                        self.broker_name(),
                        client_id,
                        inner
                            .connections
                            .get(&existing)
                            .map(|c| c.info.remote_address.clone())
                            .unwrap_or_default()
                    )));
                }
                inner.emit(
                    existing,
                    Event::ConnectionClosed {
                        reason: "client id taken over by a new connection".to_owned(),
                    },
                );
                self.disconnect_locked(&mut inner, existing);
            }
            inner.client_ids.insert(client_id, 0);
        } else {
            info.client_id = None;
        }
        let id = inner.next_id();
        if let Some(client_id) = &info.client_id {
            inner.client_ids.insert(client_id.clone(), id);
        }
        let (sender, receiver) = unbounded_channel();
        let data = AdvisoryData::ConnectionAdded {
            connection: id,
            client_id: info.client_id.clone(),
            username: Some(principal.username.clone()),
            remote_address: info.remote_address.clone(),
        };
        tracing::info!(
            connection = id,
            protocol = %info.protocol,
            remote = %info.remote_address,
            client_id = info.client_id.as_deref().unwrap_or(""),
            user = %principal.username,
            "connection established"
        );
        inner.connections.insert(
            id,
            Connection {
                info,
                principal,
                sender,
                consumers: Default::default(),
                producers: Default::default(),
                temps: Default::default(),
                connected_at: now_millis(),
            },
        );
        self.advise(&mut inner, Destination::topic(CONNECTION_ADVISORY), data);
        self.flush_batch(&mut inner)?;
        Ok((id, receiver))
    }

    /// Closes a connection, its consumers, producers, temporary destinations
    /// and rolls back its open local transactions.
    pub fn disconnect(&self, connection: ConnectionId) {
        let mut inner = self.lock();
        self.disconnect_locked(&mut inner, connection);
        if let Err(error) = self.flush_batch(&mut inner) {
            tracing::warn!(%error, "failed to persist disconnect");
        }
    }

    pub(crate) fn disconnect_locked(&self, inner: &mut Inner, connection: ConnectionId) {
        let Some(state) = inner.connections.get(&connection) else {
            return;
        };
        let consumers = state.consumers.iter().copied().collect::<Vec<_>>();
        let producers = state.producers.iter().copied().collect::<Vec<_>>();
        let temps = state.temps.iter().cloned().collect::<Vec<_>>();
        let client_id = state.info.client_id.clone();
        let transactions = inner
            .transactions
            .iter()
            .filter(|(key, tx)| {
                !tx.prepared && (key.0 == Some(connection) || tx.connection == Some(connection))
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in transactions {
            self.rollback_locked(inner, &key);
        }
        for consumer in consumers {
            self.close_consumer_locked(inner, consumer);
        }
        for producer in producers {
            self.unregister_producer_locked(inner, producer);
        }
        for temp in temps {
            self.delete_temp_locked(inner, &temp);
        }
        if let Some(client_id) = &client_id
            && inner.client_ids.get(client_id) == Some(&connection)
        {
            inner.client_ids.remove(client_id);
        }
        self.advise(
            inner,
            Destination::topic(CONNECTION_ADVISORY),
            AdvisoryData::ConnectionRemoved {
                connection,
                client_id,
            },
        );
        inner.connections.remove(&connection);
        tracing::info!(connection, "connection closed");
    }

    pub fn client_id(&self, connection: ConnectionId) -> Option<String> {
        self.lock()
            .connections
            .get(&connection)
            .and_then(|c| c.info.client_id.clone())
    }

    pub fn principal(&self, connection: ConnectionId) -> Option<Principal> {
        self.lock().principal(connection).ok()
    }

    pub fn register_producer(
        &self,
        connection: ConnectionId,
        destination: Option<Destination>,
    ) -> Result<ProducerId, CoreError> {
        let mut inner = self.lock();
        let principal = inner.principal(connection)?;
        if let Some(destination) = &destination {
            self.check_access(&inner, &principal, destination, Permission::Write)?;
        }
        let id = inner.next_id();
        inner
            .producers
            .insert(id, (connection, destination.clone()));
        if let Some(state) = inner.connections.get_mut(&connection) {
            state.producers.insert(id);
        }
        if let Some(destination) = destination {
            self.advise(
                &mut inner,
                destination.producer_advisory_topic(),
                AdvisoryData::ProducerAdded {
                    connection,
                    producer: id,
                    destination: Some(destination.clone()),
                },
            );
        }
        self.flush_batch(&mut inner)?;
        Ok(id)
    }

    pub fn unregister_producer(&self, producer: ProducerId) {
        let mut inner = self.lock();
        self.unregister_producer_locked(&mut inner, producer);
        let _ = self.flush_batch(&mut inner);
    }

    fn unregister_producer_locked(&self, inner: &mut Inner, producer: ProducerId) {
        let Some((connection, destination)) = inner.producers.remove(&producer) else {
            return;
        };
        if let Some(state) = inner.connections.get_mut(&connection) {
            state.producers.remove(&producer);
        }
        if let Some(destination) = destination {
            self.advise(
                inner,
                destination.producer_advisory_topic(),
                AdvisoryData::ProducerRemoved {
                    connection,
                    producer,
                },
            );
        }
    }

    /// Registers a temporary destination owned by `connection`.
    pub fn create_temp(
        &self,
        connection: ConnectionId,
        destination: Destination,
    ) -> Result<(), CoreError> {
        if !destination.is_temporary() {
            return Err(CoreError::InvalidDestination(format!(
                "{destination} is not a temporary destination"
            )));
        }
        let mut inner = self.lock();
        let state = inner
            .connections
            .get(&connection)
            .ok_or_else(|| CoreError::IllegalState("connection is closed".to_owned()))?;
        if inner.temps.contains(&destination) {
            return if state.temps.contains(&destination) {
                Ok(())
            } else {
                Err(CoreError::InvalidDestination(format!(
                    "temporary destination {destination} is owned by another connection"
                )))
            };
        }
        let Some(state) = inner.connections.get_mut(&connection) else {
            return Err(CoreError::IllegalState("connection is closed".to_owned()));
        };
        state.temps.insert(destination.clone());
        if !inner.temps.insert(destination.clone()) {
            return Ok(());
        }
        if destination.kind == DestinationKind::TempQueue {
            self.ensure_queue(&mut inner, &destination);
        }
        self.advise(
            &mut inner,
            destination.destination_advisory_topic(),
            AdvisoryData::DestinationAdded(destination.clone()),
        );
        self.flush_batch(&mut inner)
    }

    /// Generates a name and creates a temporary destination.
    pub fn new_temp(
        &self,
        connection: ConnectionId,
        kind: DestinationKind,
    ) -> Result<Destination, CoreError> {
        let sequence = self.lock().next_id();
        let name = format!("ID:{}-{}:{}", self.broker_name(), connection, sequence);
        let destination = Destination::new(kind, name);
        self.create_temp(connection, destination.clone())?;
        Ok(destination)
    }

    pub fn delete_temp(
        &self,
        connection: ConnectionId,
        destination: &Destination,
    ) -> Result<(), CoreError> {
        let mut inner = self.lock();
        let in_use = inner.consumers.values().any(|consumer| {
            consumer.connection != connection && consumer.options.destination == *destination
        });
        if in_use {
            return Err(CoreError::IllegalState(
                "Destination still has an active subscription".to_owned(),
            ));
        }
        if let Some(state) = inner.connections.get_mut(&connection) {
            state.temps.remove(destination);
        }
        self.delete_temp_locked(&mut inner, destination);
        self.flush_batch(&mut inner)
    }

    pub(crate) fn delete_temp_locked(&self, inner: &mut Inner, destination: &Destination) {
        if !inner.temps.remove(destination) {
            return;
        }
        let consumers = inner
            .consumers
            .iter()
            .filter(|(_, consumer)| consumer.options.destination == *destination)
            .map(|(id, consumer)| (*id, consumer.connection))
            .collect::<Vec<_>>();
        for (consumer, connection) in consumers {
            self.close_consumer_locked(inner, consumer);
            inner.emit(
                connection,
                Event::ConsumerClosed {
                    consumer,
                    reason: "temporary destination deleted".to_owned(),
                },
            );
        }
        inner.queues.remove(&queue_key(destination));
        for state in inner.connections.values_mut() {
            state.temps.remove(destination);
        }
        self.advise(
            inner,
            destination.destination_advisory_topic(),
            AdvisoryData::DestinationRemoved(destination.clone()),
        );
    }

    /// Explicitly creates a queue or topic (requires admin permission).
    pub fn add_destination(
        &self,
        connection: ConnectionId,
        destination: &Destination,
    ) -> Result<(), CoreError> {
        if destination.is_temporary() {
            return self.create_temp(connection, destination.clone());
        }
        let mut inner = self.lock();
        let principal = inner.principal(connection)?;
        for component in destination.components() {
            if inner.destination_exists(&component) {
                continue;
            }
            self.security
                .authorize(&principal, &component, Permission::Admin)?;
            if component.is_topic() {
                self.ensure_topic(&mut inner, &component);
            } else {
                self.ensure_queue(&mut inner, &component);
            }
        }
        self.flush_batch(&mut inner)
    }

    /// Sends a message, optionally inside a transaction.
    pub fn send(
        &self,
        connection: ConnectionId,
        mut message: Message,
        tx: Option<&TxId>,
    ) -> Result<(), CoreError> {
        let mut inner = self.lock();
        let principal = inner.principal(connection)?;
        for component in message.destination.components() {
            if component.is_wildcard() {
                return Err(CoreError::InvalidDestination(format!(
                    "cannot send to wildcard destination {component}"
                )));
            }
        }
        self.check_access(&inner, &principal, &message.destination, Permission::Write)?;
        message.producer_connection = Some(connection);
        message.broker_in_time = now_millis();
        if message.timestamp == 0 {
            message.timestamp = message.broker_in_time;
        }
        if self.policy.scheduler_support
            && message.destination.is_topic()
            && message.destination.name == SCHEDULER_MANAGEMENT_TOPIC
        {
            self.scheduler_management(&mut inner, &message)?;
            return self.flush_batch(&mut inner);
        }
        if let Some(tx) = tx {
            let key = super::tx::tx_key(connection, tx);
            let transaction = inner
                .transactions
                .get_mut(&key)
                .ok_or_else(|| CoreError::Transaction(format!("unknown transaction {tx}")))?;
            if transaction.prepared {
                return Err(CoreError::Transaction(format!(
                    "transaction {tx} is already prepared"
                )));
            }
            transaction.sends.push(message);
            return Ok(());
        }
        let result = self.accept(&mut inner, message);
        self.flush_batch(&mut inner)?;
        result
    }

    /// Schedules or routes a message that has been accepted by the broker.
    pub(crate) fn accept(&self, inner: &mut Inner, message: Message) -> Result<(), CoreError> {
        if self.policy.scheduler_support && is_scheduled(&message) {
            self.schedule(inner, message)
        } else {
            self.route(inner, message)
        }
    }

    pub(crate) fn route(&self, inner: &mut Inner, message: Message) -> Result<(), CoreError> {
        let components = message.destination.components();
        if components.len() == 1 {
            return self.route_single(inner, message);
        }
        let mut first_error = None;
        for component in components {
            let mut copy = message.clone();
            copy.destination = component;
            if let Err(error) = self.route_single(inner, copy) {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn route_single(&self, inner: &mut Inner, mut message: Message) -> Result<(), CoreError> {
        let destination = message.destination.clone();
        if destination.is_temporary() && !inner.temps.contains(&destination) {
            return Err(CoreError::InvalidDestination(format!(
                "Cannot publish to a deleted Destination: {destination}"
            )));
        }
        if destination.is_queue() {
            let key = self.ensure_queue(inner, &destination);
            return self.enqueue(inner, &key, Arc::new(message));
        }
        self.ensure_topic(inner, &destination);
        if message
            .properties
            .remove(RETAIN_PROPERTY)
            .is_some_and(|value| value.as_bool().unwrap_or(false))
        {
            self.retain(inner, &message);
        }
        self.route_topic(inner, Arc::new(message));
        Ok(())
    }

    fn retain(&self, inner: &mut Inner, message: &Message) {
        let topic = message.destination.name.clone();
        if message.body.is_empty() {
            inner.retained.remove(&topic);
            if let Some(store) = &self.store
                && let Err(error) = store.remove_retained(&topic)
            {
                tracing::warn!(%error, "failed to remove retained message");
            }
            return;
        }
        let mut retained = message.clone();
        retained
            .properties
            .insert(RETAINED_PROPERTY.to_owned(), Value::Bool(true));
        if let Some(store) = &self.store
            && let Err(error) = store.put_retained(&topic, &retained)
        {
            tracing::warn!(%error, "failed to persist retained message");
        }
        inner.retained.insert(topic, retained);
    }

    /// Fans a topic message out to virtual topic queues and subscriptions.
    pub(crate) fn route_topic(&self, inner: &mut Inner, message: Arc<Message>) {
        let destination = &message.destination;
        if let Some(stats) = inner.topics.get_mut(&destination.name) {
            stats.enqueued += 1;
        }
        if self.policy.virtual_topics
            && destination.kind == DestinationKind::Topic
            && destination
                .name
                .starts_with(&self.policy.virtual_topic_prefix)
        {
            let pattern = format!(
                "{}*.{}",
                self.policy.virtual_topic_consumer_prefix, destination.name
            );
            let targets = inner
                .queues
                .values()
                .filter(|queue| {
                    matches!(queue.kind, QueueKind::Queue)
                        && queue.destination.kind == DestinationKind::Queue
                        && wildcard_match(&pattern, &queue.destination.name)
                })
                .map(|queue| (queue.key.clone(), queue.destination.clone()))
                .collect::<Vec<_>>();
            for (key, queue_destination) in targets {
                let mut copy = (*message).clone();
                copy.original_destination = Some(destination.clone());
                copy.destination = queue_destination;
                let _ = self.enqueue(inner, &key, Arc::new(copy));
            }
        }
        let subscriptions = inner
            .queues
            .values()
            .filter(|queue| {
                queue
                    .filter
                    .as_ref()
                    .is_some_and(|filter| filter.accepts(&message))
            })
            .map(|queue| queue.key.clone())
            .collect::<Vec<_>>();
        for key in subscriptions {
            let _ = self.enqueue(inner, &key, message.clone());
        }
    }

    /// Appends a message to a queue (persisting it when required) and dispatches.
    pub(crate) fn enqueue(
        &self,
        inner: &mut Inner,
        key: &str,
        message: Arc<Message>,
    ) -> Result<(), CoreError> {
        let seq = inner.next_seq;
        let Some(queue) = inner.queues.get_mut(key) else {
            return Err(CoreError::InvalidDestination(format!(
                "unknown destination {key}"
            )));
        };
        if matches!(queue.kind, QueueKind::Queue)
            && self.policy.max_queue_messages > 0
            && queue.pending.len() >= self.policy.max_queue_messages
        {
            return Err(CoreError::ResourceAllocation(format!(
                "Usage limit reached for {}: {} pending messages",
                queue.destination,
                queue.pending.len()
            )));
        }
        let persisted = message.persistent && queue.durable_storage();
        queue.push(Entry {
            seq,
            message: message.clone(),
            redelivered: false,
            persisted,
        });
        queue.enqueued += 1;
        inner.next_seq += 1;
        if persisted {
            inner.batch.put_message(key, seq, &message)?;
        }
        self.dispatch(inner, key);
        Ok(())
    }

    /// Delivers retained messages matching a new topic subscription.
    pub(crate) fn deliver_retained(&self, inner: &mut Inner, key: &str, pattern: &Destination) {
        let retained = inner
            .retained
            .values()
            .filter(|message| pattern.matches(&message.destination))
            .cloned()
            .collect::<Vec<_>>();
        for message in retained {
            let _ = self.enqueue(inner, key, Arc::new(message));
        }
    }
}
