//! Consumers: subscription, dispatch, acknowledgement and flow control.

use std::sync::Arc;

use super::broker::{BrokerCore, Consumer, Detached, InFlight, Inner};
use super::destination::{ADVISORY_PREFIX, Destination, DestinationKind};
use super::message::{AdvisoryData, Body, Message, now_millis};
use super::queue::{
    Entry, MessageQueue, QueueKind, SubscriptionFilter, durable_key, subscription_key,
};
use super::security::Permission;
use super::selector::parse_optional;
use super::store::DurableRecord;
use super::types::{
    AckOp, Acks, ConnectionId, ConsumerId, ConsumerOptions, CoreError, Delivery, Event, TxId,
};

impl BrokerCore {
    /// Replays the retained snapshot to an existing topic consumer without
    /// replacing its subscription, pending messages or in-flight deliveries.
    pub fn replay_retained(
        &self,
        connection: ConnectionId,
        consumer: ConsumerId,
    ) -> Result<(), CoreError> {
        let mut inner = self.write_lock()?;
        let principal = inner.principal(connection)?;
        let state = inner
            .consumers
            .get(&consumer)
            .filter(|state| state.connection == connection)
            .ok_or_else(|| CoreError::NotFound(format!("consumer {consumer}")))?;
        let destination = state.options.destination.clone();
        if !destination.is_topic() || state.options.browser {
            return Err(CoreError::IllegalState(
                "retained replay requires a topic subscription".into(),
            ));
        }
        self.check_access(&inner, &principal, &destination, Permission::Read)?;
        let keys = state.queues.clone();
        let now = now_millis();
        let retained = inner
            .retained
            .values()
            .filter(|message| !message.is_expired(now))
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            for message in &retained {
                if inner
                    .queues
                    .get(&key)
                    .and_then(|queue| queue.filter.as_ref())
                    .is_some_and(|filter| filter.accepts(message))
                {
                    let mut copy = message.clone();
                    // A replay is a new delivery, not a retransmission of an
                    // outstanding MQTT packet with the retained message's ID.
                    copy.message_id = super::message::generate_message_id();
                    self.enqueue(&mut inner, &key, Arc::new(copy))?;
                }
            }
        }
        self.flush_batch(inner)
    }

    pub(crate) fn advisory_message(&self, topic: Destination, data: AdvisoryData) -> Message {
        let mut message = Message::new(topic, Body::Empty);
        message.persistent = false;
        message.message_type = Some("Advisory".to_owned());
        message.properties.insert(
            "originBrokerName".to_owned(),
            self.broker_name().to_owned().into(),
        );
        message.properties.insert(
            "originBrokerId".to_owned(),
            self.broker_id().to_owned().into(),
        );
        message.advisory = Some(data);
        message
    }

    fn consumer_added_data(
        &self,
        inner: &Inner,
        id: ConsumerId,
        consumer: &Consumer,
    ) -> AdvisoryData {
        AdvisoryData::ConsumerAdded {
            connection: consumer.connection,
            consumer: id,
            external_id: consumer.options.external_id.clone(),
            destination: consumer.options.destination.clone(),
            selector: consumer.options.selector.clone(),
            durable_name: consumer.options.durable_name.clone(),
            client_id: inner
                .connections
                .get(&consumer.connection)
                .and_then(|c| c.info.client_id.clone()),
            network: consumer.options.network,
            broker_path: consumer.options.broker_path.clone(),
            prefetch: consumer.options.prefetch,
        }
    }

    /// Creates a consumer (queue receiver, topic subscriber, durable
    /// subscriber or queue browser).
    pub fn subscribe(
        &self,
        connection: ConnectionId,
        options: ConsumerOptions,
    ) -> Result<ConsumerId, CoreError> {
        let selector = parse_optional(options.selector.as_deref())
            .map_err(|error| CoreError::InvalidSelector(error.to_string()))?;
        let mut inner = self.write_lock()?;
        let principal = inner.principal(connection)?;
        let destination = options.destination.clone();
        self.check_access(&inner, &principal, &destination, Permission::Read)?;
        for component in destination.components() {
            if component.is_temporary()
                && !component.is_wildcard()
                && !inner.temps.contains(&component)
            {
                return Err(CoreError::InvalidDestination(format!(
                    "Cannot subscribe to a deleted Destination: {component}"
                )));
            }
        }
        let id = inner.next_id();
        let mut consumer = Consumer {
            connection,
            options: options.clone(),
            selector,
            queues: Vec::new(),
            in_flight: Default::default(),
            window_used: 0,
            credit: 0,
            pull_deadline: None,
            durable: None,
        };

        if options.browser {
            if destination.is_topic() {
                return Err(CoreError::InvalidDestination(
                    "queue browsers require a queue destination".to_owned(),
                ));
            }
            let now = now_millis();
            let snapshot = inner
                .queues
                .values()
                .filter(|queue| {
                    matches!(queue.kind, QueueKind::Queue)
                        && destination.matches(&queue.destination)
                })
                .flat_map(|queue| queue.pending.values())
                .filter(|entry| !entry.message.is_expired(now))
                .filter(|entry| {
                    consumer
                        .selector
                        .as_ref()
                        .is_none_or(|s| s.matches(&entry.message))
                })
                .map(|entry| (entry.message.clone(), entry.redelivered))
                .collect::<Vec<_>>();
            inner.consumers.insert(id, consumer);
            if let Some(state) = inner.connections.get_mut(&connection) {
                state.consumers.insert(id);
            }
            for (message, redelivered) in snapshot {
                let delivery_id = inner.next_id();
                inner.emit(
                    connection,
                    Event::Deliver(Delivery {
                        consumer: id,
                        delivery_id,
                        message,
                        redelivered,
                    }),
                );
            }
            inner.emit(connection, Event::BrowseDone { consumer: id });
            self.flush_batch(inner)?;
            return Ok(id);
        }

        let mut keys = Vec::new();
        if destination.is_queue() {
            for component in destination.components() {
                if component.is_wildcard() {
                    keys.extend(
                        inner
                            .queues
                            .values()
                            .filter(|queue| {
                                matches!(queue.kind, QueueKind::Queue)
                                    && component.matches(&queue.destination)
                            })
                            .map(|queue| queue.key.clone()),
                    );
                } else {
                    keys.push(self.ensure_queue(&mut inner, &component));
                }
            }
        } else if let Some(name) = options.durable_name.clone() {
            let client_id = inner
                .connections
                .get(&connection)
                .and_then(|c| c.info.client_id.clone())
                .ok_or_else(|| {
                    CoreError::IllegalState(
                        "a client id is required for durable subscriptions".to_owned(),
                    )
                })?;
            let durable = (client_id.clone(), name.clone());
            if inner
                .consumers
                .values()
                .any(|other| other.durable.as_ref() == Some(&durable))
            {
                return Err(CoreError::IllegalState(format!(
                    "Durable consumer is in use for client: {client_id} and subscriptionName: {name}"
                )));
            }
            let record = DurableRecord {
                client_id: client_id.clone(),
                name: name.clone(),
                destination: destination.clone(),
                selector: consumer.selector.as_ref().map(|s| s.source().to_owned()),
                no_local: options.no_local,
            };
            let key = durable_key(&client_id, &name);
            let changed = inner
                .durables
                .get(&durable)
                .is_some_and(|existing| *existing != record);
            if changed {
                tracing::info!(client_id, name, "durable subscription changed; recreating");
                self.drop_durable_queue(&mut inner, &durable)?;
            }
            let created = !inner.durables.contains_key(&durable);
            if created {
                if self.store.is_some() {
                    inner.batch.put_durable(&record)?;
                }
                inner.durables.insert(durable.clone(), record);
                inner.queues.insert(
                    key.clone(),
                    MessageQueue::new(key.clone(), destination.clone(), QueueKind::Durable),
                );
            }
            if let Some(queue) = inner.queues.get_mut(&key) {
                queue.filter = Some(SubscriptionFilter {
                    pattern: destination.clone(),
                    selector: consumer.selector.clone(),
                    no_local_connection: options.no_local.then_some(connection),
                });
            }
            if created {
                self.deliver_retained(&mut inner, &key, &destination);
            }
            if (options.retroactive || self.policy.always_retroactive)
                && inner
                    .queues
                    .get(&key)
                    .is_some_and(|queue| queue.pending.is_empty() && queue.in_flight == 0)
            {
                self.recover_topic_history(&mut inner, &key)?;
            }
            consumer.durable = Some(durable);
            keys.push(key);
        } else {
            let key = subscription_key(id);
            let mut queue = MessageQueue::new(
                key.clone(),
                destination.clone(),
                QueueKind::TopicSubscription,
            );
            queue.filter = Some(SubscriptionFilter {
                pattern: destination.clone(),
                selector: consumer.selector.clone(),
                no_local_connection: options.no_local.then_some(connection),
            });
            inner.queues.insert(key.clone(), queue);
            for component in destination.components() {
                if component.kind == DestinationKind::Topic && !component.is_wildcard() {
                    self.ensure_topic(&mut inner, &component);
                }
            }
            self.replay_advisories(&mut inner, &key, &destination);
            self.deliver_retained(&mut inner, &key, &destination);
            if options.retroactive || self.policy.always_retroactive {
                self.recover_topic_history(&mut inner, &key)?;
            }
            keys.push(key);
        }

        keys.sort();
        keys.dedup();
        for key in &keys {
            if let Some(queue) = inner.queues.get_mut(key) {
                queue.consumers.push(id);
            }
        }
        consumer.queues = keys.clone();
        let advisory =
            (!destination.is_advisory()).then(|| self.consumer_added_data(&inner, id, &consumer));
        inner.consumers.insert(id, consumer);
        if let Some(state) = inner.connections.get_mut(&connection) {
            state.consumers.insert(id);
        }
        if let Some(data) = advisory {
            self.advise(&mut inner, destination.consumer_advisory_topic(), data);
        }
        tracing::debug!(consumer = id, connection, destination = %destination, "consumer added");
        for key in &keys {
            self.dispatch(&mut inner, key);
        }
        self.flush_batch(inner)?;
        Ok(id)
    }

    fn recover_topic_history(&self, inner: &mut Inner, key: &str) -> Result<(), CoreError> {
        let Some(filter) = inner
            .queues
            .get(key)
            .and_then(|queue| queue.filter.as_ref())
        else {
            return Ok(());
        };
        let now = now_millis();
        let messages = inner
            .topics
            .values()
            .flat_map(|topic| &topic.recovery)
            .filter(|message| !message.is_expired(now) && filter.accepts(message))
            .cloned()
            .collect::<Vec<_>>();
        for message in messages {
            self.enqueue(inner, key, message)?;
        }
        Ok(())
    }

    /// Replays the current state to a new advisory subscriber, like ActiveMQ
    /// does for consumer and destination advisories.
    fn replay_advisories(&self, inner: &mut Inner, key: &str, pattern: &Destination) {
        if !self.policy.advisory_support
            || !pattern.components().iter().any(|component| {
                component.name.starts_with(ADVISORY_PREFIX) || component.is_wildcard()
            })
        {
            return;
        }
        let mut messages = Vec::new();
        let mut consumers = inner.consumers.iter().collect::<Vec<_>>();
        consumers.sort_by_key(|(id, _)| **id);
        for (id, consumer) in consumers {
            if consumer.options.browser || consumer.options.destination.is_advisory() {
                continue;
            }
            let topic = consumer.options.destination.consumer_advisory_topic();
            if pattern.matches(&topic) {
                messages.push(
                    self.advisory_message(topic, self.consumer_added_data(inner, *id, consumer)),
                );
            }
        }
        let mut destinations = inner.temps.iter().cloned().collect::<Vec<_>>();
        destinations.extend(
            inner
                .queues
                .values()
                .filter(|queue| {
                    matches!(queue.kind, QueueKind::Queue) && !queue.destination.is_temporary()
                })
                .map(|queue| queue.destination.clone()),
        );
        destinations.extend(
            inner
                .topics
                .keys()
                .map(|name| Destination::topic(name.clone())),
        );
        for destination in destinations {
            let topic = destination.destination_advisory_topic();
            if pattern.matches(&topic) {
                messages.push(
                    self.advisory_message(topic, AdvisoryData::DestinationAdded(destination)),
                );
            }
        }
        for message in messages {
            let _ = self.enqueue(inner, key, Arc::new(message));
        }
    }

    pub fn close_consumer(&self, consumer: ConsumerId) {
        let mut inner = self.lock();
        self.close_consumer_locked(&mut inner, consumer);
        if let Err(error) = self.flush_batch(inner) {
            tracing::warn!(%error, "failed to persist consumer close");
        }
    }

    pub(crate) fn close_consumer_locked(&self, inner: &mut Inner, id: ConsumerId) {
        let Some(consumer) = inner.consumers.remove(&id) else {
            return;
        };
        if let Some(state) = inner.connections.get_mut(&consumer.connection) {
            state.consumers.remove(&id);
        }
        for key in &consumer.queues {
            if let Some(queue) = inner.queues.get_mut(key) {
                queue.consumers.retain(|other| *other != id);
                queue.groups.retain(|_, owner| *owner != id);
                if let Some(filter) = queue.filter.as_mut()
                    && filter.no_local_connection == Some(consumer.connection)
                    && matches!(queue.kind, QueueKind::Durable)
                {
                    filter.no_local_connection = None;
                }
            }
        }
        for (_, flight) in consumer.in_flight {
            if let Some(tx_key) = flight.tx.clone()
                && let Some(transaction) = inner.transactions.get_mut(&tx_key)
            {
                transaction.detached.push(Detached {
                    queue: flight.queue.clone(),
                    seq: flight.seq,
                    message: flight.message.clone(),
                    persisted: flight.persisted,
                });
                continue;
            }
            self.return_to_queue(inner, flight, true, false);
        }
        for key in &consumer.queues {
            let remove = inner
                .queues
                .get(key)
                .is_some_and(|queue| matches!(queue.kind, QueueKind::TopicSubscription));
            if remove {
                inner.queues.remove(key);
            }
        }
        if !consumer.options.browser && !consumer.options.destination.is_advisory() {
            self.advise(
                inner,
                consumer.options.destination.consumer_advisory_topic(),
                AdvisoryData::ConsumerRemoved {
                    connection: consumer.connection,
                    consumer: id,
                    external_id: consumer.options.external_id.clone(),
                    destination: consumer.options.destination.clone(),
                },
            );
        }
        for key in &consumer.queues {
            self.dispatch(inner, key);
        }
    }

    /// Puts an in-flight message back on its queue for redelivery.
    pub(crate) fn return_to_queue(
        &self,
        inner: &mut Inner,
        flight: InFlight,
        increment: bool,
        dead_letter_check: bool,
    ) {
        let mut message = (*flight.message).clone();
        if increment {
            message.redelivery_counter += 1;
        }
        let Some(queue) = inner.queues.get_mut(&flight.queue) else {
            if flight.persisted {
                inner.batch.remove_message(&flight.queue, flight.seq);
            }
            return;
        };
        queue.in_flight = queue.in_flight.saturating_sub(1);
        if matches!(queue.kind, QueueKind::TopicSubscription)
            && !inner
                .consumers
                .values()
                .any(|c| c.queues.contains(&flight.queue))
        {
            return;
        }
        if dead_letter_check && message.redelivery_counter > self.policy.max_redeliveries {
            queue.dequeued += 1;
            if flight.persisted {
                inner.batch.remove_message(&flight.queue, flight.seq);
            }
            let cause = format!(
                "Delivery[{}] exceeds redelivery policy limit: {}",
                message.redelivery_counter, self.policy.max_redeliveries
            );
            self.dead_letter(inner, &message, &cause);
            return;
        }
        queue.push(Entry {
            seq: flight.seq,
            message: Arc::new(message),
            redelivered: true,
            persisted: flight.persisted,
        });
    }

    /// Dispatches pending messages of a queue to consumers with credit.
    pub(crate) fn dispatch(&self, inner: &mut Inner, key: &str) {
        let Some(mut queue) = inner.queues.remove(key) else {
            return;
        };
        let now = now_millis();
        let mut expired = Vec::new();
        if !queue.consumers.is_empty() {
            let is_queue = matches!(queue.kind, QueueKind::Queue);
            let exclusive_owner = queue
                .consumers
                .iter()
                .filter_map(|id| inner.consumers.get(id).map(|c| (*id, c)))
                .filter(|(_, c)| c.options.exclusive)
                .max_by_key(|(id, c)| (c.options.priority, std::cmp::Reverse(*id)))
                .map(|(id, _)| id);
            let pending_keys = queue.pending.keys().copied().collect::<Vec<_>>();
            for pending_key in pending_keys {
                let has_credit = queue
                    .consumers
                    .iter()
                    .any(|id| inner.consumers.get(id).is_some_and(|c| c.available() > 0));
                if !has_credit {
                    break;
                }
                let entry = &queue.pending[&pending_key];
                if entry.message.is_expired(now) {
                    expired.push(pending_key);
                    continue;
                }
                let message = entry.message.clone();
                let group_owner = message
                    .group_id
                    .as_ref()
                    .and_then(|group| queue.groups.get(group))
                    .copied()
                    .filter(|owner| queue.consumers.contains(owner));
                let eligible = |id: &ConsumerId| -> bool {
                    let Some(consumer) = inner.consumers.get(id) else {
                        return false;
                    };
                    if consumer.available() == 0 {
                        return false;
                    }
                    if exclusive_owner.is_some_and(|owner| owner != *id) {
                        return false;
                    }
                    if group_owner.is_some_and(|owner| owner != *id) {
                        return false;
                    }
                    if is_queue
                        && let Some(selector) = &consumer.selector
                        && !selector.matches(&message)
                    {
                        return false;
                    }
                    if consumer.options.network {
                        if consumer
                            .options
                            .broker_path
                            .iter()
                            .any(|broker| message.broker_path.contains(broker))
                        {
                            return false;
                        }
                        if let Some(ttl) = consumer.options.network_ttl
                            && message.broker_path.len() as u32 >= ttl
                        {
                            return false;
                        }
                    }
                    true
                };
                let candidates = queue
                    .consumers
                    .iter()
                    .copied()
                    .filter(eligible)
                    .collect::<Vec<_>>();
                if candidates.is_empty() {
                    continue;
                }
                let best_priority = candidates
                    .iter()
                    .filter_map(|id| inner.consumers.get(id))
                    .map(|c| c.options.priority - if c.options.network { 1 } else { 0 })
                    .max()
                    .unwrap_or(0);
                let top = candidates
                    .into_iter()
                    .filter(|id| {
                        inner.consumers.get(id).is_some_and(|c| {
                            c.options.priority - if c.options.network { 1 } else { 0 }
                                >= best_priority
                        })
                    })
                    .collect::<Vec<_>>();
                let chosen = top[queue.round_robin % top.len()];
                queue.round_robin = queue.round_robin.wrapping_add(1);
                let entry = queue.pending.remove(&pending_key).expect("pending entry");
                if let Some(group) = &message.group_id {
                    if message.group_sequence == -1 {
                        queue.groups.remove(group);
                    } else {
                        queue.groups.insert(group.clone(), chosen);
                    }
                }
                queue.dispatched += 1;
                queue.in_flight += 1;
                let delivery_id = inner.next_id();
                let consumer = inner.consumers.get_mut(&chosen).expect("eligible consumer");
                if consumer.options.credit_mode {
                    consumer.credit -= 1;
                } else {
                    consumer.window_used += 1;
                    if consumer.options.prefetch == 0 {
                        consumer.pull_deadline = None;
                    }
                }
                consumer.in_flight.insert(
                    delivery_id,
                    InFlight {
                        queue: key.to_owned(),
                        seq: entry.seq,
                        message: entry.message.clone(),
                        persisted: entry.persisted,
                        window_freed: false,
                        tx: None,
                    },
                );
                let connection = consumer.connection;
                inner.emit(
                    connection,
                    Event::Deliver(Delivery {
                        consumer: chosen,
                        delivery_id,
                        redelivered: entry.redelivered || entry.message.redelivery_counter > 0,
                        message: entry.message,
                    }),
                );
            }
        }
        let mut dead = Vec::new();
        for pending_key in expired {
            if let Some(entry) = queue.pending.remove(&pending_key) {
                queue.expired += 1;
                queue.dequeued += 1;
                if entry.persisted {
                    inner.batch.remove_message(key, entry.seq);
                }
                dead.push(entry.message);
            }
        }
        inner.queues.insert(key.to_owned(), queue);
        if self.policy.dead_letter_expired {
            for message in dead {
                self.dead_letter(inner, &message, "message expired");
            }
        }
    }

    /// Acknowledges deliveries of a consumer.
    pub fn ack(
        &self,
        connection: ConnectionId,
        consumer: ConsumerId,
        acks: Acks,
        op: AckOp,
        tx: Option<&TxId>,
    ) -> Result<(), CoreError> {
        let mut inner = self.write_lock()?;
        let Some(state) = inner.consumers.get(&consumer) else {
            return Ok(());
        };
        if state.connection != connection {
            return Err(CoreError::IllegalState(
                "consumer belongs to another connection".to_owned(),
            ));
        }
        let deliveries = resolve_deliveries(state, &acks);
        if deliveries.is_empty() {
            return Ok(());
        }
        let tx_key = tx.map(|tx| super::tx::tx_key(connection, tx));
        if op == AckOp::Consumed
            && let Some(key) = &tx_key
        {
            let Some(transaction) = inner.transactions.get_mut(key) else {
                return Err(CoreError::Transaction(format!(
                    "unknown transaction {}",
                    key.1
                )));
            };
            for delivery in &deliveries {
                transaction.acks.push((consumer, *delivery));
            }
        }
        let state = inner.consumers.get_mut(&consumer).expect("consumer");
        let mut removed = Vec::new();
        for delivery in deliveries {
            let Some(flight) = state.in_flight.get_mut(&delivery) else {
                continue;
            };
            if flight.tx.is_some() {
                continue;
            }
            if !flight.window_freed {
                flight.window_freed = true;
                if !state.options.credit_mode {
                    state.window_used = state.window_used.saturating_sub(1);
                }
            }
            match op {
                AckOp::Delivered => {}
                AckOp::Consumed if tx_key.is_some() => flight.tx = tx_key.clone(),
                _ => {
                    if let Some(flight) = state.in_flight.remove(&delivery) {
                        removed.push(flight);
                    }
                }
            }
        }
        let queues = state.queues.clone();
        for flight in removed {
            match op {
                AckOp::Release { increment } => {
                    self.return_to_queue(&mut inner, flight, increment, true)
                }
                AckOp::Reject => {
                    self.consume_flight(&mut inner, &flight);
                    self.dead_letter(&mut inner, &flight.message, "poison ack");
                }
                _ => self.consume_flight(&mut inner, &flight),
            }
        }
        for key in &queues {
            self.dispatch(&mut inner, key);
        }
        self.flush_batch(inner)
    }

    /// Removes a consumed message from its queue statistics and the store.
    pub(crate) fn consume_flight(&self, inner: &mut Inner, flight: &InFlight) {
        if let Some(queue) = inner.queues.get_mut(&flight.queue) {
            queue.dequeued += 1;
            queue.in_flight = queue.in_flight.saturating_sub(1);
        }
        if flight.persisted {
            inner.batch.remove_message(&flight.queue, flight.seq);
        }
    }

    /// Requests one message for a prefetch-0 consumer. `timeout` follows
    /// ActiveMQ's MessagePull: -1 = no wait, 0 = wait forever, >0 = millis.
    pub fn pull(&self, consumer: ConsumerId, timeout: i64) {
        let mut inner = self.lock();
        let Some(state) = inner.consumers.get_mut(&consumer) else {
            return;
        };
        state.pull_deadline = Some(match timeout {
            0 => i64::MAX,
            t if t < 0 => 0,
            t => now_millis() + t,
        });
        let queues = state.queues.clone();
        for key in &queues {
            self.dispatch(&mut inner, key);
        }
        if timeout < 0
            && let Some(state) = inner.consumers.get_mut(&consumer)
            && state.pull_deadline.take().is_some()
        {
            let connection = state.connection;
            inner.emit(connection, Event::PullEmpty { consumer });
        }
        let _ = self.flush_batch(inner);
    }

    /// Sets the absolute link credit of a credit-mode consumer.
    pub fn set_credit(&self, consumer: ConsumerId, credit: u32) {
        let mut inner = self.lock();
        let Some(state) = inner.consumers.get_mut(&consumer) else {
            return;
        };
        state.credit = credit;
        let queues = state.queues.clone();
        for key in &queues {
            self.dispatch(&mut inner, key);
        }
        let _ = self.flush_batch(inner);
    }

    /// Dispatches what is available, then discards the remaining credit.
    pub fn drain(&self, consumer: ConsumerId) {
        let mut inner = self.lock();
        let Some(state) = inner.consumers.get(&consumer) else {
            return;
        };
        let queues = state.queues.clone();
        for key in &queues {
            self.dispatch(&mut inner, key);
        }
        if let Some(state) = inner.consumers.get_mut(&consumer) {
            let credit = std::mem::take(&mut state.credit);
            let connection = state.connection;
            inner.emit(connection, Event::Drained { consumer, credit });
        }
        let _ = self.flush_batch(inner);
    }

    /// Deletes a durable subscription of the connection's client id.
    pub fn remove_durable(&self, connection: ConnectionId, name: &str) -> Result<(), CoreError> {
        let mut inner = self.write_lock()?;
        let client_id = inner
            .connections
            .get(&connection)
            .and_then(|c| c.info.client_id.clone())
            .ok_or_else(|| CoreError::IllegalState("a client id is required".to_owned()))?;
        self.remove_durable_locked(&mut inner, &client_id, name)?;
        self.flush_batch(inner)
    }

    pub(crate) fn remove_durable_locked(
        &self,
        inner: &mut Inner,
        client_id: &str,
        name: &str,
    ) -> Result<(), CoreError> {
        let durable = (client_id.to_owned(), name.to_owned());
        if inner
            .consumers
            .values()
            .any(|consumer| consumer.durable.as_ref() == Some(&durable))
        {
            return Err(CoreError::IllegalState(format!(
                "Durable consumer is in use for client: {client_id} and subscriptionName: {name}"
            )));
        }
        if !inner.durables.contains_key(&durable) {
            return Err(CoreError::NotFound(format!(
                "No durable subscription exists for clientID: {client_id} and subscriptionName: {name}"
            )));
        }
        self.drop_durable_queue(inner, &durable)
    }

    fn drop_durable_queue(
        &self,
        inner: &mut Inner,
        durable: &(String, String),
    ) -> Result<(), CoreError> {
        let key = durable_key(&durable.0, &durable.1);
        if let Some(queue) = inner.queues.remove(&key) {
            for entry in queue.pending.values() {
                if entry.persisted {
                    inner.batch.remove_message(&key, entry.seq);
                }
            }
        }
        inner.durables.remove(durable);
        if self.store.is_some() {
            inner.batch.remove_durable(&durable.0, &durable.1);
        }
        Ok(())
    }
}

fn resolve_deliveries(consumer: &Consumer, acks: &Acks) -> Vec<u64> {
    match acks {
        Acks::Deliveries(ids) => ids
            .iter()
            .copied()
            .filter(|id| consumer.in_flight.contains_key(id))
            .collect(),
        Acks::UpTo(last) => consumer
            .in_flight
            .range(..=*last)
            .map(|(id, _)| *id)
            .collect(),
        Acks::All => consumer.in_flight.keys().copied().collect(),
        Acks::MessageId { id, up_to } => {
            let Some(found) = consumer
                .in_flight
                .iter()
                .find(|(_, flight)| flight.message.message_id == *id)
                .map(|(delivery, _)| *delivery)
            else {
                return Vec::new();
            };
            if *up_to {
                consumer
                    .in_flight
                    .range(..=found)
                    .map(|(id, _)| *id)
                    .collect()
            } else {
                vec![found]
            }
        }
    }
}
