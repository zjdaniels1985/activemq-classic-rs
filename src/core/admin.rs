//! Administrative operations used by the web console.

use std::sync::Arc;

use serde::Serialize;

use super::broker::{BrokerCore, LockMetrics};
use super::destination::Destination;
use super::message::{Message, now_millis};
use super::queue::{QueueKind, durable_key, queue_key};
use super::store::StoreMetrics;
use super::types::{
    ConnectionId, ConnectionStats, ConsumerStats, CoreError, DestinationStats, Event, JobStats,
    MessageView, SubscriptionStats,
};

#[derive(Clone, Debug, Serialize)]
pub struct BrokerInfo {
    pub name: String,
    pub id: String,
    pub connections: usize,
    pub consumers: usize,
    pub producers: usize,
    pub queues: usize,
    pub topics: usize,
    pub durable_subscriptions: usize,
    pub scheduled_jobs: usize,
    pub prepared_transactions: usize,
    pub store_size_bytes: u64,
    pub persistent: bool,
    pub time: i64,
    pub state_lock: LockMetrics,
    pub storage: Option<StoreMetrics>,
}

impl BrokerCore {
    pub fn info(&self) -> BrokerInfo {
        // Filesystem accounting can block; never do it under the state mutex.
        let store_size_bytes = self.store.as_ref().map_or(0, |s| s.size_on_disk());
        let inner = self.lock();
        BrokerInfo {
            name: self.broker_name().to_owned(),
            id: self.broker_id().to_owned(),
            connections: inner.connections.len(),
            consumers: inner.consumers.len(),
            producers: inner.producers.len(),
            queues: inner
                .queues
                .values()
                .filter(|q| matches!(q.kind, QueueKind::Queue))
                .count(),
            topics: inner.topics.len(),
            durable_subscriptions: inner.durables.len(),
            scheduled_jobs: inner.jobs.len(),
            prepared_transactions: inner.transactions.values().filter(|t| t.prepared).count(),
            store_size_bytes,
            persistent: self.store.is_some(),
            time: now_millis(),
            state_lock: self.lock_metrics(),
            storage: self.store.as_ref().map(|store| store.metrics()),
        }
    }

    pub fn destinations(&self) -> Vec<DestinationStats> {
        let inner = self.lock();
        let producers_on = |destination: &Destination| {
            inner
                .producers
                .values()
                .filter(|(_, d)| d.as_ref() == Some(destination))
                .count()
        };
        let mut stats = inner
            .queues
            .values()
            .filter(|queue| matches!(queue.kind, QueueKind::Queue))
            .map(|queue| DestinationStats {
                destination: queue.destination.clone(),
                uri: queue.destination.to_uri(),
                pending: queue.pending.len(),
                in_flight: queue.in_flight,
                consumers: queue.consumers.len(),
                producers: producers_on(&queue.destination),
                enqueued: queue.enqueued,
                dequeued: queue.dequeued,
                dispatched: queue.dispatched,
                expired: queue.expired,
            })
            .collect::<Vec<_>>();
        let mut topics = inner
            .topics
            .keys()
            .map(|name| Destination::topic(name.clone()))
            .collect::<Vec<_>>();
        topics.extend(inner.temps.iter().filter(|d| d.is_topic()).cloned());
        for topic in topics {
            let subscriptions = inner
                .queues
                .values()
                .filter(|queue| {
                    !matches!(queue.kind, QueueKind::Queue) && queue.destination.matches(&topic)
                })
                .collect::<Vec<_>>();
            stats.push(DestinationStats {
                uri: topic.to_uri(),
                pending: subscriptions.iter().map(|q| q.pending.len()).sum(),
                in_flight: subscriptions.iter().map(|q| q.in_flight).sum(),
                consumers: subscriptions.iter().map(|q| q.consumers.len()).sum(),
                producers: producers_on(&topic),
                enqueued: inner.topics.get(&topic.name).map_or(0, |t| t.enqueued),
                dequeued: subscriptions.iter().map(|q| q.dequeued).sum(),
                dispatched: subscriptions.iter().map(|q| q.dispatched).sum(),
                expired: subscriptions.iter().map(|q| q.expired).sum(),
                destination: topic,
            });
        }
        stats.sort_by(|a, b| a.uri.cmp(&b.uri));
        stats
    }

    pub fn connections(&self) -> Vec<ConnectionStats> {
        let inner = self.lock();
        let mut stats = inner
            .connections
            .iter()
            .filter(|(_, c)| c.info.protocol != "internal")
            .map(|(id, c)| ConnectionStats {
                id: *id,
                client_id: c.info.client_id.clone(),
                username: c.principal.username.clone(),
                protocol: c.info.protocol.clone(),
                remote_address: c.info.remote_address.clone(),
                connected_at: c.connected_at,
                consumers: c.consumers.len(),
                producers: c.producers.len(),
            })
            .collect::<Vec<_>>();
        stats.sort_by_key(|c| c.id);
        stats
    }

    pub fn consumers(&self) -> Vec<ConsumerStats> {
        let inner = self.lock();
        let mut stats = inner
            .consumers
            .iter()
            .map(|(id, c)| ConsumerStats {
                id: *id,
                connection: c.connection,
                destination: c.options.destination.to_uri(),
                selector: c.options.selector.clone(),
                prefetch: c.options.prefetch,
                in_flight: c.in_flight.len(),
                durable_name: c.options.durable_name.clone(),
                network: c.options.network,
            })
            .collect::<Vec<_>>();
        stats.sort_by_key(|c| c.id);
        stats
    }

    pub fn durable_subscriptions(&self) -> Vec<SubscriptionStats> {
        let inner = self.lock();
        inner
            .durables
            .iter()
            .map(|((client_id, name), record)| {
                let durable = (client_id.clone(), name.clone());
                SubscriptionStats {
                    client_id: client_id.clone(),
                    name: name.clone(),
                    destination: record.destination.to_uri(),
                    selector: record.selector.clone(),
                    active: inner
                        .consumers
                        .values()
                        .any(|c| c.durable.as_ref() == Some(&durable)),
                    pending: inner
                        .queues
                        .get(&durable_key(client_id, name))
                        .map_or(0, |q| q.pending.len()),
                }
            })
            .collect()
    }

    pub fn delete_durable(&self, client_id: &str, name: &str) -> Result<(), CoreError> {
        let mut inner = self.write_lock()?;
        self.remove_durable_locked(&mut inner, client_id, name)?;
        self.flush_batch(inner)
    }

    fn queue_keys_for(&self, destination: &Destination) -> Vec<String> {
        let inner = self.lock();
        if destination.is_queue() {
            vec![queue_key(destination)]
        } else {
            inner
                .queues
                .values()
                .filter(|q| {
                    !matches!(q.kind, QueueKind::Queue) && q.destination.matches(destination)
                })
                .map(|q| q.key.clone())
                .collect()
        }
    }

    /// Lists up to `limit` pending messages of a queue (or topic subscriptions).
    pub fn browse(
        &self,
        destination: &Destination,
        limit: usize,
    ) -> Result<Vec<MessageView>, CoreError> {
        let keys = self.queue_keys_for(destination);
        let inner = self.lock();
        if destination.is_queue() && !inner.queues.contains_key(&keys[0]) {
            return Err(CoreError::NotFound(format!("{destination} does not exist")));
        }
        Ok(keys
            .iter()
            .filter_map(|key| inner.queues.get(key))
            .flat_map(|queue| queue.pending.values())
            .take(limit)
            .map(|entry| MessageView::from_message(&entry.message))
            .collect())
    }

    /// Returns a pending message by id.
    pub fn get_message(&self, destination: &Destination, id: &str) -> Option<Arc<Message>> {
        let inner = self.lock();
        inner
            .queues
            .get(&queue_key(destination))?
            .pending
            .values()
            .find(|entry| entry.message.message_id == id)
            .map(|entry| entry.message.clone())
    }

    /// Removes every pending message. Returns the number removed.
    pub fn purge(&self, destination: &Destination) -> Result<usize, CoreError> {
        let keys = self.queue_keys_for(destination);
        let mut inner = self.write_lock()?;
        let mut removed = 0;
        for key in keys {
            let Some(queue) = inner.queues.get_mut(&key) else {
                continue;
            };
            let entries = std::mem::take(&mut queue.pending);
            queue.dequeued += entries.len() as u64;
            removed += entries.len();
            for entry in entries.values() {
                if entry.persisted {
                    inner.batch.remove_message(&key, entry.seq);
                }
            }
        }
        self.flush_batch(inner)?;
        tracing::info!(%destination, removed, "destination purged");
        Ok(removed)
    }

    pub fn create_destination(&self, destination: &Destination) -> Result<(), CoreError> {
        let mut inner = self.write_lock()?;
        if destination.is_topic() {
            self.ensure_topic(&mut inner, destination);
        } else {
            self.ensure_queue(&mut inner, destination);
        }
        self.flush_batch(inner)
    }

    /// Deletes a queue or topic, closing its consumers.
    pub fn delete_destination(&self, destination: &Destination) -> Result<(), CoreError> {
        let mut inner = self.write_lock()?;
        if !inner.destination_exists(destination) {
            return Err(CoreError::NotFound(format!("{destination} does not exist")));
        }
        if destination.is_temporary() {
            self.delete_temp_locked(&mut inner, destination);
            return self.flush_batch(inner);
        }
        let consumers = inner
            .consumers
            .iter()
            .filter(|(_, c)| c.options.destination == *destination && c.durable.is_none())
            .map(|(id, c)| (*id, c.connection))
            .collect::<Vec<_>>();
        for (consumer, connection) in consumers {
            self.close_consumer_locked(&mut inner, consumer);
            inner.emit(
                connection,
                Event::ConsumerClosed {
                    consumer,
                    reason: format!("destination {destination} was deleted"),
                },
            );
        }
        if destination.is_queue() {
            let key = queue_key(destination);
            if let Some(queue) = inner.queues.remove(&key) {
                for entry in queue.pending.values() {
                    if entry.persisted {
                        inner.batch.remove_message(&key, entry.seq);
                    }
                }
                for consumer in &queue.consumers {
                    if let Some(c) = inner.consumers.get_mut(consumer) {
                        c.queues.retain(|k| *k != key);
                    }
                }
            }
            let sequences = inner
                .consumers
                .values()
                .flat_map(|consumer| consumer.in_flight.values())
                .filter(|flight| flight.persisted && flight.queue == key)
                .map(|flight| flight.seq)
                .chain(
                    inner
                        .transactions
                        .values()
                        .flat_map(|tx| &tx.detached)
                        .filter(|flight| flight.persisted && flight.queue == key)
                        .map(|flight| flight.seq),
                )
                .collect::<Vec<_>>();
            for sequence in sequences {
                inner.batch.remove_message(&key, sequence);
            }
        } else {
            inner.topics.remove(&destination.name);
        }
        if self.store.is_some() {
            inner.batch.remove_destination(destination);
        }
        self.advise(
            &mut inner,
            destination.destination_advisory_topic(),
            super::message::AdvisoryData::DestinationRemoved(destination.clone()),
        );
        self.flush_batch(inner)?;
        tracing::info!(%destination, "destination deleted");
        Ok(())
    }

    /// Removes one pending message from a queue.
    pub fn remove_message(&self, destination: &Destination, id: &str) -> Result<bool, CoreError> {
        Ok(self.take_message(destination, id)?.is_some())
    }

    fn take_message(
        &self,
        destination: &Destination,
        id: &str,
    ) -> Result<Option<Message>, CoreError> {
        let key = queue_key(destination);
        let mut inner = self.write_lock()?;
        let Some(queue) = inner.queues.get_mut(&key) else {
            return Err(CoreError::NotFound(format!("{destination} does not exist")));
        };
        let Some(entry) = queue.remove_message_id(id) else {
            return Ok(None);
        };
        queue.dequeued += 1;
        if entry.persisted {
            inner.batch.remove_message(&key, entry.seq);
        }
        self.flush_batch(inner)?;
        Ok(Some((*entry.message).clone()))
    }

    /// Moves a pending message to another destination.
    pub fn move_message(
        &self,
        from: &Destination,
        id: &str,
        to: &Destination,
    ) -> Result<bool, CoreError> {
        let Some(mut message) = self.take_message(from, id)? else {
            return Ok(false);
        };
        message.destination = to.clone();
        let mut inner = self.write_lock()?;
        self.route(&mut inner, message)?;
        self.flush_batch(inner)?;
        Ok(true)
    }

    /// Moves a dead-lettered message back to its original destination.
    pub fn retry_message(&self, dlq: &Destination, id: &str) -> Result<bool, CoreError> {
        let Some(mut message) = self.take_message(dlq, id)? else {
            return Ok(false);
        };
        let Some(original) = message.original_destination.take() else {
            return Err(CoreError::IllegalState(
                "message has no original destination".to_owned(),
            ));
        };
        message.destination = original;
        message.redelivery_counter = 0;
        message.properties.remove("dlqDeliveryFailureCause");
        let mut inner = self.write_lock()?;
        self.route(&mut inner, message)?;
        self.flush_batch(inner)?;
        Ok(true)
    }

    /// Sends a message as the broker itself (bypassing authorization).
    pub fn admin_send(&self, message: Message) -> Result<(), CoreError> {
        let mut inner = self.write_lock()?;
        let result = self.accept(&mut inner, message);
        self.flush_batch(inner)?;
        result
    }

    pub fn jobs(&self) -> Vec<JobStats> {
        let inner = self.lock();
        let mut jobs = inner
            .jobs
            .values()
            .map(|job| JobStats {
                id: job.id.clone(),
                destination: job.message.destination.to_uri(),
                next_time: job.next_time,
                delay: job.delay,
                period: job.period,
                repeat: job.repeat,
                cron: job.cron.clone(),
            })
            .collect::<Vec<_>>();
        jobs.sort_by_key(|job| job.next_time);
        jobs
    }

    pub fn remove_job(&self, id: &str) -> bool {
        let mut inner = match self.write_lock() {
            Ok(inner) => inner,
            Err(error) => {
                tracing::error!(%error, "cannot remove scheduled job");
                return false;
            }
        };
        let removed = self.remove_job_locked(&mut inner, id);
        if let Err(error) = self.flush_batch(inner) {
            tracing::error!(%error, "failed to persist scheduled job removal");
            return false;
        }
        removed
    }

    /// Forcibly closes a client connection.
    pub fn close_connection(&self, connection: ConnectionId) -> bool {
        let mut inner = self.lock();
        if !inner.connections.contains_key(&connection) {
            return false;
        }
        inner.emit(
            connection,
            Event::ConnectionClosed {
                reason: "closed by administrator".to_owned(),
            },
        );
        self.disconnect_locked(&mut inner, connection);
        let _ = self.flush_batch(inner);
        true
    }
}
