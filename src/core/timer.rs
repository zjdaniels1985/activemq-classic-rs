//! Periodic work: scheduled jobs, pull timeouts and expiry scanning.

use std::sync::Arc;

use super::broker::{BrokerCore, Inner};
use super::message::{Message, Value, generate_message_id, now_millis};
use super::scheduler::{
    Job, SCHEDULED_CRON, SCHEDULED_DELAY, SCHEDULED_ID, SCHEDULED_PERIOD, SCHEDULED_REPEAT,
    SCHEDULER_ACTION, SCHEDULER_ACTION_BROWSE, SCHEDULER_ACTION_END_TIME, SCHEDULER_ACTION_REMOVE,
    SCHEDULER_ACTION_REMOVEALL, SCHEDULER_ACTION_START_TIME,
};
use super::types::{CoreError, Event};

impl BrokerCore {
    pub(crate) fn tick(&self) {
        let mut inner = self.lock();
        let now = now_millis();
        self.fire_jobs(&mut inner, now);
        self.expire_pulls(&mut inner, now);
        if now - inner.last_expiry_check >= self.policy.expire_check_interval_ms as i64 {
            inner.last_expiry_check = now;
            self.expire_scan(&mut inner, now);
        }
        if let Err(error) = self.flush_batch(&mut inner) {
            tracing::warn!(%error, "failed to persist timer work");
        }
    }

    pub(crate) fn schedule(&self, inner: &mut Inner, message: Message) -> Result<(), CoreError> {
        let id = format!("ID:{}-job-{}", self.broker_name(), inner.next_id());
        let job = Job::from_message(id.clone(), message, now_millis())
            .map_err(CoreError::IllegalState)?;
        if let Some(store) = &self.store {
            store.put_job(&job)?;
        }
        tracing::debug!(job = %id, next = job.next_time, "scheduled message");
        inner.jobs.insert(id, job);
        Ok(())
    }

    fn fire_jobs(&self, inner: &mut Inner, now: i64) {
        let due = inner
            .jobs
            .values()
            .filter(|job| job.next_time <= now)
            .map(|job| job.id.clone())
            .collect::<Vec<_>>();
        for id in due {
            let Some(mut job) = inner.jobs.remove(&id) else {
                continue;
            };
            let mut message = job.message.clone();
            for property in [
                SCHEDULED_DELAY,
                SCHEDULED_PERIOD,
                SCHEDULED_REPEAT,
                SCHEDULED_CRON,
            ] {
                message.properties.remove(property);
            }
            message
                .properties
                .insert(SCHEDULED_ID.to_owned(), Value::String(job.id.clone()));
            let repeating = job.period > 0 || job.cron.is_some() || job.remaining != 0;
            if repeating {
                message.message_id = generate_message_id();
                message.timestamp = now;
            }
            if let Err(error) = self.route(inner, message) {
                tracing::warn!(job = %id, %error, "failed to deliver scheduled message");
            }
            if job.advance(now) {
                if let Some(store) = &self.store
                    && let Err(error) = store.put_job(&job)
                {
                    tracing::warn!(%error, "failed to persist job");
                }
                inner.jobs.insert(id, job);
            } else if let Some(store) = &self.store
                && let Err(error) = store.remove_job(&id)
            {
                tracing::warn!(%error, "failed to remove job");
            }
        }
    }

    fn expire_pulls(&self, inner: &mut Inner, now: i64) {
        let expired = inner
            .consumers
            .iter_mut()
            .filter(|(_, consumer)| {
                consumer
                    .pull_deadline
                    .is_some_and(|deadline| deadline <= now)
            })
            .map(|(id, consumer)| {
                consumer.pull_deadline = None;
                (*id, consumer.connection)
            })
            .collect::<Vec<_>>();
        for (consumer, connection) in expired {
            inner.emit(connection, Event::PullEmpty { consumer });
        }
    }

    fn expire_scan(&self, inner: &mut Inner, now: i64) {
        let mut dead = Vec::new();
        let mut removals = Vec::new();
        for queue in inner.queues.values_mut() {
            let expired = queue
                .pending
                .iter()
                .filter(|(_, entry)| entry.message.is_expired(now))
                .map(|(key, _)| *key)
                .collect::<Vec<_>>();
            for key in expired {
                if let Some(entry) = queue.pending.remove(&key) {
                    queue.expired += 1;
                    queue.dequeued += 1;
                    if entry.persisted {
                        removals.push((queue.key.clone(), entry.seq));
                    }
                    dead.push(entry.message);
                }
            }
        }
        for (key, seq) in removals {
            inner.batch.remove_message(&key, seq);
        }
        if self.policy.dead_letter_expired {
            for message in dead {
                self.dead_letter(inner, &message, "message expired");
            }
        }
    }

    /// Handles messages sent to `ActiveMQ.Scheduler.Management`.
    pub(crate) fn scheduler_management(
        &self,
        inner: &mut Inner,
        request: &Message,
    ) -> Result<(), CoreError> {
        let action = request
            .property(SCHEDULER_ACTION)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_uppercase();
        let start = request
            .property(SCHEDULER_ACTION_START_TIME)
            .and_then(Value::as_i64);
        let end = request
            .property(SCHEDULER_ACTION_END_TIME)
            .and_then(Value::as_i64);
        let in_range = |job: &Job| {
            start.is_none_or(|start| job.next_time >= start)
                && end.is_none_or(|end| job.next_time <= end)
        };
        match action.as_str() {
            SCHEDULER_ACTION_BROWSE => {
                let Some(reply_to) = request.reply_to.clone() else {
                    return Err(CoreError::IllegalState(
                        "scheduler BROWSE requires a reply-to destination".to_owned(),
                    ));
                };
                let replies = inner
                    .jobs
                    .values()
                    .filter(|job| in_range(job))
                    .map(|job| {
                        let mut message = job.message.clone();
                        message.destination = reply_to.clone();
                        message
                            .properties
                            .insert(SCHEDULED_ID.to_owned(), Value::String(job.id.clone()));
                        message.message_id = generate_message_id();
                        message
                    })
                    .collect::<Vec<_>>();
                for reply in replies {
                    self.route(inner, reply)?;
                }
            }
            SCHEDULER_ACTION_REMOVE => {
                let id = request
                    .property(SCHEDULED_ID)
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                self.remove_job_locked(inner, &id);
            }
            SCHEDULER_ACTION_REMOVEALL => {
                let ids = inner
                    .jobs
                    .values()
                    .filter(|job| in_range(job))
                    .map(|job| job.id.clone())
                    .collect::<Vec<_>>();
                for id in ids {
                    self.remove_job_locked(inner, &id);
                }
            }
            other => {
                return Err(CoreError::IllegalState(format!(
                    "unknown scheduler action {other:?}"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn remove_job_locked(&self, inner: &mut Inner, id: &str) -> bool {
        let removed = inner.jobs.remove(id).is_some();
        if removed
            && let Some(store) = &self.store
            && let Err(error) = store.remove_job(id)
        {
            tracing::warn!(%error, "failed to remove job");
        }
        removed
    }
}

#[allow(dead_code)]
fn _assert_send_sync() {
    fn check<T: Send + Sync>() {}
    check::<Arc<BrokerCore>>();
}
