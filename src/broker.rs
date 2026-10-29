use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    pub body: Vec<u8>,
    pub properties: HashMap<String, String>,
}

impl Message {
    pub fn new(body: impl Into<Vec<u8>>) -> Self {
        Self {
            body: body.into(),
            properties: HashMap::new(),
        }
    }

    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum BrokerError {
    EmptyQueueName,
    PoisonedLock,
}

impl fmt::Display for BrokerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyQueueName => formatter.write_str("queue name must not be empty"),
            Self::PoisonedLock => formatter.write_str("broker state lock was poisoned"),
        }
    }
}

impl std::error::Error for BrokerError {}

#[derive(Default)]
struct State {
    queues: HashMap<String, VecDeque<Message>>,
}

/// Thread-safe, in-memory queue broker. Messages are delivered FIFO and removed on receive.
///
/// This is the initial broker core; it does not yet provide persistence, transactions,
/// acknowledgements, topics, or network protocol handling.
#[derive(Clone, Default)]
pub struct Broker {
    state: Arc<Mutex<State>>,
}

impl Broker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn send(&self, queue: &str, message: Message) -> Result<(), BrokerError> {
        let queue = validate_queue_name(queue)?;
        lock(&self.state)?
            .queues
            .entry(queue.to_owned())
            .or_default()
            .push_back(message);
        Ok(())
    }

    pub fn receive(&self, queue: &str) -> Result<Option<Message>, BrokerError> {
        let queue = validate_queue_name(queue)?;
        Ok(lock(&self.state)?
            .queues
            .get_mut(queue)
            .and_then(VecDeque::pop_front))
    }

    pub fn queue_depth(&self, queue: &str) -> Result<usize, BrokerError> {
        let queue = validate_queue_name(queue)?;
        Ok(lock(&self.state)?
            .queues
            .get(queue)
            .map_or(0, VecDeque::len))
    }
}

fn validate_queue_name(queue: &str) -> Result<&str, BrokerError> {
    if queue.trim().is_empty() {
        Err(BrokerError::EmptyQueueName)
    } else {
        Ok(queue)
    }
}

fn lock(state: &Mutex<State>) -> Result<MutexGuard<'_, State>, BrokerError> {
    state.lock().map_err(|_| BrokerError::PoisonedLock)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivers_messages_in_fifo_order_and_tracks_depth() {
        let broker = Broker::new();
        broker.send("orders", Message::new("first")).unwrap();
        broker
            .send(
                "orders",
                Message::new("second").with_property("kind", "created"),
            )
            .unwrap();

        assert_eq!(broker.queue_depth("orders").unwrap(), 2);
        assert_eq!(broker.receive("orders").unwrap().unwrap().body, b"first");
        let second = broker.receive("orders").unwrap().unwrap();
        assert_eq!(second.body, b"second");
        assert_eq!(second.properties["kind"], "created");
        assert_eq!(broker.queue_depth("orders").unwrap(), 0);
    }

    #[test]
    fn cloned_handles_share_broker_state() {
        let broker = Broker::new();
        let producer = broker.clone();
        producer.send("events", Message::new("ready")).unwrap();

        assert_eq!(broker.receive("events").unwrap().unwrap().body, b"ready");
    }

    #[test]
    fn rejects_blank_queue_names() {
        let broker = Broker::new();
        assert_eq!(
            broker.send("  ", Message::new("invalid")),
            Err(BrokerError::EmptyQueueName)
        );
    }
}
