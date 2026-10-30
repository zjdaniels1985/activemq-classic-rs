//! Local and XA transactions.

use super::broker::{BrokerCore, Detached, InFlight, Inner, Transaction, TxKey};
use super::store::{PreparedRecord, XidRecord};
use super::types::{ConnectionId, CoreError, TxId};

pub(crate) fn tx_key(connection: ConnectionId, tx: &TxId) -> TxKey {
    match tx {
        TxId::Local(_) => (Some(connection), tx.clone()),
        TxId::Xa(_) => (None, tx.clone()),
    }
}

impl BrokerCore {
    pub fn begin(&self, connection: ConnectionId, tx: TxId) -> Result<(), CoreError> {
        let mut inner = self.write_lock()?;
        inner.principal(connection)?;
        let key = tx_key(connection, &tx);
        if inner.transactions.contains_key(&key) {
            return Err(CoreError::Transaction(format!(
                "transaction {tx} has already been started"
            )));
        }
        inner.transactions.insert(
            key,
            Transaction {
                connection: Some(connection),
                sends: Vec::new(),
                acks: Vec::new(),
                detached: Vec::new(),
                prepared: false,
            },
        );
        self.flush_batch(inner)
    }

    /// Commits a transaction. `one_phase` must be true for local transactions
    /// and XA one-phase commits.
    pub fn commit(
        &self,
        connection: ConnectionId,
        tx: &TxId,
        one_phase: bool,
    ) -> Result<(), CoreError> {
        let mut inner = self.write_lock()?;
        let key = tx_key(connection, tx);
        let Some(transaction) = inner.transactions.get(&key) else {
            return Err(CoreError::Transaction(format!(
                "transaction {tx} has not been started"
            )));
        };
        if matches!(tx, TxId::Xa(_)) {
            if one_phase && transaction.prepared {
                return Err(CoreError::Transaction(format!(
                    "cannot do one phase commit of prepared transaction {tx}"
                )));
            }
            if !one_phase && !transaction.prepared {
                return Err(CoreError::Transaction(format!(
                    "cannot do two phase commit of unprepared transaction {tx}"
                )));
            }
        }
        let transaction = inner.transactions.remove(&key).expect("transaction");
        self.commit_locked(&mut inner, &key, transaction);
        self.flush_batch(inner)
    }

    fn commit_locked(&self, inner: &mut Inner, key: &TxKey, transaction: Transaction) {
        let mut queues = Vec::new();
        for (consumer, delivery) in transaction.acks {
            let Some(state) = inner.consumers.get_mut(&consumer) else {
                continue;
            };
            if state.in_flight.get(&delivery).and_then(|f| f.tx.as_ref()) != Some(key) {
                continue;
            }
            let flight = state.in_flight.remove(&delivery).expect("in flight");
            queues.extend(state.queues.iter().cloned());
            self.consume_flight(inner, &flight);
        }
        for detached in transaction.detached {
            self.consume_flight(inner, &detached_flight(detached));
        }
        for message in transaction.sends {
            if let Err(error) = self.accept(inner, message) {
                tracing::warn!(%error, "failed to route transactional message");
            }
        }
        if transaction.prepared
            && let TxId::Xa(xid) = &key.1
        {
            self.forget_prepared(inner, xid);
        }
        queues.sort();
        queues.dedup();
        for queue in queues {
            self.dispatch(inner, &queue);
        }
        tracing::debug!(tx = %key.1, "transaction committed");
    }

    pub fn rollback(&self, connection: ConnectionId, tx: &TxId) -> Result<(), CoreError> {
        let mut inner = self.write_lock()?;
        let key = tx_key(connection, tx);
        if !inner.transactions.contains_key(&key) {
            return Err(CoreError::Transaction(format!(
                "transaction {tx} has not been started"
            )));
        }
        self.rollback_locked(&mut inner, &key);
        self.flush_batch(inner)
    }

    pub(crate) fn rollback_locked(&self, inner: &mut Inner, key: &TxKey) {
        let Some(transaction) = inner.transactions.remove(key) else {
            return;
        };
        let redeliver = transaction
            .connection
            .and_then(|connection| inner.connections.get(&connection))
            .is_none_or(|connection| connection.info.redeliver_on_rollback);
        let mut queues = Vec::new();
        for (consumer, delivery) in transaction.acks {
            let Some(state) = inner.consumers.get_mut(&consumer) else {
                continue;
            };
            let Some(flight) = state.in_flight.get_mut(&delivery) else {
                continue;
            };
            if flight.tx.as_ref() != Some(key) {
                continue;
            }
            flight.tx = None;
            if redeliver {
                let flight = state.in_flight.remove(&delivery).expect("in flight");
                queues.extend(state.queues.iter().cloned());
                self.return_to_queue(inner, flight, true, true);
            }
        }
        for detached in transaction.detached {
            queues.push(detached.queue.clone());
            self.return_to_queue(inner, detached_flight(detached), true, true);
        }
        if transaction.prepared
            && let TxId::Xa(xid) = &key.1
        {
            self.forget_prepared(inner, xid);
        }
        queues.sort();
        queues.dedup();
        for queue in queues {
            self.dispatch(inner, &queue);
        }
        tracing::debug!(tx = %key.1, "transaction rolled back");
    }

    /// Prepares an XA transaction. Returns true when it was read-only (and
    /// therefore already completed), mirroring `XA_RDONLY`.
    pub fn prepare(&self, connection: ConnectionId, tx: &TxId) -> Result<bool, CoreError> {
        let TxId::Xa(xid) = tx else {
            return Err(CoreError::Transaction(
                "only XA transactions can be prepared".to_owned(),
            ));
        };
        let mut inner = self.write_lock()?;
        let key = tx_key(connection, tx);
        let Some(transaction) = inner.transactions.get(&key) else {
            return Err(CoreError::Transaction(format!(
                "transaction {tx} has not been started"
            )));
        };
        if transaction.prepared {
            return Err(CoreError::Transaction(format!(
                "transaction {tx} is already prepared"
            )));
        }
        let mut transaction = inner.transactions.remove(&key).expect("transaction");
        for (consumer, delivery) in std::mem::take(&mut transaction.acks) {
            let Some(state) = inner.consumers.get_mut(&consumer) else {
                continue;
            };
            if state.in_flight.get(&delivery).and_then(|f| f.tx.as_ref()) != Some(&key) {
                continue;
            }
            let flight = state.in_flight.remove(&delivery).expect("in flight");
            transaction.detached.push(Detached {
                queue: flight.queue,
                seq: flight.seq,
                message: flight.message,
                persisted: flight.persisted,
            });
        }
        if transaction.sends.is_empty() && transaction.detached.is_empty() {
            return Ok(true);
        }
        if self.store.is_some() {
            inner.batch.put_prepared(&PreparedRecord {
                xid: xid.clone(),
                sends: transaction.sends.clone(),
                acks: transaction
                    .detached
                    .iter()
                    .filter(|d| d.persisted)
                    .map(|d| (d.queue.clone(), d.seq))
                    .collect(),
            })?;
        }
        transaction.prepared = true;
        inner.transactions.insert(key, transaction);
        self.flush_batch(inner)?;
        Ok(false)
    }

    /// Lists prepared (in-doubt) XA transactions.
    pub fn recover(&self) -> Vec<XidRecord> {
        let inner = self.lock();
        let mut xids = inner
            .transactions
            .iter()
            .filter(|(_, tx)| tx.prepared)
            .filter_map(|((_, id), _)| match id {
                TxId::Xa(xid) => Some(xid.clone()),
                TxId::Local(_) => None,
            })
            .collect::<Vec<_>>();
        xids.sort();
        xids
    }

    /// Forgets a heuristically completed transaction (rolls it back).
    pub fn forget(&self, connection: ConnectionId, tx: &TxId) -> Result<(), CoreError> {
        let mut inner = self.write_lock()?;
        let key = tx_key(connection, tx);
        self.rollback_locked(&mut inner, &key);
        self.flush_batch(inner)
    }

    fn forget_prepared(&self, inner: &mut Inner, xid: &XidRecord) {
        if self.store.is_some()
            && let Err(error) = inner.batch.remove_prepared(xid)
        {
            tracing::warn!(%error, "failed to remove prepared transaction");
        }
    }
}

fn detached_flight(detached: Detached) -> InFlight {
    InFlight {
        queue: detached.queue,
        seq: detached.seq,
        message: detached.message,
        persisted: detached.persisted,
        window_freed: true,
        tx: None,
    }
}
