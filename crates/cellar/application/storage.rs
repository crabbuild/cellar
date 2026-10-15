//! Activation-bound storage, transactions and lazy cursors.

use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};

mod cursor;
mod inbox;
mod outbox;
mod outbox_inspection;
mod transaction;
pub use cellar_native::sql::Value as SqlValue;
pub use cursor::{KvCursor, SqlCursor, SqlRow};
pub use inbox::Inbox;
pub use outbox_inspection::{
    OutboxBlocked, OutboxLane, OutboxObservation, OutboxOptions, OutboxPage, OutboxRevision,
};
pub(crate) use transaction::Current as TransactionCurrent;
use transaction::Transaction;

#[derive(Default)]
pub(crate) struct Resources {
    pub(crate) retired: AtomicBool,
    pub(crate) source_generation: AtomicU64,
    pub(crate) source_closed: AtomicBool,
    pub(crate) failure: Mutex<Option<String>>,
    pub(crate) input_gates: Mutex<Vec<std::sync::Weak<crate::native_worker::InputGate>>>,
    turns: Mutex<Vec<std::sync::Weak<crate::native_worker::WakeTurn>>>,
    pub(crate) retired_position: Mutex<Option<u64>>,
    cleanup: Mutex<Vec<Cleanup>>,
    pub(crate) transaction: Mutex<Option<Arc<Transaction>>>,
    pub(crate) facets: Mutex<Option<Arc<super::facets::State>>>,
    open_transaction: Mutex<Option<Arc<Transaction>>>,
    transaction_queue: transaction::Queue,
    transaction_serial: AtomicU64,
}

enum Cleanup {
    Sql(u64),
    Kv(u64),
}

impl Cleanup {
    fn close(self) {
        match self {
            Self::Sql(id) => crate::storage::sql_cursor_close(id),
            Self::Kv(id) => crate::storage::sync_list_close(id),
        }
    }
}

impl Activation {
    pub(crate) fn fail(&self, reason: String) {
        self.resources
            .failure
            .lock()
            .unwrap()
            .get_or_insert_with(|| reason.clone());
        let gates: Vec<_> = self
            .resources
            .input_gates
            .lock()
            .unwrap()
            .iter()
            .filter_map(|gate| gate.upgrade())
            .collect();
        for gate in gates {
            gate.fail(reason.clone());
        }
        let turns = std::mem::take(&mut *self.resources.turns.lock().unwrap());
        for wake in turns.into_iter().filter_map(|turn| turn.upgrade()) {
            futures_util::task::ArcWake::wake_by_ref(&wake);
        }
    }

    pub(crate) fn source_admission(&self, generation: u64, closed: bool) {
        self.resources
            .source_generation
            .store(generation, Ordering::Release);
        self.resources
            .source_closed
            .store(closed, Ordering::Release);
        let turns = self.resources.turns.lock().unwrap();
        for wake in turns.iter().filter_map(|turn| turn.upgrade()) {
            futures_util::task::ArcWake::wake_by_ref(&wake);
        }
    }

    pub(crate) fn retire(&self) {
        self.websocket_states.lock().unwrap().clear();
        self.resources.retired.store(true, Ordering::Release);
        let gates = std::mem::take(&mut *self.resources.input_gates.lock().unwrap());
        for gate in gates.into_iter().filter_map(|gate| gate.upgrade()) {
            gate.retire();
        }
        // A user future can remain pending without retaining its waker.
        // Retirement must still bring its host turn back to drop the old
        // handler/background work under the original I/O context.
        let turns = std::mem::take(&mut *self.resources.turns.lock().unwrap());
        for wake in turns.into_iter().filter_map(|turn| turn.upgrade()) {
            futures_util::task::ArcWake::wake_by_ref(&wake);
        }
    }

    pub(crate) fn register_wake(&self, wake: &Arc<crate::native_worker::WakeTurn>) {
        let mut turns = self.resources.turns.lock().unwrap();
        turns.retain(|turn| turn.upgrade().is_some_and(|wake| wake.pending()));
        if self.resources.retired.load(Ordering::Acquire) {
            drop(turns);
            futures_util::task::ArcWake::wake_by_ref(wake);
        } else if !turns
            .iter()
            .any(|turn| std::sync::Weak::ptr_eq(turn, &Arc::downgrade(wake)))
        {
            turns.push(Arc::downgrade(wake));
        }
    }

    pub(crate) fn drain_resources(&self) {
        for cleanup in std::mem::take(&mut *self.resources.cleanup.lock().unwrap()) {
            cleanup.close();
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ListOptions {
    pub start: Option<String>,
    pub end: Option<String>,
    pub start_after: Option<String>,
    pub prefix: Option<String>,
    pub limit: Option<usize>,
    pub reverse: bool,
}

#[derive(Clone)]
pub struct Storage(
    pub(super) Arc<Activation>,
    pub(super) Option<Arc<Transaction>>,
    pub(super) u64,
);

impl TryFrom<crate::storage::StoredValue> for StoredClone {
    type Error = anyhow::Error;

    fn try_from(value: crate::storage::StoredValue) -> Result<Self> {
        match value {
            crate::storage::StoredValue::Json(json) => Ok(Self {
                value: cellar_native::clone::StructuredValue::from_json(serde_json::from_str(
                    &json,
                )?)?,
                stored_stubs: false,
            }),
            crate::storage::StoredValue::Structured(bytes) => {
                let stored_stubs = bytes.first() == Some(&1);
                let payload = if stored_stubs { &bytes[1..] } else { &bytes };
                Ok(Self {
                    value: cellar_native::clone::decode(payload)?,
                    stored_stubs,
                })
            }
        }
    }
}

impl StoredClone {
    fn encode(&self) -> Result<Vec<u8>> {
        let mut bytes = cellar_native::clone::encode(&self.value)?;
        if self.stored_stubs {
            bytes.insert(0, 1);
        }
        Ok(bytes)
    }
}

impl Storage {
    pub(crate) fn clear_current_work(&self, reason: cellar_logic::work::Reason) -> Result<()> {
        if let Some(installation) = crate::storage::work::current(self.scope()?, reason)? {
            self.clear_work(installation)?;
        }
        Ok(())
    }

    pub(crate) fn schedule_work(
        &self,
        reason: cellar_logic::work::Reason,
        at_ms: i64,
    ) -> Result<()> {
        let scope = self.scope()?;
        if let Some(effective) = crate::storage::work::set(scope, reason, at_ms)? {
            if effective >= 0 {
                crate::host_gates::spawn_arm_gate(
                    scope,
                    effective,
                    crate::host_io::installed_context(),
                );
            }
        }
        Ok(())
    }

    pub(crate) fn clear_work(
        &self,
        installation: cellar_logic::work::Installation,
    ) -> Result<bool> {
        let scope = self.scope()?;
        let (cleared, effective) = crate::storage::work::clear(scope, installation)?;
        if let Some(effective) = effective.filter(|deadline| *deadline >= 0) {
            crate::host_gates::spawn_arm_gate(
                scope,
                effective,
                crate::host_io::installed_context(),
            );
        }
        Ok(cleared)
    }

    pub(crate) fn work_installation(
        &self,
        reason: cellar_logic::work::Reason,
    ) -> Result<Option<cellar_logic::work::Installation>> {
        crate::storage::work::installation(self.scope()?, reason)
    }

    pub(crate) fn system_sql(&self, query: &str, bindings: &[SqlValue]) -> Result<Vec<SqlRow>> {
        crate::storage::system_sql(self.scope()?, query, bindings)
    }

    /// Built-in maintenance can remove a key range without reading its values.
    pub(crate) fn system_delete_range(&self, start: &str, end: &str) -> Result<()> {
        let scope = self.scope()?;
        crate::storage::system_sql(
            scope,
            "DELETE FROM __sys_cell_kv WHERE scope=?1 AND k>=?2 AND k<?3",
            &[
                SqlValue::Text(scope.into()),
                SqlValue::Text(start.into()),
                SqlValue::Text(end.into()),
            ],
        )?;
        Ok(())
    }

    pub(crate) fn system_sql_schema(&self, ddl: &str) -> Result<()> {
        crate::storage::system_sql_schema(self.scope()?, ddl)
    }

    pub(crate) fn sql_request(
        &self,
        request: &cellar_native::sql::SqlRequest,
    ) -> Result<cellar_native::sql::SqlResponse> {
        crate::storage::sql_run(self.scope()?, request).map_err(Into::into)
    }

    pub(crate) fn sql_console(
        &self,
        sql: &str,
        readonly: bool,
    ) -> Result<cellar_native::sql::SqlResponse> {
        crate::storage::sql_console(self.scope()?, sql, readonly).map_err(Into::into)
    }

    pub(crate) fn sql_console_migrations(
        &self,
        table: &str,
        after: u64,
    ) -> Result<serde_json::Value> {
        crate::storage::sql_console_migrations(self.scope()?, table, after)
    }

    fn installed_activation_scope(&self) -> Result<&str> {
        let io = crate::host_io::installed_context()
            .ok_or_else(|| anyhow!("native storage is outside an executing turn"))?;
        anyhow::ensure!(
            io.native_activation
                .as_ref()
                .is_some_and(|activation| Arc::ptr_eq(activation, &self.0))
                && crate::storage::activation_epoch(&self.0.scope) == Some(self.0.epoch)
                && !self.0.resources.retired.load(Ordering::Acquire),
            "native storage belongs to another cell or a retired activation"
        );
        anyhow::ensure!(
            cellar_logic::event_stream::source_admitted(
                self.2,
                self.0.resources.source_generation.load(Ordering::Acquire),
                self.0.resources.source_closed.load(Ordering::Acquire),
                io.framework_source.load(Ordering::Acquire)
            ),
            "source business admission is closed or its incarnation has changed"
        );
        self.0.drain_resources();
        Ok(&self.0.scope)
    }

    fn activation_scope(&self) -> Result<&str> {
        let scope = self.installed_activation_scope()?;
        if let Some(failure) = self.0.resources.failure.lock().unwrap().as_ref() {
            anyhow::bail!("native cell was reset: {failure}");
        }
        Ok(scope)
    }

    fn check_transaction_authority(&self) -> Result<()> {
        if let Some(transaction) = &self.1 {
            transaction.check_active()?;
        }
        let current = self.0.resources.transaction.lock().unwrap().clone();
        if let Some(current) = &current {
            current.check_active()?;
        }
        if let Some(open) = self.0.resources.open_transaction.lock().unwrap().as_ref() {
            anyhow::ensure!(
                current
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, open)),
                "native storage operation is outside the active transaction callback"
            );
        }
        Ok(())
    }

    fn scope(&self) -> Result<&str> {
        self.check_transaction_authority()?;
        let scope = self.activation_scope()?;
        if let Some(error) = crate::storage::sql_critical_error(scope) {
            anyhow::bail!(error);
        }
        Ok(scope)
    }

    fn cleanup(&self, cleanup: Cleanup) {
        if self.0.resources.retired.load(Ordering::Acquire) {
            return;
        }
        if self.activation_scope().is_ok() {
            cleanup.close();
        } else {
            self.0.resources.cleanup.lock().unwrap().push(cleanup);
        }
    }

    /// Read the original representation without discarding persisted clone data.
    pub fn get(&self, key: &str) -> Result<Option<crate::storage::StoredValue>> {
        crate::storage::get_stored(self.scope()?, key)
    }

    pub fn get_many(&self, keys: &[String]) -> Result<Vec<(String, crate::storage::StoredValue)>> {
        crate::storage::get_many_stored(self.scope()?, keys)
    }

    pub fn list(
        &self,
        options: &ListOptions,
    ) -> Result<Vec<(String, crate::storage::StoredValue)>> {
        crate::storage::list_stored_with_options(
            self.scope()?,
            options.start.as_deref(),
            options.end.as_deref(),
            options.start_after.as_deref(),
            options.prefix.as_deref(),
            options.limit,
            options.reverse,
        )
    }

    /// A keyset iterator with no retained SQLite read snapshot. Starting a
    /// second iterator invalidates the first, matching the existing sync API.
    pub fn list_cursor(&self, options: &ListOptions) -> Result<KvCursor> {
        KvCursor::new(self, options)
    }

    pub fn get_json(&self, key: &str) -> Result<Option<serde_json::Value>> {
        match self.get(key)? {
            Some(crate::storage::StoredValue::Json(json)) => Ok(Some(serde_json::from_str(&json)?)),
            Some(crate::storage::StoredValue::Structured(_)) => {
                Err(anyhow!("stored value is a structured clone; use get_clone"))
            }
            None => Ok(None),
        }
    }

    pub fn get_clone(&self, key: &str) -> Result<Option<StoredClone>> {
        self.get(key)?.map(StoredClone::try_from).transpose()
    }

    pub fn get_many_clone(&self, keys: &[String]) -> Result<Vec<(String, StoredClone)>> {
        self.get_many(keys)?
            .into_iter()
            .map(|(key, value)| Ok((key, StoredClone::try_from(value)?)))
            .collect()
    }

    pub fn list_clone(&self, options: &ListOptions) -> Result<Vec<(String, StoredClone)>> {
        self.list(options)?
            .into_iter()
            .map(|(key, value)| Ok((key, StoredClone::try_from(value)?)))
            .collect()
    }

    pub fn put_clone(
        &self,
        key: &str,
        value: &cellar_native::clone::StructuredValue,
    ) -> Result<()> {
        let scope = self.scope()?;
        crate::storage::put_serialized(scope, key, &cellar_native::clone::encode(value)?)
    }

    pub fn put_stored_clone(&self, key: &str, value: &StoredClone) -> Result<()> {
        crate::storage::put_serialized(self.scope()?, key, &value.encode()?)
    }

    pub fn put_many_stored_clone(&self, entries: &[(String, StoredClone)]) -> Result<()> {
        let scope = self.scope()?;
        let entries = entries
            .iter()
            .map(|(key, value)| Ok((key.clone(), value.encode()?)))
            .collect::<Result<Vec<_>>>()?;
        crate::storage::put_many_serialized(scope, &entries)
    }

    pub fn put_many_clone(
        &self,
        entries: &[(String, cellar_native::clone::StructuredValue)],
    ) -> Result<()> {
        let scope = self.scope()?;
        let entries = entries
            .iter()
            .map(|(key, value)| Ok((key.clone(), cellar_native::clone::encode(value)?)))
            .collect::<Result<Vec<_>>>()?;
        crate::storage::put_many_serialized(scope, &entries)
    }

    pub fn put_json(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        crate::storage::put_json(self.scope()?, key, value)
    }

    pub fn put_many_json(&self, entries: &[(String, serde_json::Value)]) -> Result<()> {
        crate::storage::put_many_json(self.scope()?, entries)
    }

    pub fn delete(&self, key: &str) -> Result<bool> {
        crate::storage::delete(self.scope()?, key)
    }

    pub fn delete_many(&self, keys: &[String]) -> Result<usize> {
        crate::storage::delete_many(self.scope()?, keys)
    }

    /// The compatibility flag chooses whether deleteAll also removes alarms.
    pub fn delete_all(&self) -> Result<()> {
        let scope = self.scope()?;
        crate::storage::delete_all_with_alarm(scope, self.0.delete_all_deletes_alarm)?;
        crate::native_worker::refresh_wake_gate(scope)
    }

    pub fn sync(&self) -> HandlerFuture<()> {
        match self.scope() {
            Ok(scope) => crate::native_worker::storage_sync(scope),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    pub fn sql(&self, query: &str, bindings: &[serde_json::Value]) -> Result<SqlResult> {
        crate::storage::sql_exec(self.scope()?, query, bindings).map_err(anyhow::Error::msg)
    }

    pub fn sql_cursor(&self, query: &str, bindings: &[SqlValue]) -> Result<SqlCursor> {
        SqlCursor::new(self, query, bindings)
    }

    pub fn sql_database_size(&self) -> Result<u64> {
        crate::storage::sql_database_size(self.scope()?).map_err(anyhow::Error::msg)
    }

    pub(crate) fn sqlite_allocation(&self) -> Result<(i64, i64)> {
        let (allocated, used) =
            crate::storage::sql_page_usage(self.scope()?).map_err(anyhow::Error::msg)?;
        Ok((i64::try_from(allocated)?, i64::try_from(used)?))
    }

    /// Execute complete SQL statements and retain the unfinished suffix for
    /// incremental import, using SQLite's existing tokenizer.
    pub fn sql_ingest(&self, input: &str) -> Result<(String, u64, u64)> {
        crate::storage::sql_ingest(self.scope()?, input).map_err(anyhow::Error::msg)
    }

    pub fn get_alarm(&self) -> Result<Option<i64>> {
        Ok(crate::storage::get_alarm(self.scope()?))
    }

    pub fn set_alarm(&self, at_ms: i64) -> Result<()> {
        crate::native_worker::set_alarm(self.scope()?, at_ms)
    }

    pub fn delete_alarm(&self) -> Result<()> {
        let scope = self.scope()?;
        crate::storage::delete_alarm(scope)?;
        crate::native_worker::refresh_wake_gate(scope)
    }
}
