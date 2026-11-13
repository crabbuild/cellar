// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Activation-bound storage, transactions and lazy cursors.

use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};

mod cursor;
mod dispatch;
pub(crate) use dispatch::{RaftDispatch, RaftDispatchContext, RaftDispatchPermit};
mod inbox;
mod outbox;
mod outbox_inspection;
pub(crate) mod transaction;
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
    /// Installed from the admitted native definition before exposing Context.
    /// Plain registered SQL callbacks retain the default compatibility policy.
    pub(crate) delete_all_deletes_alarm: AtomicBool,
    pub(crate) durable_callbacks: AtomicBool,
    pub(crate) provider_lifecycle: super::container_bindings::LifecycleLocks,
    pub(crate) retired: AtomicBool,
    pub(crate) source_generation: AtomicU64,
    pub(crate) source_closed: AtomicBool,
    /// Permanent erasure cannot be undone by recreating an event source.
    pub(crate) erasure_closed: AtomicBool,
    pub(crate) failure: Mutex<Option<String>>,
    pub(crate) input_gates: Mutex<Vec<std::sync::Weak<crate::native_worker::InputGate>>>,
    turns: Mutex<Vec<std::sync::Weak<crate::native_worker::WakeTurn>>>,
    pub(crate) retired_position: Mutex<Option<u64>>,
    cleanup: Mutex<Vec<Cleanup>>,
    pub(crate) transaction: Mutex<Option<Arc<Transaction>>>,
    pub(crate) facets: Mutex<Option<Arc<super::facets::State>>>,
    pub(crate) filesystem: super::filesystem::Registry,
    open_transaction: Mutex<Option<Arc<Transaction>>>,
    transaction_queue: transaction::Queue,
    transaction_serial: AtomicU64,
    kv_cursor: Mutex<cursor::RaftKvCursor>,
}

enum Cleanup {
    Sql(u64),
    Kv(u64),
}

impl Resources {
    pub(crate) fn raft_can_park(&self) -> bool {
        !self.retired.load(Ordering::Acquire)
            && self.open_transaction.lock().unwrap().is_none()
            && self.transaction.lock().unwrap().is_none()
            && self.cleanup.lock().unwrap().is_empty()
            && !self.filesystem.has_live()
    }

    pub(crate) fn business_closed(&self) -> bool {
        self.erasure_closed.load(Ordering::Acquire) || self.source_closed.load(Ordering::Acquire)
    }

    pub(crate) fn source_admitted(
        &self,
        generation: u64,
        framework: bool,
        erasure_cleanup: bool,
    ) -> bool {
        (!self.erasure_closed.load(Ordering::Acquire) || (framework && erasure_cleanup))
            && cellar_logic::event_stream::source_admitted(
                generation,
                self.source_generation.load(Ordering::Acquire),
                self.source_closed.load(Ordering::Acquire),
                framework,
            )
    }
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
    pub(crate) fn close_for_erasure(&self) {
        self.resources.erasure_closed.store(true, Ordering::Release);
        self.source_admission(
            self.resources.source_generation.load(Ordering::Acquire),
            true,
        );
    }

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
pub struct Storage(Owner, pub(super) Option<Arc<Transaction>>, pub(super) u64);

#[derive(Clone)]
enum Owner {
    Native(Arc<Activation>),
    RaftCommand(RaftStorageAuthority),
    RaftQuery(Arc<Resources>),
}

/// Constructor handles follow one activation; ordinary command handles follow
/// one invocation. Neither authority owns the SQLite connection or image pin.
#[derive(Clone)]
pub(crate) struct RaftStorageAuthority {
    pub(crate) resources: Arc<Resources>,
    pub(crate) invocation: Option<Arc<()>>,
}

impl RaftStorageAuthority {
    pub(crate) fn activation(&self) -> Self {
        Self {
            resources: self.resources.clone(),
            invocation: None,
        }
    }
}

impl Owner {
    fn resources(&self) -> &Resources {
        match self {
            Self::Native(activation) => &activation.resources,
            Self::RaftCommand(authority) => &authority.resources,
            Self::RaftQuery(resources) => resources,
        }
    }
}

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
    pub(super) fn native(
        activation: Arc<Activation>,
        transaction: Option<Arc<Transaction>>,
        source_generation: u64,
    ) -> Self {
        Self(Owner::Native(activation), transaction, source_generation)
    }

    pub(crate) fn is_raft(&self) -> bool {
        matches!(self.0, Owner::RaftCommand(_) | Owner::RaftQuery(_))
    }

    pub(crate) fn arm_raft_work(&self, reason: &str, at_ms: i64) -> Result<()> {
        self.write_scope()?;
        anyhow::ensure!(
            self.resources().durable_callbacks.load(Ordering::Acquire),
            consensus::CommandFailure::UnsupportedCapability
        );
        crate::storage::raft_native_work(|db| consensus::native_work::arm(db, reason, at_ms))
    }

    pub(crate) fn cancel_raft_work(&self, reason: &str) -> Result<()> {
        self.write_scope()?;
        anyhow::ensure!(
            self.resources().durable_callbacks.load(Ordering::Acquire),
            consensus::CommandFailure::UnsupportedCapability
        );
        crate::storage::raft_native_work(|db| consensus::native_work::cancel(db, reason))
    }

    pub(crate) fn raft_command_authority(&self) -> Result<RaftStorageAuthority> {
        match &self.0 {
            Owner::RaftCommand(resources) => Ok(resources.clone()),
            _ => Err(consensus::CommandFailure::UnsupportedCapability.into()),
        }
    }

    pub(crate) fn raft_command(resources: RaftStorageAuthority) -> Self {
        Self(Owner::RaftCommand(resources), None, 0)
    }

    pub(crate) fn raft_query(resources: Arc<Resources>) -> Self {
        Self(Owner::RaftQuery(resources), None, 0)
    }

    pub(super) fn check_raft_turn(&self) -> Result<()> {
        self.data_scope().map(|_| ())
    }

    pub(super) fn native_activation(&self) -> Result<&Arc<Activation>> {
        match &self.0 {
            Owner::Native(activation) => Ok(activation),
            Owner::RaftCommand(_) | Owner::RaftQuery(_) => {
                Err(consensus::CommandFailure::UnsupportedCapability.into())
            }
        }
    }

    pub(super) fn resources(&self) -> &Resources {
        self.0.resources()
    }

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
        crate::storage::system_sql(self.write_scope()?, query, bindings)
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
        crate::storage::system_sql_schema(self.write_scope()?, ddl)
    }

    pub(crate) fn sql_request(
        &self,
        request: &cellar_native::sql::SqlRequest,
    ) -> Result<cellar_native::sql::SqlResponse> {
        if matches!(&self.0, Owner::RaftCommand(_)) {
            self.transaction_sync(|transaction| {
                crate::storage::sql_run_in_transaction(transaction.data_scope()?, request)
                    .map_err(Into::into)
            })
        } else {
            crate::storage::sql_run(self.scope()?, request).map_err(Into::into)
        }
    }

    pub(crate) fn sql_console(
        &self,
        sql: &str,
        readonly: bool,
    ) -> Result<cellar_native::sql::SqlResponse> {
        if matches!(&self.0, Owner::RaftCommand(_)) {
            self.transaction_sync(|transaction| {
                crate::storage::sql_console_in_transaction(transaction.data_scope()?, sql, readonly)
                    .map_err(Into::into)
            })
        } else {
            crate::storage::sql_console(self.scope()?, sql, readonly).map_err(Into::into)
        }
    }

    pub(crate) fn sql_console_migrations(
        &self,
        table: &str,
        after: u64,
    ) -> Result<serde_json::Value> {
        let scope = if matches!(&self.0, Owner::RaftCommand(_)) {
            self.data_scope()?
        } else {
            self.scope()?
        };
        crate::storage::sql_console_migrations(scope, table, after)
    }

    fn installed_activation_scope(&self) -> Result<&str> {
        let activation = self.native_activation()?;
        anyhow::ensure!(
            !super::consensus::callback_active(),
            "native storage cannot be used during a Raft callback"
        );
        let io = crate::host_io::installed_context()
            .ok_or_else(|| anyhow!("native storage is outside an executing turn"))?;
        anyhow::ensure!(
            Arc::ptr_eq(io.host_services(), &crate::asyncrt::services()),
            "native storage belongs to another execution domain"
        );
        anyhow::ensure!(
            io.native_activation
                .as_ref()
                .is_some_and(|installed| Arc::ptr_eq(installed, activation))
                && crate::storage::activation_epoch(&activation.scope) == Some(activation.epoch)
                && !activation.resources.retired.load(Ordering::Acquire),
            "native storage belongs to another cell or a retired activation"
        );
        anyhow::ensure!(
            activation.resources.source_admitted(
                self.2,
                io.framework_source.load(Ordering::Acquire),
                io.erasure_cleanup.load(Ordering::Acquire),
            ),
            "source business admission is closed or its incarnation has changed"
        );
        activation.drain_resources();
        Ok(&activation.scope)
    }

    fn activation_scope(&self) -> Result<&str> {
        let scope = self.installed_activation_scope()?;
        // Cached activation-bound handles consult the executing invocation's
        // budget too. Rollback/retirement deliberately use the installed scope
        // directly, so expiry can never prevent transaction cleanup.
        crate::host_io::installed_context()
            .expect("installed activation scope has a context")
            .check_rpc_authority()?;
        // A synchronous application poll can outlive forced host closure.
        // Recheck ordinary operations and commit here; rollback/retirement
        // deliberately retain installed_activation_scope on the original DB.
        anyhow::ensure!(
            crate::asyncrt::services().accepts_work(),
            "native storage execution domain is closed"
        );
        if let Some(failure) = self.resources().failure.lock().unwrap().as_ref() {
            anyhow::bail!("native cell was reset: {failure}");
        }
        Ok(scope)
    }

    fn data_activation_scope(&self) -> Result<&str> {
        match &self.0 {
            Owner::Native(_) => self.activation_scope(),
            Owner::RaftCommand(resources) => {
                crate::storage::require_raft_command_storage(resources, true)?;
                Ok(crate::storage::RAFT_STORAGE_SCOPE)
            }
            Owner::RaftQuery(resources) => {
                crate::storage::require_raft_query_storage(resources)?;
                Ok(crate::storage::RAFT_STORAGE_SCOPE)
            }
        }
    }

    fn installed_data_scope(&self) -> Result<&str> {
        match &self.0 {
            Owner::Native(_) => self.installed_activation_scope(),
            Owner::RaftCommand(resources) => {
                crate::storage::require_raft_command_storage(resources, false)?;
                Ok(crate::storage::RAFT_STORAGE_SCOPE)
            }
            Owner::RaftQuery(_) => Err(consensus::CommandFailure::UnsupportedCapability.into()),
        }
    }

    fn check_transaction_authority(&self) -> Result<()> {
        if let Some(transaction) = &self.1 {
            transaction.check_active()?;
        }
        let current = self.resources().transaction.lock().unwrap().clone();
        if let Some(current) = &current {
            current.check_active()?;
        }
        if let Some(open) = self.resources().open_transaction.lock().unwrap().as_ref() {
            anyhow::ensure!(
                current
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, open)),
                "native storage operation is outside the active transaction callback"
            );
        }
        Ok(())
    }

    pub(crate) fn scope(&self) -> Result<&str> {
        self.native_activation()?;
        self.data_scope()
    }

    fn cursor_view(&self) -> Result<Self> {
        self.data_scope()?;
        let owner = match &self.0 {
            Owner::RaftCommand(authority) => {
                Owner::RaftCommand(crate::storage::bind_raft_command_storage(authority)?)
            }
            owner => owner.clone(),
        };
        // Cursors retain execution authority rather than an ended savepoint.
        Ok(Self(owner, None, self.2))
    }

    fn data_scope(&self) -> Result<&str> {
        self.check_transaction_authority()?;
        let scope = self.data_activation_scope()?;
        if let Some(error) = crate::storage::sql_critical_error(scope) {
            anyhow::bail!(error);
        }
        Ok(scope)
    }

    fn write_scope(&self) -> Result<&str> {
        let scope = self.data_scope()?;
        anyhow::ensure!(
            !matches!(self.0, Owner::RaftQuery(_)),
            consensus::CommandFailure::UnsupportedCapability
        );
        Ok(scope)
    }

    fn cleanup(&self, cleanup: Cleanup) {
        if self.resources().retired.load(Ordering::Acquire) {
            return;
        }
        if self.activation_scope().is_ok() {
            cleanup.close();
        } else {
            self.resources().cleanup.lock().unwrap().push(cleanup);
        }
    }

    /// Read the original representation without discarding persisted clone data.
    pub fn get(&self, key: &str) -> Result<Option<crate::storage::StoredValue>> {
        crate::storage::get_stored(self.data_scope()?, key)
    }

    pub fn get_many(&self, keys: &[String]) -> Result<Vec<(String, crate::storage::StoredValue)>> {
        crate::storage::get_many_stored(self.data_scope()?, keys)
    }

    pub fn list(
        &self,
        options: &ListOptions,
    ) -> Result<Vec<(String, crate::storage::StoredValue)>> {
        crate::storage::list_stored_with_options(
            self.data_scope()?,
            options.start.as_deref(),
            options.end.as_deref(),
            options.start_after.as_deref(),
            options.prefix.as_deref(),
            options.limit,
            options.reverse,
        )
    }

    /// A keyset iterator with no retained SQLite read snapshot. Starting a
    /// second iterator on the same storage owner invalidates the first.
    /// Raft cursors check the original invocation or committed query on every
    /// step and retain no SQL worker, connection or image pin.
    pub fn list_cursor(&self, options: &ListOptions) -> Result<KvCursor> {
        KvCursor::new(self, options)
    }

    pub fn get_json(&self, key: &str) -> Result<Option<serde_json::Value>> {
        crate::storage::get_json_value(self.data_scope()?, key)
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
        let scope = self.write_scope()?;
        crate::storage::put_serialized(scope, key, &cellar_native::clone::encode(value)?)
    }

    pub fn put_stored_clone(&self, key: &str, value: &StoredClone) -> Result<()> {
        crate::storage::put_serialized(self.write_scope()?, key, &value.encode()?)
    }

    pub fn put_many_stored_clone(&self, entries: &[(String, StoredClone)]) -> Result<()> {
        let scope = self.write_scope()?;
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
        let scope = self.write_scope()?;
        let entries = entries
            .iter()
            .map(|(key, value)| Ok((key.clone(), cellar_native::clone::encode(value)?)))
            .collect::<Result<Vec<_>>>()?;
        crate::storage::put_many_serialized(scope, &entries)
    }

    pub fn put_json(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        crate::storage::put_json(self.write_scope()?, key, value)
    }

    pub fn put_many_json(&self, entries: &[(String, serde_json::Value)]) -> Result<()> {
        crate::storage::put_many_json(self.write_scope()?, entries)
    }

    pub fn delete(&self, key: &str) -> Result<bool> {
        crate::storage::delete(self.write_scope()?, key)
    }

    pub fn delete_many(&self, keys: &[String]) -> Result<usize> {
        crate::storage::delete_many(self.write_scope()?, keys)
    }

    /// Delete business storage in the current transaction. Raft receipt and
    /// client-sequence metadata survive erasure, so an exact retry cannot erase
    /// state created afterward. Committed queries refuse this operation.
    /// The admitted compatibility flag chooses whether alarms are also removed.
    pub fn delete_all(&self) -> Result<()> {
        let scope = self.write_scope()?;
        if self.resources().filesystem.has_live() {
            return Err(super::filesystem::Error::new(
                super::filesystem::Errno::EBUSY,
                "filesystem descriptors remain open",
            )
            .with_code("FilesystemPresent")
            .into());
        }
        let delete_alarm = match &self.0 {
            Owner::Native(activation) => activation.delete_all_deletes_alarm,
            Owner::RaftCommand(_) => self
                .resources()
                .delete_all_deletes_alarm
                .load(Ordering::Acquire),
            Owner::RaftQuery(_) => unreachable!("write_scope refuses committed queries"),
        };
        crate::storage::delete_all_with_alarm(scope, delete_alarm)?;
        match &self.0 {
            Owner::Native(_) => crate::native_worker::refresh_wake_gate(scope),
            _ => Ok(()),
        }
    }

    /// Wait for the current cell's write durability. In an ordinary Raft RPC,
    /// this owns a checkpoint after preceding root transactions and follows the
    /// invocation's quorum/S3 minimum. Call it outside transaction callbacks.
    pub fn sync(&self) -> HandlerFuture<()> {
        if matches!(&self.0, Owner::RaftCommand(_)) {
            return self.raft_sync();
        }
        match self.scope() {
            Ok(scope) => crate::native_worker::storage_sync(scope),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    pub fn sql(&self, query: &str, bindings: &[serde_json::Value]) -> Result<SqlResult> {
        crate::storage::sql_exec(self.data_scope()?, query, bindings).map_err(anyhow::Error::msg)
    }

    /// A lazy statement in the active native turn, Raft command or committed
    /// query. Live Raft statements retain their private connection; unfinished
    /// writes must be consumed or dropped before checkpoint/output publication.
    pub fn sql_cursor(&self, query: &str, bindings: &[SqlValue]) -> Result<SqlCursor> {
        SqlCursor::new(self, query, bindings)
    }

    pub fn sql_database_size(&self) -> Result<u64> {
        crate::storage::sql_database_size(self.data_scope()?).map_err(anyhow::Error::msg)
    }

    pub(crate) fn sqlite_allocation(&self) -> Result<(i64, i64)> {
        let (allocated, used) =
            crate::storage::sql_page_usage(self.scope()?).map_err(anyhow::Error::msg)?;
        Ok((i64::try_from(allocated)?, i64::try_from(used)?))
    }

    /// Execute complete SQL statements and retain the unfinished suffix for
    /// incremental import, using SQLite's existing tokenizer.
    pub fn sql_ingest(&self, input: &str) -> Result<(String, u64, u64)> {
        crate::storage::sql_ingest(self.write_scope()?, input).map_err(anyhow::Error::msg)
    }

    pub fn get_alarm(&self) -> Result<Option<i64>> {
        if self.is_raft() {
            self.data_scope()?;
            return crate::storage::raft_native_work(|db| consensus::native_work::get(db, "alarm"));
        }
        Ok(crate::storage::get_alarm(self.scope()?))
    }

    pub fn set_alarm(&self, at_ms: i64) -> Result<()> {
        if self.is_raft() {
            return self.arm_raft_work("alarm", at_ms);
        }
        crate::native_worker::set_alarm(self.scope()?, at_ms)
    }

    pub fn delete_alarm(&self) -> Result<()> {
        if self.is_raft() {
            return self.cancel_raft_work("alarm");
        }
        let scope = self.scope()?;
        crate::storage::delete_alarm(scope)?;
        crate::native_worker::refresh_wake_gate(scope)
    }
}
