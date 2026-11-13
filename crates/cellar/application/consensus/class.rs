// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Compiled Raft operations and their private application callbacks.

use super::{
    native_cell::{CellRpcActivation, CellRpcFactory, CellRpcMethod, NativeCellFactory},
    CommandId, CommittedVersion, Durability, RouteEpoch,
};
use crate::application::HandlerFuture;
use crate::raft::version::CellVersionObservation;
use crate::storage::{CommandInputs, CommandTransaction, ImmutableQuery, QueryFrame};
use cellar_logic::{cell_group::CellHistory, NamespaceMode};
use futures_util::FutureExt;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
    task::{Context as TaskContext, Poll},
};

thread_local! { static CALLBACK_ACTIVE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
thread_local! { static FRAMEWORK_DISPATCH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

pub(crate) fn require_framework_dispatch() -> anyhow::Result<()> {
    anyhow::ensure!(
        FRAMEWORK_DISPATCH.with(std::cell::Cell::get),
        super::CommandFailure::UnsupportedCapability
    );
    Ok(())
}

pub(crate) fn callback_active() -> bool {
    CALLBACK_ACTIVE.with(std::cell::Cell::get)
}

/// The private SDK adapter alone polls under this scope, after its source root
/// is proven. Application SQL callbacks retain the ordinary dispatch refusal.
pub(crate) fn framework_dispatch<T>(
    permit: &crate::application::storage::RaftDispatchPermit,
    callback: impl FnOnce() -> T,
) -> anyhow::Result<T> {
    permit.check()?;

    struct Restore(bool, bool);

    impl Drop for Restore {
        fn drop(&mut self) {
            CALLBACK_ACTIVE.with(|active| active.set(self.0));
            FRAMEWORK_DISPATCH.with(|active| active.set(self.1));
        }
    }
    let _restore = Restore(
        CALLBACK_ACTIVE.with(|active| active.replace(false)),
        FRAMEWORK_DISPATCH.with(|active| active.replace(true)),
    );
    Ok(callback())
}

struct CallbackGuard;

impl CallbackGuard {
    fn enter() -> anyhow::Result<Self> {
        if CALLBACK_ACTIVE.with(|active| active.replace(true)) {
            anyhow::bail!(super::CommandFailure::UnsupportedCapability);
        }
        Ok(Self)
    }
}

impl Drop for CallbackGuard {
    fn drop(&mut self) {
        CALLBACK_ACTIVE.with(|active| active.set(false));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationKind {
    Command,
    Query,
}

/// Ingress and destination both check the operation's kind and compiled digest
/// before running application code. A version token is not access permission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationContract {
    pub operation: String,
    pub kind: OperationKind,
    pub digest: [u8; 32],
}

/// Digest of the exact normalized input bytes sent by native or HTTP ingress.
/// Operation and contract identity are separately retained in the receipt.
pub fn command_request_digest(input: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"cellar-raft-command-input-1");
    hash.update((input.len() as u64).to_be_bytes());
    hash.update(input);
    hash.finalize().into()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegistrationError {
    Invalid(&'static str),
    Duplicate(String),
    Limit,
    UnknownClass(String),
    UnsupportedFactory(String),
    WrongMode { class: String, mode: NamespaceMode },
    UnknownOperation(String),
    KindMismatch,
    ContractMismatch,
}

impl fmt::Display for RegistrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Raft registration: {self:?}")
    }
}

impl std::error::Error for RegistrationError {}

/// Borrowed for one synchronous command transaction. This context has no
/// outbound network, cell-call or task-spawn capability. SQL and KV are
/// scoped to the private command transaction.
/// Explicitly declared alarms persist intents through the command transaction.
/// Native code is trusted; this contract is not an operating-system sandbox.
/// Mutable Fleet capabilities are absent from this context, including bindings
/// declared elsewhere in a mixed Fleet/Raft native application.
///
/// ```compile_fail
/// use cellar::sdk::consensus::CommandContext;
/// fn workflow(context: &CommandContext<'_>) { let _ = context.workflow("FLOW"); }
/// ```
/// ```compile_fail
/// use cellar::sdk::consensus::CommandContext;
/// fn queue(context: &CommandContext<'_>) { let _ = context.queue("JOBS"); }
/// ```
/// ```compile_fail
/// use cellar::sdk::consensus::CommandContext;
/// fn container(context: &CommandContext<'_>) { let _ = context.container(); }
/// ```
/// ```compile_fail
/// use cellar::sdk::consensus::CommandContext;
/// fn socket(context: &CommandContext<'_>) { let _ = context.websocket_pair(); }
/// ```
pub struct CommandContext<'a> {
    scope: &'a CellHistory,
    command: CommandId,
    transaction: &'a CommandTransaction<'a>,
    blobs: Option<super::RaftBlobLimits>,
    alarm_limits: Option<super::RaftAlarmLimits>,
    alarm_names: &'a BTreeSet<String>,
    durability: Durability,
    schema_digest: [u8; 32],
    definition_digest: [u8; 32],
    outbox: Option<&'a super::RaftOutbox>,
}

impl<'a> CommandContext<'a> {
    pub fn identity(&self) -> &CellHistory {
        self.scope
    }

    pub fn command(&self) -> CommandId {
        self.command
    }

    pub fn schema_digest(&self) -> [u8; 32] {
        self.schema_digest
    }

    pub(crate) fn definition_digest(&self) -> [u8; 32] {
        self.definition_digest
    }

    pub(crate) fn durability(&self) -> Durability {
        self.durability
    }

    pub fn inputs(&self) -> CommandInputs {
        self.transaction.inputs()
    }

    /// SQL runs inside the framework's receipt transaction. The connection and
    /// commit/rollback operations are never exposed to the callback.
    pub fn sql(&self) -> &CommandTransaction<'a> {
        self.transaction
    }

    /// Access SQL and KV storage inside this command's private transaction.
    /// The view expires when the command callback ends. Successful local
    /// operations are included in the command receipt's durability proof.
    pub fn storage(&self) -> anyhow::Result<crate::application::Storage> {
        self.transaction.storage()
    }

    /// Retain actual immutable bytes in this command's transaction. Returned
    /// references supply no durability proof before the command commits.
    pub fn put_blob(&self, bytes: &[u8]) -> anyhow::Result<super::RaftBlobReference> {
        crate::raft::effects::put_blob(
            &*self.transaction.framework()?,
            self.blobs
                .ok_or(super::CommandFailure::UnsupportedCapability)?,
            bytes,
        )
    }

    pub fn blob(&self, reference: super::RaftBlobReference) -> anyhow::Result<Vec<u8>> {
        crate::raft::effects::get_blob(
            &*self.transaction.framework()?,
            self.blobs
                .ok_or(super::CommandFailure::UnsupportedCapability)?,
            reference,
        )
    }
}

impl CommandContext<'_> {
    /// Append immutable bytes to a declared delivery lane in this transaction.
    /// The returned lane sequence supplies no commit or external-effect proof.
    pub fn emit(&self, lane: &str, payload: &[u8]) -> anyhow::Result<u64> {
        self.outbox
            .ok_or(super::CommandFailure::UnsupportedCapability)?
            .emit(self, lane, payload)
    }

    /// Arm a declared alarm in this transaction. Only its committed generation
    /// can fire; setting it again replaces the previously selected generation.
    pub fn set_alarm(
        &self,
        name: &str,
        due_unix_millis: u64,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.alarm_names.contains(name),
            super::CommandFailure::UnsupportedCapability
        );
        crate::raft::effects::alarm::arm(
            self,
            self.durability,
            name,
            due_unix_millis,
            payload,
            self.alarm_limits
                .ok_or(super::CommandFailure::UnsupportedCapability)?,
        )
    }

    pub fn cancel_alarm(&self, name: &str) -> anyhow::Result<bool> {
        anyhow::ensure!(
            self.alarm_names.contains(name),
            super::CommandFailure::UnsupportedCapability
        );
        crate::raft::effects::alarm::cancel(&*self.transaction.framework()?, name)
    }
}

/// Exact observation supplied by the owning query path. A group cut carries no
/// cell mutation sequence and must never be relabeled as a cell version.
#[derive(Clone, Copy, Debug)]
pub enum QueryVersion<'a> {
    Cell(&'a CellVersionObservation),
    /// Temporary single-cell host boundary, removed with that host in CG-16.
    /// This is an in-memory observation, not an old-format recovery reader.
    GroupCut(&'a CommittedVersion),
}

/// A pinned, read-only committed image. The host establishes consistency before
/// constructing this context. Query code cannot open or migrate writable state.
pub struct QueryContext<'a> {
    identity: &'a CellHistory,
    version: QueryVersion<'a>,
    frame: &'a QueryFrame,
    blobs: Option<super::RaftBlobLimits>,
    outbox: Option<&'a super::RaftOutbox>,
    schema: [u8; 32],
}

impl QueryContext<'_> {
    pub(super) fn sql_ingress_sequence(&self, input: &[u8]) -> anyhow::Result<Vec<u8>> {
        let QueryVersion::Cell(observation) = self.version else {
            anyhow::bail!(super::CommandFailure::UnsupportedCapability);
        };
        super::sql_ingress_identity::observe(
            self.frame.database().connection(),
            observation.version.sequence.get(),
            input,
        )
    }

    pub fn outbox_progress(&self, lane: &str) -> anyhow::Result<super::RaftOutboxProgress> {
        crate::raft::effects::outbox::progress(
            self.frame.database().connection(),
            self.outbox
                .ok_or(super::CommandFailure::UnsupportedCapability)?,
            self.identity,
            self.schema,
            lane,
        )
    }

    pub fn identity(&self) -> &CellHistory {
        self.identity
    }

    pub fn version(&self) -> QueryVersion<'_> {
        self.version
    }

    /// Read SQL and KV from the same committed image as this query's version.
    /// Storage views expire when the callback returns and refuse mutations.
    pub fn storage(&self) -> crate::application::Storage {
        self.frame.storage()
    }

    /// Read only bytes present in this query's pinned committed generation.
    pub fn blob(&self, reference: super::RaftBlobReference) -> anyhow::Result<Vec<u8>> {
        crate::raft::effects::get_blob(
            self.frame.database().connection(),
            self.blobs
                .ok_or(super::CommandFailure::UnsupportedCapability)?,
            reference,
        )
    }

    pub fn query_row<T>(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
        read: impl FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    ) -> anyhow::Result<T> {
        self.frame.database().query_row(sql, params, read)
    }
}

type CommandCallback =
    dyn for<'a> Fn(&CommandContext<'a>, &[u8]) -> anyhow::Result<Vec<u8>> + Send + Sync;
type QueryCallback =
    dyn for<'a> Fn(&QueryContext<'a>, &[u8]) -> anyhow::Result<Vec<u8>> + Send + Sync;
type InitializerCallback =
    dyn Fn(&CellHistory, &CommandTransaction<'_>) -> anyhow::Result<()> + Send + Sync;

#[derive(Clone)]
struct Initializer {
    digest: [u8; 32],
    callback: Arc<InitializerCallback>,
}

#[derive(Clone)]
struct Command {
    digest: [u8; 32],
    handler: CommandHandler,
}

#[derive(Clone)]
enum CommandHandler {
    Registered(Arc<CommandCallback>),
    JsonCellRpc,
    NativeCellRpc { method: String, json_only: bool },
    NativeWork,
}

pub(crate) struct CommandTarget<'a> {
    pub(crate) cell: &'a CellHistory,
    pub(crate) route_epoch: Option<RouteEpoch>,
}

enum RegisteredCommand<'a> {
    Application(&'a OperationContract),
    System(&'a OperationContract),
}

#[derive(Clone)]
struct Query {
    digest: [u8; 32],
    callback: Arc<QueryCallback>,
}

/// A compiled definition for a Raft namespace. Register direct callbacks with
/// `Registry::raft_class` or bind ordinary cell RPC handlers with
/// `Registry::raft_cell`. Schema versions do not authorize automatic migration.
pub struct RaftClass {
    name: String,
    schema_version: u32,
    base_schema_digest: [u8; 32],
    schema_digest: [u8; 32],
    commands: BTreeMap<String, Command>,
    initializer: Option<Initializer>,
    system_commands: BTreeMap<String, Command>,
    queries: BTreeMap<String, Query>,
    blobs: Option<super::RaftBlobLimits>,
    alarm_limits: Option<super::RaftAlarmLimits>,
    alarm_names: BTreeSet<String>,
    inboxes: BTreeMap<String, super::RaftInbox>,
    outbox: Option<Arc<super::RaftOutbox>>,
    cell_factory: Option<Arc<CellRpcFactory>>,
}

impl RaftClass {
    pub fn new(
        name: impl Into<String>,
        schema_version: u32,
        schema_digest: [u8; 32],
    ) -> Result<Self, RegistrationError> {
        let name = name.into();
        validate_name(&name)?;
        if crate::deploy::is_reserved_class(&name) {
            return Err(RegistrationError::Invalid("reserved class"));
        }
        Self::definition(name, schema_version, schema_digest)
    }

    // Only the runtime can define the reserved SQL service. Public factories
    // cannot substitute an application handler for a shared database identity.
    pub(super) fn sql_service() -> Result<Self, RegistrationError> {
        Self::definition(
            crate::deploy::SQL_CLASS.into(),
            1,
            Sha256::digest(b"cellar-raft-sql-service-operator-1").into(),
        )
    }

    pub(super) fn queue_service() -> Result<Self, RegistrationError> {
        Self::definition(
            crate::deploy::QUEUE_CLASS.into(),
            1,
            Sha256::digest(b"cellar-raft-queue-service-1").into(),
        )
    }

    fn definition(
        name: String,
        schema_version: u32,
        schema_digest: [u8; 32],
    ) -> Result<Self, RegistrationError> {
        if schema_version == 0 || schema_digest == [0; 32] {
            return Err(RegistrationError::Invalid("schema version/digest"));
        }
        // The namespace's immutable schema identity includes the explicit
        // version; changing it cannot retain the old root merely by repeating
        // the user-supplied definition digest.
        let mut hash = Sha256::new();
        hash.update(b"cellar-raft-schema-1");
        hash.update((name.len() as u16).to_be_bytes());
        hash.update(name.as_bytes());
        hash.update(schema_version.to_be_bytes());
        hash.update(schema_digest);
        let schema_digest = hash.finalize().into();
        let mut class = Self {
            name,
            schema_version,
            base_schema_digest: schema_digest,
            schema_digest,
            commands: BTreeMap::new(),
            initializer: None,
            system_commands: BTreeMap::new(),
            queries: BTreeMap::new(),
            blobs: None,
            alarm_limits: None,
            alarm_names: BTreeSet::new(),
            inboxes: BTreeMap::new(),
            outbox: None,
            cell_factory: None,
        };
        class.bind_command_manifest();
        Ok(class)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// Namespace/application identity binding class name, explicit version and
    /// the supplied schema-definition digest and required command contracts.
    /// Query contracts are checked independently and do not change voting
    /// compatibility. Ordinary cell factories also bind their admitted deployment
    /// configuration. Resolve the active application definition before provisioning
    /// or preparing a host; unconfigured definitions cannot serve its history.
    pub fn schema_digest(&self) -> [u8; 32] {
        self.schema_digest
    }

    /// Stable declaration identity binding class name, explicit schema version
    /// and supplied definition digest. Effect targets pair this value with an
    /// exact operation contract, avoiding recursive hashes for self/cyclic
    /// delivery. Namespace admission still requires the complete schema_digest.
    pub fn definition_digest(&self) -> [u8; 32] {
        self.base_schema_digest
    }

    /// Register schema initialization for shared-group enrollment. The digest
    /// participates in voting compatibility. Initialization runs once on a
    /// private SQL transaction; only its committed creation record is success.
    pub fn initializer(
        mut self,
        digest: [u8; 32],
        callback: impl Fn(&CellHistory, &CommandTransaction<'_>) -> anyhow::Result<()>
            + Send
            + Sync
            + 'static,
    ) -> Result<Self, RegistrationError> {
        if digest == [0; 32] || self.initializer.is_some() {
            return Err(RegistrationError::Invalid("initializer registration"));
        }
        self.initializer = Some(Initializer {
            digest,
            callback: Arc::new(callback),
        });
        self.bind_command_manifest();
        Ok(self)
    }

    pub(crate) fn require_initializer(&self) -> Result<(), RegistrationError> {
        self.initializer
            .as_ref()
            .map(|_| ())
            .ok_or(RegistrationError::Invalid("missing initializer"))
    }

    pub(crate) fn invoke_initializer(
        &self,
        cell: &CellHistory,
        transaction: &CommandTransaction<'_>,
    ) -> anyhow::Result<()> {
        let initializer = self
            .initializer
            .as_ref()
            .ok_or(RegistrationError::Invalid("missing initializer"))?;
        let _guard = CallbackGuard::enter()?;
        (initializer.callback)(cell, transaction)
    }

    /// Opt in to bounded immutable bytes replicated with the cell. This changes
    /// the class identity and requires a fresh compatible namespace.
    pub fn immutable_blobs(
        mut self,
        limits: super::RaftBlobLimits,
    ) -> Result<Self, RegistrationError> {
        limits
            .validate()
            .map_err(|_| RegistrationError::Invalid("Blob limits"))?;
        if self.blobs.is_some() {
            return Err(RegistrationError::Invalid("duplicate Blob capability"));
        }
        self.blobs = Some(limits);
        self.bind_command_manifest();
        Ok(self)
    }

    pub fn alarms(mut self, limits: super::RaftAlarmLimits) -> Result<Self, RegistrationError> {
        limits
            .validate()
            .map_err(|_| RegistrationError::Invalid("alarm limits"))?;
        if self.alarm_limits.is_some() {
            return Err(RegistrationError::Invalid("duplicate alarm capability"));
        }
        self.alarm_limits = Some(limits);
        self.bind_command_manifest();
        Ok(self)
    }

    /// The owned scheduler invokes this synchronous transaction callback through
    /// a private replicated command after selecting its committed generation.
    pub fn alarm(
        mut self,
        name: impl Into<String>,
        digest: [u8; 32],
        callback: impl for<'a> Fn(&CommandContext<'a>, &[u8]) -> anyhow::Result<()>
            + Send
            + Sync
            + 'static,
    ) -> Result<Self, RegistrationError> {
        let name = name.into();
        self.validate_new(&name, digest)?;
        let limits = self.alarm_limits.unwrap_or_default();
        if name.len() > 64 || self.alarm_names.len() >= limits.max_alarms {
            return Err(RegistrationError::Limit);
        }
        self.alarm_limits = Some(limits);
        self.alarm_names.insert(name.clone());
        let operation = crate::raft::effects::alarm::operation(&name);
        let mut hash = Sha256::new();
        hash.update(b"cellar-raft-alarm-callback-1");
        hash.update(digest);
        self.register_system_command(operation, hash.finalize().into(), move |context, input| {
            crate::raft::effects::alarm::fire(context, &name, limits, input, &callback)
        })?;
        Ok(self)
    }

    pub(crate) fn alarm_limits(&self) -> Option<super::RaftAlarmLimits> {
        self.alarm_limits
    }

    pub fn outbox(mut self, outbox: super::RaftOutbox) -> Result<Self, RegistrationError> {
        if self.outbox.is_some() || outbox.bindings.is_empty() {
            return Err(RegistrationError::Invalid("empty/duplicate outbox"));
        }
        let identity = outbox
            .identity()
            .map_err(|_| RegistrationError::Invalid("outbox identity"))?;
        let outbox = Arc::new(outbox);
        self.outbox = Some(outbox.clone());
        let claim = outbox.clone();
        self.register_system_command(
            crate::raft::effects::outbox::CLAIM.into(),
            identity,
            move |context, input| claim.claim(context, input),
        )?;
        self.register_system_command(
            crate::raft::effects::outbox::COMPLETE.into(),
            identity,
            move |context, input| outbox.complete(context, input),
        )?;
        Ok(self)
    }

    pub(crate) fn outbox_definition(&self) -> Option<&Arc<super::RaftOutbox>> {
        self.outbox.as_ref()
    }

    /// Shared groups support initializers. All other declared capabilities
    /// retain the same execution, receipt, scheduler and archive bounds.
    pub(crate) fn validate_shared_host(
        &self,
        host: &crate::raft::host::RaftHostOptions,
    ) -> anyhow::Result<()> {
        self.validate_cell_factory()?;
        if self.has_native_callbacks() {
            anyhow::ensure!(
                host.execution.max_pending_transactions > 0
                    && host.max_input_bytes >= 1024
                    && host.execution.max_client_identities >= 2
                    && host.execution.max_retained_receipts >= 2,
                "durable callbacks require checkpoint and receipt capacity"
            );
        }
        if let Some(outbox) = &self.outbox {
            anyhow::ensure!(host.effects.is_some_and(|options|options.metadata_bytes>=512*1024)
                && host.max_input_bytes>=32*1024
                && host.execution.max_result_bytes>=33+crate::raft::effects::delivery::MAX_REQUEST
                && host.execution.max_client_identities>=2
                && host.execution.max_retained_receipts>=3
                && (outbox.limits.max_total_bytes as u64).checked_add(128*1024).is_some_and(|n|n<=host.execution.max_image_bytes),
                "outbox requires owned scheduler and bounded metadata/command/receipt/image capacity");
        }
        for inbox in self.inboxes.values() {
            let internal = usize::from(self.has_system_commands());
            anyhow::ensure!(
                host.max_input_bytes >= crate::raft::effects::delivery::MAX_REQUEST
                    && host.execution.max_result_bytes
                        >= crate::raft::effects::delivery::MAX_RECEIPT
                    && inbox.max_sources + 1 + internal <= host.execution.max_client_identities
                    && inbox.max_sources + 1 + 2 * internal <= host.execution.max_retained_receipts
                    && (inbox.max_sources as u64)
                        .checked_mul(crate::raft::effects::delivery::MAX_RECEIPT as u64)
                        .and_then(|n| n.checked_add(128 * 1024))
                        .is_some_and(|n| n <= host.execution.max_image_bytes),
                "inbox exceeds host command/receipt/image limits"
            );
        }
        if let Some(limits) = self.alarm_limits {
            anyhow::ensure!(
                host.effects.is_some()
                    && host.max_input_bytes >= 1024
                    && host.execution.max_client_identities >= 2
                    && host.execution.max_retained_receipts >= 2
                    && limits
                        .max_payload_bytes
                        .checked_mul(limits.max_alarms)
                        .and_then(|n| n.checked_add(128 * 1024))
                        .is_some_and(|n| n as u64 <= host.execution.max_image_bytes),
                "alarm capability requires an enabled scheduler, internal command capacity and image capacity"
            );
        }
        if let Some(limits) = self.blobs {
            let archive = host.archive.limits;
            anyhow::ensure!(
                limits.max_objects <= archive.max_dependencies
                    && limits.max_blob_bytes <= archive.max_object_bytes
                    && limits.max_blob_bytes <= host.execution.max_result_bytes
                    && (limits.max_total_bytes as u64)
                        .checked_add(128 * 1024)
                        .is_some_and(|n| n <= host.execution.max_image_bytes)
                    && limits
                        .max_total_bytes
                        .checked_add(128 * 1024)
                        .is_some_and(|n| n <= archive.max_capture_bytes)
                    && (limits.max_total_bytes as u64)
                        .checked_add(host.execution.max_image_bytes)
                        .is_some_and(|n| n <= archive.max_set_bytes as u64),
                "Blob capability exceeds host archive/execution bounds"
            );
        }
        Ok(())
    }

    pub fn command(
        mut self,
        operation: impl Into<String>,
        digest: [u8; 32],
        callback: impl for<'a> Fn(&CommandContext<'a>, &[u8]) -> anyhow::Result<Vec<u8>>
            + Send
            + Sync
            + 'static,
    ) -> Result<Self, RegistrationError> {
        let operation = operation.into();
        self.validate_new(&operation, digest)?;
        self.commands.insert(
            operation,
            Command {
                digest,
                handler: CommandHandler::Registered(Arc::new(callback)),
            },
        );
        self.bind_command_manifest();
        Ok(self)
    }

    /// Route a JSON RPC operation to the ordinary `Cell::invoke` handler
    /// supplied by `Registry::raft_cell`. The factory and handler share one
    /// private Storage transaction and the JSON reply is retained in its receipt.
    /// The shared SQL owner resumes a suspended handler under the original
    /// transaction and routing authority.
    pub fn json_cell_rpc(
        mut self,
        operation: impl Into<String>,
        digest: [u8; 32],
    ) -> Result<Self, RegistrationError> {
        let operation = operation.into();
        self.validate_new(&operation, digest)?;
        self.commands.insert(
            operation,
            Command {
                digest,
                handler: CommandHandler::JsonCellRpc,
            },
        );
        self.bind_command_manifest();
        Ok(self)
    }

    /// Enable ordinary durable Alarm callbacks on the shared scheduler. Queue
    /// services use this same kernel. Hosts install bounded scheduler defaults;
    /// configure `RaftEffectOptions` to tune them.
    /// Callbacks are at least once after interrupted execution; external effects
    /// require application idempotency. Followers apply bytes, never callbacks.
    pub fn durable_callbacks(mut self) -> Result<Self, RegistrationError> {
        if self
            .system_commands
            .contains_key(super::native_work::OPERATION)
        {
            return Err(RegistrationError::Invalid("duplicate durable callbacks"));
        }
        self.system_commands.insert(
            super::native_work::OPERATION.into(),
            Command {
                digest: Sha256::digest(b"cellar-native-work-callback-1").into(),
                handler: CommandHandler::NativeWork,
            },
        );
        self.bind_command_manifest();
        Ok(self)
    }

    /// Route owned JSON or structured RPC values through the ordinary cell.
    /// Leased capabilities require their separate session adapter.
    pub fn cell_rpc(
        mut self,
        operation: impl Into<String>,
        digest: [u8; 32],
    ) -> Result<Self, RegistrationError> {
        let operation = operation.into();
        self.validate_new(&operation, digest)?;
        self.commands.insert(
            operation.clone(),
            Command {
                digest,
                handler: CommandHandler::NativeCellRpc {
                    method: operation,
                    json_only: false,
                },
            },
        );
        self.bind_command_manifest();
        Ok(self)
    }

    pub(crate) fn binding_contract(&self, operation: &str) -> Option<OperationContract> {
        if (self.name == crate::deploy::SQL_CLASS
            && operation == crate::application::sql::operator::METHOD)
            || (self.name == crate::deploy::QUEUE_CLASS
                && operation == crate::application::queue::OPERATOR)
        {
            return None;
        }
        let command = self.commands.get(operation)?;
        match &command.handler {
            CommandHandler::NativeCellRpc { .. } => self.contract(operation),
            CommandHandler::JsonCellRpc => {
                self.contract(&super::native_rpc::binding_operation(operation))
            }
            CommandHandler::Registered(_) | CommandHandler::NativeWork => None,
        }
    }

    pub(crate) fn sql_operator_contract(&self) -> Option<OperationContract> {
        (self.name == crate::deploy::SQL_CLASS)
            .then(|| self.contract(crate::application::sql::operator::METHOD))
            .flatten()
    }

    pub(crate) fn queue_operator_contract(&self) -> Option<OperationContract> {
        (self.name == crate::deploy::QUEUE_CLASS)
            .then(|| self.contract(crate::application::queue::OPERATOR))
            .flatten()
    }

    pub(crate) fn binding_json_only(&self, operation: &str) -> Option<bool> {
        match &self.commands.get(operation)?.handler {
            CommandHandler::JsonCellRpc => Some(true),
            CommandHandler::NativeCellRpc { json_only, .. } => Some(*json_only),
            CommandHandler::Registered(_) | CommandHandler::NativeWork => None,
        }
    }

    pub(in crate::application) fn validate_binding_configuration(
        &self,
    ) -> Result<(), RegistrationError> {
        let mut operations: BTreeSet<_> = self
            .commands
            .keys()
            .chain(self.queries.keys())
            .cloned()
            .collect();
        for (name, command) in &self.commands {
            if matches!(command.handler, CommandHandler::JsonCellRpc) {
                let wire = super::native_rpc::binding_operation(name);
                if !operations.insert(wire.clone()) {
                    return Err(RegistrationError::Duplicate(wire));
                }
            }
        }
        if operations.len() > 256 {
            return Err(RegistrationError::Limit);
        }
        Ok(())
    }

    pub(in crate::application) fn with_cell_factory(
        mut self,
        factory: crate::application::Factory,
    ) -> Result<Self, RegistrationError> {
        if self.cell_factory.is_some() || !self.uses_cell_factory() {
            return Err(RegistrationError::Invalid("Raft cell factory registration"));
        }
        self.cell_factory = Some(NativeCellFactory::bind(
            &self.name,
            factory,
            self.system_commands
                .contains_key(super::native_work::OPERATION),
        ));
        Ok(self)
    }

    pub(crate) fn has_cell_factory(&self) -> bool {
        self.cell_factory.is_some()
    }

    pub(crate) fn cell_configuration_digest(&self) -> Option<[u8; 32]> {
        self.cell_factory
            .as_ref()
            .and_then(|factory| factory.configuration_digest())
    }

    pub(crate) fn cell_binding_classes(&self) -> Option<&BTreeSet<String>> {
        self.cell_factory
            .as_ref()
            .map(|factory| factory.binding_classes())
    }

    pub(crate) fn allows_cell_binding(&self, scope: &str) -> bool {
        self.cell_factory
            .as_ref()
            .is_some_and(|factory| factory.allows_binding(scope))
    }

    pub(in crate::application) fn configured_cell(
        &self,
        configuration: &super::AdmittedCellConfiguration,
    ) -> Self {
        let mut class = Self {
            name: self.name.clone(),
            schema_version: self.schema_version,
            base_schema_digest: self.base_schema_digest,
            schema_digest: self.schema_digest,
            commands: self.commands.clone(),
            initializer: self.initializer.clone(),
            system_commands: self.system_commands.clone(),
            queries: self.queries.clone(),
            blobs: self.blobs,
            alarm_limits: self.alarm_limits,
            alarm_names: self.alarm_names.clone(),
            inboxes: self.inboxes.clone(),
            outbox: self.outbox.clone(),
            cell_factory: self.cell_factory.clone(),
        };
        if let Some(factory) = &self.cell_factory {
            if configuration.has_cell_bindings() {
                for (name, command) in &self.commands {
                    if matches!(command.handler, CommandHandler::JsonCellRpc) {
                        class.commands.insert(
                            super::native_rpc::binding_operation(name),
                            Command {
                                digest: super::native_rpc::binding_digest(name, command.digest),
                                handler: CommandHandler::NativeCellRpc {
                                    method: name.clone(),
                                    json_only: true,
                                },
                            },
                        );
                    }
                }
            }
            class.cell_factory = Some(factory.configured(configuration));
            class.bind_command_manifest();
        }
        class
    }

    pub(crate) fn validate_cell_factory(&self) -> Result<(), RegistrationError> {
        if self.uses_cell_factory() && self.cell_factory.is_none() {
            return Err(RegistrationError::UnsupportedFactory(self.name.clone()));
        }
        Ok(())
    }

    fn uses_cell_factory(&self) -> bool {
        self.commands
            .values()
            .chain(self.system_commands.values())
            .any(|command| {
                matches!(
                    command.handler,
                    CommandHandler::JsonCellRpc
                        | CommandHandler::NativeCellRpc { .. }
                        | CommandHandler::NativeWork
                )
            })
    }

    pub(crate) fn is_cell_rpc(&self, operation: &str) -> bool {
        self.commands
            .get(operation)
            .or_else(|| self.system_commands.get(operation))
            .is_some_and(|command| {
                matches!(
                    command.handler,
                    CommandHandler::JsonCellRpc
                        | CommandHandler::NativeCellRpc { .. }
                        | CommandHandler::NativeWork
                )
            })
    }

    pub(crate) fn start_cell_rpc(
        &self,
        contract: &OperationContract,
        target: CommandTarget<'_>,
        input: &[u8],
        transaction: &CommandTransaction<'_>,
        activation: &mut Option<CellRpcActivation>,
    ) -> anyhow::Result<HandlerFuture<Vec<u8>>> {
        if contract.operation == super::native_work::OPERATION {
            self.validate_system_operation(contract)?;
        } else {
            self.validate_operation(contract)?;
        }
        anyhow::ensure!(
            self.is_cell_rpc(&contract.operation),
            RegistrationError::KindMismatch
        );
        let factory = self
            .cell_factory
            .as_ref()
            .ok_or(super::CommandFailure::UnsupportedCapability)?;
        let epoch = target
            .route_epoch
            .ok_or(super::CommandFailure::UnsupportedCapability)?;
        let _guard = CallbackGuard::enter()?;
        let method = match &self
            .commands
            .get(&contract.operation)
            .or_else(|| self.system_commands.get(&contract.operation))
            .ok_or(RegistrationError::KindMismatch)?
            .handler
        {
            CommandHandler::JsonCellRpc => CellRpcMethod::Json(&contract.operation),
            CommandHandler::NativeCellRpc { method, json_only } => CellRpcMethod::Owned {
                method,
                json_only: *json_only,
            },
            CommandHandler::NativeWork => CellRpcMethod::Work,
            CommandHandler::Registered(_) => return Err(RegistrationError::KindMismatch.into()),
        };
        factory.start(target.cell, epoch, method, input, transaction, activation)
    }

    pub(crate) fn accepts_cell_activation(&self, activation: &CellRpcActivation) -> bool {
        self.cell_factory
            .as_ref()
            .is_some_and(|factory| factory.accepts(activation))
    }

    pub(crate) fn poll_cell_rpc(
        future: &mut HandlerFuture<Vec<u8>>,
        context: &mut TaskContext<'_>,
    ) -> Poll<anyhow::Result<Vec<u8>>> {
        let _guard = CallbackGuard::enter()?;
        future.as_mut().poll(context)
    }

    pub(crate) fn poll_cell_work(
        future: &mut HandlerFuture<()>,
        context: &mut TaskContext<'_>,
    ) -> Poll<anyhow::Result<()>> {
        let _guard = CallbackGuard::enter()?;
        future.as_mut().poll(context)
    }

    /// Register a normal command with authenticated, durable receiver receipts.
    /// Business changes and the deduplication record commit atomically. Native
    /// callbacks remain trusted and must not leak a receipt before commit.
    pub fn inbox(
        mut self,
        operation: impl Into<String>,
        digest: [u8; 32],
        inbox: super::RaftInbox,
        callback: impl for<'a> Fn(&CommandContext<'a>, &[u8]) -> anyhow::Result<Vec<u8>>
            + Send
            + Sync
            + 'static,
    ) -> Result<Self, RegistrationError> {
        let operation = operation.into();
        self.validate_new(&operation, digest)?;
        if self
            .inboxes
            .values()
            .any(|existing| existing.max_sources != inbox.max_sources)
        {
            return Err(RegistrationError::Invalid(
                "inconsistent class inbox source limit",
            ));
        }
        self.inboxes.insert(operation.clone(), inbox.clone());
        let contract = OperationContract {
            operation: operation.clone(),
            kind: OperationKind::Command,
            digest,
        };
        self.commands.insert(
            operation,
            Command {
                digest,
                handler: CommandHandler::Registered(Arc::new(move |context, input| {
                    inbox.receive(context, &contract, input, &callback)
                })),
            },
        );
        self.bind_command_manifest();
        Ok(self)
    }

    pub(crate) fn validate_command_identity(
        &self,
        scope: &CellHistory,
        contract: &OperationContract,
        id: CommandId,
        durability: Durability,
        input: &[u8],
    ) -> anyhow::Result<()> {
        let ingress = id.client == super::sql_ingress_identity::client();
        if (matches!(
            self.name.as_str(),
            crate::deploy::SQL_CLASS | crate::deploy::QUEUE_CLASS
        ) && ingress)
            || (self.is_cell_rpc(&contract.operation)
                && super::native_rpc::has_ingress_nonce(input))
        {
            anyhow::ensure!(
                ingress
                    && matches!(
                        self.name.as_str(),
                        crate::deploy::SQL_CLASS | crate::deploy::QUEUE_CLASS
                    )
                    && self.is_cell_rpc(&contract.operation)
                    && super::native_rpc::has_ingress_nonce(input),
                super::CommandFailure::UnsupportedCapability
            );
            super::native_rpc::decode_request(input)?;
        }
        if let Some(inbox) = self.inboxes.get(&contract.operation) {
            anyhow::ensure!(
                durability >= inbox.durability,
                super::CommandFailure::UnsupportedCapability
            );
            let incoming = crate::raft::effects::delivery::authenticate(
                input,
                &inbox.grants,
                scope,
                self.base_schema_digest,
                contract,
                inbox.signer.verification_key(),
                inbox.durability,
            )?;
            anyhow::ensure!(
                incoming.command == id,
                super::CommandFailure::CommandConflict
            );
        } else if !self.inboxes.is_empty() {
            anyhow::ensure!(
                !crate::raft::effects::delivery::reserved_client(id.client),
                super::CommandFailure::UnsupportedCapability
            );
        }
        if self.has_system_commands() {
            anyhow::ensure!(
                id.client != crate::raft::effects::system_client(scope)?,
                super::CommandFailure::UnsupportedCapability
            );
        }
        Ok(())
    }

    pub fn query(
        mut self,
        operation: impl Into<String>,
        digest: [u8; 32],
        callback: impl for<'a> Fn(&QueryContext<'a>, &[u8]) -> anyhow::Result<Vec<u8>>
            + Send
            + Sync
            + 'static,
    ) -> Result<Self, RegistrationError> {
        let operation = operation.into();
        self.validate_new(&operation, digest)?;
        self.queries.insert(
            operation,
            Query {
                digest,
                callback: Arc::new(callback),
            },
        );
        Ok(self)
    }

    pub fn contract(&self, operation: &str) -> Option<OperationContract> {
        let (kind, digest) = if let Some(command) = self.commands.get(operation) {
            (OperationKind::Command, command.digest)
        } else {
            (OperationKind::Query, self.queries.get(operation)?.digest)
        };
        Some(OperationContract {
            operation: operation.into(),
            kind,
            digest,
        })
    }

    pub fn validate_operation(
        &self,
        contract: &OperationContract,
    ) -> Result<(), RegistrationError> {
        validate_name(&contract.operation)?;
        let registered = self
            .contract(&contract.operation)
            .ok_or_else(|| RegistrationError::UnknownOperation(contract.operation.clone()))?;
        if registered.kind != contract.kind {
            return Err(RegistrationError::KindMismatch);
        }
        if registered.digest != contract.digest {
            return Err(RegistrationError::ContractMismatch);
        }
        Ok(())
    }

    /// System operations are absent from public discovery and normal command
    /// validation. Only the node's owned business-feature adapters use them.
    pub(crate) fn system_contract(&self, operation: &str) -> Option<OperationContract> {
        if self.has_binding_maintenance() {
            if operation == crate::raft::execution::binding_seal::OPERATION {
                return Some(crate::raft::execution::binding_seal::contract());
            }
            if matches!(
                operation,
                crate::storage::raft_binding_recovery::CLAIM
                    | crate::storage::raft_binding_recovery::SETTLE
            ) {
                return Some(OperationContract {
                    operation: operation.into(),
                    kind: OperationKind::Command,
                    digest: Sha256::digest(operation.as_bytes()).into(),
                });
            }
        }
        let command = self.system_commands.get(operation)?;
        Some(OperationContract {
            operation: operation.into(),
            kind: OperationKind::Command,
            digest: command.digest,
        })
    }

    pub(crate) fn has_system_commands(&self) -> bool {
        !self.system_commands.is_empty() || self.has_binding_maintenance()
    }

    /// Bindings require the shared recovery owner even without user effects.
    /// Target retirement itself is invoked by the claiming source owner.
    pub(crate) fn has_native_callbacks(&self) -> bool {
        self.system_commands
            .contains_key(super::native_work::OPERATION)
    }

    pub(crate) fn has_scheduled_work(&self) -> bool {
        !self.system_commands.is_empty() || self.has_binding_maintenance()
    }

    pub(crate) fn has_binding_maintenance(&self) -> bool {
        self.cell_binding_classes()
            .is_some_and(|classes| !classes.is_empty())
    }

    pub(crate) fn validate_system_operation(
        &self,
        contract: &OperationContract,
    ) -> Result<(), RegistrationError> {
        if self.system_contract(&contract.operation).as_ref() != Some(contract) {
            return Err(RegistrationError::ContractMismatch);
        }
        Ok(())
    }

    pub(crate) fn register_system_command(
        &mut self,
        operation: String,
        digest: [u8; 32],
        callback: impl for<'a> Fn(&CommandContext<'a>, &[u8]) -> anyhow::Result<Vec<u8>>
            + Send
            + Sync
            + 'static,
    ) -> Result<(), RegistrationError> {
        validate_name(&operation)?;
        if !operation.starts_with("__sys_raft_")
            || digest == [0; 32]
            || self.system_commands.contains_key(&operation)
            || self.system_commands.len() >= 64
        {
            return Err(RegistrationError::Invalid("system command"));
        }
        self.system_commands.insert(
            operation,
            Command {
                digest,
                handler: CommandHandler::Registered(Arc::new(callback)),
            },
        );
        self.bind_command_manifest();
        Ok(())
    }

    pub(crate) fn invoke_command(
        &self,
        contract: &OperationContract,
        target: CommandTarget<'_>,
        command: CommandId,
        durability: Durability,
        transaction: &CommandTransaction<'_>,
        input: &[u8],
    ) -> anyhow::Result<Vec<u8>> {
        self.invoke_registered_command(
            RegisteredCommand::Application(contract),
            target,
            command,
            durability,
            transaction,
            input,
        )
    }

    pub(crate) fn invoke_system_command(
        &self,
        scope: &CellHistory,
        request: &crate::raft::execution::CommandRequest,
        transaction: &CommandTransaction<'_>,
        input: &[u8],
        limits: crate::raft::execution::ExecutionLimits,
        origin: crate::storage::raft_binding_recovery::Origin,
    ) -> anyhow::Result<Vec<u8>> {
        let contract = OperationContract {
            operation: request.operation.clone(),
            kind: OperationKind::Command,
            digest: request.contract_digest,
        };
        self.validate_system_operation(&contract)?;
        if contract.operation == crate::raft::execution::binding_seal::OPERATION {
            let _guard = CallbackGuard::enter()?;
            return crate::raft::execution::binding_seal::execute(
                self,
                request.id,
                request.durability,
                transaction,
                input,
                limits,
            );
        }
        if matches!(
            contract.operation.as_str(),
            crate::storage::raft_binding_recovery::CLAIM
                | crate::storage::raft_binding_recovery::SETTLE
        ) {
            let _guard = CallbackGuard::enter()?;
            anyhow::ensure!(
                input.len() <= crate::storage::raft_binding_recovery::MAX_METADATA,
                super::CommandFailure::Overloaded
            );
            let connection = transaction.framework()?;
            if contract.operation == crate::storage::raft_binding_recovery::CLAIM {
                let key = serde_json::from_slice(input)?;
                return Ok(serde_json::to_vec(
                    &crate::storage::raft_binding_recovery::claim(
                        &connection,
                        scope,
                        origin,
                        request.durability,
                        transaction.inputs().now_unix_millis,
                        &key,
                    )?,
                )?);
            }
            let settlement = serde_json::from_slice(input)?;
            crate::storage::raft_binding_recovery::settle(
                &connection,
                scope,
                origin,
                request.durability,
                &settlement,
            )?;
            return Ok(vec![1]);
        }
        self.invoke_registered_command(
            RegisteredCommand::System(&contract),
            CommandTarget {
                cell: scope,
                route_epoch: None,
            },
            request.id,
            request.durability,
            transaction,
            input,
        )
    }

    fn invoke_registered_command(
        &self,
        registration: RegisteredCommand<'_>,
        target: CommandTarget<'_>,
        command: CommandId,
        durability: Durability,
        transaction: &CommandTransaction<'_>,
        input: &[u8],
    ) -> anyhow::Result<Vec<u8>> {
        let (contract, commands) = match registration {
            RegisteredCommand::Application(contract) => {
                self.validate_operation(contract)?;
                (contract, &self.commands)
            }
            RegisteredCommand::System(contract) => {
                self.validate_system_operation(contract)?;
                (contract, &self.system_commands)
            }
        };
        anyhow::ensure!(
            contract.kind == OperationKind::Command,
            RegistrationError::KindMismatch
        );
        let handler = &commands
            .get(&contract.operation)
            .ok_or(RegistrationError::KindMismatch)?
            .handler;
        let CommandHandler::Registered(callback) = handler else {
            let mut activation = None;
            let result = (|| {
                let future =
                    self.start_cell_rpc(contract, target, input, transaction, &mut activation)?;
                // The low-level synchronous verification entry point cannot park
                // or retain a native instance after returning its private view.
                let _guard = CallbackGuard::enter()?;
                future
                    .now_or_never()
                    .ok_or(super::CommandFailure::UnsupportedCapability)?
            })();
            if let Some(activation) = activation {
                activation.close()?;
            }
            return result;
        };
        let _guard = CallbackGuard::enter()?;
        callback(
            &CommandContext {
                scope: target.cell,
                command,
                transaction,
                blobs: self.blobs,
                alarm_limits: self.alarm_limits,
                alarm_names: &self.alarm_names,
                durability,
                schema_digest: self.schema_digest,
                definition_digest: self.base_schema_digest,
                outbox: self.outbox.as_deref(),
            },
            input,
        )
    }

    pub(crate) fn invoke_group_query(
        &self,
        contract: &OperationContract,
        version: &CommittedVersion,
        database: ImmutableQuery,
        input: &[u8],
    ) -> anyhow::Result<Vec<u8>> {
        let identity = CellHistory::try_from(&version.scope)?;
        self.invoke_query(
            contract,
            &identity,
            QueryVersion::GroupCut(version),
            database,
            input,
        )
    }

    pub(crate) fn invoke_cell_query(
        &self,
        contract: &OperationContract,
        observation: &CellVersionObservation,
        database: ImmutableQuery,
        input: &[u8],
    ) -> anyhow::Result<Vec<u8>> {
        self.invoke_query(
            contract,
            &observation.version.history,
            QueryVersion::Cell(observation),
            database,
            input,
        )
    }

    fn invoke_query(
        &self,
        contract: &OperationContract,
        identity: &CellHistory,
        version: QueryVersion<'_>,
        database: ImmutableQuery,
        input: &[u8],
    ) -> anyhow::Result<Vec<u8>> {
        self.validate_operation(contract)?;
        anyhow::ensure!(
            contract.kind == OperationKind::Query,
            RegistrationError::KindMismatch
        );
        let callback = &self
            .queries
            .get(&contract.operation)
            .ok_or(RegistrationError::KindMismatch)?
            .callback;
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = CallbackGuard::enter()?;
            QueryFrame::run(database, |frame| {
                callback(
                    &QueryContext {
                        identity,
                        version,
                        frame,
                        blobs: self.blobs,
                        outbox: self.outbox.as_deref(),
                        schema: self.schema_digest,
                    },
                    input,
                )
            })
        }))
        .unwrap_or_else(|panic| Err(crate::native_worker::panic_error(panic)))
    }

    fn bind_command_manifest(&mut self) {
        let mut hash = Sha256::new();
        hash.update(b"cellar-cell-class-call-schedule-1");
        hash.update(self.base_schema_digest);
        if let Some(initializer) = &self.initializer {
            hash.update(b"initializer-1");
            hash.update(initializer.digest);
        }
        if let Some(limits) = self.blobs {
            hash.update(b"immutable-blobs-1");
            for size in [
                limits.max_blob_bytes,
                limits.max_objects,
                limits.max_total_bytes,
            ] {
                hash.update((size as u64).to_be_bytes());
            }
        }
        if let Some(limits) = self.alarm_limits {
            hash.update(b"alarms-1");
            hash.update((limits.max_alarms as u64).to_be_bytes());
            hash.update((limits.max_payload_bytes as u64).to_be_bytes());
        }
        if !self.inboxes.is_empty() {
            hash.update(b"inboxes-1");
            hash.update((self.inboxes.len() as u64).to_be_bytes());
            for (name, inbox) in &self.inboxes {
                hash.update((name.len() as u16).to_be_bytes());
                hash.update(name.as_bytes());
                hash.update(inbox.identity());
            }
        }
        // Outbox limits, keys and bindings are included by the mandatory system
        // command digests. Definitions cannot silently change after restart.
        hash.update((self.commands.len() as u16).to_be_bytes());
        // BTree order makes declaration order irrelevant. A voter cannot omit
        // or alter required commands while claiming the same application root.
        for (name, command) in &self.commands {
            hash.update((name.len() as u16).to_be_bytes());
            hash.update(name.as_bytes());
            hash.update(command.digest);
        }
        if self.uses_cell_factory() {
            if let Some(configuration) = self
                .cell_factory
                .as_ref()
                .and_then(|f| f.configuration_digest())
            {
                hash.update(b"cell-configuration-1");
                hash.update(configuration);
            }
            hash.update(b"json-cell-rpc-1");
            for (name, command) in &self.commands {
                if matches!(command.handler, CommandHandler::JsonCellRpc) {
                    hash.update((name.len() as u16).to_be_bytes());
                    hash.update(name.as_bytes());
                }
            }
        }
        for (name, command) in &self.commands {
            if let CommandHandler::NativeCellRpc { method, json_only } = &command.handler {
                hash.update(b"native-cell-rpc-error-replies-1");
                hash.update((name.len() as u16).to_be_bytes());
                hash.update(name.as_bytes());
                hash.update((method.len() as u16).to_be_bytes());
                hash.update(method.as_bytes());
                hash.update([u8::from(*json_only)]);
            }
        }
        if self.has_binding_maintenance() {
            hash.update(b"binding-sequence-seal-1");
            hash.update(crate::raft::execution::binding_seal::contract().digest);
            hash.update(crate::storage::raft_binding_recovery::CLAIM.as_bytes());
            hash.update(crate::storage::raft_binding_recovery::SETTLE.as_bytes());
        }
        if !self.system_commands.is_empty() {
            hash.update(b"system-commands-1");
            hash.update((self.system_commands.len() as u16).to_be_bytes());
            for (name, command) in &self.system_commands {
                hash.update((name.len() as u16).to_be_bytes());
                hash.update(name.as_bytes());
                hash.update(command.digest);
            }
        }
        self.schema_digest = hash.finalize().into();
    }

    fn validate_new(&self, operation: &str, digest: [u8; 32]) -> Result<(), RegistrationError> {
        validate_name(operation)?;
        if operation.starts_with("__sys_raft_") {
            return Err(RegistrationError::Invalid("reserved system command"));
        }
        if digest == [0; 32] {
            return Err(RegistrationError::Invalid("operation digest"));
        }
        if self.commands.contains_key(operation)
            || self.queries.contains_key(operation)
            || self.alarm_names.contains(operation)
        {
            return Err(RegistrationError::Duplicate(operation.into()));
        }
        if self.commands.len() + self.queries.len() >= 256 {
            return Err(RegistrationError::Limit);
        }
        Ok(())
    }
}

pub(super) fn validate_name(name: &str) -> Result<(), RegistrationError> {
    if !cellar_logic::cell::valid_cell_scope(name) || name.len() > 128 || name.contains(':') {
        return Err(RegistrationError::Invalid("class/operation name"));
    }
    Ok(())
}

#[cfg(test)]
#[path = "class_initializer_tests.rs"]
mod initializer_tests;
