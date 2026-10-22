//! Compiled, synchronous Raft operations with no ordinary mutable cell factory.

use super::{CommandId, CommittedVersion, Durability, GroupScope};
use crate::storage::{CommandInputs, CommandTransaction, ImmutableQuery};
use cellar_logic::NamespaceMode;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

thread_local! { static CALLBACK_ACTIVE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

pub(super) fn callback_active() -> bool {
    CALLBACK_ACTIVE.with(std::cell::Cell::get)
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
/// outbound network, cell-call, task-spawn or general storage capability.
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
    scope: &'a GroupScope,
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
    pub fn identity(&self) -> &GroupScope {
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

    /// Retain actual immutable bytes in this command's transaction. Returned
    /// references supply no durability proof before the command commits.
    pub fn put_blob(&self, bytes: &[u8]) -> anyhow::Result<super::RaftBlobReference> {
        crate::raft::effects::put_blob(
            self.transaction.framework(),
            self.blobs
                .ok_or(super::CommandFailure::UnsupportedCapability)?,
            bytes,
        )
    }

    pub fn blob(&self, reference: super::RaftBlobReference) -> anyhow::Result<Vec<u8>> {
        crate::raft::effects::get_blob(
            self.transaction.framework(),
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
        crate::raft::effects::alarm::cancel(self.transaction.framework(), name)
    }
}

/// A pinned, read-only committed image. The host establishes consistency before
/// constructing this context. Query code cannot open or migrate writable state.
pub struct QueryContext<'a> {
    version: &'a CommittedVersion,
    database: &'a ImmutableQuery,
    blobs: Option<super::RaftBlobLimits>,
    outbox: Option<&'a super::RaftOutbox>,
}

impl QueryContext<'_> {
    pub fn outbox_progress(&self, lane: &str) -> anyhow::Result<super::RaftOutboxProgress> {
        crate::raft::effects::outbox::progress(
            self.database.connection(),
            self.outbox
                .ok_or(super::CommandFailure::UnsupportedCapability)?,
            lane,
        )
    }

    pub fn identity(&self) -> &GroupScope {
        &self.version.scope
    }

    pub fn version(&self) -> &CommittedVersion {
        self.version
    }

    /// Read only bytes present in this query's pinned committed generation.
    pub fn blob(&self, reference: super::RaftBlobReference) -> anyhow::Result<Vec<u8>> {
        crate::raft::effects::get_blob(
            self.database.connection(),
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
        self.database.query_row(sql, params, read)
    }
}

type CommandCallback =
    dyn for<'a> Fn(&CommandContext<'a>, &[u8]) -> anyhow::Result<Vec<u8>> + Send + Sync;
type QueryCallback =
    dyn for<'a> Fn(&QueryContext<'a>, &[u8]) -> anyhow::Result<Vec<u8>> + Send + Sync;

struct Command {
    digest: [u8; 32],
    callback: Arc<CommandCallback>,
}

enum RegisteredCommand<'a> {
    Application(&'a OperationContract),
    System(&'a OperationContract),
}

struct Query {
    digest: [u8; 32],
    callback: Arc<QueryCallback>,
}

/// A compiled definition, separate from `Cell` and its mutable factory. Register
/// with `Registry::raft_class`; the node binds the class to a Raft namespace.
/// Schema versions are explicit and do not authorize an automatic migration.
pub struct RaftClass {
    name: String,
    schema_version: u32,
    base_schema_digest: [u8; 32],
    schema_digest: [u8; 32],
    commands: BTreeMap<String, Command>,
    system_commands: BTreeMap<String, Command>,
    queries: BTreeMap<String, Query>,
    blobs: Option<super::RaftBlobLimits>,
    alarm_limits: Option<super::RaftAlarmLimits>,
    alarm_names: BTreeSet<String>,
    inboxes: BTreeMap<String, super::RaftInbox>,
    outbox: Option<Arc<super::RaftOutbox>>,
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
            system_commands: BTreeMap::new(),
            queries: BTreeMap::new(),
            blobs: None,
            alarm_limits: None,
            alarm_names: BTreeSet::new(),
            inboxes: BTreeMap::new(),
            outbox: None,
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
    /// compatibility. Read this value after registering commands for the catalog.
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

    pub(crate) fn blob_limits(&self) -> Option<super::RaftBlobLimits> {
        self.blobs
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

    pub(crate) fn validate_host(
        &self,
        host: &crate::raft::host::RaftHostOptions,
    ) -> anyhow::Result<()> {
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
                callback: Arc::new(callback),
            },
        );
        self.bind_command_manifest();
        Ok(self)
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
                callback: Arc::new(move |context, input| {
                    inbox.receive(context, &contract, input, &callback)
                }),
            },
        );
        self.bind_command_manifest();
        Ok(self)
    }

    pub(crate) fn validate_command_identity(
        &self,
        scope: &GroupScope,
        contract: &OperationContract,
        id: CommandId,
        durability: Durability,
        input: &[u8],
    ) -> anyhow::Result<()> {
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
        let command = self.system_commands.get(operation)?;
        Some(OperationContract {
            operation: operation.into(),
            kind: OperationKind::Command,
            digest: command.digest,
        })
    }

    pub(crate) fn has_system_commands(&self) -> bool {
        !self.system_commands.is_empty()
    }

    pub(crate) fn has_queries(&self) -> bool {
        !self.queries.is_empty()
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
                callback: Arc::new(callback),
            },
        );
        self.bind_command_manifest();
        Ok(())
    }

    pub(crate) fn invoke_command(
        &self,
        contract: &OperationContract,
        scope: &GroupScope,
        command: CommandId,
        durability: Durability,
        transaction: &CommandTransaction<'_>,
        input: &[u8],
    ) -> anyhow::Result<Vec<u8>> {
        self.invoke_registered_command(
            RegisteredCommand::Application(contract),
            scope,
            command,
            durability,
            transaction,
            input,
        )
    }

    pub(crate) fn invoke_system_command(
        &self,
        contract: &OperationContract,
        scope: &GroupScope,
        command: CommandId,
        durability: Durability,
        transaction: &CommandTransaction<'_>,
        input: &[u8],
    ) -> anyhow::Result<Vec<u8>> {
        self.invoke_registered_command(
            RegisteredCommand::System(contract),
            scope,
            command,
            durability,
            transaction,
            input,
        )
    }

    fn invoke_registered_command(
        &self,
        registration: RegisteredCommand<'_>,
        scope: &GroupScope,
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
        let callback = &commands
            .get(&contract.operation)
            .ok_or(RegistrationError::KindMismatch)?
            .callback;
        let _guard = CallbackGuard::enter()?;
        callback(
            &CommandContext {
                scope,
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

    pub(crate) fn invoke_query(
        &self,
        contract: &OperationContract,
        version: &CommittedVersion,
        database: &ImmutableQuery,
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
            callback(
                &QueryContext {
                    version,
                    database,
                    blobs: self.blobs,
                    outbox: self.outbox.as_deref(),
                },
                input,
            )
        }))
        .unwrap_or_else(|panic| Err(crate::native_worker::panic_error(panic)))
    }

    fn bind_command_manifest(&mut self) {
        let mut hash = Sha256::new();
        hash.update(b"cellar-raft-class-1");
        hash.update(self.base_schema_digest);
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
