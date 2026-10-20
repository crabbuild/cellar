//! Compiled, synchronous Raft operations with no ordinary mutable cell factory.

use super::{CommandId, CommittedVersion, GroupScope};
use crate::storage::{CommandInputs, CommandTransaction, ImmutableQuery};
use cellar_logic::NamespaceMode;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fmt, sync::Arc};

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
/// outbound network, cell-call, task-spawn, alarm or general storage capability.
/// Native code is trusted; this contract is not an operating-system sandbox.
pub struct CommandContext<'a> {
    scope: &'a GroupScope,
    command: CommandId,
    transaction: &'a CommandTransaction<'a>,
}

impl<'a> CommandContext<'a> {
    pub fn identity(&self) -> &GroupScope {
        self.scope
    }

    pub fn command(&self) -> CommandId {
        self.command
    }

    pub fn inputs(&self) -> CommandInputs {
        self.transaction.inputs()
    }

    /// SQL runs inside the framework's receipt transaction. The connection and
    /// commit/rollback operations are never exposed to the callback.
    pub fn sql(&self) -> &CommandTransaction<'a> {
        self.transaction
    }
}

/// A pinned, read-only committed image. The host establishes consistency before
/// constructing this context. Query code cannot open or migrate writable state.
pub struct QueryContext<'a> {
    version: &'a CommittedVersion,
    database: &'a ImmutableQuery,
}

impl QueryContext<'_> {
    pub fn identity(&self) -> &GroupScope {
        &self.version.scope
    }

    pub fn version(&self) -> &CommittedVersion {
        self.version
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
    schema_digest: [u8; 32],
    commands: BTreeMap<String, Command>,
    queries: BTreeMap<String, Query>,
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
        Ok(Self {
            name,
            schema_version,
            schema_digest,
            commands: BTreeMap::new(),
            queries: BTreeMap::new(),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// Namespace/application identity binding class name, explicit version and
    /// the supplied schema-definition digest. Use this value in the catalog.
    pub fn schema_digest(&self) -> [u8; 32] {
        self.schema_digest
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
        Ok(self)
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

    pub(crate) fn invoke_command(
        &self,
        contract: &OperationContract,
        scope: &GroupScope,
        command: CommandId,
        transaction: &CommandTransaction<'_>,
        input: &[u8],
    ) -> anyhow::Result<Vec<u8>> {
        self.validate_operation(contract)?;
        anyhow::ensure!(
            contract.kind == OperationKind::Command,
            RegistrationError::KindMismatch
        );
        let callback = &self
            .commands
            .get(&contract.operation)
            .ok_or(RegistrationError::KindMismatch)?
            .callback;
        callback(
            &CommandContext {
                scope,
                command,
                transaction,
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
            callback(&QueryContext { version, database }, input)
        }))
        .unwrap_or_else(|panic| Err(crate::native_worker::panic_error(panic)))
    }

    fn validate_new(&self, operation: &str, digest: [u8; 32]) -> Result<(), RegistrationError> {
        validate_name(operation)?;
        if digest == [0; 32] {
            return Err(RegistrationError::Invalid("operation digest"));
        }
        if self.commands.contains_key(operation) || self.queries.contains_key(operation) {
            return Err(RegistrationError::Duplicate(operation.into()));
        }
        if self.commands.len() + self.queries.len() >= 256 {
            return Err(RegistrationError::Limit);
        }
        Ok(())
    }
}

fn validate_name(name: &str) -> Result<(), RegistrationError> {
    if !cellar_logic::cell::valid_cell_scope(name) || name.len() > 128 || name.contains(':') {
        return Err(RegistrationError::Invalid("class/operation name"));
    }
    Ok(())
}
