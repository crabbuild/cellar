//! One private writable image and one outstanding command proposal per cell.
//! These host mechanics do not replace ingress authorization or output gating.

use super::apply::{marker, PublishedImage};
use super::cache::{DurableApply, GroupLog};
use super::completion::GroupStatus;
use super::{receipts, working::Working};
use crate::application::consensus::{
    command_request_digest, CommittedVersion, EnvelopeLimits, OperationContract, OperationKind,
    ProtocolError, RaftClass, RegistrationError,
};
use crate::storage::{CommandInputs, CommandTransaction};
use cellar_logic::consensus::Durability;
use cellar_logic::consensus::{
    ClientId, CommandFailure, CommandId, LogPosition, ProposalId, ReplicaIdentity, Term,
};
use cellar_ltx::generation::{GenerationPin, GenerationStore, ImageId, ReadServices};
use cellar_ltx::{HostFileLock, LtxHost};
use std::{fmt, path::PathBuf};

#[derive(Clone, Copy, Debug)]
pub struct ExecutionLimits {
    pub envelope: EnvelopeLimits,
    pub max_image_bytes: u64,
    pub max_result_bytes: usize,
    pub max_client_identities: usize,
    pub max_retained_receipts: usize,
    pub max_retry_window_ms: u64,
}

impl Default for ExecutionLimits {
    fn default() -> Self {
        Self {
            envelope: EnvelopeLimits::default(),
            max_image_bytes: 64 * 1024 * 1024,
            max_result_bytes: 64 * 1024,
            max_client_identities: 1024,
            max_retained_receipts: 4096,
            max_retry_window_ms: 600_000,
        }
    }
}

impl ExecutionLimits {
    fn validate(&self) -> Result<(), ExecutionError> {
        self.envelope.validate().map_err(ExecutionError::Protocol)?;
        if !(4096..=64 * 1024 * 1024).contains(&self.max_image_bytes)
            || !(1..=1024 * 1024).contains(&self.max_result_bytes)
            || !(1..=65_536).contains(&self.max_client_identities)
            || !(1..=65_536).contains(&self.max_retained_receipts)
            || !(1..=90 * 24 * 60 * 60 * 1000).contains(&self.max_retry_window_ms)
        {
            return Err(ExecutionError::Invalid("execution limits"));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum ExecutionError {
    Invalid(&'static str),
    Refused(CommandFailure),
    Busy,
    Uncertain,
    RebuildRequired,
    RetentionNotExpired,
    Storage(anyhow::Error),
    Protocol(ProtocolError),
    Registration(RegistrationError),
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Raft private execution: {self:?}")
    }
}

impl std::error::Error for ExecutionError {}

/// A local committed-image read, with no linearizability or freshness claim.
/// The node's query controller establishes the requested consistency before
/// selecting the image. Never relabel this result with a newer sampled head.
pub struct RegisteredQueryOutput {
    pub result: Vec<u8>,
    pub version: CommittedVersion,
}

pub fn query_registered(
    published: &PublishedImage,
    class: &RaftClass,
    contract: &OperationContract,
    input: &[u8],
    services: ReadServices,
    limits: ExecutionLimits,
) -> Result<RegisteredQueryOutput, ExecutionError> {
    limits.validate()?;
    class
        .validate_operation(contract)
        .map_err(ExecutionError::Registration)?;
    if contract.kind != OperationKind::Query {
        return Err(ExecutionError::Registration(
            RegistrationError::KindMismatch,
        ));
    }
    if class.schema_digest() != published.application_digest() {
        return Err(ExecutionError::Registration(
            RegistrationError::ContractMismatch,
        ));
    }
    if input.len() > limits.envelope.max_payload_bytes {
        return Err(ExecutionError::Refused(CommandFailure::Overloaded));
    }
    let database = crate::storage::open_immutable_generation(
        published.pin(),
        crate::storage::ImmutableQueryOptions {
            services,
            sqlite_vec: false,
        },
    )
    .map_err(storage)?;
    let version = CommittedVersion {
        scope: published.scope().clone(),
        position: published.position(),
    };
    let result = class
        .invoke_query(contract, &version, &database, input)
        .map_err(storage)?;
    if result.len() > limits.max_result_bytes {
        return Err(ExecutionError::Refused(CommandFailure::Overloaded));
    }
    Ok(RegisteredQueryOutput { result, version })
}

/// A checked observation of a caught-up leader. The host must recheck runtime
/// authority before proposal submission; a synchronous callback cannot hold an
/// election open. Private effects remain invisible if that later check fails.
#[derive(Clone)]
pub struct CommandAuthority {
    group: GroupLog,
    replica: ReplicaIdentity,
    term: Term,
    applied: DurableApply,
}

impl CommandAuthority {
    pub fn from_status(group: GroupLog, status: &GroupStatus) -> Result<Self, ExecutionError> {
        if group.identity() != &status.replica
            || !status.is_leader
            || !status.protocol_ready
            || status.awaiting_apply
            || status.awaiting_ready
            || status.persisted != status.committed
            || status.committed != status.applied.index
            || status.applied.term != status.term
        {
            return Err(ExecutionError::Refused(CommandFailure::Unavailable));
        }
        let applied = group
            .applied()
            .map_err(|e| ExecutionError::Storage(e.into()))?;
        if applied.position != status.applied {
            return Err(ExecutionError::Refused(CommandFailure::Unavailable));
        }
        let value = Self {
            group,
            replica: status.replica.clone(),
            term: status.term,
            applied,
        };
        value.check()?;
        Ok(value)
    }

    fn check(&self) -> Result<(), ExecutionError> {
        let cache = self
            .group
            .read()
            .map_err(|e| ExecutionError::Storage(e.into()))?;
        if cache.applied != self.applied
            || cache.hard_state.term != self.term.get()
            || cache.hard_state.commit != self.applied.position.index.get()
            || cache.last_index() != self.applied.position.index.get()
        {
            return Err(ExecutionError::Refused(CommandFailure::Unavailable));
        }
        Ok(())
    }

    fn check_base(
        &self,
        base: Option<&PublishedImage>,
        application_digest: [u8; 32],
    ) -> Result<(), ExecutionError> {
        self.check()?;
        if base.is_some_and(|b| {
            b.scope() != &self.replica.scope
                || b.application_digest() != application_digest
                || b.position().index > self.applied.position.index
        }) || base.map(|b| marker(&b.pin())) != self.applied.image
        {
            return Err(ExecutionError::Invalid("authoritative command base"));
        }
        Ok(())
    }
}

pub struct PrivateOptions {
    pub directory: PathBuf,
    pub host: LtxHost,
    /// Must select the same SQLite storage surface as the supplied host. None
    /// selects the native SQLite VFS, as in the ordinary native storage opener.
    pub sqlite_vfs: Option<String>,
    pub images: GenerationStore,
    pub query_services: ReadServices,
    pub application_digest: [u8; 32],
    pub limits: ExecutionLimits,
}

#[derive(Clone, Debug)]
pub struct CommandRequest {
    pub id: CommandId,
    pub operation: String,
    pub contract_digest: [u8; 32],
    pub request_digest: [u8; 32],
    pub durability: Durability,
    pub retry_until_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredReceipt {
    pub(super) id: CommandId,
    pub(super) operation: String,
    pub(super) contract_digest: [u8; 32],
    pub(super) request_digest: [u8; 32],
    pub(super) result: Vec<u8>,
    pub(super) minimum_durability: Durability,
    pub(super) retry_until_ms: u64,
}

impl StoredReceipt {
    pub fn command(&self) -> CommandId {
        self.id
    }

    pub fn result(&self) -> &[u8] {
        &self.result
    }

    pub fn minimum_durability(&self) -> Durability {
        self.minimum_durability
    }

    pub fn retry_until_ms(&self) -> u64 {
        self.retry_until_ms
    }
}

/// Covers a durable image/receipt, not an archive or SDK output-gate proof.
#[derive(Clone, Debug)]
pub struct CommittedReceipt {
    receipt: StoredReceipt,
    position: LogPosition,
}

impl CommittedReceipt {
    pub fn receipt(&self) -> &StoredReceipt {
        &self.receipt
    }

    pub fn position(&self) -> LogPosition {
        self.position
    }
}

pub struct CommandAttempt {
    pub request: CommandRequest,
    pub proposal: ProposalId,
    pub preview: ImageId,
    pub inputs: CommandInputs,
}

#[derive(Clone)]
pub struct PreparedCommand {
    bytes: Vec<u8>,
    proposal: ProposalId,
    command: Option<CommandId>,
}

impl PreparedCommand {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn proposal(&self) -> ProposalId {
        self.proposal
    }

    pub fn command(&self) -> Option<CommandId> {
        self.command
    }

    pub fn context(&self) -> Vec<u8> {
        self.proposal.bytes().to_vec()
    }
}

pub enum StageOutcome {
    Prepared(PreparedCommand),
    Duplicate(CommittedReceipt),
}

pub enum AppliedExecution {
    Command(CommittedReceipt),
    Pruned {
        client: ClientId,
        through: u64,
        position: LogPosition,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionStatus {
    Ready,
    Prepared,
    Submitted,
    RebuildRequired,
}

enum Kind {
    Command(CommandRequest),
    Prune { client: ClientId, through: u64 },
}

struct Pending {
    proposal: ProposalId,
    preview: GenerationPin,
    kind: Kind,
    submitted: bool,
}

pub struct PrivateExecutor {
    options: PrivateOptions,
    authority: CommandAuthority,
    base: Option<PublishedImage>,
    working: Option<Working>,
    pending: Option<Pending>,
    rebuild_required: bool,
    retired_preview: Option<GenerationPin>,
    _lock: HostFileLock,
}

impl PrivateExecutor {
    /// Run a compiled command after validating registration and the input's
    /// normalized-byte digest, including for a duplicate receipt. The normal
    /// host selects `class` from its authorized namespace binding, not from an
    /// unchecked client-supplied class name.
    pub fn stage_registered(
        &mut self,
        attempt: CommandAttempt,
        class: &RaftClass,
        input: &[u8],
    ) -> Result<StageOutcome, ExecutionError> {
        let contract = OperationContract {
            operation: attempt.request.operation.clone(),
            kind: OperationKind::Command,
            digest: attempt.request.contract_digest,
        };
        class
            .validate_operation(&contract)
            .map_err(ExecutionError::Registration)?;
        if class.schema_digest() != self.options.application_digest {
            return Err(ExecutionError::Registration(
                RegistrationError::ContractMismatch,
            ));
        }
        if input.len() > self.options.limits.envelope.max_payload_bytes {
            return Err(ExecutionError::Refused(CommandFailure::Overloaded));
        }
        if command_request_digest(input) != attempt.request.request_digest {
            return Err(ExecutionError::Refused(CommandFailure::CommandConflict));
        }
        let scope = self.authority.replica.scope.clone();
        let command = attempt.request.id;
        self.stage(attempt, |transaction| {
            class.invoke_command(&contract, &scope, command, transaction, input)
        })
    }

    pub fn open(
        options: PrivateOptions,
        authority: CommandAuthority,
        base: Option<PublishedImage>,
    ) -> Result<Self, ExecutionError> {
        options.limits.validate()?;
        authority.check_base(base.as_ref(), options.application_digest)?;
        let filesystem = options.host.filesystem();
        cellar_ltx::host::create_dir_all_durable(filesystem.as_ref(), &options.directory)
            .map_err(storage)?;
        let lock = filesystem
            .lock_file(&options.directory.join("working.lock"))
            .map_err(storage)?;
        let working = Working::open(&options, base.as_ref()).map_err(ExecutionError::Storage)?;
        Ok(Self {
            options,
            authority,
            base,
            working: Some(working),
            pending: None,
            rebuild_required: false,
            retired_preview: None,
            _lock: lock,
        })
    }

    pub fn status(&self) -> ExecutionStatus {
        if self.rebuild_required {
            ExecutionStatus::RebuildRequired
        } else if let Some(pending) = &self.pending {
            if pending.submitted {
                ExecutionStatus::Submitted
            } else {
                ExecutionStatus::Prepared
            }
        } else {
            ExecutionStatus::Ready
        }
    }

    fn ready(&mut self) -> Result<(), ExecutionError> {
        if self.pending.is_some() {
            return Err(ExecutionError::Busy);
        }
        if self.rebuild_required {
            return Err(ExecutionError::RebuildRequired);
        }
        self.authority
            .check_base(self.base.as_ref(), self.options.application_digest)?;
        if let Some(pin) = self.retired_preview.take() {
            let id = pin.id();
            if let Err(error) = self.options.images.retire(id) {
                self.retired_preview = Some(pin);
                self.rebuild_required = true;
                return Err(storage(error));
            }
            drop(pin);
            if let Err(error) = self.options.images.reclaim(1) {
                self.rebuild_required = true;
                return Err(storage(error));
            }
        }
        if let Err(error) = self
            .working
            .as_ref()
            .ok_or(ExecutionError::RebuildRequired)?
            .trim(&self.options, self.base.as_ref())
        {
            self.rebuild_required = true;
            return Err(ExecutionError::Storage(error));
        }
        Ok(())
    }

    pub fn refresh_authority(&mut self, authority: CommandAuthority) -> Result<(), ExecutionError> {
        if self.pending.is_some() {
            return Err(ExecutionError::Busy);
        }
        authority.check_base(self.base.as_ref(), self.options.application_digest)?;
        if authority.replica.scope != self.authority.replica.scope
            || self.base.as_ref().is_none_or(|base| {
                self.working
                    .as_ref()
                    .is_none_or(|working| !working.matches(base))
            })
        {
            return Err(ExecutionError::RebuildRequired);
        }
        self.authority = authority;
        self.rebuild_required = false;
        Ok(())
    }

    pub fn stage(
        &mut self,
        attempt: CommandAttempt,
        command: impl FnOnce(&CommandTransaction<'_>) -> anyhow::Result<Vec<u8>>,
    ) -> Result<StageOutcome, ExecutionError> {
        self.ready()?;
        if attempt.request.operation.is_empty() || attempt.request.operation.len() > 128 {
            return Err(ExecutionError::Invalid("command operation"));
        }
        let working = self
            .working
            .as_ref()
            .ok_or(ExecutionError::RebuildRequired)?;
        if receipts::preflight(
            working.database.framework(),
            &attempt.request,
            &self.options.limits,
        )?
        .is_some()
        {
            let receipt = self.read_committed(
                &attempt.request,
                self.base
                    .as_ref()
                    .ok_or(ExecutionError::Invalid("receipt without base"))?,
            )?;
            return Ok(StageOutcome::Duplicate(receipt));
        }
        if attempt.request.retry_until_ms < attempt.inputs.now_unix_millis
            || attempt
                .request
                .retry_until_ms
                .checked_sub(attempt.inputs.now_unix_millis)
                .is_none_or(|window| window > self.options.limits.max_retry_window_ms)
        {
            return Err(ExecutionError::Invalid("command retry window"));
        }
        self.rebuild_required = true;
        self.working
            .as_mut()
            .ok_or(ExecutionError::RebuildRequired)?
            .begin_mutation();
        self.working
            .as_ref()
            .ok_or(ExecutionError::RebuildRequired)?
            .database
            .transaction(attempt.inputs, |connection, transaction| {
                let result = command(transaction)?;
                if result.len() > self.options.limits.max_result_bytes {
                    return Err(anyhow::Error::new(CommandFailure::Overloaded));
                }
                receipts::record(connection, &attempt.request, result)?;
                Ok(())
            })
            .map_err(transaction_failure)?;
        let prepared = self.freeze(
            attempt.proposal,
            attempt.preview,
            Kind::Command(attempt.request),
        )?;
        Ok(StageOutcome::Prepared(prepared))
    }

    pub fn stage_prune(
        &mut self,
        client: ClientId,
        through: u64,
        proposal: ProposalId,
        preview: ImageId,
        inputs: CommandInputs,
    ) -> Result<PreparedCommand, ExecutionError> {
        self.ready()?;
        self.rebuild_required = true;
        self.working
            .as_mut()
            .ok_or(ExecutionError::RebuildRequired)?
            .begin_mutation();
        self.working
            .as_ref()
            .ok_or(ExecutionError::RebuildRequired)?
            .database
            .transaction(inputs, |connection, _| {
                receipts::prune(connection, client, through, inputs.now_unix_millis)
                    .map_err(anyhow::Error::new)
            })
            .map_err(transaction_failure)?;
        self.freeze(proposal, preview, Kind::Prune { client, through })
    }

    fn freeze(
        &mut self,
        proposal: ProposalId,
        preview: ImageId,
        kind: Kind,
    ) -> Result<PreparedCommand, ExecutionError> {
        let update = self
            .working
            .as_mut()
            .ok_or(ExecutionError::RebuildRequired)?
            .capture(
                &self.options,
                &self.authority.replica.scope,
                proposal,
                self.base.as_ref(),
            )
            .map_err(ExecutionError::Storage)?;
        let bytes = update
            .encode(self.options.limits.envelope)
            .map_err(ExecutionError::Protocol)?;
        let pin =
            super::working::verify_preview(&self.options, self.base.as_ref(), preview, &update)
                .map_err(ExecutionError::Storage)?;
        self.retired_preview = Some(pin);
        self.authority.check()?;
        let command = match &kind {
            Kind::Command(request) => Some(request.id),
            Kind::Prune { .. } => None,
        };
        let preview = self
            .retired_preview
            .take()
            .ok_or(ExecutionError::RebuildRequired)?;
        self.pending = Some(Pending {
            proposal,
            preview,
            kind,
            submitted: false,
        });
        self.rebuild_required = false;
        Ok(PreparedCommand {
            bytes,
            proposal,
            command,
        })
    }

    /// Call before awaiting proposal submission. Cancellation or a lost response
    /// then retains the uncertainty fence rather than freeing the command slot.
    pub fn mark_submitted(&mut self, proposal: ProposalId) -> Result<(), ExecutionError> {
        let pending = self
            .pending
            .as_mut()
            .ok_or(ExecutionError::Invalid("no staged proposal"))?;
        if pending.proposal != proposal || pending.submitted {
            return Err(ExecutionError::Invalid("proposal submission"));
        }
        pending.submitted = true;
        Ok(())
    }

    pub fn finish(
        &mut self,
        published: PublishedImage,
    ) -> Result<AppliedExecution, ExecutionError> {
        let pending = self
            .pending
            .as_ref()
            .ok_or(ExecutionError::Invalid("no pending proposal"))?;
        if published.scope() != &self.authority.replica.scope
            || published.application_digest() != self.options.application_digest
            || published.proposal() != Some(pending.proposal)
            || published.pin().state() != pending.preview.state()
            || published.position().index <= self.authority.applied.position.index
            || published.position().term < self.authority.term
        {
            return Err(ExecutionError::Invalid("committed proposal/image"));
        }
        let result = match &pending.kind {
            Kind::Command(request) => {
                AppliedExecution::Command(self.read_committed(request, &published)?)
            }
            Kind::Prune { client, through } => AppliedExecution::Pruned {
                client: *client,
                through: *through,
                position: published.position(),
            },
        };
        let pending = self
            .pending
            .take()
            .ok_or(ExecutionError::Invalid("pending proposal changed"))?;
        self.retired_preview = Some(pending.preview);
        self.base = Some(published);
        // A new caught-up authority observation is required before another
        // callback; finish cannot turn an old pre-proposal observation current.
        self.rebuild_required = true;
        Ok(result)
    }

    fn read_committed(
        &self,
        request: &CommandRequest,
        published: &PublishedImage,
    ) -> Result<CommittedReceipt, ExecutionError> {
        let database = cellar_ltx::generation::open_immutable(
            published.pin(),
            self.options.query_services.clone(),
        )
        .map_err(storage)?;
        let receipt = receipts::preflight(database.connection(), request, &self.options.limits)?
            .ok_or(ExecutionError::Invalid("committed receipt missing"))?;
        Ok(CommittedReceipt {
            receipt,
            position: published.position(),
        })
    }

    /// Rebuild from the authoritative prefix, never the surviving writable file.
    /// A submitted proposal requires a new leader term/process before it can be
    /// abandoned. That authority has already applied its current-term barrier.
    pub fn rebuild(
        &mut self,
        base: Option<PublishedImage>,
        authority: CommandAuthority,
    ) -> Result<(), ExecutionError> {
        if authority.replica.scope != self.authority.replica.scope {
            return Err(ExecutionError::Refused(CommandFailure::WrongIncarnation));
        }
        authority.check_base(base.as_ref(), self.options.application_digest)?;
        if self.pending.as_ref().is_some_and(|p| p.submitted)
            && authority.replica == self.authority.replica
            && authority.term <= self.authority.term
        {
            return Err(ExecutionError::Uncertain);
        }
        self.rebuild_required = true;
        if let Some(pending) = self.pending.take() {
            self.retired_preview = Some(pending.preview);
        }
        if let Some(working) = self.working.take() {
            working.close().map_err(ExecutionError::Storage)?;
        }
        self.base = base;
        self.authority = authority;
        self.working = Some(
            Working::open(&self.options, self.base.as_ref()).map_err(ExecutionError::Storage)?,
        );
        self.rebuild_required = false;
        Ok(())
    }
}

pub(super) fn storage(error: impl Into<anyhow::Error>) -> ExecutionError {
    ExecutionError::Storage(error.into())
}

fn transaction_failure(error: anyhow::Error) -> ExecutionError {
    match error.downcast::<ExecutionError>() {
        Ok(error) => error,
        Err(error) => match error.downcast::<CommandFailure>() {
            Ok(error) => ExecutionError::Refused(error),
            Err(error) => ExecutionError::Storage(error),
        },
    }
}
