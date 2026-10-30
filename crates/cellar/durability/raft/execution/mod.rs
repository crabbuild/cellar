// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One private writable image and one outstanding command proposal per cell.
//! These host mechanics do not replace ingress authorization or output gating.

use super::apply::{marker, PublishedImage};
use super::cache::{DurableApply, GroupLog};
use super::completion::GroupStatus;
use super::receipts;
mod cell;
mod submission;
pub(crate) use cell::authority_failure;
pub use submission::{CellPreparedCommand, CellProposalRejection, CellSubmissionOutcome};
mod command;
mod enrollment;
mod registered;
pub use enrollment::{CellEnrollmentAttempt, CellEnrollmentResolution, CellPrivateEnrollment};
pub(crate) use registered::validate as validate_registered_command;
mod images;
pub use cell::{CellCommittedReceipt, CellPrivateExecutor, CellReconciliation, CellStageOutcome};
mod rooted_working;
use crate::application::consensus::{
    command_request_digest, CommittedVersion, EnvelopeLimits, OperationContract, OperationKind,
    ProtocolError, RaftClass, RegistrationError,
};
use crate::storage::{CommandInputs, CommandTransaction};
use cellar_logic::consensus::Durability;
use cellar_logic::consensus::{
    ClientId, CommandFailure, CommandId, LogIndex, LogPosition, ProposalId, ReplicaIdentity, Term,
};
use cellar_ltx::apply::ImageState;
use cellar_ltx::generation::{GenerationPin, GenerationStore, ImageId, ReadServices};
use cellar_ltx::{HostFileLock, LtxHost};
pub(crate) use images::Published as PublishedExecutionImage;
pub use images::RootPrivateOptions;
use images::{Images, Options, Preview, Published, State, WorkingImage};
use sha2::{Digest, Sha256};
use std::{fmt, path::PathBuf};

#[cfg(test)]
#[path = "../execution_preview_tests.rs"]
mod preview_tests;

#[derive(Clone, Copy, Debug)]
pub struct ExecutionLimits {
    pub envelope: EnvelopeLimits,
    pub max_image_bytes: u64,
    pub max_result_bytes: usize,
    pub max_client_identities: usize,
    pub max_retained_receipts: usize,
    pub max_retry_window_ms: u64,
    /// Virtual space within max_image_bytes reserved for durable receipts,
    /// client sequence floors and LTX control tables. Zero disables this policy.
    /// Application Blob/outbox data still uses the application allowance.
    pub receipt_reserve_bytes: u64,
    /// Bounded maintenance in the same replicated transaction as a new command.
    /// Latest receipts and all client sequence floors remain protected.
    pub receipt_maintenance: Option<super::receipt_maintenance::ReceiptMaintenanceLimits>,
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
            receipt_reserve_bytes: 0,
            receipt_maintenance: Some(
                super::receipt_maintenance::ReceiptMaintenanceLimits::default(),
            ),
        }
    }
}

impl ExecutionLimits {
    pub(super) fn check_capacity(
        &self,
        application: u64,
        receipts: u64,
        structural: u64,
    ) -> Result<(), CommandFailure> {
        if self.receipt_reserve_bytes == 0 {
            return Ok(());
        }
        if receipts > self.receipt_reserve_bytes {
            return Err(CommandFailure::Overloaded);
        }
        let available = self
            .max_image_bytes
            .checked_sub(self.receipt_reserve_bytes)
            .and_then(|n| n.checked_sub(structural))
            .ok_or(CommandFailure::StorageFull)?;
        if application > available {
            return Err(CommandFailure::StorageFull);
        }
        Ok(())
    }

    pub(crate) fn validate(&self) -> Result<(), ExecutionError> {
        self.envelope.validate().map_err(ExecutionError::Protocol)?;
        if let Some(maintenance) = self.receipt_maintenance {
            maintenance.validate()?;
        }
        if !(4096..=cellar_ltx::stream_snapshot::MAX_IMAGE_BYTES).contains(&self.max_image_bytes)
            || !(1..=1024 * 1024).contains(&self.max_result_bytes)
            || !(1..=65_536).contains(&self.max_client_identities)
            || !(1..=65_536).contains(&self.max_retained_receipts)
            || !(1..=90 * 24 * 60 * 60 * 1000).contains(&self.max_retry_window_ms)
            || self.receipt_reserve_bytes > self.max_image_bytes / 4
            || !self.receipt_reserve_bytes.is_multiple_of(512)
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
    query_registered_controlled(published, class, contract, input, services, limits, None)
}

pub(crate) fn query_registered_controlled(
    published: &PublishedImage,
    class: &RaftClass,
    contract: &OperationContract,
    input: &[u8],
    services: ReadServices,
    limits: ExecutionLimits,
    cancel: Option<std::sync::Arc<crate::application::consensus::client::AttemptControl>>,
) -> Result<RegisteredQueryOutput, ExecutionError> {
    validate_registered_query(
        class,
        contract,
        input,
        published.application_digest(),
        limits,
    )?;
    let database = crate::storage::open_immutable_generation(
        published.pin(),
        crate::storage::ImmutableQueryOptions {
            services,
            sqlite_vec: false,
        },
    )
    .map_err(storage)?;
    if let Some(cancel) = cancel {
        database
            .connection()
            .progress_handler(1000, Some(cancel.interrupt_check()))
            .map_err(storage)?;
    }
    let version = CommittedVersion {
        scope: published.scope().clone(),
        position: published.position(),
    };
    let result = class
        .invoke_group_query(contract, &version, &database, input)
        .map_err(storage)?;
    if result.len() > limits.max_result_bytes {
        return Err(ExecutionError::Refused(CommandFailure::Overloaded));
    }
    Ok(RegisteredQueryOutput { result, version })
}

pub(super) fn validate_registered_query(
    class: &RaftClass,
    contract: &OperationContract,
    input: &[u8],
    application_digest: [u8; 32],
    limits: ExecutionLimits,
) -> Result<(), ExecutionError> {
    limits.validate()?;
    class
        .validate_operation(contract)
        .map_err(ExecutionError::Registration)?;
    if contract.kind != OperationKind::Query {
        return Err(ExecutionError::Registration(
            RegistrationError::KindMismatch,
        ));
    }
    if class.schema_digest() != application_digest {
        return Err(ExecutionError::Registration(
            RegistrationError::ContractMismatch,
        ));
    }
    if input.len() > limits.envelope.max_payload_bytes {
        return Err(ExecutionError::Refused(CommandFailure::Overloaded));
    }
    Ok(())
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
        base: Option<&Published>,
        application_digest: [u8; 32],
    ) -> Result<(), ExecutionError> {
        self.check()?;
        if base.is_some_and(|b| {
            b.scope() != &self.replica.scope
                || b.application_digest() != application_digest
                || b.position().index > self.applied.position.index
        }) || base.map(Published::marker) != self.applied.image
        {
            return Err(ExecutionError::Invalid("authoritative command base"));
        }
        Ok(())
    }
}

#[derive(Clone)]
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
    pub(crate) ltx_bytes: u64,
    proposal: ProposalId,
    command: Option<CommandId>,
    archive_bytes: u64,
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

    /// Retention bound after the physical preview has verified the image size.
    /// Includes the encoded log entry and room for its protobuf/context framing.
    pub(crate) fn archive_bytes(&self) -> u64 {
        self.archive_bytes
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
    // Taking the pin transfers its retention responsibility to ImageApplier.
    // Private cleanup must never retire a promoted committed generation.
    preview: Option<Preview>,
    base: State,
    result: State,
    payload_digest: [u8; 32],
    kind: Kind,
    submitted: bool,
}

impl Pending {
    fn take_preview(
        &mut self,
        images: &GenerationStore,
        bytes: &[u8],
        base: ImageState,
        result: ImageState,
    ) -> Option<GenerationPin> {
        let Preview::Materialized(pin) = self.preview.as_ref()? else {
            return None;
        };
        if !self.submitted
            || self.base != State::Materialized(base)
            || self.result != State::Materialized(result)
            || !images.owns(pin)
            || self.payload_digest != <[u8; 32]>::from(Sha256::digest(bytes))
        {
            return None;
        }
        match self.preview.take()? {
            Preview::Materialized(pin) => Some(pin),
            Preview::Rooted(_) => None,
        }
    }
}

pub struct PrivateExecutor {
    options: Options,
    authority: CommandAuthority,
    base: Option<Published>,
    working: Option<WorkingImage>,
    pending: Option<Pending>,
    rebuild_required: bool,
    retired_preview: Option<Preview>,
    receipt_cursor: Option<ClientId>,
    _lock: PrivateLock,
}

// Fields drop in declaration order: close the lock before releasing the final
// directory pin, including implicit drop and partially failed opens.
struct PrivateLock {
    _lock: HostFileLock,
    _workspace: Option<cellar_ltx::internal::NodeWorkspace>,
}

impl PrivateExecutor {
    /// Dispose unsubmitted private changes after their synchronous callback has
    /// joined. An uncertain submission cannot use cancellation as rejection.
    pub fn discard_unsubmitted(&mut self) -> Result<(), ExecutionError> {
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.submitted)
        {
            return Err(ExecutionError::Uncertain);
        }
        self.dispose_shadow()
    }

    /// Retire uncertainty on a follower after a newer term has a coherent
    /// applied image/barrier. This closes the old private owner without ever
    /// opening a new writable activation on that follower.
    pub fn retire_after_barrier(
        &mut self,
        base: Option<&PublishedImage>,
        barrier: LogPosition,
    ) -> Result<(), ExecutionError> {
        self.retire_image_after_barrier(
            base.cloned().map(Published::Materialized).as_ref(),
            barrier,
        )
    }

    pub fn retire_rooted_after_barrier(
        &mut self,
        base: Option<&crate::raft::rooted::RootPublishedImage>,
        barrier: LogPosition,
    ) -> Result<(), ExecutionError> {
        self.retire_image_after_barrier(base.cloned().map(Published::Rooted).as_ref(), barrier)
    }

    pub(crate) fn retire_image_after_barrier(
        &mut self,
        base: Option<&Published>,
        barrier: LogPosition,
    ) -> Result<(), ExecutionError> {
        let cache = self
            .authority
            .group
            .read()
            .map_err(|e| storage(anyhow::Error::new(e)))?;
        if barrier != cache.applied.position
            || barrier.term.get() != cache.hard_state.term
            || barrier.term <= self.authority.term
            || barrier.index > LogIndex::new(cache.hard_state.commit)
            || base.is_some_and(|base| {
                base.scope() != &self.authority.replica.scope
                    || base.application_digest() != self.options.application_digest
                    || base.position().index > barrier.index
            })
            || base.map(Published::marker) != cache.applied.image
        {
            return Err(ExecutionError::Uncertain);
        }
        drop(cache);
        self.dispose_shadow()
    }

    fn dispose_shadow(&mut self) -> Result<(), ExecutionError> {
        self.rebuild_required = true;
        if let Some(working) = self.working.take() {
            working.close().map_err(ExecutionError::Storage)?;
        }
        let mut previews = Vec::new();
        if let Some(pending) = self.pending.take() {
            previews.extend(pending.preview);
        }
        if let Some(preview) = self.retired_preview.take() {
            previews.push(preview);
        }
        for pin in previews {
            self.options.images.retire(&pin).map_err(storage)?;
            drop(pin);
        }
        self.options.images.reclaim_private().map_err(storage)?;
        Ok(())
    }

    /// Join and close the private SQL/capture owner. This is no durability proof;
    /// submitted history is recovered from the Raft engine, never this file.
    pub fn close(mut self) -> Result<(), ExecutionError> {
        let store = self.options.images.root_store().cloned();
        if let Some(working) = self.working.take() {
            working.close().map_err(ExecutionError::Storage)?;
        }
        drop(self.options);
        drop(self._lock);
        if let Some(store) = store {
            if store.usage().map_err(storage)?.quarantined {
                return Err(storage(cellar_ltx::internal::StoreError::Quarantined));
            }
        }
        Ok(())
    }

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
        self.stage_registered_admitted(attempt, class, input, true)
    }

    pub(crate) fn stage_registered_admitted(
        &mut self,
        attempt: CommandAttempt,
        class: &RaftClass,
        input: &[u8],
        allow_new: bool,
    ) -> Result<StageOutcome, ExecutionError> {
        self.stage_registered_kind(attempt, class, input, allow_new, false)
    }

    pub(crate) fn stage_system_admitted(
        &mut self,
        attempt: CommandAttempt,
        class: &RaftClass,
        input: &[u8],
        allow_new: bool,
    ) -> Result<StageOutcome, ExecutionError> {
        self.stage_registered_kind(attempt, class, input, allow_new, true)
    }

    fn stage_registered_kind(
        &mut self,
        attempt: CommandAttempt,
        class: &RaftClass,
        input: &[u8],
        allow_new: bool,
        system: bool,
    ) -> Result<StageOutcome, ExecutionError> {
        let scope = cellar_logic::cell_group::CellHistory::try_from(&self.authority.replica.scope)
            .map_err(storage)?;
        let contract = registered::validate(
            self.options.limits,
            self.options.application_digest,
            &scope,
            &attempt.request,
            class,
            input,
            system,
        )?;
        let command = attempt.request.id;
        let durability = attempt.request.durability;
        self.stage_admitted(attempt, allow_new, |transaction| {
            if system {
                class.invoke_system_command(
                    &contract,
                    &scope,
                    command,
                    durability,
                    transaction,
                    input,
                )
            } else {
                class.invoke_command(&contract, &scope, command, durability, transaction, input)
            }
        })
    }

    pub fn open(
        options: PrivateOptions,
        authority: CommandAuthority,
        base: Option<PublishedImage>,
    ) -> Result<Self, ExecutionError> {
        Self::open_image(options.into(), authority, base.map(Published::Materialized))
    }

    pub fn open_rooted(
        options: RootPrivateOptions,
        authority: CommandAuthority,
        base: Option<crate::raft::rooted::RootPublishedImage>,
    ) -> Result<Self, ExecutionError> {
        Self::open_image(options.into(), authority, base.map(Published::Rooted))
    }

    fn open_image(
        options: Options,
        authority: CommandAuthority,
        base: Option<Published>,
    ) -> Result<Self, ExecutionError> {
        options.limits.validate()?;
        options.images.validate_workspace(options.limits)?;
        if !options.images.matches(base.as_ref()) {
            return Err(ExecutionError::Invalid("private storage kind differs"));
        }
        authority.check_base(base.as_ref(), options.application_digest)?;
        let filesystem = options.host.filesystem();
        cellar_ltx::host::create_dir_all_durable(filesystem.as_ref(), &options.directory)
            .map_err(storage)?;
        let lock = filesystem
            .lock_file(&options.directory.join("working.lock"))
            .map_err(storage)?;
        let lock = PrivateLock {
            _lock: lock,
            _workspace: options.images.workspace().cloned(),
        };
        let working = WorkingImage::open(&options, base.as_ref(), authority.group.birth_digest)
            .map_err(ExecutionError::Storage)?;
        Ok(Self {
            options,
            authority,
            base,
            working: Some(working),
            pending: None,
            rebuild_required: false,
            retired_preview: None,
            receipt_cursor: None,
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

    pub(crate) fn has_pending_proposal(&self) -> bool {
        self.pending.is_some()
    }

    /// Logical bytes hashed by the last extent observation. This excludes
    /// source/cache/sealing I/O and is not an RSS or transfer measurement.
    pub fn extent_observation_work(&self) -> Option<cellar_ltx::internal::db::ObservationWork> {
        self.working
            .as_ref()
            .and_then(WorkingImage::observation_work)
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
            if let Err(error) = self.options.images.retire(&pin) {
                self.retired_preview = Some(pin);
                self.rebuild_required = true;
                return Err(storage(error));
            }
            drop(pin);
            // The next image preparation reclaims eligible files with its
            // publication sync, or before reservation if capacity is exhausted.
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
                    .is_none_or(|working| !working.matches(&self.options, base))
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
        self.stage_admitted(attempt, true, command)
    }

    fn stage_admitted(
        &mut self,
        attempt: CommandAttempt,
        allow_new: bool,
        command: impl FnOnce(&CommandTransaction<'_>) -> anyhow::Result<Vec<u8>>,
    ) -> Result<StageOutcome, ExecutionError> {
        self.ready()?;
        let working = self
            .working
            .as_ref()
            .ok_or(ExecutionError::RebuildRequired)?;
        let maintenance = match command::prepare(
            working.database().framework(),
            self.options.limits,
            &mut self.receipt_cursor,
            &attempt,
            allow_new,
        )? {
            command::Admission::Duplicate => {
                let receipt = self.read_committed(
                    &attempt.request,
                    self.base
                        .as_ref()
                        .ok_or(ExecutionError::Invalid("receipt without base"))?,
                )?;
                return Ok(StageOutcome::Duplicate(receipt));
            }
            command::Admission::New(maintenance) => maintenance,
        };
        self.rebuild_required = true;
        let working = self
            .working
            .as_mut()
            .ok_or(ExecutionError::RebuildRequired)?;
        working.begin_mutation();
        command::apply(
            working.database(),
            self.options.limits,
            &attempt,
            maintenance,
            command,
        )?;
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
            .database()
            .transaction(inputs, |connection, _| {
                receipts::prune(connection, client, through, inputs.now_unix_millis)
                    .map_err(anyhow::Error::new)
            })
            .map_err(private_failure)?;
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
            .map_err(private_failure)?;
        let bytes = update
            .encode(self.options.limits.envelope)
            .map_err(ExecutionError::Protocol)?;
        let base = match self.base.as_ref() {
            Some(base) => base.state(),
            None => match &self.options.images {
                Images::Materialized(_) => {
                    State::Materialized(ImageState::empty(update.base.epoch).map_err(storage)?)
                }
                Images::Extents { .. } => State::Rooted(images::RootState {
                    image: update.base,
                    geometry: cellar_ltx::internal::Geometry::new(0, 0).map_err(storage)?,
                }),
            },
        };
        let pin = self
            .options
            .images
            .preview(
                &self.options,
                self.base.as_ref(),
                preview,
                &update,
                self.authority.group.birth_digest,
            )
            .map_err(storage)?;
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
        let archive_bytes = preview
            .length()
            .saturating_add(bytes.len() as u64)
            .saturating_add(1024);
        self.pending = Some(Pending {
            proposal,
            result: preview.state(),
            preview: Some(preview),
            base,
            payload_digest: Sha256::digest(&bytes).into(),
            kind,
            submitted: false,
        });
        self.rebuild_required = false;
        Ok(PreparedCommand {
            ltx_bytes: update
                .files
                .iter()
                .map(|file| file.bytes.len() as u64)
                .sum(),
            bytes,
            proposal,
            command,
            archive_bytes,
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

    /// Called only after the applier validates its durable committed entry and
    /// exact apply ticket. A mismatch preserves the private pin and falls back
    /// to physical replay; a match transfers ownership without publishing it.
    pub(super) fn take_preview(
        &mut self,
        replica: &ReplicaIdentity,
        images: &GenerationStore,
        bytes: &[u8],
        base: ImageState,
        result: ImageState,
    ) -> Option<GenerationPin> {
        if &self.authority.replica != replica {
            return None;
        }
        self.pending
            .as_mut()?
            .take_preview(images, bytes, base, result)
    }

    pub fn finish(
        &mut self,
        published: PublishedImage,
    ) -> Result<AppliedExecution, ExecutionError> {
        self.finish_image(Published::Materialized(published))
    }

    pub fn finish_rooted(
        &mut self,
        published: crate::raft::rooted::RootPublishedImage,
    ) -> Result<AppliedExecution, ExecutionError> {
        self.finish_image(Published::Rooted(published))
    }

    pub(crate) fn finish_image(
        &mut self,
        published: Published,
    ) -> Result<AppliedExecution, ExecutionError> {
        let pending = self
            .pending
            .as_ref()
            .ok_or(ExecutionError::Invalid("no pending proposal"))?;
        if published.scope() != &self.authority.replica.scope
            || published.application_digest() != self.options.application_digest
            || published.proposal() != Some(pending.proposal)
            || published.state() != pending.result
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
        self.retired_preview = pending.preview;
        self.base = Some(published);
        // A new caught-up authority observation is required before another
        // callback; finish cannot turn an old pre-proposal observation current.
        self.rebuild_required = true;
        Ok(result)
    }

    fn read_committed(
        &self,
        request: &CommandRequest,
        published: &Published,
    ) -> Result<CommittedReceipt, ExecutionError> {
        let database = self
            .options
            .images
            .read(&self.options, published)
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
        self.rebuild_image(base.map(Published::Materialized), authority)
    }

    pub fn rebuild_rooted(
        &mut self,
        base: Option<crate::raft::rooted::RootPublishedImage>,
        authority: CommandAuthority,
    ) -> Result<(), ExecutionError> {
        self.rebuild_image(base.map(Published::Rooted), authority)
    }

    pub(crate) fn rebuild_image(
        &mut self,
        base: Option<Published>,
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
            self.retired_preview = pending.preview;
        }
        if let Some(working) = self.working.take() {
            working.close().map_err(ExecutionError::Storage)?;
        }
        self.base = base;
        self.authority = authority;
        self.working = Some(
            WorkingImage::open(
                &self.options,
                self.base.as_ref(),
                self.authority.group.birth_digest,
            )
            .map_err(ExecutionError::Storage)?,
        );
        self.rebuild_required = false;
        Ok(())
    }
}

pub(super) fn storage(error: impl Into<anyhow::Error>) -> ExecutionError {
    let error = error.into();
    if matches!(
        error.downcast_ref::<cellar_ltx::internal::StoreError>(),
        Some(cellar_ltx::internal::StoreError::Limit | cellar_ltx::internal::StoreError::Busy)
    ) || matches!(
        error.downcast_ref::<cellar_ltx::generation::GenerationError>(),
        Some(
            cellar_ltx::generation::GenerationError::Limit
                | cellar_ltx::generation::GenerationError::Snapshot(
                    cellar_ltx::stream_snapshot::Error::ScratchFull
                )
        )
    ) || error
        .downcast_ref::<super::snapshot::SnapshotError>()
        .is_some_and(|e| e.scratch_full())
        || error
            .downcast_ref::<super::archive::ArchiveError>()
            .is_some_and(|e| e.scratch_full())
    {
        ExecutionError::Refused(CommandFailure::Overloaded)
    } else {
        ExecutionError::Storage(error)
    }
}

fn private_failure(error: anyhow::Error) -> ExecutionError {
    // This is still private execution: no proposal or output proof exists.
    // Preserve rebuild_required even for SQLITE_FULL. SQLite may have rolled
    // back automatically, and an error does not prove the connection reusable.
    if matches!(
        error.downcast_ref::<rusqlite::Error>(),
        Some(rusqlite::Error::SqliteFailure(failure, _))
            if failure.code == rusqlite::ErrorCode::DiskFull
    ) {
        return ExecutionError::Refused(CommandFailure::StorageFull);
    }
    match error.downcast::<ExecutionError>() {
        Ok(error) => error,
        Err(error) => match error.downcast::<CommandFailure>() {
            Ok(error) => ExecutionError::Refused(error),
            Err(error) => ExecutionError::Storage(error),
        },
    }
}
