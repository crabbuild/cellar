// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded waiters for a node-owned Raft dispatcher. Dropping a waiter never
//! drops its submitted producer or grants another writer a private image.

use super::{
    CommandFailure, CommandId, CommittedVersion, Durability, GroupScope, OperationContract,
    OperationKind, RaftClass, VersionToken,
};
use crate::raft::budget::{Budget, Permit};
use crate::raft::mailbox;
use cellar_logic::consensus::{LogIndex, LogPosition, MemberId, NamespaceId, Term};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Notify};

#[path = "client_fleet.rs"]
mod fleet;
pub(crate) use fleet::FleetRequest;

#[path = "client_bindings.rs"]
mod bindings;
#[path = "client_node.rs"]
mod node;
pub(crate) use bindings::Bindings;
use node::send_failure;
pub(crate) use node::PeerFleetRequest;
pub(crate) use node::{ControlRequest, NodeKind, NodeRequest};

/// A compare-and-set admission request for a preconfigured fresh learner.
#[derive(Clone, Debug)]
pub struct LearnerOptions {
    pub expected_revision: LogIndex,
    pub timeout: Duration,
}

/// Initialize a registered standby at an already committed learner binding.
/// These fields are hints: a fresh source quorum grant and durable claim are
/// still required. Retrying preserves the target member identity.
#[derive(Clone, Debug)]
pub struct LearnerActivation {
    pub member: MemberId,
    pub source: MemberId,
    pub source_node: cellar_logic::consensus::NodeId,
    pub from_s3: bool,
    pub timeout: Duration,
}

/// Durable admission observation. This is not a live-quorum seeding grant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LearnerReceipt {
    pub scope: GroupScope,
    pub member: crate::raft::member_state::JoiningMember,
    pub joined_at: LogIndex,
}

#[derive(Clone, Debug)]
pub struct CommandOptions {
    /// Persist the pending request before calling and reuse this ID and input
    /// after an ambiguous outcome. The dispatcher never invents a retry ID.
    pub id: CommandId,
    pub durability: Durability,
    pub retry_until_ms: u64,
    pub timeout: Duration,
}

impl CommandOptions {
    pub fn new(id: CommandId, retry_until_ms: u64) -> Self {
        Self {
            id,
            durability: Durability::Quorum,
            retry_until_ms,
            timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandReceipt {
    pub id: CommandId,
    pub result: Vec<u8>,
    pub version: CommittedVersion,
    pub version_token: VersionToken,
    pub durability: Durability,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryConsistency {
    Stale,
    AtLeast(VersionToken),
    Linearizable,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StaleReadPolicy {
    /// Refuse after the configured deadline for observing a current source.
    #[default]
    RecentSource,
    /// Permit a verified historical image during disconnection. This supplies
    /// no wall-clock staleness bound and does not strengthen its consistency.
    DisconnectedHistorical,
}

#[derive(Clone, Debug, Default)]
pub enum QueryRouting {
    #[default]
    Local,
    /// Try at most eight distinct members in priority order, then optionally
    /// one untried leader hint. Each attempt retains the same consistency,
    /// token and captured input under the original invocation deadline.
    Replicas {
        members: Vec<MemberId>,
        attempt_timeout: Duration,
        leader_fallback: bool,
    },
}

#[derive(Clone, Debug)]
pub struct QueryOptions {
    pub consistency: QueryConsistency,
    pub timeout: Duration,
    pub stale_policy: StaleReadPolicy,
    pub routing: QueryRouting,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            consistency: QueryConsistency::Stale,
            timeout: Duration::from_secs(5),
            stale_policy: StaleReadPolicy::RecentSource,
            routing: QueryRouting::Local,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryResult {
    pub result: Vec<u8>,
    pub version: CommittedVersion,
    pub version_token: VersionToken,
    pub served_by: MemberId,
}

/// Discovery/readiness observation, never a grant of mutation authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellStatus {
    pub role: super::RaftReplicaRole,
    pub queries: super::RaftQueryReadiness,
    pub leader: Option<MemberId>,
    pub term: Term,
    pub command_ready: bool,
    pub version: Option<CommittedVersion>,
    /// Coherent runtime observations for diagnosis. A persisted suffix can be
    /// uncommitted; these fields are never an SDK acknowledgement proof.
    pub persisted: LogIndex,
    pub committed: LogIndex,
    pub applied: LogPosition,
    /// The durable Raft checkpoint boundary; it grants no read or write proof.
    pub checkpoint: LogPosition,
    pub archived: LogIndex,
    pub membership_revision: LogIndex,
    pub member_high_water: u64,
}

pub use crate::raft::operations::{RaftCloseReport, UnfinishedCommand};

#[derive(Clone)]
pub(crate) struct Binding {
    pub scope: GroupScope,
    pub class: Arc<RaftClass>,
    pub minimum: Durability,
    pub member: Arc<LocalMember>,
    pub leader_hint: Option<MemberId>,
}

/// One-way local admission. Retained dormant handles share this slot; only the
/// host publishes it after durable initialization, claim and runtime attachment.
pub(crate) struct LocalMember(AtomicU64);

impl LocalMember {
    pub fn new(member: Option<MemberId>) -> Arc<Self> {
        Arc::new(Self(AtomicU64::new(member.map_or(0, MemberId::get))))
    }

    pub fn attach(&self, member: MemberId) -> Result<(), CommandFailure> {
        self.0
            .compare_exchange(0, member.get(), Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .or_else(|old| {
                if old == member.get() {
                    Ok(())
                } else {
                    Err(CommandFailure::CommandConflict)
                }
            })
    }
}

impl Binding {
    pub fn member(&self) -> Option<MemberId> {
        MemberId::new(self.member.0.load(Ordering::Acquire)).ok()
    }
}

pub(crate) struct Terminal {
    pub result: Mutex<Option<Result<RaftCloseReport, CommandFailure>>>,
    pub changed: Notify,
}

pub(crate) struct Handle {
    pub performance: Arc<crate::raft::performance::Registry>,
    pub requests: mailbox::Sender<Request>,
    pub control_requests: mailbox::Sender<ControlRequest>,
    pub control_budget: Arc<Budget>,
    pub close: mpsc::UnboundedSender<()>,
    pub domain: crate::asyncrt::DomainToken,
    pub closed: Arc<AtomicBool>,
    pub closing: Arc<AtomicBool>,
    pub admission: Arc<super::admission::Admission>,
    pub terminal: Arc<Terminal>,
    pub budget: Arc<Budget>,
    pub archive_capture_budget: Arc<Budget>,
    pub query_pins: Arc<Budget>,
    pub sql: Option<crate::raft::SqlResourceObserver>,
    pub groups: crate::raft::host::GroupResourceObserver,
    pub controller: Mutex<Option<(u64, super::RaftControllerResources)>>,
    pub transport: crate::raft::transport::ResourceObserver,
    pub max_input_bytes: usize,
    pub bindings: Bindings,
    pub work: Arc<crate::host_services::ApplicationWork>,
}

/// Registered Raft cells owned by the node's execution domain. Handles can
/// outlive their caller, while node shutdown closes admission and joins work.
#[derive(Clone)]
pub struct RaftCells {
    pub(crate) handle: Arc<Handle>,
}

impl RaftCells {
    /// Fixed-cardinality cumulative work and I/O API observations. Remains readable
    /// after close; clock availability and source identity are explicit.
    pub fn performance(&self) -> super::RaftPerformanceSnapshot {
        self.handle.performance.snapshot()
    }

    /// Read actual fixed-size local admission counters without I/O, enumeration
    /// or callbacks. Remains available during drain and after joined closure.
    /// Independent atomic loads can observe concurrent acquisition/release.
    pub fn resource_usage(&self) -> super::RaftResourceUsage {
        super::RaftResourceUsage {
            business: super::RaftBudgetUsage::capture(&self.handle.budget),
            control: super::RaftBudgetUsage::capture(&self.handle.control_budget),
            archive_capture: super::RaftBudgetUsage::capture(&self.handle.archive_capture_budget),
            query_pins: super::RaftQueryPinUsage {
                in_use: self.handle.query_pins.usage().1,
                limit: self.handle.query_pins.capacity().1,
            },
            sql: self.handle.sql.as_ref().and_then(|sql| sql.capture()),
            images: self
                .handle
                .sql
                .as_ref()
                .and_then(|sql| sql.images())
                .map(Into::into),
            transport: self.handle.transport.capture(),
            controller: self.handle.controller.lock().ok().and_then(|source| {
                let (at, mut sample) = (*source)?;
                sample.sample_age_ms = self.handle.domain.mono_ms().ok()?.checked_sub(at)?;
                Some(sample)
            }),
            groups: self
                .handle
                .domain
                .mono_ms()
                .ok()
                .and_then(|now| self.handle.groups.capture(now)),
        }
    }

    pub(crate) fn begin_node_drain(&self) {
        // Even a poisoned gate remains sealed. Final physical close reports
        // its failure after joining all owners, rather than inventing proof.
        let _ = self.handle.admission.seal();
    }

    pub(crate) async fn drain_node(
        &self,
        deadline: u64,
    ) -> Result<super::RaftDrainReport, CommandFailure> {
        let now = self
            .handle
            .domain
            .mono_ms()
            .map_err(|_| CommandFailure::Unavailable)?;
        if now >= deadline {
            return Ok(super::RaftDrainReport {
                deadline_elapsed: true,
                ..Default::default()
            });
        }
        // An earlier explicit close remains immediate. It cannot retroactively
        // acquire a graceful handoff certificate after releasing its owners.
        if self.handle.closing.load(Ordering::Acquire) {
            return Err(CommandFailure::Unavailable);
        }
        match self
            .node_call(
                NodeKind::Drain,
                Duration::from_millis((deadline - now).min(3_600_000)),
            )
            .await?
        {
            Response::Drained(report) => Ok(report),
            _ => Err(CommandFailure::CorruptState),
        }
    }

    /// Observe the optional local controller. This is not a readiness, quorum
    /// or membership proof; attempt counters reset with the node process.
    pub fn controller_status(
        &self,
    ) -> impl Future<Output = Result<super::RaftControllerStatus, CommandFailure>> + Send + 'static
    {
        let call = self.node_call(NodeKind::ControllerStatus, Duration::from_secs(5));
        async move {
            match call.await? {
                Response::Controller(status) => Ok(status),
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }

    /// Pause/resume new automatic work without closing quorum or inspection.
    /// Admitted mutations and target-owned initialization may still complete;
    /// resumption reconciles their committed state before creating another plan.
    pub fn set_controller_enabled(
        &self,
        enabled: bool,
    ) -> impl Future<Output = Result<super::RaftControllerStatus, CommandFailure>> + Send + 'static
    {
        let call = self.node_call(
            NodeKind::SetControllerEnabled(enabled),
            Duration::from_secs(5),
        );
        async move {
            match call.await? {
                Response::Controller(status) => Ok(status),
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }

    pub fn cell(
        &self,
        namespace: NamespaceId,
        cell: &str,
    ) -> Result<RaftCellHandle, CommandFailure> {
        if self.is_closed() || !cellar_logic::cell::valid_cell_scope(cell) {
            return Err(CommandFailure::Unavailable);
        }
        let binding = self.handle.bindings.get(&(namespace, cell.into()))?;
        Ok(RaftCellHandle {
            handle: self.handle.clone(),
            binding,
        })
    }

    pub fn is_closed(&self) -> bool {
        self.handle.closed.load(Ordering::Acquire)
            || self.handle.closing.load(Ordering::Acquire)
            || self.handle.admission.is_draining()
            || self.handle.domain.mono_ms().is_err()
    }

    pub async fn close(&self) -> Result<RaftCloseReport, CommandFailure> {
        if super::class::callback_active() {
            return Err(CommandFailure::UnsupportedCapability);
        }
        if !self.handle.closing.swap(true, Ordering::AcqRel) {
            // Finish any short registration before signalling physical close.
            // A poisoned gate must still close/join; the driver reports error.
            let _ = self.handle.admission.barrier();
            let _ = self.handle.close.send(());
        }
        loop {
            let changed = self.handle.terminal.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(result) = self
                .handle
                .terminal
                .result
                .lock()
                .map_err(|_| CommandFailure::Unavailable)?
                .clone()
            {
                return result;
            }
            if self.handle.closed.load(Ordering::Acquire) || self.handle.domain.mono_ms().is_err() {
                return Err(CommandFailure::Unavailable);
            }
            changed.await;
        }
    }
}

/// A namespace/cell binding. This is a routing handle, not a retained leader
/// activation: every command obtains fresh checked authority in the host core.
#[derive(Clone)]
pub struct RaftCellHandle {
    pub(crate) handle: Arc<Handle>,
    pub(crate) binding: Binding,
}

impl RaftCellHandle {
    /// Activate this history inside the existing node's engine and SQL pool.
    /// An unavailable or dropped response may follow durable initialization;
    /// retry the same member. Existing claimed state is never reset or rebound.
    pub fn activate_learner(
        &self,
        activation: LearnerActivation,
    ) -> impl Future<Output = Result<LearnerReceipt, CommandFailure>> + Send + 'static {
        let timeout = activation.timeout;
        let call = self.call(Kind::ActivateLearner(activation), timeout);
        async move {
            match call.await? {
                Response::Learner(receipt) => Ok(receipt),
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }

    /// Inspect bounded leader-local replication progress. This grants no
    /// quorum, apply, seeding or membership authority.
    pub fn replication_status(
        &self,
    ) -> impl Future<Output = Result<super::ReplicationStatus, CommandFailure>> + Send + 'static
    {
        let call = self.call(Kind::ReplicationStatus, Duration::from_secs(5));
        async move {
            match call.await? {
                Response::Replication(status) => Ok(status),
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }

    /// Request movement to a caught-up current voter. Success is admission
    /// only; observe the newly elected leader separately. Unavailable or a lost
    /// response does not establish that the transfer did not occur.
    pub fn transfer_leadership(
        &self,
        transfer: super::LeadershipTransfer,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), CommandFailure>> + Send + 'static {
        let call = self.call(Kind::TransferLeadership(transfer), timeout);
        async move {
            match call.await? {
                Response::LeadershipRequested => Ok(()),
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }

    pub(crate) fn prepare_seed(
        &self,
        target: cellar_logic::consensus::ReplicaIdentity,
        kind: crate::raft::transport::seeds::SeedKind,
        charge: Arc<Permit>,
        timeout: Duration,
    ) -> impl Future<Output = Result<Box<crate::raft::transport::seeds::SourceSeed>, CommandFailure>>
           + Send
           + 'static {
        let call = self.call(
            Kind::Seed {
                target,
                kind,
                charge,
            },
            timeout,
        );
        async move {
            match call.await? {
                Response::Seed(seed) => Ok(seed),
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }

    /// Admit a fresh identity as a learner at a trusted configured endpoint.
    /// Unavailable/cancellation may follow a committed change: retry the same
    /// member/node/disk and expected revision, never a newly invented identity.
    pub fn add_learner(
        &self,
        member: crate::raft::member_state::JoiningMember,
        options: LearnerOptions,
    ) -> impl Future<Output = Result<LearnerReceipt, CommandFailure>> + Send + 'static {
        let call = self.call(
            Kind::AddLearner {
                member,
                expected_revision: options.expected_revision,
            },
            options.timeout,
        );
        async move {
            match call.await? {
                Response::Learner(receipt) => Ok(receipt),
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }

    /// Observe registered-history occupancy, declared capacity and published
    /// progress. Available for members and dormant standbys when configured;
    /// this metadata supplies no read, seed or membership capability.
    pub fn inspect(
        &self,
    ) -> impl Future<Output = Result<super::CellInspection, CommandFailure>> + Send + 'static {
        let call = self.inspect_snapshot(Duration::from_secs(5));
        async move { Ok(call.await?.view) }
    }

    /// Inspect an explicitly configured physical peer for this registered
    /// history. A dormant source needs no member identity. Metadata grants and
    /// discovery must be enabled on both endpoints. This call grants no quorum
    /// or activation authority; only `Unreachable` reports a failed TCP probe.
    pub fn inspect_peer(
        &self,
        peer: cellar_logic::consensus::NodeId,
        timeout: Duration,
    ) -> impl Future<Output = Result<super::CellInspection, super::InspectionError>> + Send + 'static
    {
        let call = self.call(Kind::InspectPeer(peer), timeout);
        async move {
            use super::InspectionError;
            let response = call.await.map_err(|error| match error {
                CommandFailure::Overloaded => InspectionError::Overloaded,
                CommandFailure::UnsupportedCapability => InspectionError::Refused,
                _ => InspectionError::Unavailable,
            })?;
            let snapshot = match response {
                Response::Inspection(snapshot) => snapshot,
                Response::PeerInspection(result) => result?,
                _ => return Err(InspectionError::Unavailable),
            };
            snapshot
                .validate()
                .map_err(|_| InspectionError::Unavailable)?;
            Ok(snapshot.view)
        }
    }

    pub(crate) fn inspect_snapshot(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<Box<crate::raft::inspection::InspectionSnapshot>, CommandFailure>>
           + Send
           + 'static {
        let call = self.call(Kind::Inspect, timeout);
        async move {
            match call.await? {
                Response::Inspection(snapshot) => {
                    snapshot.validate()?;
                    Ok(snapshot)
                }
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }

    /// Observe the synced plan, without a fresh quorum or activation proof.
    pub fn repair_status(
        &self,
    ) -> impl Future<Output = Result<Option<crate::raft::member_state::RepairStatus>, CommandFailure>>
           + Send
           + 'static {
        let call = self.call(Kind::RepairStatus, Duration::from_secs(5));
        async move {
            match call.await? {
                Response::Repair(status) => Ok(status),
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }

    /// Commit a revision-bound learner repair plan. Lost responses may follow
    /// commitment; retry the same plan identity and immutable payload.
    pub fn begin_repair(
        &self,
        plan: crate::raft::member_state::RepairPlan,
        timeout: Duration,
    ) -> impl Future<Output = Result<crate::raft::member_state::RepairStatus, CommandFailure>>
           + Send
           + 'static {
        let call = self.call(Kind::BeginRepair { plan }, timeout);
        async move {
            match call.await? {
                Response::Repair(Some(status)) => Ok(status),
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }

    /// Advance the current exact plan. Promotion still requires actual matched
    /// progress at the consensus owner; neither status nor S3 supplies proof.
    pub fn advance_repair(
        &self,
        id: cellar_logic::consensus::ProposalId,
        step: crate::raft::member_state::RepairStep,
        expected_revision: LogIndex,
        timeout: Duration,
    ) -> impl Future<Output = Result<crate::raft::member_state::RepairStatus, CommandFailure>>
           + Send
           + 'static {
        let call = self.call(
            Kind::AdvanceRepair {
                id,
                step,
                expected_revision,
            },
            timeout,
        );
        async move {
            match call.await? {
                Response::Repair(Some(status)) => Ok(status),
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }

    pub fn scope(&self) -> &GroupScope {
        &self.binding.scope
    }

    /// Admit synchronously into the node's bounded queue. Even an unpolled
    /// returned future may start private execution; dropping it cancels the
    /// waiter, while the node retains any submitted producer. Persist and reuse
    /// the supplied command ID after an ambiguous outcome.
    pub fn command(
        &self,
        contract: OperationContract,
        input: Vec<u8>,
        mut options: CommandOptions,
    ) -> impl Future<Output = Result<CommandReceipt, CommandFailure>> + Send + 'static {
        options.durability = options.durability.max(self.binding.minimum);
        let timeout = options.timeout;
        let id = options.id;
        let call = self.call(
            Kind::Command {
                contract,
                input,
                options,
            },
            timeout,
        );
        async move {
            match call.await? {
                Response::Command(receipt) if receipt.id == id => Ok(receipt),
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }

    pub(crate) fn system_command(
        &self,
        contract: OperationContract,
        input: Vec<u8>,
        mut options: CommandOptions,
    ) -> impl Future<Output = Result<CommandReceipt, CommandFailure>> + Send + 'static {
        options.durability = options.durability.max(self.binding.minimum);
        let timeout = options.timeout;
        let id = options.id;
        let call = self.call(
            Kind::SystemCommand {
                contract,
                input,
                options,
            },
            timeout,
        );
        async move {
            match call.await? {
                Response::Command(receipt) if receipt.id == id => Ok(receipt),
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }

    /// Queue a read immediately; the returned version names its pinned image.
    /// Dropping the returned future cancels the waiter.
    pub fn query(
        &self,
        contract: OperationContract,
        input: Vec<u8>,
        options: QueryOptions,
    ) -> impl Future<Output = Result<QueryResult, CommandFailure>> + Send + 'static {
        let timeout = options.timeout;
        let call = self.call(
            Kind::Query {
                contract,
                input,
                options,
            },
            timeout,
        );
        async move {
            match call.await? {
                Response::Query(result) => Ok(result),
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }

    /// Queue a readiness observation immediately. Readiness may change before
    /// a subsequent command; the observation grants no write authority.
    pub fn status(
        &self,
    ) -> impl Future<Output = Result<CellStatus, CommandFailure>> + Send + 'static {
        let call = self.call(Kind::Status, Duration::from_secs(5));
        async move {
            match call.await? {
                Response::Status(status) => Ok(status),
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }

    fn call(
        &self,
        kind: Kind,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Response, CommandFailure>> + Send>> {
        let control = Arc::new(AttemptControl::new(self.handle.domain.clone()));
        let guard = Waiter(control.clone());
        let failure = kind.failure();
        let accepted = (|| {
            if super::class::callback_active() {
                return Err(CommandFailure::UnsupportedCapability);
            }
            if self.handle.closed.load(Ordering::Acquire)
                || self.handle.closing.load(Ordering::Acquire)
                || self.handle.admission.is_draining()
                    && !matches!(kind, Kind::SystemCommand { .. })
            {
                return Err(CommandFailure::Unavailable);
            }
            let now = self
                .handle
                .domain
                .mono_ms()
                .map_err(|_| CommandFailure::Unavailable)?;
            if timeout.is_zero() || timeout > Duration::from_secs(3600) {
                return Err(CommandFailure::Unavailable);
            }
            match &kind {
                Kind::ActivateInitial(authorization) => {
                    if !authorization.matches_scope(&self.binding.scope) {
                        return Err(CommandFailure::CommandConflict);
                    }
                    if timeout > Duration::from_secs(60) {
                        return Err(CommandFailure::UnsupportedCapability);
                    }
                }
                Kind::ActivateLearner(activation) | Kind::ActivateManaged { activation, .. } => {
                    if let Kind::ActivateManaged { authorization, .. } = &kind {
                        if !authorization.matches_scope(&self.binding.scope)
                            || authorization
                                .permit()
                                .replacement()
                                .map(|member| member.member)
                                != Ok(activation.member)
                        {
                            return Err(CommandFailure::CommandConflict);
                        }
                    }
                    if activation.member == activation.source || timeout > Duration::from_secs(300)
                    {
                        return Err(CommandFailure::UnsupportedCapability);
                    }
                    if self
                        .binding
                        .member()
                        .is_some_and(|member| member != activation.member)
                    {
                        return Err(CommandFailure::CommandConflict);
                    }
                }
                Kind::Command {
                    contract,
                    input,
                    options,
                } => {
                    if self.binding.member().is_none() {
                        return Err(CommandFailure::UnsupportedCapability);
                    }
                    self.validate(contract, OperationKind::Command, input)?;
                    self.binding
                        .class
                        .validate_command_identity(
                            &self.binding.scope,
                            contract,
                            options.id,
                            options.durability,
                            input,
                        )
                        .map_err(|error| {
                            error
                                .downcast_ref::<CommandFailure>()
                                .cloned()
                                .unwrap_or(CommandFailure::UnsupportedCapability)
                        })?;
                }
                Kind::SystemCommand {
                    contract,
                    input,
                    options,
                } => {
                    if self.binding.member().is_none()
                        || self
                            .binding
                            .class
                            .validate_system_operation(contract)
                            .is_err()
                        || options.id.client
                            != crate::raft::effects::system_client(&self.binding.scope)?
                    {
                        return Err(CommandFailure::UnsupportedCapability);
                    }
                    if input.len() > self.handle.max_input_bytes {
                        return Err(CommandFailure::Overloaded);
                    }
                }
                Kind::Query {
                    contract,
                    input,
                    options,
                } => {
                    if matches!(options.routing, QueryRouting::Local) {
                        if self.binding.member().is_none() {
                            return Err(CommandFailure::UnsupportedCapability);
                        }
                        self.validate(contract, OperationKind::Query, input)?;
                    } else {
                        // A voter may forward a query whose implementation is
                        // only installed on another replica. Destination code
                        // compatibility remains mandatory before execution.
                        if contract.kind != OperationKind::Query
                            || contract.digest == [0; 32]
                            || super::class::validate_name(&contract.operation).is_err()
                        {
                            return Err(CommandFailure::UnsupportedCapability);
                        }
                        if input.len() > self.handle.max_input_bytes {
                            return Err(CommandFailure::Overloaded);
                        }
                    }
                    if let QueryRouting::Replicas {
                        members,
                        attempt_timeout,
                        ..
                    } = &options.routing
                    {
                        if members.is_empty()
                            || members.len() > 8
                            || members
                                .iter()
                                .collect::<std::collections::BTreeSet<_>>()
                                .len()
                                != members.len()
                            || attempt_timeout.as_millis() == 0
                            || *attempt_timeout > Duration::from_secs(60)
                        {
                            return Err(CommandFailure::UnsupportedConsistency);
                        }
                    }
                }
                Kind::Inspect => (),
                Kind::InspectPeer(_) => {
                    if timeout > Duration::from_secs(60) {
                        return Err(CommandFailure::UnsupportedCapability);
                    }
                }
                Kind::Status | Kind::RepairStatus | Kind::ReplicationStatus | Kind::FleetStatus => {
                    if self.binding.member().is_none() {
                        return Err(CommandFailure::UnsupportedCapability);
                    }
                }
                Kind::TransferLeadership(transfer) => {
                    if self.binding.member().is_none()
                        || transfer.expected_term.get() == 0
                        || transfer.expected_revision.get() == 0
                    {
                        return Err(CommandFailure::UnsupportedCapability);
                    }
                }
                Kind::BeginRepair { plan } => {
                    if self.binding.member().is_none()
                        || plan.expected_revision.get() == 0
                        || plan.policy_revision == 0
                    {
                        return Err(CommandFailure::UnsupportedCapability);
                    }
                }
                Kind::Fleet(action) => {
                    if self.binding.member().is_none()
                        || timeout > Duration::from_secs(60)
                        || !action.matches_scope(&self.binding.scope)
                    {
                        return Err(CommandFailure::UnsupportedCapability);
                    }
                }
                Kind::ObserveFleet(authorization) => {
                    if self.binding.member().is_none()
                        || timeout > Duration::from_secs(60)
                        || !authorization.matches_scope(&self.binding.scope)
                    {
                        return Err(CommandFailure::UnsupportedCapability);
                    }
                }
                Kind::AdvanceRepair {
                    expected_revision, ..
                } => {
                    if self.binding.member().is_none() || expected_revision.get() == 0 {
                        return Err(CommandFailure::UnsupportedCapability);
                    }
                }
                Kind::AddLearner {
                    expected_revision, ..
                } => {
                    if self.binding.member().is_none() || expected_revision.get() == 0 {
                        return Err(CommandFailure::UnsupportedCapability);
                    }
                }
                Kind::Seed { target, .. } => {
                    if self.binding.member().is_none() || target.scope != self.binding.scope {
                        return Err(CommandFailure::UnsupportedCapability);
                    }
                }
            }
            let deadline = now
                .checked_add(timeout.as_millis() as u64)
                .ok_or(CommandFailure::Unavailable)?;
            control.deadline.store(deadline, Ordering::Release);
            let timer = self
                .handle
                .domain
                .sleep_until(deadline)
                .map_err(|_| CommandFailure::Unavailable)?;
            let _registration = self
                .handle
                .admission
                .registration(matches!(kind, Kind::SystemCommand { .. }))?;
            if self.handle.closed.load(Ordering::Acquire)
                || self.handle.closing.load(Ordering::Acquire)
            {
                return Err(CommandFailure::Unavailable);
            }
            let administrative = kind.is_control();
            let budget = if administrative {
                &self.handle.control_budget
            } else {
                &self.handle.budget
            };
            let permit = Arc::new(
                budget
                    .acquire(kind.bytes())
                    .ok_or(CommandFailure::Overloaded)?,
            );
            let work = Arc::new(Work {
                _guard: self.handle.work.enter(),
            });
            let (reply, receiver) = oneshot::channel();
            let request = Request {
                scope: self.binding.scope.clone(),
                kind,
                reply: Some(reply),
                control: control.clone(),
                _permit: permit.clone(),
                work,
            };
            if administrative {
                self.handle
                    .control_requests
                    .try_send(ControlRequest::Cell(Box::new(request)))
                    .map_err(send_failure)?;
            } else {
                self.handle
                    .requests
                    .try_send(request)
                    .map_err(send_failure)?;
            }
            Ok((receiver, timer, permit))
        })();
        Box::pin(async move {
            let _guard = guard;
            let (receiver, timer, _permit) = accepted?;
            crate::asyncrt::select! {
                result = receiver => result.unwrap_or_else(|_| Err(failure.cancel(&control))).map(|response| response.response),
                _ = timer => Err(failure.cancel(&control)),
            }
        })
    }

    fn validate(
        &self,
        contract: &OperationContract,
        kind: OperationKind,
        input: &[u8],
    ) -> Result<(), CommandFailure> {
        if contract.kind != kind || self.binding.class.validate_operation(contract).is_err() {
            return Err(CommandFailure::UnsupportedCapability);
        }
        if input.len() > self.handle.max_input_bytes {
            return Err(CommandFailure::Overloaded);
        }
        Ok(())
    }
}

pub(crate) struct Work {
    _guard: crate::host_services::ApplicationWorkGuard,
}

pub(crate) enum Kind {
    ActivateInitial(Box<crate::raft::fleet_catalog::FleetGroupAuthorization>),
    Fleet(Box<FleetRequest>),
    FleetStatus,
    ObserveFleet(Box<crate::raft::fleet_catalog::FleetGroupAuthorization>),
    ActivateManaged {
        authorization: Box<crate::raft::fleet_catalog::FleetPermitAuthorization>,
        activation: LearnerActivation,
    },
    Inspect,
    InspectPeer(cellar_logic::consensus::NodeId),
    ActivateLearner(LearnerActivation),
    ReplicationStatus,
    TransferLeadership(super::LeadershipTransfer),
    BeginRepair {
        plan: crate::raft::member_state::RepairPlan,
    },
    AdvanceRepair {
        id: cellar_logic::consensus::ProposalId,
        step: crate::raft::member_state::RepairStep,
        expected_revision: LogIndex,
    },
    RepairStatus,

    Seed {
        target: cellar_logic::consensus::ReplicaIdentity,
        kind: crate::raft::transport::seeds::SeedKind,
        charge: Arc<Permit>,
    },
    AddLearner {
        member: crate::raft::member_state::JoiningMember,
        expected_revision: LogIndex,
    },
    Command {
        contract: OperationContract,
        input: Vec<u8>,
        options: CommandOptions,
    },
    SystemCommand {
        contract: OperationContract,
        input: Vec<u8>,
        options: CommandOptions,
    },
    Query {
        contract: OperationContract,
        input: Vec<u8>,
        options: QueryOptions,
    },
    Status,
}

impl Kind {
    fn is_control(&self) -> bool {
        !matches!(self, Self::Command { .. } | Self::Query { .. })
    }

    fn bytes(&self) -> usize {
        4096 + match self {
            Self::Command { input, .. }
            | Self::SystemCommand { input, .. }
            | Self::Query { input, .. } => input.len(),
            Self::Inspect
            | Self::InspectPeer(_)
            | Self::Status
            | Self::AddLearner { .. }
            | Self::Seed { .. }
            | Self::BeginRepair { .. }
            | Self::AdvanceRepair { .. }
            | Self::ReplicationStatus
            | Self::TransferLeadership(_)
            | Self::ActivateLearner(_)
            | Self::ActivateManaged { .. }
            | Self::ActivateInitial(_)
            | Self::Fleet(_)
            | Self::FleetStatus
            | Self::ObserveFleet(_)
            | Self::RepairStatus => 0,
        }
    }

    fn failure(&self) -> CancelFailure {
        match self {
            Self::Command { options, .. } | Self::SystemCommand { options, .. } => {
                CancelFailure::Command(options.id)
            }
            Self::Query { .. } | Self::Seed { .. } => CancelFailure::Query,
            Self::Inspect
            | Self::InspectPeer(_)
            | Self::Status
            | Self::AddLearner { .. }
            | Self::BeginRepair { .. }
            | Self::AdvanceRepair { .. }
            | Self::ReplicationStatus
            | Self::TransferLeadership(_)
            | Self::ActivateLearner(_)
            | Self::ActivateManaged { .. }
            | Self::ActivateInitial(_)
            | Self::Fleet(_)
            | Self::FleetStatus
            | Self::ObserveFleet(_)
            | Self::RepairStatus => CancelFailure::Status,
        }
    }
}

enum CancelFailure {
    Command(CommandId),
    Query,
    Status,
}

impl CancelFailure {
    fn cancel(&self, control: &AttemptControl) -> CommandFailure {
        let submitted = control.cancel();
        match self {
            Self::Command(id) if submitted => CommandFailure::Ambiguous { command: *id },
            Self::Query => CommandFailure::VersionUnavailable,
            _ => CommandFailure::Unavailable,
        }
    }
}

pub(crate) enum Response {
    PeerFleetObservation(
        Result<Box<super::RaftPeerFleetMembershipObservation>, super::InspectionError>,
    ),
    LocalFleetObservation(Box<crate::raft::fleet_inspection_wire::LocalObservation>),
    PeerNodeInspection(Result<Box<super::RaftPeerNodeObservation>, super::InspectionError>),
    HistoryRegistered,
    Node(Box<super::RaftHostObservation>),
    Fleet(Option<Box<crate::raft::member_state::FleetMembershipStatus>>),
    FleetObservation(Box<crate::raft::host::FleetMembershipObservation>),
    Drained(super::RaftDrainReport),
    Controller(super::RaftControllerStatus),
    PeerInspection(
        Result<Box<crate::raft::inspection::InspectionSnapshot>, super::InspectionError>,
    ),
    Inspection(Box<crate::raft::inspection::InspectionSnapshot>),
    Replication(super::ReplicationStatus),
    LeadershipRequested,
    Repair(Option<crate::raft::member_state::RepairStatus>),
    Seed(Box<crate::raft::transport::seeds::SourceSeed>),
    Learner(LearnerReceipt),
    Command(CommandReceipt),
    Query(QueryResult),
    Status(CellStatus),
}

/// The channel owns response bytes until its waiter consumes or drops them.
/// Completing a producer must not release admission for an unpolled response.
pub(crate) struct ChargedResponse {
    pub response: Response,
    pub _bytes: Option<Permit>,
}

pub(crate) type ResponseSender = oneshot::Sender<Result<ChargedResponse, CommandFailure>>;

pub(crate) struct Request {
    pub scope: GroupScope,
    pub kind: Kind,
    pub reply: Option<ResponseSender>,
    pub control: Arc<AttemptControl>,
    pub _permit: Arc<Permit>,
    pub work: Arc<Work>,
}

pub(crate) struct AttemptControl {
    state: Arc<AtomicU8>,
    deadline: AtomicU64,
    domain: crate::asyncrt::DomainToken,
    changed: Notify,
}

impl AttemptControl {
    pub(crate) fn deadline(&self) -> u64 {
        self.deadline.load(Ordering::Acquire)
    }

    pub(crate) async fn cancellation(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.cancelled() {
                return;
            }
            let Ok(timer) = self.domain.sleep_until(self.deadline()) else {
                return;
            };
            crate::asyncrt::select! { _ = &mut changed => (), _ = timer => { self.cancel(); return; } }
        }
    }

    /// SQLite's panic-contained progress hook captures only this atomic flag.
    /// The node read controller checks the injected deadline even for an
    /// unpolled waiter and raises it on expiry, cancellation or shutdown.
    pub(crate) fn interrupt_check(
        &self,
    ) -> impl FnMut() -> bool + Send + std::panic::UnwindSafe + std::panic::RefUnwindSafe + 'static
    {
        let state = self.state.clone();
        move || state.load(Ordering::Acquire) & 1 != 0
    }

    pub(crate) fn before(&self, deadline: u64) -> bool {
        self.domain.mono_ms().is_ok_and(|now| now < deadline)
    }

    fn new(domain: crate::asyncrt::DomainToken) -> Self {
        Self {
            state: Arc::new(AtomicU8::new(0)),
            deadline: AtomicU64::new(u64::MAX),
            domain,
            changed: Notify::new(),
        }
    }

    pub fn cancel(&self) -> bool {
        let old = self.state.fetch_or(1, Ordering::AcqRel);
        if old & 1 == 0 {
            self.changed.notify_waiters();
        }
        old & 6 != 0
    }

    pub fn cancelled(&self) -> bool {
        if self.state.load(Ordering::Acquire) & 1 != 0 {
            return true;
        }
        if self
            .domain
            .mono_ms()
            .map_or(true, |now| now >= self.deadline.load(Ordering::Acquire))
        {
            self.cancel();
            return true;
        }
        false
    }

    pub fn submitted(&self) -> bool {
        self.state.load(Ordering::Acquire) & 2 != 0
    }

    /// A verified duplicate receipt is already committed even though this
    /// attempt proposed nothing. Its cancellation cannot imply rollback.
    pub fn observe_committed(&self) {
        self.state.fetch_or(4, Ordering::AcqRel);
    }

    pub fn begin_submission(&self) -> bool {
        if self.cancelled() {
            return false;
        }
        self.state
            .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Only the host's synchronous preflight may revoke this mark, before a
    /// private submit job or runtime proposal has been dispatched.
    pub fn refuse_preflight(&self) {
        self.state.fetch_and(!2, Ordering::AcqRel);
    }
}

struct Waiter(Arc<AttemptControl>);

impl Drop for Waiter {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
