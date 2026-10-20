//! Bounded waiters for a node-owned Raft dispatcher. Dropping a waiter never
//! drops its submitted producer or grants another writer a private image.

use super::{
    CommandFailure, CommandId, CommittedVersion, Durability, GroupScope, OperationContract,
    OperationKind, RaftClass, VersionToken,
};
use crate::raft::budget::{Budget, Permit};
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

#[derive(Clone, Debug)]
pub struct QueryOptions {
    pub consistency: QueryConsistency,
    pub timeout: Duration,
    pub stale_policy: StaleReadPolicy,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            consistency: QueryConsistency::Stale,
            timeout: Duration::from_secs(5),
            stale_policy: StaleReadPolicy::RecentSource,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryResult {
    pub result: Vec<u8>,
    pub version: CommittedVersion,
    pub version_token: VersionToken,
}

/// Discovery/readiness observation, never a grant of mutation authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellStatus {
    pub leader: Option<MemberId>,
    pub term: Term,
    pub command_ready: bool,
    pub version: Option<CommittedVersion>,
    /// Coherent runtime observations for diagnosis. A persisted suffix can be
    /// uncommitted; these fields are never an SDK acknowledgement proof.
    pub persisted: LogIndex,
    pub committed: LogIndex,
    pub applied: LogPosition,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnfinishedCommand {
    pub scope: GroupScope,
    pub id: CommandId,
}

/// Local ownership was joined. Unfinished commands remain ambiguous; this
/// report is neither S3 durability nor permission to discard replica disks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftCloseReport {
    pub unfinished: Vec<UnfinishedCommand>,
}

#[derive(Clone)]
pub(crate) struct Binding {
    pub scope: GroupScope,
    pub class: Arc<RaftClass>,
    pub minimum: Durability,
}

pub(crate) struct Terminal {
    pub result: Mutex<Option<Result<RaftCloseReport, CommandFailure>>>,
    pub changed: Notify,
}

pub(crate) struct Handle {
    pub requests: mpsc::Sender<Request>,
    pub close: mpsc::UnboundedSender<()>,
    pub domain: crate::asyncrt::DomainToken,
    pub closed: Arc<AtomicBool>,
    pub closing: Arc<AtomicBool>,
    pub terminal: Arc<Terminal>,
    pub budget: Arc<Budget>,
    pub max_input_bytes: usize,
    pub bindings: BTreeMap<(NamespaceId, String), Binding>,
    pub work: Arc<crate::host_services::ApplicationWork>,
}

/// Registered Raft cells owned by the node's execution domain. Handles can
/// outlive their caller, while node shutdown closes admission and joins work.
#[derive(Clone)]
pub struct RaftCells {
    pub(crate) handle: Arc<Handle>,
}

impl RaftCells {
    pub fn cell(
        &self,
        namespace: NamespaceId,
        cell: &str,
    ) -> Result<RaftCellHandle, CommandFailure> {
        if self.is_closed() || !cellar_logic::cell::valid_cell_scope(cell) {
            return Err(CommandFailure::Unavailable);
        }
        let binding = self
            .handle
            .bindings
            .get(&(namespace, cell.into()))
            .cloned()
            .ok_or(CommandFailure::Unavailable)?;
        Ok(RaftCellHandle {
            handle: self.handle.clone(),
            binding,
        })
    }

    pub fn is_closed(&self) -> bool {
        self.handle.closed.load(Ordering::Acquire)
            || self.handle.closing.load(Ordering::Acquire)
            || self.handle.domain.mono_ms().is_err()
    }

    pub async fn close(&self) -> Result<RaftCloseReport, CommandFailure> {
        if super::class::callback_active() {
            return Err(CommandFailure::UnsupportedCapability);
        }
        if !self.handle.closing.swap(true, Ordering::AcqRel) {
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
        options: CommandOptions,
    ) -> impl Future<Output = Result<CommandReceipt, CommandFailure>> + Send + 'static {
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
                Kind::Command {
                    contract,
                    input,
                    options,
                } => {
                    self.validate(contract, OperationKind::Command, input)?;
                    // Archive integration is supplied by MR-09. Refuse before
                    // any private work rather than weaken a requested proof.
                    if options.durability.max(self.binding.minimum) == Durability::S3 {
                        return Err(CommandFailure::UnsupportedCapability);
                    }
                }
                Kind::Query {
                    contract, input, ..
                } => {
                    self.validate(contract, OperationKind::Query, input)?;
                }
                Kind::Status => (),
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
            let permit = Arc::new(
                self.handle
                    .budget
                    .acquire(kind.bytes())
                    .ok_or(CommandFailure::Overloaded)?,
            );
            let work = Arc::new(Work {
                _guard: self.handle.work.enter(),
            });
            let (reply, receiver) = oneshot::channel();
            self.handle
                .requests
                .try_send(Request {
                    scope: self.binding.scope.clone(),
                    kind,
                    reply: Some(reply),
                    control: control.clone(),
                    _permit: permit.clone(),
                    work,
                })
                .map_err(|e| match e {
                    mpsc::error::TrySendError::Full(_) => CommandFailure::Overloaded,
                    mpsc::error::TrySendError::Closed(_) => CommandFailure::Unavailable,
                })?;
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
    Command {
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
    fn bytes(&self) -> usize {
        4096 + match self {
            Self::Command { input, .. } | Self::Query { input, .. } => input.len(),
            Self::Status => 0,
        }
    }

    fn failure(&self) -> CancelFailure {
        match self {
            Self::Command { options, .. } => CancelFailure::Command(options.id),
            Self::Query { .. } => CancelFailure::Query,
            Self::Status => CancelFailure::Status,
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

pub(crate) struct Request {
    pub scope: GroupScope,
    pub kind: Kind,
    pub reply: Option<oneshot::Sender<Result<ChargedResponse, CommandFailure>>>,
    pub control: Arc<AttemptControl>,
    pub _permit: Arc<Permit>,
    pub work: Arc<Work>,
}

pub(crate) struct AttemptControl {
    state: Arc<AtomicU8>,
    deadline: AtomicU64,
    domain: crate::asyncrt::DomainToken,
}

impl AttemptControl {
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
        }
    }

    pub fn cancel(&self) -> bool {
        self.state.fetch_or(1, Ordering::AcqRel) & 2 != 0
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
