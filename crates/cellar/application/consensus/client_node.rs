//! Node controls share bounded administrative admission without borrowing a
//! cell identity. Empty hosts never manufacture a business-cell binding.
use super::*;

pub(crate) enum NodeKind {
    Drain,
    Observe,
    RegisterHistory(Box<crate::raft::fleet_catalog::FleetGroupAuthorization>),
    ControllerStatus,
    SetControllerEnabled(bool),
}

pub(crate) enum ControlRequest {
    Cell(Box<Request>),
    Node(Box<NodeRequest>),
}

pub(crate) struct NodeRequest {
    pub kind: NodeKind,
    pub reply: Option<ResponseSender>,
    pub control: Arc<AttemptControl>,
    pub _permit: Arc<Permit>,
    pub _work: Arc<Work>,
}

impl RaftCells {
    /// Register dormant metadata for an already enrolled history under this
    /// host's declared class. This creates no member, image or voting authority.
    /// A lost response can race publication; retry the same authorization.
    pub fn register_managed_history(
        &self,
        authorization: crate::raft::fleet_catalog::FleetGroupAuthorization,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<RaftCellHandle, CommandFailure>> + Send>> {
        if timeout > Duration::from_secs(60) {
            return Box::pin(async { Err(CommandFailure::UnsupportedCapability) });
        }
        let scope = authorization
            .binding()
            .scope()
            .map_err(|_| CommandFailure::CommandConflict);
        let response = self.node_call(NodeKind::RegisterHistory(Box::new(authorization)), timeout);
        let host = self.clone();
        Box::pin(async move {
            let scope = scope?;
            match response.await? {
                Response::HistoryRegistered => {
                    let cell = host.cell(scope.namespace, &scope.cell)?;
                    if cell.binding.scope != scope {
                        return Err(CommandFailure::CommandConflict);
                    }
                    Ok(cell)
                }
                _ => Err(CommandFailure::CorruptState),
            }
        })
    }

    /// Point-in-time local physical inventory. Enrollment is the startup cut,
    /// explicitly not a fresh capacity-ready, quorum or removal certificate.
    pub fn observe_node(
        &self,
        timeout: Duration,
    ) -> Pin<
        Box<dyn Future<Output = Result<super::super::RaftHostObservation, CommandFailure>> + Send>,
    > {
        let response = self.node_call(NodeKind::Observe, timeout);
        Box::pin(async move {
            match response.await? {
                Response::Node(observation) => Ok(*observation),
                _ => Err(CommandFailure::CorruptState),
            }
        })
    }

    pub(super) fn node_call(
        &self,
        kind: NodeKind,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Response, CommandFailure>> + Send>> {
        let control = Arc::new(AttemptControl::new(self.handle.domain.clone()));
        let guard = Waiter(control.clone());
        let accepted = (|| {
            if super::super::class::callback_active() {
                return Err(CommandFailure::UnsupportedCapability);
            }
            let draining = matches!(&kind, NodeKind::Drain);
            if self.handle.closed.load(Ordering::Acquire)
                || self.handle.closing.load(Ordering::Acquire)
                || self.handle.admission.is_draining() && !draining
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
            let deadline = now
                .checked_add(timeout.as_millis() as u64)
                .ok_or(CommandFailure::Unavailable)?;
            control.deadline.store(deadline, Ordering::Release);
            let timer = self
                .handle
                .domain
                .sleep_until(deadline)
                .map_err(|_| CommandFailure::Unavailable)?;
            let _registration = self.handle.admission.registration(draining)?;
            if self.handle.closed.load(Ordering::Acquire)
                || self.handle.closing.load(Ordering::Acquire)
            {
                return Err(CommandFailure::Unavailable);
            }
            let permit = Arc::new(
                self.handle
                    .control_budget
                    .acquire(4096)
                    .ok_or(CommandFailure::Overloaded)?,
            );
            let work = Arc::new(Work {
                _guard: self.handle.work.enter(),
            });
            let (reply, receiver) = oneshot::channel();
            self.handle
                .control_requests
                .try_send(ControlRequest::Node(Box::new(NodeRequest {
                    kind,
                    reply: Some(reply),
                    control: control.clone(),
                    _permit: permit.clone(),
                    _work: work,
                })))
                .map_err(send_failure)?;
            Ok((receiver, timer, permit))
        })();
        Box::pin(async move {
            let _guard = guard;
            let (receiver, timer, _permit) = accepted?;
            crate::asyncrt::select! {
                result=receiver=>result.unwrap_or_else(|_|Err(CancelFailure::Status.cancel(&control))).map(|reply|reply.response),
                _=timer=>Err(CancelFailure::Status.cancel(&control)),
            }
        })
    }
}

pub(super) fn send_failure<T>(error: mpsc::error::TrySendError<T>) -> CommandFailure {
    match error {
        mpsc::error::TrySendError::Full(_) => CommandFailure::Overloaded,
        mpsc::error::TrySendError::Closed(_) => CommandFailure::Unavailable,
    }
}
