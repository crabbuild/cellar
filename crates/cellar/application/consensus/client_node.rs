//! Node controls share bounded administrative admission without borrowing a
//! cell identity. Empty hosts never manufacture a business-cell binding.
use super::*;

pub(crate) enum NodeKind {
    StartMovementService(Box<crate::raft::fleet_catalog::MovementServiceWork>),
    PeerMovement(Box<super::PeerMovementCall>),
    LocalMovement(Box<super::LocalMovementCall>),
    Drain,
    Observe,
    ObservePhysical,
    InspectPeer {
        node: cellar_logic::consensus::NodeId,
        minimum_revision: u64,
        physical: bool,
    },
    InspectPeerFleet(Box<PeerFleetRequest>),
    PeerFleetStop(Box<PeerStopRequest>),
    ObserveFleet {
        history: Box<crate::raft::fleet_inspection_wire::History>,
        minimum_revision: u64,
    },
    RegisterHistory(Box<crate::raft::fleet_catalog::FleetGroupAuthorization>),
    ControllerStatus,
    SetControllerEnabled(bool),
}

pub(crate) struct PeerFleetRequest {
    pub node: cellar_logic::consensus::NodeId,
    pub minimum_revision: u64,
    pub authorization: crate::raft::fleet_catalog::FleetGroupAuthorization,
}

pub(crate) struct PeerStopRequest {
    pub request: crate::raft::fleet_stop::FleetStopRequest,
    pub minimum_revision: u64,
    pub deadline: u64,
}

pub(crate) enum ControlRequest {
    Cell(Box<Request>),
    Node(Box<NodeRequest>),
}

pub(crate) struct NodeRequest {
    pub started: u64,
    pub kind: NodeKind,
    pub reply: Option<ResponseSender>,
    pub control: Arc<AttemptControl>,
    pub _permit: Arc<Permit>,
    pub _work: Arc<Work>,
}

impl RaftCells {
    /// Submit an exact-boot stop/fence through the authenticated Raft control
    /// lane. The origin and target may both have zero business groups; no Actor
    /// lease is used. Acceptance is historical guard admission, never physical
    /// closure or terminal removal. A lost response is unconfirmed: reconcile
    /// the same request and its signed closure instead of issuing a new scope.
    pub fn request_peer_fleet_stop(
        &self,
        request: crate::raft::fleet_stop::FleetStopRequest,
        minimum_revision: u64,
        timeout: Duration,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        crate::raft::fleet_stop::FleetStopAcceptance,
                        crate::raft::fleet_catalog::FleetCatalogError,
                    >,
                > + Send,
        >,
    > {
        use crate::raft::fleet_catalog::FleetCatalogError;
        if request.selector.validate().is_err()
            || minimum_revision == 0
            || minimum_revision >= u64::MAX - 2
            || timeout.as_millis() == 0
            || timeout > Duration::from_secs(60)
        {
            return Box::pin(async { Err(FleetCatalogError::Invalid("peer stop request")) });
        }
        let domain = self.handle.domain.clone();
        let deadline = match domain
            .mono_ms()
            .ok()
            .and_then(|now| now.checked_add(timeout.as_millis() as u64))
        {
            Some(deadline) => deadline,
            None => return Box::pin(async { Err(FleetCatalogError::Closed) }),
        };
        let call = self.node_call(
            NodeKind::PeerFleetStop(Box::new(PeerStopRequest {
                request,
                minimum_revision,
                deadline,
            })),
            timeout,
        );
        Box::pin(async move {
            let result = call.await.map_err(|error| match error {
                CommandFailure::Overloaded => FleetCatalogError::Overloaded,
                CommandFailure::UnsupportedCapability => {
                    FleetCatalogError::Invalid("peer stop capability")
                }
                _ => FleetCatalogError::Unconfirmed,
            })?;
            if !domain.mono_ms().is_ok_and(|now| now < deadline) {
                return Err(FleetCatalogError::Unconfirmed);
            }
            match result {
                Response::PeerFleetStop(result) => result.map(|accepted| *accepted),
                _ => Err(FleetCatalogError::Unconfirmed),
            }
        })
    }

    /// Obtain a fresh safe-ReadIndex observation from an enrolled group. A local
    /// target uses the same guarded quorum observer without a loopback RPC.
    /// The origin may have zero local groups. Both endpoints must install the
    /// requested catalog cut; the target independently authorizes this exact
    /// history. This creates no query, seeding or membership-mutation grant.
    /// Retaining a response does not establish current absence or removal proof.
    pub fn observe_peer_fleet_membership(
        &self,
        node: cellar_logic::consensus::NodeId,
        authorization: crate::raft::fleet_catalog::FleetGroupAuthorization,
        minimum_revision: u64,
        timeout: Duration,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        super::super::RaftPeerFleetMembershipObservation,
                        super::super::InspectionError,
                    >,
                > + Send,
        >,
    > {
        use super::super::InspectionError;
        if minimum_revision == 0
            || minimum_revision >= u64::MAX - 2
            || timeout.is_zero()
            || timeout > Duration::from_secs(60)
        {
            return Box::pin(async { Err(InspectionError::Refused) });
        }
        let domain = self.handle.domain.clone();
        let started = match domain.mono_ms() {
            Ok(started) => started,
            Err(_) => return Box::pin(async { Err(InspectionError::Unavailable) }),
        };
        let call = self.node_call(
            NodeKind::InspectPeerFleet(Box::new(PeerFleetRequest {
                node,
                minimum_revision,
                authorization,
            })),
            timeout,
        );
        Box::pin(async move {
            let response = call.await.map_err(|error| match error {
                CommandFailure::Overloaded => InspectionError::Overloaded,
                CommandFailure::UnsupportedCapability => InspectionError::Refused,
                _ => InspectionError::Unavailable,
            })?;
            let Response::PeerFleetObservation(result) = response else {
                return Err(InspectionError::Unavailable);
            };
            let mut observed = result?;
            let age = domain
                .mono_ms()
                .ok()
                .and_then(|now| now.checked_sub(started))
                .ok_or(InspectionError::Unavailable)?;
            observed.record_delivery_age(age)?;
            Ok(*observed)
        })
    }

    pub(crate) fn observe_local_fleet_membership(
        &self,
        history: crate::raft::fleet_inspection_wire::History,
        minimum_revision: u64,
        timeout: Duration,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Box<crate::raft::fleet_inspection_wire::LocalObservation>,
                        CommandFailure,
                    >,
                > + Send,
        >,
    > {
        let call = self.node_call(
            NodeKind::ObserveFleet {
                history: Box::new(history),
                minimum_revision,
            },
            timeout,
        );
        Box::pin(async move {
            match call.await? {
                Response::LocalFleetObservation(result) => Ok(result),
                _ => Err(CommandFailure::CorruptState),
            }
        })
    }

    /// Inspect an enrolled remote process without a cell. Both endpoints must
    /// acknowledge at least this trust revision; stale or missing metadata is
    /// not evidence of node failure. No placement or membership is activated.
    pub fn inspect_peer_node(
        &self,
        node: cellar_logic::consensus::NodeId,
        minimum_revision: u64,
        timeout: Duration,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        super::super::RaftPeerNodeObservation,
                        super::super::InspectionError,
                    >,
                > + Send,
        >,
    > {
        self.inspect_peer_node_mode(node, minimum_revision, timeout, false)
    }

    /// Obtain complete physical engine occupancy over authenticated node
    /// control, including unregistered and retired histories. No business cell
    /// or Raft quorum is required. A local target uses the same bounded host
    /// control path and trust checks. The response retains no health/absence lease.
    pub fn inspect_peer_physical_inventory(
        &self,
        node: cellar_logic::consensus::NodeId,
        minimum_revision: u64,
        timeout: Duration,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        super::super::RaftPeerNodeObservation,
                        super::super::InspectionError,
                    >,
                > + Send,
        >,
    > {
        self.inspect_peer_node_mode(node, minimum_revision, timeout, true)
    }

    fn inspect_peer_node_mode(
        &self,
        node: cellar_logic::consensus::NodeId,
        minimum_revision: u64,
        timeout: Duration,
        physical: bool,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        super::super::RaftPeerNodeObservation,
                        super::super::InspectionError,
                    >,
                > + Send,
        >,
    > {
        use super::super::InspectionError;
        if minimum_revision == 0
            || minimum_revision >= u64::MAX - 2
            || timeout.is_zero()
            || timeout > Duration::from_secs(60)
        {
            return Box::pin(async { Err(InspectionError::Refused) });
        }
        let domain = self.handle.domain.clone();
        let started = match domain.mono_ms() {
            Ok(started) => started,
            Err(_) => return Box::pin(async { Err(InspectionError::Unavailable) }),
        };
        let response = self.node_call(
            NodeKind::InspectPeer {
                node,
                minimum_revision,
                physical,
            },
            timeout,
        );
        Box::pin(async move {
            match response.await.map_err(|error| match error {
                CommandFailure::Overloaded => InspectionError::Overloaded,
                CommandFailure::UnsupportedCapability => InspectionError::Refused,
                _ => InspectionError::Unavailable,
            })? {
                Response::PeerNodeInspection(result) => {
                    let mut observed = result?;
                    let age = domain
                        .mono_ms()
                        .ok()
                        .and_then(|now| now.checked_sub(started))
                        .ok_or(InspectionError::Unavailable)?;
                    observed.sample_age_ms = observed.sample_age_ms.max(age);
                    Ok(*observed)
                }
                _ => Err(InspectionError::Unavailable),
            }
        })
    }

    pub(crate) fn observe_physical_node(
        &self,
        timeout: Duration,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        crate::raft::physical_inventory::PhysicalNodeObservation,
                        CommandFailure,
                    >,
                > + Send,
        >,
    > {
        let response = self.node_call(NodeKind::ObservePhysical, timeout);
        Box::pin(async move {
            match response.await? {
                Response::PhysicalNode(observation) => Ok(*observation),
                _ => Err(CommandFailure::CorruptState),
            }
        })
    }

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

    /// Point-in-time local physical inventory with startup and installed current
    /// enrollment. This grants no capacity activation, quorum or removal proof.
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
                    started: now,
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
