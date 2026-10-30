// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded attachment ownership survives response loss and joins before shutdown.
use super::calls::Delivery;
use super::*;
use crate::raft::budget::Permit;
use crate::raft::cell_groups::{CellGroupBootstrapPermit, PreparedGroupManifest};
use crate::raft::transport::{GroupRoute, PhysicalRegistration};
use cellar_logic::cell_group::GroupHistory;
use tokio::sync::{mpsc, oneshot};

pub(crate) struct GroupRoster {
    groups: Mutex<BTreeMap<GroupHistory, bool>>,
    limit: usize,
}

impl GroupRoster {
    pub(super) fn new(groups: impl Iterator<Item = GroupHistory>, limit: usize) -> Arc<Self> {
        Arc::new(Self {
            groups: Mutex::new(groups.map(|g| (g, true)).collect()),
            limit,
        })
    }

    pub(crate) fn contains(&self, group: &GroupHistory) -> Result<bool, RuntimeError> {
        Ok(self
            .groups
            .lock()
            .map_err(|_| RuntimeError::Closed)?
            .get(group)
            == Some(&true))
    }

    pub(super) fn len(&self) -> usize {
        self.groups
            .lock()
            .map(|g| g.values().filter(|active| **active).count())
            .unwrap_or(0)
    }

    pub(super) fn pending(&self) -> usize {
        self.groups
            .lock()
            .map(|g| g.values().filter(|active| !**active).count())
            .unwrap_or(0)
    }

    fn reserve(self: &Arc<Self>, group: GroupHistory) -> Result<Reservation, RuntimeError> {
        let mut groups = self.groups.lock().map_err(|_| RuntimeError::Closed)?;
        if groups.contains_key(&group) {
            return Err(RuntimeError::AlreadyAttached);
        }
        if groups.len() >= self.limit {
            return Err(RuntimeError::Overloaded);
        }
        groups.insert(group, false);
        Ok(Reservation {
            roster: self.clone(),
            group,
            active: false,
        })
    }
}

struct Reservation {
    roster: Arc<GroupRoster>,
    group: GroupHistory,
    active: bool,
}

impl Reservation {
    fn activate(&mut self) -> Result<(), RuntimeError> {
        let mut groups = self
            .roster
            .groups
            .lock()
            .map_err(|_| RuntimeError::Closed)?;
        let entry = groups.get_mut(&self.group).ok_or(RuntimeError::NotFound)?;
        *entry = true;
        self.active = true;
        Ok(())
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.active {
            if let Ok(mut groups) = self.roster.groups.lock() {
                groups.remove(&self.group);
            }
        }
    }
}

/// The applier must own the exact page store used to verify the selected root.
/// Bootstrap/recovery grants no serving authority; attachment revalidates the
/// catalog and application admission still requires a current-term quorum.
pub struct CellGroupRegistration {
    pub permit: CellGroupBootstrapPermit,
    pub current: PreparedGroupManifest,
    pub applier: CellGroupApplier,
    pub transport: GroupRoute<GroupHistory>,
}

pub struct CellGroupRegistrationError {
    pub error: CellGroupControllerError,
    pub registration: Box<CellGroupRegistration>,
}

impl std::fmt::Debug for CellGroupRegistrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CellGroupRegistrationError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for CellGroupRegistrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.error, f)
    }
}

impl std::error::Error for CellGroupRegistrationError {}

/// Dropping this waiter does not cancel attachment or release its group slot.
pub struct CellGroupRegistrationCall {
    receiver: oneshot::Receiver<Delivery<(), CellGroupControllerError>>,
}

impl CellGroupRegistrationCall {
    pub async fn response(self) -> Result<(), Arc<CellGroupControllerError>> {
        self.receiver
            .await
            .map_err(|_| Arc::new(CellGroupControllerError::Closed))?
            .result
    }
}

pub(super) struct Request {
    pub registration: CellGroupRegistration,
    reservation: Reservation,
    reply: oneshot::Sender<Delivery<(), CellGroupControllerError>>,
    permit: Arc<Permit>,
}

pub(super) struct Completed {
    pub group: GroupHistory,
    result: Result<PhysicalRegistration, CellGroupControllerError>,
    reservation: Reservation,
    reply: oneshot::Sender<Delivery<(), CellGroupControllerError>>,
    permit: Arc<Permit>,
}

impl CellGroupController {
    /// Atomically reserve the controller slot and transfer attachment ownership.
    /// Admission failure returns all prepared owners without starting attachment.
    pub fn register(
        &self,
        mut registration: CellGroupRegistration,
    ) -> Result<CellGroupRegistrationCall, CellGroupRegistrationError> {
        let shared = &self.handle.0;
        let admitted = (|| {
            if shared.closing.load(Ordering::Acquire) {
                return Err(CellGroupControllerError::Closed);
            }
            let route = registration.permit.route();
            if route != registration.current.marker().route()
                || route.history != registration.applier.history()
                || route.history != registration.transport.scope
                || route.schema_digest != registration.transport.schema_digest
                || !Arc::ptr_eq(
                    &registration.current.engine_owner,
                    &shared.runtime.log().owner(),
                )
                || !Arc::ptr_eq(
                    &registration.permit.group().owner,
                    &shared.runtime.log().owner(),
                )
            {
                return Err(CellGroupControllerError::Invalid(
                    "registration identity/engine",
                ));
            }
            registration
                .applier
                .check_startup(&registration.current)
                .map_err(|e| CellGroupControllerError::Apply(Arc::new(e)))?;
            if registration
                .permit
                .group()
                .consensus_claimed
                .load(Ordering::Acquire)
            {
                return Err(RuntimeError::AlreadyAttached.into());
            }
            if registration.transport.peers.is_empty() || registration.transport.peers.len() > 128 {
                return Err(CellGroupControllerError::Invalid(
                    "registration peer bounds",
                ));
            }
            let mut bytes = registration
                .current
                .reserved_bytes()
                .checked_add(16384)
                .ok_or(RuntimeError::Overloaded)?;
            for peer in &mut registration.transport.peers {
                if peer.label.len() > 4096 || peer.server_name.len() > 4096 {
                    return Err(RuntimeError::Overloaded.into());
                }
                bytes = bytes
                    .checked_add(peer.label.len() + peer.server_name.len() + 512)
                    .ok_or(RuntimeError::Overloaded)?;
                peer.label = std::mem::take(&mut peer.label)
                    .into_boxed_str()
                    .into_string();
                peer.server_name = std::mem::take(&mut peer.server_name)
                    .into_boxed_str()
                    .into_string();
            }
            registration.transport.peers = std::mem::take(&mut registration.transport.peers)
                .into_boxed_slice()
                .into_vec();
            let permit = shared
                .commands
                .acquire(bytes)
                .ok_or(RuntimeError::Overloaded)?;
            let reservation = shared.groups.reserve(route.history)?;
            Ok((permit, reservation))
        })();
        let (permit, reservation) = match admitted {
            Ok(pair) => pair,
            Err(error) => {
                return Err(CellGroupRegistrationError {
                    error,
                    registration: Box::new(registration),
                })
            }
        };
        let (reply, receiver) = oneshot::channel();
        shared
            .requests
            .try_send(requests::Request::Registration(Box::new(Request {
                registration,
                reservation,
                reply,
                permit: Arc::new(permit),
            })))
            .map_err(|error| {
                let (request, error) = match error {
                    mpsc::error::TrySendError::Closed(request) => {
                        (request, CellGroupControllerError::Closed)
                    }
                    mpsc::error::TrySendError::Full(request) => {
                        (request, RuntimeError::Overloaded.into())
                    }
                };
                let requests::Request::Registration(request) = request else {
                    unreachable!("registration envelope")
                };
                CellGroupRegistrationError {
                    error,
                    registration: Box::new(request.registration),
                }
            })?;
        Ok(CellGroupRegistrationCall { receiver })
    }
}

pub(super) async fn drive(
    request: Request,
    runtime: CellGroupRuntime,
    transport: RaftTransport,
) -> Completed {
    let Request {
        registration,
        reservation,
        reply,
        permit,
    } = request;
    let group = registration.permit.route().history;
    let result = async {
        let guard = transport
            .register_physical_group(registration.transport)
            .map_err(CellGroupControllerError::Transport)?;
        runtime
            .attach(registration.permit, registration.current)
            .await?;
        Ok(guard)
    }
    .await;
    Completed {
        group,
        result,
        reservation,
        reply,
        permit,
    }
}

impl Completed {
    /// A runtime-wide close or competing external owner is not a safely failed
    /// registration. Preserve the conservative joined node shutdown boundary.
    pub fn uncertain(&self) -> bool {
        matches!(
            &self.result,
            Err(CellGroupControllerError::Runtime(
                RuntimeError::Closed | RuntimeError::AlreadyAttached
            ))
        )
    }

    pub fn finish(
        self,
        closing: bool,
    ) -> Result<Option<PhysicalRegistration>, CellGroupControllerError> {
        let Self {
            result,
            mut reservation,
            reply,
            permit,
            ..
        } = self;
        let outcome = if closing {
            Err(CellGroupControllerError::Closed)
        } else {
            result.and_then(|guard| {
                reservation.activate()?;
                Ok(guard)
            })
        };
        // A failed attachment's group capacity is free before publishing its
        // refusal; success keeps the active roster entry after this guard drops.
        drop(reservation);
        let _ = reply.send(Delivery {
            result: outcome
                .as_ref()
                .map(|_| ())
                .map_err(|e| Arc::new(e.clone())),
            _permit: permit,
        });
        match outcome {
            Ok(guard) => Ok(Some(guard)),
            Err(_) if closing => Ok(None),
            Err(error) => Err(error),
        }
    }
}
