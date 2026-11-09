// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded attachment ownership survives response loss and joins before shutdown.
use super::calls::Delivery;
use super::*;
use crate::raft::budget::Permit;
use crate::raft::cell_groups::{CellGroupBootstrapPermit, PreparedGroupManifest};
use crate::raft::transport::{GroupRoute, PhysicalRegistration};
use cellar_logic::cell_group::GroupHistory;
use tokio::sync::{mpsc, oneshot};

#[derive(Clone)]
enum GroupState {
    Pending,
    Active,
    Retiring,
    ParkedRetired,
    ParkedRemoved,
    Quarantined(CellGroupControllerError),
}

pub(crate) struct GroupRoster {
    groups: Mutex<RegisteredGroups>,
    limit: usize,
}

struct RegisteredGroups {
    entries: BTreeMap<GroupHistory, GroupState>,
    inventory: super::registration_inventory::Inventory,
}

impl GroupRoster {
    fn partition_revision(
        &self,
        scopes: &[(
            cellar_logic::consensus::ClusterId,
            cellar_logic::consensus::NamespaceId,
        )],
    ) -> Result<[u8; 32], RuntimeError> {
        self.groups
            .lock()
            .map_err(|_| RuntimeError::Closed)?
            .inventory
            .revision(scopes)
    }

    fn partition_page(
        &self,
        scopes: &[(
            cellar_logic::consensus::ClusterId,
            cellar_logic::consensus::NamespaceId,
        )],
        revision: Option<[u8; 32]>,
        after: Option<GroupHistory>,
        capacity: usize,
        limit: usize,
    ) -> Result<super::PartitionGroupPage, RuntimeError> {
        let groups = self.groups.lock().map_err(|_| RuntimeError::Closed)?;
        groups
            .inventory
            .page(scopes, revision, after, capacity, limit)
    }

    pub(super) fn producers_quiesced(&self) -> bool {
        self.groups.lock().is_ok_and(|groups| {
            groups.entries.values().all(|state| {
                matches!(
                    state,
                    GroupState::Active | GroupState::ParkedRetired | GroupState::ParkedRemoved
                )
            })
        })
    }

    pub(super) fn new(groups: impl Iterator<Item = GroupHistory>, limit: usize) -> Arc<Self> {
        let entries: BTreeMap<_, _> = groups.map(|g| (g, GroupState::Active)).collect();
        let inventory = super::registration_inventory::Inventory::new(entries.keys().copied());
        Arc::new(Self {
            groups: Mutex::new(RegisteredGroups { entries, inventory }),
            limit,
        })
    }

    pub(crate) fn contains(&self, group: &GroupHistory) -> Result<bool, RuntimeError> {
        match self
            .groups
            .lock()
            .map_err(|_| RuntimeError::Closed)?
            .entries
            .get(group)
        {
            Some(GroupState::Active) => Ok(true),
            Some(GroupState::Quarantined(_)) => Err(RuntimeError::GroupQuarantined),
            _ => Ok(false),
        }
    }

    pub(super) fn quarantine(
        &self,
        group: GroupHistory,
        error: CellGroupControllerError,
    ) -> Result<(), RuntimeError> {
        let mut groups = self.groups.lock().map_err(|_| RuntimeError::Closed)?;
        let state = groups
            .entries
            .get_mut(&group)
            .ok_or(RuntimeError::NotFound)?;
        if matches!(state, GroupState::ParkedRetired | GroupState::ParkedRemoved) {
            return Err(RuntimeError::NotFound);
        }
        if !matches!(state, GroupState::Quarantined(_)) {
            *state = GroupState::Quarantined(error);
            groups.inventory.remove(&group);
        }
        Ok(())
    }

    pub(super) fn failure(
        &self,
        group: &GroupHistory,
    ) -> Result<Option<CellGroupControllerError>, RuntimeError> {
        Ok(
            match self
                .groups
                .lock()
                .map_err(|_| RuntimeError::Closed)?
                .entries
                .get(group)
            {
                Some(GroupState::Quarantined(error)) => Some(error.clone()),
                _ => None,
            },
        )
    }

    pub(super) fn len(&self) -> usize {
        self.groups
            .lock()
            .map(|g| {
                g.entries
                    .values()
                    .filter(|s| matches!(s, GroupState::Active | GroupState::Retiring))
                    .count()
            })
            .unwrap_or(0)
    }

    pub(super) fn pending(&self) -> usize {
        self.groups
            .lock()
            .map(|g| {
                g.entries
                    .values()
                    .filter(|s| matches!(s, GroupState::Pending))
                    .count()
            })
            .unwrap_or(0)
    }

    pub(super) fn quarantined(&self) -> usize {
        self.groups
            .lock()
            .map(|g| {
                g.entries
                    .values()
                    .filter(|s| matches!(s, GroupState::Quarantined(_)))
                    .count()
            })
            .unwrap_or(0)
    }

    pub(super) fn begin_retirement(&self, group: GroupHistory) -> Result<bool, RuntimeError> {
        let mut groups = self.groups.lock().map_err(|_| RuntimeError::Closed)?;
        match groups.entries.get_mut(&group) {
            Some(state @ (GroupState::Active | GroupState::Retiring)) => {
                *state = GroupState::Retiring;
                groups.inventory.remove(&group);
                Ok(false)
            }
            Some(GroupState::ParkedRetired | GroupState::ParkedRemoved) => Ok(true),
            Some(GroupState::Quarantined(_)) => Err(RuntimeError::GroupQuarantined),
            _ => Err(RuntimeError::NotReady),
        }
    }

    pub(super) fn finish_retirement(&self, group: GroupHistory) -> Result<(), RuntimeError> {
        let mut groups = self.groups.lock().map_err(|_| RuntimeError::Closed)?;
        match groups.entries.get_mut(&group) {
            Some(state @ GroupState::Retiring) => {
                *state = GroupState::ParkedRetired;
                Ok(())
            }
            _ => Err(RuntimeError::NotReady),
        }
    }

    pub(super) fn require_reclaimable(
        &self,
        group: &GroupHistory,
        tombstoned: bool,
    ) -> Result<(), RuntimeError> {
        let groups = self.groups.lock().map_err(|_| RuntimeError::Closed)?;
        match groups.entries.get(group) {
            Some(GroupState::ParkedRetired | GroupState::ParkedRemoved) => Ok(()),
            None if tombstoned => Ok(()),
            _ => Err(RuntimeError::NotReady),
        }
    }

    pub(super) fn finish_reclamation(&self, group: &GroupHistory) -> Result<(), RuntimeError> {
        let mut groups = self.groups.lock().map_err(|_| RuntimeError::Closed)?;
        match groups.entries.get(group) {
            Some(GroupState::ParkedRetired | GroupState::ParkedRemoved) | None => {
                groups.entries.remove(group);
                Ok(())
            }
            _ => Err(RuntimeError::NotReady),
        }
    }

    pub(super) fn reclaimed(&self, group: &GroupHistory) -> Result<bool, RuntimeError> {
        Ok(!self
            .groups
            .lock()
            .map_err(|_| RuntimeError::Closed)?
            .entries
            .contains_key(group))
    }

    pub(super) fn retired(&self, group: &GroupHistory) -> bool {
        self.groups.lock().is_ok_and(|groups| {
            matches!(groups.entries.get(group), Some(GroupState::ParkedRetired))
        })
    }

    pub(super) fn reserve(
        self: &Arc<Self>,
        group: GroupHistory,
    ) -> Result<Reservation, RuntimeError> {
        let mut groups = self.groups.lock().map_err(|_| RuntimeError::Closed)?;
        if groups.entries.contains_key(&group) {
            return Err(RuntimeError::AlreadyAttached);
        }
        if groups.entries.len() >= self.limit {
            return Err(RuntimeError::Overloaded);
        }
        groups.entries.insert(group, GroupState::Pending);
        Ok(Reservation {
            roster: self.clone(),
            group,
            active: false,
        })
    }
}

pub(super) struct Reservation {
    roster: Arc<GroupRoster>,
    group: GroupHistory,
    active: bool,
}

impl Reservation {
    pub(super) fn activate(&mut self) -> Result<(), RuntimeError> {
        let mut groups = self
            .roster
            .groups
            .lock()
            .map_err(|_| RuntimeError::Closed)?;
        let entry = groups
            .entries
            .get_mut(&self.group)
            .ok_or(RuntimeError::NotFound)?;
        if !matches!(entry, GroupState::Pending) {
            return Err(RuntimeError::GroupQuarantined);
        }
        *entry = GroupState::Active;
        groups.inventory.insert(self.group);
        self.active = true;
        Ok(())
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.active {
            if let Ok(mut groups) = self.roster.groups.lock() {
                if matches!(groups.entries.get(&self.group), Some(GroupState::Pending)) {
                    groups.entries.remove(&self.group);
                }
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

#[derive(PartialEq, Eq)]
pub(crate) struct GroupRegistrationInventory {
    pub(crate) attached: Vec<GroupHistory>,
    pub(crate) pending: usize,
    pub(crate) quarantined: usize,
    pub(crate) retired_sources: Vec<GroupHistory>,
}

impl CellGroupController {
    pub(crate) fn partition_revision(
        &self,
        scopes: &[(
            cellar_logic::consensus::ClusterId,
            cellar_logic::consensus::NamespaceId,
        )],
    ) -> Result<[u8; 32], RuntimeError> {
        self.handle.0.groups.partition_revision(scopes)
    }

    /// A page from one exact active roster generation. The complete scoped
    /// count must fit capacity before even its first page may be returned.
    pub(crate) fn partition_page(
        &self,
        scopes: &[(
            cellar_logic::consensus::ClusterId,
            cellar_logic::consensus::NamespaceId,
        )],
        revision: Option<[u8; 32]>,
        after: Option<GroupHistory>,
        capacity: usize,
        limit: usize,
    ) -> Result<super::PartitionGroupPage, RuntimeError> {
        self.handle
            .0
            .groups
            .partition_page(scopes, revision, after, capacity, limit)
    }

    pub(crate) fn retain_parked_retired_sources(
        &self,
        histories: Vec<GroupHistory>,
    ) -> Result<(), RuntimeError> {
        self.retain_parked(histories, GroupState::ParkedRetired)
    }

    /// Removed members own retained images but have no voting or route owner.
    /// Their storage remains capacity debt until authorized physical collection.
    pub(crate) fn retain_parked_removed_members(
        &self,
        histories: Vec<GroupHistory>,
    ) -> Result<(), RuntimeError> {
        self.retain_parked(histories, GroupState::ParkedRemoved)
    }

    fn retain_parked(
        &self,
        histories: Vec<GroupHistory>,
        state: GroupState,
    ) -> Result<(), RuntimeError> {
        let roster = &self.handle.0.groups;
        let mut groups = roster.groups.lock().map_err(|_| RuntimeError::Closed)?;
        if groups.entries.len().saturating_add(histories.len()) > roster.limit {
            return Err(RuntimeError::Overloaded);
        }
        if histories.windows(2).any(|pair| pair[0] >= pair[1])
            || histories
                .iter()
                .any(|history| groups.entries.contains_key(history))
        {
            return Err(RuntimeError::AlreadyAttached);
        }
        groups.entries.extend(
            histories
                .into_iter()
                .map(|history| (history, state.clone())),
        );
        Ok(())
    }

    /// One locked roster cut. Failed attachments remain registered physical
    /// debt; pending slots are separate from attached histories.
    pub(crate) fn registration_inventory(
        &self,
    ) -> Result<GroupRegistrationInventory, RuntimeError> {
        let groups = self
            .handle
            .0
            .groups
            .groups
            .lock()
            .map_err(|_| RuntimeError::Closed)?;
        let mut inventory = GroupRegistrationInventory {
            attached: Vec::new(),
            pending: 0,
            quarantined: 0,
            retired_sources: Vec::new(),
        };
        for (history, state) in groups.entries.iter() {
            match state {
                GroupState::Pending => inventory.pending += 1,
                GroupState::Active | GroupState::Retiring => inventory.attached.push(*history),
                GroupState::ParkedRetired => inventory.retired_sources.push(*history),
                // Engine inventory counts this retained physical debt. It is
                // neither an attached owner nor a retired whole-group source.
                GroupState::ParkedRemoved => (),
                GroupState::Quarantined(_) => {
                    inventory.attached.push(*history);
                    inventory.quarantined += 1;
                }
            }
        }
        Ok(inventory)
    }

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
    let result = attach(registration, runtime, transport).await;
    Completed {
        group,
        result,
        reservation,
        reply,
        permit,
    }
}

pub(super) async fn attach(
    registration: CellGroupRegistration,
    runtime: CellGroupRuntime,
    transport: RaftTransport,
) -> Result<PhysicalRegistration, CellGroupControllerError> {
    registration
        .applier
        .check_startup(&registration.current)
        .map_err(|e| CellGroupControllerError::Apply(Arc::new(e)))?;
    let mut guard = transport
        .register_physical_group(registration.transport)
        .map_err(CellGroupControllerError::Transport)?;
    guard.checkpoint = Some(
        transport
            .register_group_checkpoint(registration.applier)
            .map_err(CellGroupControllerError::Transport)?,
    );
    runtime
        .attach(registration.permit, registration.current)
        .await?;
    Ok(guard)
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

#[cfg(test)]
mod partition_inventory_tests {
    use super::*;
    use cellar_logic::consensus::{ClusterId, GroupId, IncarnationId, NamespaceId};

    fn history(id: u8, namespace: u8) -> GroupHistory {
        GroupHistory {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([namespace; 16]).unwrap(),
            group: GroupId::new([id; 16]).unwrap(),
            incarnation: IncarnationId::new([id; 16]).unwrap(),
        }
    }

    fn complete(
        roster: &GroupRoster,
        scopes: &[(ClusterId, NamespaceId)],
        capacity: usize,
    ) -> Result<Vec<GroupHistory>, RuntimeError> {
        let page = roster.partition_page(scopes, None, None, capacity, capacity.min(16))?;
        assert!(page.next.is_none());
        Ok(page.groups)
    }

    #[test]
    fn sizing_inventory_is_complete_scoped_and_excludes_unavailable_owners() {
        let a = history(1, 2);
        let b = history(2, 2);
        let foreign = history(3, 3);
        let retiring = history(4, 2);
        let parked = history(5, 2);
        let roster = GroupRoster::new([a, b, foreign, retiring, parked].into_iter(), 8);
        roster.begin_retirement(retiring).unwrap();
        roster.begin_retirement(parked).unwrap();
        roster.finish_retirement(parked).unwrap();
        let mut pending = roster.reserve(history(6, 2)).unwrap();
        let quarantined = history(7, 2);
        let mut installed = roster.reserve(quarantined).unwrap();
        installed.activate().unwrap();
        roster
            .quarantine(
                quarantined,
                CellGroupControllerError::Invalid("qualification fault"),
            )
            .unwrap();
        let scopes = [(a.cluster, a.namespace)];
        assert_eq!(complete(&roster, &scopes, 2).unwrap(), vec![a, b]);
        assert!(matches!(
            complete(&roster, &scopes, 1),
            Err(RuntimeError::Overloaded)
        ));
        assert!(complete(&roster, &[], 1).unwrap().is_empty());
        let revision = roster.partition_revision(&scopes).unwrap();
        pending.activate().unwrap();
        assert_ne!(roster.partition_revision(&scopes).unwrap(), revision);
        assert!(matches!(
            complete(&roster, &scopes, 2),
            Err(RuntimeError::Overloaded)
        ));
        assert_eq!(
            complete(&roster, &scopes, 3).unwrap(),
            vec![a, b, history(6, 2)]
        );
    }
}
