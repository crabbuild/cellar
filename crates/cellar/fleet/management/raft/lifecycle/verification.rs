// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! At most one in-memory complete-inventory walk. Restart repeats its proofs.
use super::*;
use crate::raft::fleet_catalog::{
    FleetEvacuationArchive, FleetEvacuationVerification, FleetGroupArchiveVerifier,
    FleetHistoryState,
};
use cellar_logic::consensus::NodeId;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};

pub(in super::super) struct Verification {
    archive: FleetGroupArchiveVerifier,
    session: Mutex<Option<Session>>,
    busy: AtomicBool,
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct Key {
    pub identity: FleetRequestIdentity,
    pub node: NodeId,
    pub stop: Option<[u8; 16]>,
}

pub(super) struct Session {
    pub key: Key,
    pub walk: FleetEvacuationVerification,
    pub checked: u64,
    pub total: u64,
    pub waiting: Option<RaftFleetVerificationBlocker>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Advance {
    Progress,
    Waiting,
    Exhausted,
}

pub(super) struct Lease<'a> {
    runtime: &'a Verification,
    pub session: Option<Session>,
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        if let Ok(mut slot) = self.runtime.session.lock() {
            *slot = self.session.take();
        }
        self.runtime.busy.store(false, Ordering::Release);
    }
}

impl Verification {
    pub(super) fn snapshot(
        &self,
        control: &RaftFleetControl,
        node: NodeId,
        operation: [u8; 16],
        revision: u64,
    ) -> Result<Option<RaftFleetEvacuationProgress>> {
        let slot = self.session.lock().map_err(|_| FleetCatalogError::Closed)?;
        let Some(session) = slot.as_ref().filter(|s| {
            s.key.node == node
                && s.key.identity.operation == operation
                && s.key.identity.expected_revision == revision
        }) else {
            return Ok(None);
        };
        let request = request_from(&session.key.identity, &control.catalog_id);
        let mut value = progress(
            control,
            &hex::encode(node.bytes()),
            &request,
            if session.key.stop.is_some() {
                Phase::VerifyingRemoval
            } else {
                Phase::VerifyingBeforeStop
            },
            session.checked,
            session.total,
            revision,
        );
        value.stop_id = session.key.stop.map(hex::encode);
        value.blocker = session.waiting.clone();
        value.validate()?;
        Ok(Some(value))
    }

    pub(super) fn new(archive: FleetGroupArchiveVerifier) -> Self {
        Self {
            archive,
            session: Mutex::new(None),
            busy: AtomicBool::new(false),
        }
    }

    pub(super) fn lease(&self, key: &Key) -> Result<Lease<'_>> {
        self.busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| RaftFleetFailure::from(FleetCatalogError::Overloaded))?;
        let mut lease = Lease {
            runtime: self,
            session: None,
        };
        let mut slot = self.session.lock().map_err(|_| FleetCatalogError::Closed)?;
        lease.session = slot.take();
        drop(slot);
        if let Some(session) = &lease.session {
            if session.key.identity.id == key.identity.id && session.key != *key {
                return Err(FleetCatalogError::PayloadConflict.into());
            }
            if session.key != *key {
                lease.session = None;
            }
        }
        Ok(lease)
    }

    /// One history per successful invocation. The core retains the same pending
    /// cut on errors, independently authenticates all three survivors and owns
    /// physical replay. Archive membership selects candidates, never authority.
    pub(super) async fn advance(
        &self,
        control: &RaftFleetControl,
        session: &mut Session,
        observer: &RaftCells,
        cut: &FleetSnapshot,
        turn: &Turn<'_>,
    ) -> Result<Advance> {
        session.waiting = None;
        let Some(history) = session.walk.next_history(turn.remaining()?).await? else {
            return Ok(Advance::Exhausted);
        };
        match history.state() {
            FleetHistoryState::Cancelled { .. } => {
                session.walk.verify_cancelled(turn.remaining()?).await?
            }
            FleetHistoryState::Live => {
                let scope = history
                    .binding()
                    .scope()
                    .map_err(|_| FleetCatalogError::Corrupt("removal history scope"))?;
                let namespace = control
                    .namespaces
                    .iter()
                    .find(|n| n.namespace_config().namespace == scope.namespace)
                    .ok_or(FleetCatalogError::InventoryIncomplete)?;
                // Rooted current membership only selects candidates; the
                // walk independently authenticates all three live survivors.
                let (voters, _) = turn
                    .run(control.catalog.settled_group_voters(cut, &history))
                    .await??;
                let nodes: [NodeId; 3] = voters
                    .into_keys()
                    .collect::<Vec<_>>()
                    .try_into()
                    .map_err(|_| FleetCatalogError::InventoryIncomplete)?;
                let fixed = match session
                    .walk
                    .freeze_live(observer, nodes, turn.remaining()?)
                    .await
                {
                    Ok(fixed) => fixed,
                    Err(error) => return waiting(session, &history, error),
                };
                let registry = turn
                    .run(self.archive.open(&control.domain, namespace, &history))
                    .await??;
                let hold = session
                    .walk
                    .acquire_archive_hold(&registry, turn.remaining()?)
                    .await?;
                let receiver = turn
                    .run(self.archive.receiver(
                        &control.domain,
                        fixed.root().route(),
                        namespace.namespace_config().image_storage,
                    ))
                    .await??;
                let result = session
                    .walk
                    .verify_live(
                        observer,
                        nodes,
                        FleetEvacuationArchive {
                            registry: &registry,
                            hold: &hold,
                            tails: self.archive.tails(),
                            receiver,
                        },
                        turn.remaining()?,
                    )
                    .await;
                if let Err(error) = result {
                    return waiting(session, &history, error);
                }
            }
            _ => return Err(FleetCatalogError::Unconfirmed.into()),
        }
        session.checked = session
            .checked
            .checked_add(1)
            .ok_or(FleetCatalogError::Limit)?;
        Ok(Advance::Progress)
    }
}

fn waiting(
    session: &mut Session,
    history: &crate::raft::fleet_catalog::FleetHistory,
    error: FleetCatalogError,
) -> Result<Advance> {
    let Some(blocker) = RaftFleetVerificationBlocker::observation(
        &error,
        RaftFleetVerificationStage::HistoryQuorum,
        Some(
            history
                .binding()
                .key()
                .map_err(|_| FleetCatalogError::Corrupt("waiting history key"))?,
        ),
    ) else {
        return Err(error.into());
    };
    session.waiting = Some(blocker);
    Ok(Advance::Waiting)
}
