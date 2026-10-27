// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! At most one in-memory complete-inventory walk. Restart repeats its proofs.
use super::*;
use crate::raft::archive::{ArchiveError, ArchiveLimits, ArchiveVerifier};
use crate::raft::fleet_catalog::{
    FleetEvacuationArchive, FleetEvacuationVerification, FleetHistoryState,
};
use cellar_logic::consensus::{MemberId, NodeId};
use cellar_ltx::LtxHost;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};

pub(in super::super) struct Verification {
    host: LtxHost,
    verifier: ArchiveVerifier,
    limits: ArchiveLimits,
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

    pub(super) fn new(
        host: LtxHost,
        verifier: ArchiveVerifier,
        limits: ArchiveLimits,
    ) -> Result<Self> {
        limits
            .validate()
            .map_err(|_| RaftFleetFailure::invalid("removal archive bounds"))?;
        Ok(Self {
            host,
            verifier,
            limits,
            session: Mutex::new(None),
            busy: AtomicBool::new(false),
        })
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
                    .find(|n| n.config().namespace == scope.namespace)
                    .ok_or(FleetCatalogError::InventoryIncomplete)?;
                let birth = turn
                    .run(namespace.birth(&scope.cell))
                    .await?
                    .map_err(FleetCatalogError::from)?
                    .ok_or(FleetCatalogError::InventoryIncomplete)?;
                if birth.scope() != &scope || birth.digest() != history.birth_digest() {
                    return Err(FleetCatalogError::Conflict.into());
                }
                // Bind archive I/O to the management owner's execution domain
                // even for SDK calls made outside an HTTP/domain task.
                let archive = turn
                    .run(async { namespace.cell_archive(&birth, self.host.clone(), self.limits) })
                    .await?
                    .map_err(FleetCatalogError::from)?;
                let metadata = turn
                    .run(archive.recovery_metadata())
                    .await?
                    .map_err(archive_error)?
                    .ok_or(FleetCatalogError::Unconfirmed)?;
                let configuration = metadata.configuration().map_err(archive_error)?;
                if configuration.voters.len() != 3
                    || !configuration.voters_outgoing.is_empty()
                    || !configuration.learners.is_empty()
                    || !configuration.learners_next.is_empty()
                {
                    return Err(FleetCatalogError::Unconfirmed.into());
                }
                let membership = metadata.membership();
                let mut nodes = Vec::with_capacity(3);
                for member in configuration.voters {
                    let member = MemberId::new(member)
                        .map_err(|_| FleetCatalogError::Corrupt("archive voter"))?;
                    let node = match membership
                        .joined(member)
                        .map_err(|_| FleetCatalogError::Corrupt("archive binding"))?
                    {
                        Some((binding, _)) => binding.node,
                        None => birth
                            .voters()
                            .iter()
                            .chain(birth.learners())
                            .find(|v| v.member == member)
                            .map(|v| v.node)
                            .ok_or(FleetCatalogError::InventoryIncomplete)?,
                    };
                    nodes.push(node);
                }
                let nodes: [NodeId; 3] = nodes
                    .try_into()
                    .map_err(|_| FleetCatalogError::InventoryIncomplete)?;
                let reference = metadata.reference();
                let result = session
                    .walk
                    .verify_live(
                        observer,
                        nodes,
                        FleetEvacuationArchive {
                            archive: &archive,
                            reference: &reference,
                            verifier: &self.verifier,
                        },
                        turn.remaining()?,
                    )
                    .await;
                if let Err(error) = result {
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
                    return Ok(Advance::Waiting);
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

fn archive_error(error: ArchiveError) -> FleetCatalogError {
    match error {
        ArchiveError::Invalid(reason) => FleetCatalogError::Invalid(reason),
        ArchiveError::Corrupt(reason) => FleetCatalogError::Corrupt(reason),
        ArchiveError::Conflict => FleetCatalogError::Conflict,
        ArchiveError::Limit => FleetCatalogError::Limit,
        ArchiveError::Unconfirmed | ArchiveError::Timeout => FleetCatalogError::Unconfirmed,
        ArchiveError::Stopped => FleetCatalogError::Closed,
        error => FleetCatalogError::Storage(error.into()),
    }
}
