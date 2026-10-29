// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Own probes outside the bounded catalog turn, then order all debits through
//! one expected-root CAS. Exact historical retries precede live prerequisites.
use super::records::{Book, Record, Reference, Request};
use super::*;
use crate::application::consensus::RaftCells;
use crate::raft::catalog::NamespaceCatalog;
use crate::raft::host::RaftPeerNodeObservation;
use cellar_logic::consensus::{
    ClusterId, DiskLineage, EngineInstanceId, IncarnationId, NodeIdentity, ProcessGeneration,
    ProposalId,
};
use cellar_logic::raft_fleet::{
    CapacityScope, LedgerPolicy, LedgerSnapshot, PhysicalSample, Reservation, ResourceLedger,
};

#[derive(Default)]
pub(super) struct AdmissionOptions {
    pub topology: bool,
    pub reserve: FleetResourceBounds,
}

// Registration and the engine have separate slot limits. Registered metadata
// can exist without an engine history; retained engines can lack metadata.
// The observation does not identify their dormant overlap, so require room for
// a conservative union plus admitted prospective registrations. This guards the
// host table independently; metadata must not inflate engine usage or its floor.
pub(super) fn registration_room(
    view: &crate::raft::host::RaftHostObservation,
    pending: usize,
) -> Result<bool> {
    let pending =
        u32::try_from(pending).map_err(|_| resource_error(LedgerError::ArithmeticOverflow))?;
    view.registered_histories
        .checked_add(view.unattached_engine_histories)
        .map(|n| n.max(view.node.groups))
        .and_then(|n| n.checked_add(pending))
        .and_then(|n| n.checked_add(1))
        .map(|needed| needed <= view.startup_bounds.host_groups)
        .ok_or(resource_error(LedgerError::ArithmeticOverflow))
}

impl FleetCatalog {
    /// Reserve one prospective learner's resources and one active movement
    /// slot. No membership permit is created. Occupancy evidence is explicitly
    /// deployment-attested plus owned authenticated runtime lower bounds.
    /// Unresolved debits survive exclusion, reboot and caller cancellation.
    pub async fn reserve_resources(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetResourceRequest,
        observer: &RaftCells,
        namespaces: &[NamespaceCatalog],
        timeout: Duration,
    ) -> Result<FleetResourceAdmission> {
        self.reserve_resource_request(
            snapshot,
            Request::new(self, request)?,
            observer,
            namespaces,
            AdmissionOptions::default(),
            timeout,
        )
        .await
    }

    pub(super) async fn reserve_resource_request(
        &self,
        snapshot: &FleetSnapshot,
        canonical: Request,
        observer: &RaftCells,
        namespaces: &[NamespaceCatalog],
        options: AdmissionOptions,
        timeout: Duration,
    ) -> Result<FleetResourceAdmission> {
        self.check_snapshot(snapshot)?;
        if namespaces.len() > namespaces::MAX_NAMESPACES {
            return Err(FleetCatalogError::Limit);
        }
        let deadline = super::super::deadline::Deadline::new(self, timeout)?;
        let start = self
            .run(deadline.remaining()?, async {
                let current = self.inner.current().await?;
                let previous = self.resource_request(&canonical).await?;
                if let Some((mutation, record)) = &previous {
                    if let Some(applied) = self.resource_applied(&current, mutation).await? {
                        return Ok(Err(FleetResourceAdmission {
                            record: record.clone(),
                            applied,
                            observed: current.revision(),
                        }));
                    }
                }
                if current.token != snapshot.token
                    || canonical.identity.expected_revision != snapshot.revision()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                self.inner
                    .domain
                    .run(Box::pin(self.check_inventory_inner(snapshot, namespaces)))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)??;
                let node = NodeId::new(canonical.attestation.boot.node)
                    .map_err(|_| FleetCatalogError::Invalid("resource node"))?;
                let capacity = self.capacity_from(snapshot, node).await?;
                if !capacity.receive_replicas()
                    || capacity.attestation().boot != canonical.attestation.boot
                    || capacity.attestation().profile_revision
                        != canonical.attestation.profile_revision
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let target = self.host_from(snapshot, node).await?;
                Ok(Ok((capacity, target, previous)))
            })
            .await?;
        let (capacity, target, previous) = match start {
            Ok(inputs) => inputs,
            Err(admission) => {
                deadline.remaining()?;
                return Ok(admission);
            }
        };
        let observation = observer
            .inspect_peer_physical_inventory(
                target.node(),
                snapshot.revision(),
                deadline.remaining()?,
            )
            .await
            .map_err(FleetCatalogError::Observation)?;
        deadline.remaining()?;
        super::super::capacity::check_observation(&target, capacity.attestation(), &observation)?;
        let delivered = self.resource_now()?;
        let probe_started = delivered
            .checked_sub(observation.sample_age_ms)
            .ok_or(FleetCatalogError::Unconfirmed)?;
        self.require_resource_age(probe_started)?;
        let source = self
            .observe_resource_source(snapshot, &canonical, observer, &deadline)
            .await?;
        self.run(deadline.remaining()?, async {
            if self.inner.current().await?.token != snapshot.token {
                return Err(FleetCatalogError::Conflict);
            }
            self.check_resource_birth_debt(snapshot, target.node(), &observation)
                .await?;
            let inventory = observation
                .inventory
                .as_ref()
                .ok_or(resource_error(LedgerError::Incomplete))?;
            let history = self.history_inner(snapshot, &canonical.group).await?;
            let scope = history
                .binding()
                .scope()
                .map_err(|_| FleetCatalogError::Corrupt("resource scope"))?;
            if inventory
                .history(&scope)
                .map_err(|_| FleetCatalogError::Corrupt("resource destination scope"))?
                .is_some()
            {
                return Err(FleetCatalogError::Invalid("occupied resource destination"));
            }
            if *history.state() != FleetHistoryState::Live {
                return Err(FleetCatalogError::Conflict);
            }
            let mut book = self.resource_book(snapshot).await?;
            let rows = self.resource_rows(snapshot, &book).await?;
            self.check_resource_permits(snapshot, &rows).await?;
            if rows.iter().any(|row| row.request.group == canonical.group) {
                return Err(FleetCatalogError::Conflict);
            }
            if book.rows.len() >= MAX_MOVES {
                return Err(resource_error(LedgerError::MovementLimit));
            }
            let pending = rows.iter().filter(|row| row.node == target.node()).count();
            if !registration_room(&observation.view, pending)? {
                return Err(FleetCatalogError::Overloaded);
            }
            let physical = self
                .check_resource_fit(
                    snapshot,
                    &canonical,
                    &capacity,
                    &observation,
                    &rows,
                    options.reserve,
                )
                .await?;
            let record = Record::new(
                self,
                canonical.clone(),
                snapshot,
                target.epoch(),
                physical,
                inventory.digest(),
                self.resource_permit(snapshot, &canonical, source.as_ref())
                    .await?,
            )?;
            let row_bytes = record.encode()?;
            if let Some((_, old)) = &previous {
                if old.encode()? != row_bytes {
                    // The immutable request captured different observations.
                    // Do not hide a replan under the same identity.
                    return Err(FleetCatalogError::Conflict);
                }
            }
            book.rows.push(Reference {
                id: canonical.identity.id,
                digest: hash(&row_bytes),
            });
            book.rows.sort_by_key(|row| row.id);
            let mut mutation = self.resource_mutation(&record, row_bytes, &book)?;
            if options.topology {
                self.append_topology_movement(snapshot, &record, &mut mutation)
                    .await?;
            }
            let committed = self
                .commit_with_disruption(
                    snapshot,
                    &mutation,
                    None,
                    if options.topology {
                        Authority::TopologyAdmission
                    } else if canonical.movement.is_some() {
                        Authority::Admission
                    } else {
                        Authority::None
                    },
                    || {
                        deadline.remaining()?;
                        self.require_resource_age(probe_started)?;
                        if let Some(source) = &source {
                            self.require_resource_age(source.started)?;
                        }
                        Ok(())
                    },
                )
                .await?;
            let applied = self
                .resource_applied(&committed.snapshot, &mutation)
                .await?
                .ok_or(FleetCatalogError::Unconfirmed)?;
            deadline.remaining()?;
            Ok(FleetResourceAdmission {
                record,
                applied,
                observed: committed.snapshot.revision(),
            })
        })
        .await
    }

    fn resource_mutation(
        &self,
        record: &Record,
        row: Vec<u8>,
        book: &Book,
    ) -> Result<FleetMutation> {
        let request = &record.request;
        let mut mutation = FleetMutation {
            id: request.identity.id,
            operation: request.identity.operation,
            expected_revision: request.identity.expected_revision,
            principal: request.identity.principal.clone(),
            changes: vec![
                FleetChange {
                    table: FleetTable::Operations,
                    key: row_key(request.identity.id),
                    value: Some(row),
                },
                FleetChange {
                    table: FleetTable::Policy,
                    key: BOOK_KEY.into(),
                    value: Some(book.encode()?),
                },
            ],
        };
        if let Some(permit) = &record.permit {
            mutation.changes.push(FleetChange {
                table: FleetTable::Admissions,
                key: request.group.clone(),
                value: Some(
                    permit
                        .encode()
                        .map_err(|_| FleetCatalogError::Corrupt("resource permit"))?,
                ),
            });
            mutation.changes.push(super::bindings::change(record)?);
        }
        mutation.validate()?;
        Ok(mutation)
    }

    async fn check_resource_fit(
        &self,
        snapshot: &FleetSnapshot,
        request: &Request,
        capacity: &FleetCapacityAuthorization,
        observation: &RaftPeerNodeObservation,
        rows: &[Record],
        reserve: FleetResourceBounds,
    ) -> Result<FleetResourceBounds> {
        let qualified = capacity.attestation();
        let view = &observation.view;
        let runtime = &view.startup_bounds;
        if request.peak.durable_bytes < view.node.profile.bytes_per_group {
            return Err(FleetCatalogError::Invalid("resource learner byte bound"));
        }
        let charged = self
            .charged_resource_inputs(snapshot, &request.attestation, qualified, view, rows)
            .await?;
        let physical = charged.physical;
        let current_scope = charged.scope;
        let now = self.resource_now()?;
        let mut ledger = ResourceLedger::restore(LedgerSnapshot {
            scope: current_scope,
            policy: LedgerPolicy {
                capacity: Resources {
                    engine_slots: u64::from(
                        qualified
                            .engine_slots
                            .min(view.node.profile.group_capacity)
                            .min(runtime.host_groups)
                            .min(runtime.consensus_groups)
                            .min(runtime.engine_groups),
                    ),
                    durable_bytes: qualified.durable_bytes.min(view.node.profile.byte_capacity),
                    memory_bytes: qualified.memory_bytes,
                },
                // Required-repair selection supplies explicit node headroom.
                // Ordinary explicit moves retain their existing zero reserve;
                // neither path claims complete-fleet recovery feasibility.
                reserve: reserve.resources(),
                max_active_moves: MAX_MOVES as u32,
                max_sample_age_ms: MAX_SAMPLE_AGE_MS,
            },
            sample: PhysicalSample {
                scope: current_scope,
                sequence: snapshot.revision(),
                observed_ms: now,
                physical: physical.resources(),
            },
            reservations: charged.reservations,
            complete: true,
        })
        .map_err(resource_error)?;
        ledger
            .reserve(
                Reservation {
                    id: ProposalId::new(request.identity.id)
                        .map_err(|_| FleetCatalogError::Invalid("resource reservation identity"))?,
                    group: self
                        .history_inner(snapshot, &request.group)
                        .await?
                        .binding()
                        .scope()
                        .map_err(|_| FleetCatalogError::Corrupt("resource reservation scope"))?,
                    scope: current_scope,
                    peak: request.peak.resources(),
                    materialized: None,
                },
                now,
            )
            .map_err(resource_error)?;
        Ok(physical)
    }

    pub(super) fn resource_now(&self) -> Result<u64> {
        self.inner
            .domain
            .mono_ms()
            .map_err(|_| FleetCatalogError::Closed)
    }

    pub(super) fn require_resource_age(&self, started: u64) -> Result<()> {
        if self
            .resource_now()?
            .checked_sub(started)
            .is_none_or(|age| age > MAX_SAMPLE_AGE_MS)
        {
            return Err(FleetCatalogError::Unconfirmed);
        }
        Ok(())
    }

    async fn check_resource_birth_debt(
        &self,
        snapshot: &FleetSnapshot,
        target: NodeId,
        observation: &RaftPeerNodeObservation,
    ) -> Result<()> {
        let inventory = observation
            .inventory
            .as_ref()
            .ok_or(resource_error(LedgerError::Incomplete))?;
        if inventory.initializing().is_some() {
            return Err(resource_error(LedgerError::Incomplete));
        }
        let mut cursor = None;
        let mut count = 0u64;
        loop {
            let page = self
                .inner
                .scan(
                    snapshot.head.tables[FleetTable::Histories.slot()],
                    cursor.as_ref(),
                    128,
                )
                .await?;
            for entry in page.entries {
                count = count
                    .checked_add(1)
                    .filter(|n| *n <= HISTORY_LIMIT)
                    .ok_or(FleetCatalogError::Limit)?;
                let history = FleetHistory::decode(&entry.value)?;
                if !matches!(history.state(), FleetHistoryState::Cancelled { .. })
                    && history
                        .voters
                        .iter()
                        .chain(&history.learners)
                        .any(|m| m.node == target.bytes())
                {
                    if !matches!(history.state(), FleetHistoryState::Live) {
                        return Err(resource_error(LedgerError::Incomplete));
                    }
                    let scope = history
                        .binding()
                        .scope()
                        .map_err(|_| FleetCatalogError::Corrupt("resource genesis scope"))?;
                    let occupied = inventory
                        .history(&scope)
                        .map_err(|_| FleetCatalogError::Corrupt("resource genesis scope"))?
                        .ok_or(resource_error(LedgerError::Incomplete))?;
                    if occupied.birth_digest() != history.birth_digest()
                        || !history.voters.iter().chain(&history.learners).any(|m| {
                            m.node == target.bytes() && m.member == occupied.member().get()
                        })
                    {
                        return Err(FleetCatalogError::Corrupt("resource genesis binding"));
                    }
                }
            }
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        if count != snapshot.head.history_count {
            return Err(FleetCatalogError::Corrupt("resource history count"));
        }
        Ok(())
    }
}

pub(super) fn capacity_scope(
    catalog: &FleetCatalog,
    attestation: &FleetResourceAttestation,
) -> Result<CapacityScope> {
    let boot = &attestation.boot;
    let invalid = || FleetCatalogError::Corrupt("resource capacity scope");
    Ok(CapacityScope {
        cluster: ClusterId::new(catalog.inner.cluster).map_err(|_| invalid())?,
        incarnation: IncarnationId::new(catalog.inner.incarnation).map_err(|_| invalid())?,
        catalog: boot.catalog,
        node: NodeIdentity {
            node: NodeId::new(boot.node).map_err(|_| invalid())?,
            disk: DiskLineage::new(boot.disk).map_err(|_| invalid())?,
            process: ProcessGeneration::new(boot.process).map_err(|_| invalid())?,
        },
        engine: EngineInstanceId::new(boot.engine).map_err(|_| invalid())?,
        boot: boot.boot,
        profile_revision: attestation.profile_revision,
    })
}
