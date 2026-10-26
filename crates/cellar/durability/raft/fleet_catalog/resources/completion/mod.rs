// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Terminal quorum plus physical work exit, ordered with retained floors.
mod proofs;
mod records;
use super::records::{Book, Record, Reference};
use super::*;
use crate::application::consensus::RaftCells;
use cellar_logic::consensus::ProposalId;
use records::{Completion, Floor};

const COMPLETE_PREFIX: &str = "resource-completion/";
const FLOOR_PREFIX: &str = "resource-floor/";
const COMPLETE_KIND: u8 = 63;
const FLOOR_KIND: u8 = 64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetMovementCompletionRequest {
    pub identity: FleetRequestIdentity,
    pub reservation: [u8; 16],
    /// Current deployment occupancy evidence. It must cover retained state;
    /// the adapter independently adds physical inventory/runtime lower bounds.
    pub attestation: FleetResourceAttestation,
}

pub struct FleetMovementCompletion {
    reservation: [u8; 16],
    applied: u64,
    observed: u64,
}

impl FleetMovementCompletion {
    pub fn reservation(&self) -> [u8; 16] {
        self.reservation
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }
}

impl FleetMovementCompletionRequest {
    fn validate(&self) -> Result<()> {
        crate::raft::fleet_catalog::histories::validate_request(&self.identity)?;
        ProposalId::new(self.reservation).map_err(|_| invalid())?;
        self.attestation.validate()
    }
}

fn invalid() -> FleetCatalogError {
    FleetCatalogError::Invalid("movement completion")
}

fn completion_key(id: [u8; 16]) -> String {
    format!("{COMPLETE_PREFIX}{}", hex::encode(id))
}

fn floor_key(node: [u8; 16]) -> String {
    format!("{FLOOR_PREFIX}{}", hex::encode(node))
}

impl FleetCatalog {
    /// Complete a successful membership move and release its active work slot.
    /// Retains the consumed permit, original debit, journal and physical floors.
    /// Expiry, uncertainty, active workers and incomplete local apply retain debt.
    pub async fn complete_movement(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetMovementCompletionRequest,
        observer: &RaftCells,
        timeout: Duration,
    ) -> Result<FleetMovementCompletion> {
        self.check_snapshot(snapshot)?;
        request.validate()?;
        let deadline = super::super::deadline::Deadline::new(self, timeout)?;
        let start = self
            .run(deadline.remaining()?, async {
                let current = self.inner.current().await?;
                let previous = self.completion_request(request).await?;
                if let Some((mutation, _)) = &previous {
                    if let Some(applied) = self.resource_applied(&current, mutation).await? {
                        return Ok(Err(FleetMovementCompletion {
                            reservation: request.reservation,
                            applied,
                            observed: current.revision(),
                        }));
                    }
                }
                if current.token != snapshot.token
                    || request.identity.expected_revision != snapshot.revision()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let row = self
                    .admitted_resource(snapshot, request.reservation)
                    .await?;
                if row.permit.is_none()
                    || row.seed_owners.is_none()
                    || row.request.identity.operation != request.identity.operation
                    || row.request.attestation.boot != request.attestation.boot
                    || row.request.attestation.profile_revision
                        != request.attestation.profile_revision
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let book = self.resource_book(snapshot).await?;
                if !book.rows.contains(&Reference {
                    id: request.reservation,
                    digest: hash(&row.encode()?),
                }) {
                    return Err(FleetCatalogError::Conflict);
                }
                let scope = row
                    .permit
                    .as_ref()
                    .ok_or_else(invalid)?
                    .binding()
                    .scope()
                    .map_err(|_| invalid())?;
                let authorization = self.binding(&scope)?;
                let history = self.history_inner(snapshot, &row.request.group).await?;
                if history.binding() != &authorization
                    || *history.state() != FleetHistoryState::Live
                {
                    return Err(FleetCatalogError::Conflict);
                }
                Ok(Ok((row, previous, scope)))
            })
            .await?;
        let (row, previous, scope) = match start {
            Ok(inputs) => inputs,
            Err(receipt) => {
                deadline.remaining()?;
                return Ok(receipt);
            }
        };
        let authorization = self
            .authorize_group(snapshot, &scope, deadline.remaining()?)
            .await?;
        let probe_started = self.resource_now()?;
        let quorum = observer
            .observe_peer_fleet_membership(
                row.node,
                authorization,
                snapshot.revision(),
                deadline.remaining()?,
            )
            .await
            .map_err(FleetCatalogError::Observation)?;
        let target = observer
            .inspect_peer_physical_inventory(row.node, snapshot.revision(), deadline.remaining()?)
            .await
            .map_err(FleetCatalogError::Observation)?;
        // Every original voter may have served this learner. Pin and probe all
        // of them, including a now-retired obsolete member; selected ReadIndex
        // provenance does not restrict the physical seed source.
        let owners = row.seed_owners.as_ref().ok_or_else(invalid)?;
        let mut sources = Vec::with_capacity(owners.len());
        for owner in owners {
            sources.push(
                observer
                    .inspect_peer_physical_inventory(
                        NodeId::new(owner.node).map_err(|_| invalid())?,
                        snapshot.revision(),
                        deadline.remaining()?,
                    )
                    .await
                    .map_err(FleetCatalogError::Observation)?,
            );
        }
        self.require_resource_age(probe_started)?;
        if [quorum.sample_age_ms(), target.sample_age_ms]
            .into_iter()
            .chain(sources.iter().map(|source| source.sample_age_ms))
            .any(|age| age > MAX_SAMPLE_AGE_MS)
        {
            return Err(FleetCatalogError::Unconfirmed);
        }
        self.run(deadline.remaining()?, async {
            if self.inner.current().await?.token != snapshot.token {
                return Err(FleetCatalogError::Conflict);
            }
            self.check_movement_completion(snapshot, &row, &quorum, &target, &sources)
                .await?;
            let current = &target.view;
            let physical = request
                .attestation
                .used
                .high_water(row.physical)
                .high_water(FleetResourceBounds {
                    engine_slots: current.node.groups,
                    durable_bytes: current.node.reserved_bytes,
                    memory_bytes: current.startup_bounds.bounded_buffer_bytes,
                })
                .high_water(self.resource_floor(snapshot, &request.attestation).await?);
            let floor = Floor {
                format: 1,
                boot: request.attestation.boot.clone(),
                physical,
            };
            let record = Completion {
                format: 1,
                request: request.clone(),
                input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
                resource: hash(&row.encode()?),
                permit: hash(
                    &row.permit
                        .as_ref()
                        .ok_or_else(invalid)?
                        .encode()
                        .map_err(|_| invalid())?,
                ),
                target_inventory: target
                    .inventory
                    .as_ref()
                    .ok_or(FleetCatalogError::Unconfirmed)?
                    .digest(),
                source_inventory: {
                    let mut digests = b"cellar-completed-seed-owners-1".to_vec();
                    for source in &sources {
                        digests.extend_from_slice(
                            &source
                                .inventory
                                .as_ref()
                                .ok_or(FleetCatalogError::Unconfirmed)?
                                .digest(),
                        );
                    }
                    hash(&digests)
                },
                membership_revision: quorum.membership().membership_revision().get(),
                barrier: quorum.membership().barrier().get(),
                physical,
            };
            let mut book = self.resource_book(snapshot).await?;
            let reference = Reference {
                id: request.reservation,
                digest: record.resource,
            };
            let index = book
                .rows
                .iter()
                .position(|r| r == &reference)
                .ok_or(FleetCatalogError::Conflict)?;
            book.rows.remove(index);
            let mutation = FleetMutation {
                id: request.identity.id,
                operation: request.identity.operation,
                principal: request.identity.principal.clone(),
                expected_revision: request.identity.expected_revision,
                changes: vec![
                    FleetChange {
                        table: FleetTable::Operations,
                        key: completion_key(request.reservation),
                        value: Some(record.encode()?),
                    },
                    FleetChange {
                        table: FleetTable::Operations,
                        key: floor_key(row.node.bytes()),
                        value: Some(floor.encode()?),
                    },
                    FleetChange {
                        table: FleetTable::Policy,
                        key: BOOK_KEY.into(),
                        value: Some(book.encode()?),
                    },
                ],
            };
            if previous.as_ref().is_some_and(|(old, _)| old != &mutation) {
                return Err(FleetCatalogError::Conflict);
            }
            mutation.validate()?;
            let committed = self
                .commit_with_disruption(snapshot, &mutation, None, Authority::Completion, || {
                    deadline.remaining()?;
                    self.require_resource_age(probe_started)?;
                    Ok(())
                })
                .await?;
            let applied = self
                .resource_applied(&committed.snapshot, &mutation)
                .await?
                .ok_or(FleetCatalogError::Unconfirmed)?;
            deadline.remaining()?;
            Ok(FleetMovementCompletion {
                reservation: request.reservation,
                applied,
                observed: committed.snapshot.revision(),
            })
        })
        .await
    }
}
