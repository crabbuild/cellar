// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded incremental collection or one root decision per empty-book round.
use super::*;
use crate::raft::budget::{Budget, Permit};
use crate::raft::fleet_catalog::deadline::Deadline;
use sha2::{Digest, Sha256};

struct Collection {
    revision: u64,
    verification: FleetTopologyVerification,
    memory: Arc<Vec<Permit>>,
}

// The time quantum is checked between complete history proofs. It never
// replaces their original deadline or interrupts a partially verified history.

impl Collection {
    async fn advance(
        &mut self,
        catalog: &FleetCatalog,
        deadline: &Deadline<'_>,
        status: &watch::Sender<FleetMovementServiceStatus>,
        options: &FleetTopologyCollectionOptions,
    ) -> Result<bool> {
        let started = catalog.resource_now()?;
        for _ in 0..options.max_histories_per_round {
            let advanced = self.verification.verify_next(deadline.remaining()?).await?;
            status.send_modify(|s| {
                if let Some(t) = &mut s.topology {
                    t.histories_checked = self.verification.histories_checked();
                }
            });
            if !advanced {
                return Ok(false);
            }
            if catalog.resource_now()?.saturating_sub(started) >= options.quantum.as_millis() as u64
            {
                return Ok(true);
            }
        }
        Ok(true)
    }
}

// The worker already owns two root buffers. Reject an intrinsic retained-memory
// misfit independently of transient pool occupancy. This lower-bound check does
// not promise enough headroom for subsequent live metadata and physical probes.
pub(super) fn reserve_collection_memory(
    budget: &Arc<Budget>,
    bytes: usize,
    retained: usize,
) -> Result<Permit> {
    let needed = (2 * ROOT_LIMIT)
        .checked_add(retained)
        .and_then(|n| n.checked_add(bytes))
        .ok_or(FleetCatalogError::Limit)?;
    if needed > budget.capacity().0 {
        return Err(FleetCatalogError::Limit);
    }
    budget.acquire(bytes).ok_or(FleetCatalogError::Overloaded)
}

struct Candidate {
    revision: u64,
    id: [u8; 16],
    seen_at: u64,
}

pub(super) struct TopologyService {
    evidence: Arc<dyn FleetTopologyEvidence>,
    collection_options: FleetTopologyCollectionOptions,
    collection: Option<Collection>,
    candidate: Option<Candidate>,
    last_operation: Option<[u8; 16]>,
    terminal: bool,
}

impl TopologyService {
    pub fn new(
        evidence: Arc<dyn FleetTopologyEvidence>,
        collection_options: FleetTopologyCollectionOptions,
    ) -> Self {
        Self {
            evidence,
            collection_options,
            collection: None,
            candidate: None,
            last_operation: None,
            terminal: false,
        }
    }

    pub fn collection_delay(&self) -> Option<Duration> {
        self.collection
            .as_ref()
            .map(|_| self.collection_options.interval)
    }

    pub fn interrupt(&mut self) {
        self.collection = None;
        self.candidate = None;
    }

    fn publish(
        &mut self,
        operation: &FleetTopologyOperation,
        status: &watch::Sender<FleetMovementServiceStatus>,
    ) {
        let same = self.last_operation == Some(operation.identity.operation);
        self.last_operation = Some(operation.identity.operation);
        self.terminal = matches!(
            operation.phase,
            FleetTopologyPhase::Complete | FleetTopologyPhase::Cancelled
        );
        status.send_modify(|view| {
            let previous = if same {
                view.topology
                    .take()
                    .filter(|p| p.observed_revision == operation.observed_revision)
            } else {
                None
            };
            view.topology = Some(FleetTopologyServiceStatus {
                operation: operation.identity.operation,
                phase: operation.phase,
                observed_revision: operation.observed_revision,
                histories_checked: previous.as_ref().map_or(0, |p| p.histories_checked),
                planned_movements: if operation.phase == FleetTopologyPhase::Complete {
                    Some(0)
                } else {
                    previous.and_then(|p| p.planned_movements)
                },
                admitted_movements: operation.admitted_movements,
                last_superseded_admission: operation.last_superseded_admission,
            });
        });
    }

    pub async fn round(
        &mut self,
        catalog: &FleetCatalog,
        observer: &RaftCells,
        cut: &FleetSnapshot,
        options: &FleetMovementServiceOptions,
        deadline: &Deadline<'_>,
        status: &watch::Sender<FleetMovementServiceStatus>,
    ) -> Result<()> {
        let Some(operation) = catalog
            .active_topology_operation(cut, deadline.remaining()?)
            .await?
        else {
            self.interrupt();
            if !self.terminal {
                if let Some(id) = self.last_operation {
                    let finished = catalog
                        .topology_operation(
                            cut,
                            ProposalId::new(id).map_err(|_| {
                                FleetCatalogError::Corrupt("topology service operation")
                            })?,
                            deadline.remaining()?,
                        )
                        .await?;
                    self.publish(&finished, status);
                }
            }
            return Ok(());
        };
        if self
            .collection
            .as_ref()
            .is_some_and(|c| c.revision != cut.revision())
        {
            self.collection = None;
        }
        if self
            .candidate
            .as_ref()
            .is_some_and(|c| c.revision != cut.revision())
        {
            self.candidate = None;
        }
        self.publish(&operation, status);
        if operation.phase == FleetTopologyPhase::Cancelling {
            self.interrupt();
            let request = service_request(catalog, &operation, cut, b"cancel-complete")?;
            let completed = catalog
                .complete_topology_cancellation(cut, &request, deadline.remaining()?)
                .await?;
            self.publish(&completed, status);
            return Ok(());
        }

        let request = service_request(catalog, &operation, cut, b"movement")?;
        let unresolved = catalog
            .run(
                deadline.remaining()?,
                catalog.topology_candidate(cut, &request),
            )
            .await?;
        if unresolved {
            self.collection = None;
            let now = catalog.resource_now()?;
            let candidate = self.candidate.get_or_insert(Candidate {
                revision: cut.revision(),
                id: request.id,
                seen_at: now,
            });
            if candidate.id != request.id {
                return Err(FleetCatalogError::Conflict);
            }
            // A competing healthy controller may be between immutable request
            // publication and its head CAS. Give that bounded turn time to end;
            // the later fence remains safe even if its response was lost.
            if now.saturating_sub(candidate.seen_at) < options.turn_timeout.as_millis() as u64 {
                return Ok(());
            }
            let fence = service_request(catalog, &operation, cut, b"supersede")?;
            let resolved = catalog
                .supersede_topology_admission(cut, &fence, &request, deadline.remaining()?)
                .await?;
            self.candidate = None;
            self.publish(&resolved, status);
            return Ok(());
        }
        self.candidate = None;

        if self.collection.is_none() {
            let nodes = catalog
                .run(deadline.remaining()?, async {
                    Ok(catalog.node_records(cut).await?.len())
                })
                .await?;
            // Decoded physical inventory is <=4096 compact 80-byte entries per
            // node. Input copies, scopes (<=1024 bytes), maps and temporary
            // request values are conservatively admitted before the callback.
            let histories =
                usize::try_from(cut.head.history_count).map_err(|_| FleetCatalogError::Limit)?;
            let input_bytes = ROOT_LIMIT
                .checked_add(
                    nodes
                        .checked_mul(512 * 1024)
                        .ok_or(FleetCatalogError::Limit)?,
                )
                .and_then(|n| n.checked_add(histories.checked_mul(4096)?))
                .ok_or(FleetCatalogError::Limit)?;
            let input = reserve_collection_memory(&observer.handle.control_budget, input_bytes, 0)?;
            let inputs = self
                .evidence
                .refresh(&operation, deadline.remaining()?)
                .await?;
            validate_inputs(&operation, &inputs, nodes, histories)?;
            // Cover compact group state, view/model copies, path/recovery output
            // and the generator's capped action vector, including growth copies.
            let live = inputs.request.histories.len();
            let actions = live
                .checked_mul(3)
                .and_then(|n| n.checked_mul(nodes))
                .ok_or(FleetCatalogError::Limit)?
                .min(1_000_000);
            let planning_bytes = live
                .checked_mul(28 * 1024)
                .and_then(|n| n.checked_add(actions.checked_mul(48)?))
                .ok_or(FleetCatalogError::Limit)?;
            let planning = reserve_collection_memory(
                &observer.handle.control_budget,
                planning_bytes,
                input_bytes,
            )?;
            let memory = Arc::new(vec![input, planning]);
            let verification = catalog
                .prepare_topology_verification(
                    cut,
                    &inputs.request,
                    observer,
                    &inputs.namespaces,
                    deadline.remaining()?,
                )
                .await?;
            self.collection = Some(Collection {
                revision: cut.revision(),
                verification,
                memory,
            });
            status.send_modify(|s| {
                if let Some(t) = &mut s.topology {
                    t.histories_checked = 0;
                    t.planned_movements = None;
                }
            });
            return Ok(());
        }

        let collection = self
            .collection
            .as_mut()
            .ok_or(FleetCatalogError::Conflict)?;
        if collection
            .advance(catalog, deadline, status, &self.collection_options)
            .await?
        {
            return Ok(());
        }
        let inputs = self
            .evidence
            .refresh(&operation, deadline.remaining()?)
            .await?;
        validate_inputs(&operation, &inputs, 64, cut.head.history_count as usize)?;
        collection
            .verification
            .refresh_resources(&inputs.request, deadline.remaining()?)
            .await?;
        let memory = collection.memory.clone();
        let preview = collection
            .verification
            .preview_with_memory(deadline.remaining()?, Some(memory.clone()))
            .await?;
        status.send_modify(|s| {
            if let Some(t) = &mut s.topology {
                t.planned_movements = Some(preview.plan().moves.len() as u64);
            }
        });
        // Keep this local reservation through the action and its response too.
        // Blocking work owns a clone independently if its waiter expired.
        let _memory = memory;
        self.collection = None;
        if preview.plan().moves.is_empty() {
            let request = service_request(catalog, &operation, cut, b"complete")?;
            let completed = catalog
                .complete_topology(cut, &request, &preview, deadline.remaining()?)
                .await?;
            self.publish(&completed, status);
        } else {
            catalog
                .reserve_topology_movement(
                    &preview,
                    request,
                    observer,
                    &inputs.namespaces,
                    deadline.remaining()?,
                )
                .await?
                .ok_or(FleetCatalogError::Corrupt(
                    "missing generated topology move",
                ))?;
        }
        Ok(())
    }
}

fn validate_inputs(
    operation: &FleetTopologyOperation,
    inputs: &FleetTopologyServiceInputs,
    nodes: usize,
    histories: usize,
) -> Result<()> {
    let keep = inputs
        .request
        .keep
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    if keep.len() != inputs.request.keep.len()
        || keep.iter().copied().collect::<Vec<_>>() != operation.keep
        || inputs.request.policy != operation.policy
        || inputs.request.nodes.len() > nodes
        || inputs.request.histories.len() > histories
        || inputs.namespaces.len() > 64
    {
        return Err(FleetCatalogError::Invalid("topology evidence intent"));
    }
    Ok(())
}

pub(super) fn service_request(
    catalog: &FleetCatalog,
    operation: &FleetTopologyOperation,
    cut: &FleetSnapshot,
    kind: &[u8],
) -> Result<FleetRequestIdentity> {
    let mut digest = Sha256::new();
    digest.update(b"cellar-topology-service-request-1");
    digest.update(catalog.birth_owner()?.catalog);
    digest.update(operation.identity.operation);
    digest.update(cut.revision().to_be_bytes());
    digest.update(kind);
    let digest: [u8; 32] = digest.finalize().into();
    let id: [u8; 16] = digest[..16]
        .try_into()
        .map_err(|_| FleetCatalogError::Limit)?;
    ProposalId::new(id).map_err(|_| FleetCatalogError::Invalid("topology service identity"))?;
    Ok(FleetRequestIdentity {
        id,
        operation: operation.identity.operation,
        principal: operation.identity.principal.clone(),
        expected_revision: cut.revision(),
    })
}
