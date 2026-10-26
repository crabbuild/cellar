// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded receipt material for inventory-consuming terminal verification.
//! Stored descriptors grant no completion or disruption-release authority.
mod fencing;
mod receipts;
mod survivors;
mod terminal;
pub use fencing::FleetFinalizationAdmission;
pub use terminal::{FleetRemovalAdmission, FleetRemovalRequest};

use super::*;
use crate::application::consensus::RaftCells;
use crate::raft::archive::{ArchiveReference, ArchiveVerifier, CellArchive};
use crate::raft::catalog::NamespaceCatalog;
use crate::raft::host::FleetPublishedCut;
use cellar_logic::consensus::{NodeId, ProposalId};
use receipts::{Basis, Book, Candidate, Entry, Evidence, LiveEvidence};

/// Services and the exact immutable archive selected for one history. Scratch
/// storage and recovery-memory admission remain owned by the embedding runtime.
pub struct FleetEvacuationArchive<'a> {
    pub archive: &'a CellArchive,
    pub reference: &'a ArchiveReference,
    pub verifier: &'a ArchiveVerifier,
}

/// A bounded, complete-inventory walk that owns its authenticated quorum calls
/// and physical archive replay. Each history is checked against the same rooted
/// policy, node, history and permit cut. This is candidate preparation only:
/// it grants no current-health lease, stop/removal authority or slot release.
pub struct FleetEvacuationVerification {
    catalog: FleetCatalog,
    snapshot: FleetSnapshot,
    book: Book,
    cursor: Option<FleetCursor>,
    pending: Option<(FleetEntry, FleetHistory)>,
    frozen: Option<FrozenLive>,
    exhausted: bool,
    prior: Option<Candidate>,
    oldest_sample: Option<u64>,
    observer: Option<RaftCells>,
    survivor_nodes: std::collections::BTreeSet<NodeId>,
}

struct FrozenLive {
    fixed: FleetPublishedCut,
    survivors: [receipts::Survivor; 3],
    sampled: u64,
}

/// Immutable historical verification material, never a terminal receipt. Only
/// a future terminal root CAS may establish completion after fresh survivor
/// checks and physical reconciliation. Stored candidate bytes cannot mint this
/// value or reconstitute the runtime's verification work after a crash.
pub struct FleetEvacuationCandidate {
    catalog: FleetCatalog,
    digest: [u8; 32],
    candidate: Candidate,
}

impl FleetEvacuationCandidate {
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn histories(&self) -> u64 {
        self.candidate.count
    }

    pub fn operation(&self) -> [u8; 16] {
        self.candidate.basis.operation
    }

    pub fn inventory_revision(&self) -> u64 {
        self.candidate.basis.revision
    }
}

use super::deadline::Deadline;

impl FleetCatalog {
    /// Begin from the current exact root and internally checked complete
    /// inventory. A caller-provided count or saved admission cannot substitute.
    pub async fn prepare_evacuation_verification(
        &self,
        snapshot: &FleetSnapshot,
        operation: ProposalId,
        namespaces: &[NamespaceCatalog],
        timeout: Duration,
    ) -> Result<FleetEvacuationVerification> {
        self.check_snapshot(snapshot)?;
        if namespaces.len() > namespaces::MAX_NAMESPACES {
            return Err(FleetCatalogError::Limit);
        }
        let deadline = Deadline::new(self, timeout)?;
        let (book, prior) = self
            .run(deadline.remaining()?, async {
                if self.inner.current().await?.token != snapshot.token {
                    return Err(FleetCatalogError::Conflict);
                }
                let inventory = self
                    .inner
                    .domain
                    .run(Box::pin(self.check_inventory_inner(snapshot, namespaces)))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)??;
                let (basis, prior) = if self
                    .inner
                    .lookup(
                        snapshot.head.tables[FleetTable::Operations.slot()],
                        &format!("finalizing/{}", hex::encode(operation.bytes())),
                    )
                    .await?
                    .is_some()
                {
                    let fence = self.stop_record(snapshot, operation.bytes()).await?;
                    let previous = Candidate::read(self, fence.candidate).await?;
                    (
                        Basis::capture_finalizing(
                            self,
                            snapshot,
                            &fence,
                            &inventory,
                            &previous.basis,
                        )?,
                        Some(previous),
                    )
                } else {
                    let active = self.checked_active_evacuation(snapshot, operation).await?;
                    (Basis::capture(self, snapshot, &active, &inventory)?, None)
                };
                basis.check_current(self).await?;
                Ok((Book::new(basis), prior))
            })
            .await?;
        deadline.remaining()?;
        Ok(FleetEvacuationVerification {
            catalog: self.clone(),
            snapshot: snapshot.clone(),
            book,
            cursor: None,
            pending: None,
            frozen: None,
            exhausted: false,
            prior,
            oldest_sample: None,
            observer: None,
            survivor_nodes: std::collections::BTreeSet::new(),
        })
    }
}

impl FleetEvacuationVerification {
    /// Return the next rooted history, with at most one 4-KiB record retained.
    /// Failed attempts keep this same history pending. Reserved births block
    /// verification until their immutable decision is reconciled into a new root.
    pub async fn next_history(&mut self, timeout: Duration) -> Result<Option<FleetHistory>> {
        let catalog = self.catalog.clone();
        let deadline = Deadline::new(&catalog, timeout)?;
        let page = catalog
            .run(deadline.remaining()?, async {
                self.book.basis.check_current(&catalog).await?;
                if self.pending.is_some() || self.exhausted {
                    return Ok(None);
                }
                let page = catalog
                    .inner
                    .scan(
                        self.snapshot.head.tables[FleetTable::Histories.slot()],
                        self.cursor.as_ref(),
                        1,
                    )
                    .await?;
                self.book.basis.check_current(&catalog).await?;
                Ok(Some(page))
            })
            .await?;
        deadline.remaining()?;
        if let Some(mut page) = page {
            if page.entries.len() > 1 || page.entries.is_empty() && page.next.is_some() {
                return Err(FleetCatalogError::Corrupt("receipt inventory page"));
            }
            let pending = page
                .entries
                .pop()
                .map(|entry| {
                    let history = FleetHistory::decode(&entry.value)?;
                    if history
                        .binding
                        .key()
                        .map_err(|_| FleetCatalogError::Corrupt("receipt binding"))?
                        != entry.key
                    {
                        return Err(FleetCatalogError::Corrupt("receipt history key"));
                    }
                    Ok((entry, history))
                })
                .transpose()?;
            self.cursor = page.next;
            self.exhausted = self.cursor.is_none();
            self.pending = pending;
        }
        Ok(self.pending.as_ref().map(|(_, history)| history.clone()))
    }

    /// Consume a canonical cancellation already checked against its immutable
    /// namespace decision by complete inventory. Timeouts and Reserved births
    /// cannot be recorded as cancellations.
    pub async fn verify_cancelled(&mut self, timeout: Duration) -> Result<()> {
        let catalog = self.catalog.clone();
        let deadline = Deadline::new(&catalog, timeout)?;
        self.next_history(deadline.remaining()?).await?;
        let (_, history) = self
            .pending
            .as_ref()
            .ok_or(FleetCatalogError::InventoryIncomplete)?;
        let FleetHistoryState::Cancelled { operation } = *history.state() else {
            return Err(FleetCatalogError::Conflict);
        };
        self.append(Evidence::Cancelled { operation }, &deadline)
            .await
    }

    /// Freeze this pending history's fixed cut through three actual quorum
    /// responses. Repeated calls return that same historical cut, allowing
    /// archive catch-up while writes continue. Rooted-input changes invalidate
    /// it; it never supplies a current-health lease for terminal completion.
    pub async fn freeze_live(
        &mut self,
        observer: &RaftCells,
        nodes: [NodeId; 3],
        timeout: Duration,
    ) -> Result<FleetPublishedCut> {
        if nodes[0] == nodes[1] || nodes[0] == nodes[2] || nodes[1] == nodes[2] {
            return Err(FleetCatalogError::Invalid("duplicate survivor"));
        }
        let catalog = self.catalog.clone();
        let deadline = Deadline::new(&catalog, timeout)?;
        self.next_history(deadline.remaining()?).await?;
        if let Some(frozen) = &self.frozen {
            if frozen
                .survivors
                .iter()
                .zip(nodes)
                .any(|(proof, node)| proof.node != node.bytes())
            {
                return Err(FleetCatalogError::Conflict);
            }
            return Ok(frozen.fixed.clone());
        }
        let (_, history) = self
            .pending
            .as_ref()
            .ok_or(FleetCatalogError::InventoryIncomplete)?;
        if *history.state() != FleetHistoryState::Live {
            return Err(FleetCatalogError::Unconfirmed);
        }
        let scope = history
            .binding
            .scope()
            .map_err(|_| FleetCatalogError::Corrupt("receipt scope"))?;
        let authorization = catalog
            .authorize_group(&self.snapshot, &scope, deadline.remaining()?)
            .await?;
        let minimum_revision = self.book.basis.revision;
        let mut sampled = catalog
            .inner
            .domain
            .mono_ms()
            .map_err(|_| FleetCatalogError::Closed)?;
        // Each invocation performs fresh metadata authorization through its
        // bounded catalog turn. Sequence calls instead of contending with our
        // own other observations; every phase retains the original deadline.
        let mut observations = Vec::with_capacity(3);
        for node in nodes {
            observations.push(
                observer
                    .observe_peer_fleet_membership(
                        node,
                        authorization.clone(),
                        minimum_revision,
                        deadline.remaining()?,
                    )
                    .await
                    .map_err(FleetCatalogError::Observation)?,
            );
        }
        for observation in &observations {
            let now = catalog
                .inner
                .domain
                .mono_ms()
                .map_err(|_| FleetCatalogError::Closed)?;
            sampled = sampled.min(
                now.checked_sub(observation.sample_age_ms())
                    .ok_or(FleetCatalogError::Unconfirmed)?,
            );
        }
        deadline.remaining()?;
        let observations = observations
            .try_into()
            .map_err(|_| FleetCatalogError::Corrupt("receipt observation count"))?;
        let observations: [crate::raft::host::RaftPeerFleetMembershipObservation; 3] = observations;
        let current = observations[0]
            .published_cut()
            .ok_or(FleetCatalogError::Unconfirmed)?;
        let (fixed, survivors) = catalog
            .run(deadline.remaining()?, async {
                self.book.basis.check_current(&catalog).await?;
                let mut fixed = current.clone();
                if let Some(prior) = &self.prior {
                    let (pending, _) = self
                        .pending
                        .as_ref()
                        .ok_or(FleetCatalogError::InventoryIncomplete)?;
                    if let Some(entry) = prior.entry(&catalog, &pending.key).await? {
                        if entry.record == hash(&pending.value) {
                            if let Evidence::Live(live) = entry.evidence {
                                if live.fixed.revision == current.membership_revision().get() {
                                    fixed = live.fixed.restore(current)?;
                                }
                            }
                        }
                    }
                }
                let survivors =
                    survivors::verify(&catalog, &self.snapshot, history, &observations, &fixed)
                        .await?;
                Ok((fixed, survivors))
            })
            .await?;
        deadline.remaining()?;
        self.frozen = Some(FrozenLive {
            fixed: fixed.clone(),
            survivors,
            sampled,
        });
        self.observer = Some(observer.clone());
        self.survivor_nodes.extend(nodes);
        Ok(fixed.clone())
    }

    /// Verify exactly the selected archive against the pending fixed cut. If
    /// needed, capture that cut first through actual quorum calls under this
    /// same method deadline. Prior freeze_live work remains historical; retry
    /// does not move its business-write boundary or grant a freshness lease.
    pub async fn verify_live(
        &mut self,
        observer: &RaftCells,
        nodes: [NodeId; 3],
        archive: FleetEvacuationArchive<'_>,
        timeout: Duration,
    ) -> Result<()> {
        let catalog = self.catalog.clone();
        let deadline = Deadline::new(&catalog, timeout)?;
        let fixed = self
            .freeze_live(observer, nodes, deadline.remaining()?)
            .await?;
        let coverage = catalog
            .inner
            .domain
            .run(archive.archive.verify_published_at(
                archive.reference,
                &fixed,
                archive.verifier,
                deadline.remaining()?,
            ))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|error| FleetCatalogError::Storage(error.into()))?;
        deadline.remaining()?;
        if !coverage
            .covers(&fixed)
            .map_err(|_| FleetCatalogError::Conflict)?
        {
            return Err(FleetCatalogError::Unconfirmed);
        }
        self.append(
            Evidence::Live(Box::new(LiveEvidence {
                fixed: (&fixed).into(),
                archive: (&coverage).into(),
                survivors: self
                    .frozen
                    .as_ref()
                    .ok_or(FleetCatalogError::Unconfirmed)?
                    .survivors
                    .clone(),
            })),
            &deadline,
        )
        .await
    }

    async fn append(&mut self, evidence: Evidence, deadline: &Deadline<'_>) -> Result<()> {
        let (pending, _) = self
            .pending
            .as_ref()
            .ok_or(FleetCatalogError::InventoryIncomplete)?;
        let history = hex::decode(&pending.key)
            .map_err(|_| FleetCatalogError::Corrupt("receipt key"))?
            .try_into()
            .map_err(|_| FleetCatalogError::Corrupt("receipt key length"))?;
        let entry = Entry {
            history,
            record: hash(&pending.value),
            evidence,
        };
        // Only install progress after the whole bounded turn succeeds. A dropped
        // or expired future may leave immutable pages, never an advanced cursor.
        let catalog = self.catalog.clone();
        let mut staged = self.book.clone();
        let book = catalog
            .run(deadline.remaining()?, async {
                staged.basis.check_current(&catalog).await?;
                staged.append(&catalog, &[entry]).await?;
                staged.basis.check_current(&catalog).await?;
                Ok(staged)
            })
            .await?;
        deadline.remaining()?;
        self.book = book;
        if let Some(frozen) = &self.frozen {
            self.oldest_sample = Some(
                self.oldest_sample
                    .map_or(frozen.sampled, |old| old.min(frozen.sampled)),
            );
        }
        self.pending = None;
        self.frozen = None;
        Ok(())
    }

    /// Persist digest-addressed candidate material after every rooted history
    /// has been consumed. This does not change the root, node or disruption slot.
    /// A restarted controller must repeat physical verification; decoding this
    /// candidate is not a substitute for the proofs it no longer owns.
    pub async fn publish_candidate(
        &mut self,
        timeout: Duration,
    ) -> Result<FleetEvacuationCandidate> {
        let catalog = self.catalog.clone();
        let deadline = Deadline::new(&catalog, timeout)?;
        // Index cursors can require a final empty page to confirm exhaustion.
        // Inspect that bounded page here; any unverified history stays pending.
        self.next_history(deadline.remaining()?).await?;
        if self.pending.is_some() || !self.exhausted {
            return Err(FleetCatalogError::InventoryIncomplete);
        }
        let (digest, candidate) = self
            .catalog
            .run(deadline.remaining()?, async {
                self.book.basis.check_current(&self.catalog).await?;
                let result = self.book.candidate(&self.catalog).await?;
                self.book.basis.check_current(&self.catalog).await?;
                Ok(result)
            })
            .await?;
        deadline.remaining()?;
        Ok(FleetEvacuationCandidate {
            catalog: self.catalog.clone(),
            digest,
            candidate,
        })
    }
}
