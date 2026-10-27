// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Owned runtime reconciliation for intact-engine maintenance. No stop grant.
mod action;
mod basis;
mod genesis;
mod handoff;
mod proof;
mod readiness;
mod reconcile;
mod records;
mod stop;

use super::gate::Gate;
use super::*;
use crate::application::consensus::RaftCells;
use crate::raft::fleet_catalog::deadline::Deadline;
use crate::raft::fleet_catalog::finalization::receipts::Publication;
use crate::raft::physical_inventory::{RaftPhysicalHistory, RaftPhysicalInventory};
pub(crate) use action::RestartActionContext;
use basis::Basis;
use cellar_logic::consensus::{GroupScope, MemberId};
use futures_util::future::BoxFuture;
pub(crate) use handoff::RestartLeadershipCommand;
pub use readiness::{FleetRestartReadyAdmission, FleetRestartReadyRequest};
pub use reconcile::FleetRestartRuntimeProgress;
use records::{Candidate, Entry, Evidence};

/// A complete history walk with one pending history and one bounded physical
/// snapshot. It owns actual quorum calls; saved descriptors cannot restore it.
pub struct FleetRestartVerification {
    catalog: FleetCatalog,
    snapshot: FleetSnapshot,
    gate: Gate,
    observer: RaftCells,
    book: Candidate,
    inventory: Box<RaftPhysicalInventory>,
    cursor: Option<FleetCursor>,
    pending: Option<(FleetEntry, FleetHistory)>,
    exhausted: bool,
    // At most three idempotent attachment checks for this pending history.
    genesis_checked: std::collections::BTreeSet<NodeId>,
    // Monotonic clock of this catalog owner only; never persisted.
    oldest_matched_sample: Option<u64>,
    local_quiescence: Option<crate::raft::fleet_stop::LocalQuiescence>,
}

/// Descriptive progress for the history just consumed, never a readiness lease.
#[derive(Clone, Debug)]
pub enum FleetRestartHistoryProgress {
    Cancelled {
        group: GroupScope,
    },
    Unrelated {
        group: GroupScope,
    },
    Retained {
        group: GroupScope,
        member: MemberId,
    },
    Voting {
        group: GroupScope,
        member: MemberId,
        survivors: [NodeId; 2],
    },
}

/// Historical runtime evidence only. This cannot stop a node, advance it to
/// RestartReady, release a slot or authorize a new boot. A later readiness
/// transition must independently renew health and leadership proofs.
pub struct FleetRestartRuntimeCandidate {
    digest: [u8; 32],
    candidate: Candidate,
}

impl FleetRestartRuntimeCandidate {
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn operation(&self) -> [u8; 16] {
        self.candidate.basis.operation
    }

    pub fn inventory_revision(&self) -> u64 {
        self.candidate.basis.revision
    }

    pub fn histories(&self) -> u64 {
        self.candidate.count
    }

    pub fn physical_histories(&self) -> u64 {
        self.candidate.covered_physical
    }

    pub fn voting_histories(&self) -> u64 {
        self.candidate.voting
    }

    pub fn retained_histories(&self) -> u64 {
        self.candidate.retained
    }

    /// Histories whose surviving leader and matched progress were observed by
    /// owned reconciliation. Historical coverage is not a readiness lease.
    pub fn matched_histories(&self) -> u64 {
        self.candidate.matched
    }
}

impl FleetCatalog {
    /// Settle M2 first. This method checks complete canonical inventory, owns
    /// physical quiescence, then captures every durable slot on the exact boot.
    pub fn prepare_restart_verification<'a>(
        &'a self,
        snapshot: &'a FleetSnapshot,
        operation: ProposalId,
        namespaces: &'a [NamespaceCatalog],
        observer: &'a RaftCells,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<FleetRestartVerification>> {
        Box::pin(async move {
            self.check_snapshot(snapshot)?;
            let deadline = Deadline::new(self, timeout)?;
            let pending = self
                .pending_restart_work(snapshot, operation, namespaces, deadline.remaining()?)
                .await?;
            if pending.pending_births != 0
                || pending.active_birth.is_some()
                || !pending.movements.is_empty()
                || !pending.reservations.is_empty()
            {
                return Err(FleetCatalogError::InventoryIncomplete);
            }
            let (gate, basis) = self
                .run(deadline.remaining()?, async {
                    if self.inner.current().await?.token != snapshot.token {
                        return Err(FleetCatalogError::Conflict);
                    }
                    let gate = self
                        .checked_restart_gate(snapshot, operation.bytes())
                        .await?;
                    let basis = Basis::capture(snapshot, &gate)?;
                    basis.check_current(self).await?;
                    Ok((gate, basis))
                })
                .await?;
            let inventory =
                proof::physical(self, snapshot, &basis, &gate, observer, &deadline).await?;
            deadline.remaining()?;
            Ok(FleetRestartVerification {
                catalog: self.clone(),
                snapshot: snapshot.clone(),
                gate,
                observer: observer.clone(),
                book: Candidate {
                    format: 1,
                    basis,
                    physical: inventory.digest(),
                    index: None,
                    count: 0,
                    covered_physical: 0,
                    voting: 0,
                    retained: 0,
                    matched: 0,
                },
                inventory,
                cursor: None,
                pending: None,
                exhausted: false,
                genesis_checked: std::collections::BTreeSet::new(),
                oldest_matched_sample: None,
                local_quiescence: None,
            })
        })
    }
}

impl FleetRestartVerification {
    /// Failed verification retains the same pending history. No all-history
    /// runtime vector is collected, including for namespaces unrelated to target.
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
                return Err(FleetCatalogError::Corrupt("restart runtime page"));
            }
            let pending = page
                .entries
                .pop()
                .map(|entry| {
                    let history = FleetHistory::decode(&entry.value)?;
                    if history
                        .binding()
                        .key()
                        .map_err(|_| FleetCatalogError::Corrupt("restart history key"))?
                        != entry.key
                    {
                        return Err(FleetCatalogError::Corrupt("restart history key"));
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

    /// Verify target membership and both other voters against a fixed published
    /// cut. A single surviving quorum partner is insufficient for maintenance.
    pub fn verify_next(
        &mut self,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<Option<FleetRestartHistoryProgress>>> {
        Box::pin(async move {
            match self.verify_one(timeout, false).await? {
                Some(FleetRestartRuntimeProgress::Verified(progress)) => Ok(Some(progress)),
                None => Ok(None),
                _ => Err(FleetCatalogError::Corrupt(
                    "inspection produced a runtime action",
                )),
            }
        })
    }

    fn verify_one(
        &mut self,
        timeout: Duration,
        handoff: bool,
    ) -> BoxFuture<'_, Result<Option<FleetRestartRuntimeProgress>>> {
        Box::pin(async move {
            let catalog = self.catalog.clone();
            let deadline = Deadline::new(&catalog, timeout)?;
            let started = proof::now(&catalog)?;
            let Some(history) = self.next_history(deadline.remaining()?).await? else {
                return Ok(None);
            };
            let group = history
                .binding()
                .scope()
                .map_err(|_| FleetCatalogError::Corrupt("restart scope"))?;
            let physical = self
                .inventory
                .history(&group)
                .map_err(|_| FleetCatalogError::Conflict)?
                .copied();
            let target = NodeId::new(self.gate.node.node)
                .map_err(|_| FleetCatalogError::Corrupt("restart target"))?;
            let voters = catalog
                .run(deadline.remaining()?, async {
                    self.book.basis.check_current(&catalog).await?;
                    if let Some(slot) = physical {
                        if slot.birth_digest() != history.birth_digest() {
                            return Err(FleetCatalogError::Conflict);
                        }
                        let genesis =
                            history.voters.iter().chain(&history.learners).any(|m| {
                                m.node == target.bytes() && m.member == slot.member().get()
                            });
                        if !genesis
                            && !catalog
                                .retained_resource_binding(
                                    &self.snapshot,
                                    &history.binding().key().map_err(|_| {
                                        FleetCatalogError::Corrupt("restart binding")
                                    })?,
                                    slot.member(),
                                    &self.gate.node.boot_binding(self.gate.preparation.catalog)?,
                                )
                                .await?
                        {
                            return Err(FleetCatalogError::Conflict);
                        }
                    }
                    if *history.state() == FleetHistoryState::Live {
                        catalog
                            .restart_voters(&self.snapshot, &history, target)
                            .await
                    } else {
                        Ok(None)
                    }
                })
                .await?;
            let mut matched_sample = None;
            let (evidence, progress) = match history.state() {
                FleetHistoryState::Reserved => return Err(FleetCatalogError::InventoryIncomplete),
                FleetHistoryState::Cancelled { operation } => {
                    if physical.is_some() {
                        return Err(FleetCatalogError::Conflict);
                    }
                    (
                        Evidence::Cancelled {
                            operation: *operation,
                        },
                        FleetRestartHistoryProgress::Cancelled { group },
                    )
                }
                FleetHistoryState::Live => {
                    if let Some((voters, permit)) = voters {
                        let member = *voters.get(&target).ok_or(FleetCatalogError::Conflict)?;
                        if physical
                            .is_none_or(|slot| slot.member() != member || !slot.in_configuration())
                        {
                            return Err(FleetCatalogError::InventoryIncomplete);
                        }
                        let observed =
                            proof::voting(self, &history, &voters, permit.as_ref(), &deadline)
                                .await?;
                        let mut voting = observed.voting;
                        if handoff {
                            if let Some(progress) =
                                handoff::reconcile(self, &history, &voters, &voting, &deadline)
                                    .await?
                            {
                                return Ok(Some(progress));
                            }
                            proof::fresh(&catalog, started, observed.sampled)?;
                            voting.leadership = records::Leadership::Matched {
                                through: voting.fixed.applied_position()?.index.get(),
                            };
                            matched_sample = Some(observed.sampled);
                        }
                        let survivors = voters
                            .keys()
                            .copied()
                            .filter(|node| *node != target)
                            .collect::<Vec<_>>()
                            .try_into()
                            .map_err(|_| FleetCatalogError::Corrupt("restart survivors"))?;
                        (
                            Evidence::Voting(Box::new(voting)),
                            FleetRestartHistoryProgress::Voting {
                                group,
                                member,
                                survivors,
                            },
                        )
                    } else if let Some(slot) = physical {
                        if slot.in_configuration() {
                            return Err(FleetCatalogError::Conflict);
                        }
                        (
                            Evidence::Retained,
                            FleetRestartHistoryProgress::Retained {
                                group,
                                member: slot.member(),
                            },
                        )
                    } else {
                        (
                            Evidence::Unrelated,
                            FleetRestartHistoryProgress::Unrelated { group },
                        )
                    }
                }
            };
            let row = self
                .pending
                .as_ref()
                .ok_or(FleetCatalogError::InventoryIncomplete)?;
            let entry = Entry {
                format: 1,
                record: hash(&row.0.value),
                physical: physical.map(Into::into),
                evidence,
            };
            let count = self
                .book
                .count
                .checked_add(1)
                .filter(|n| *n <= self.book.basis.histories)
                .ok_or(FleetCatalogError::Limit)?;
            let covered = self.book.covered_physical + u64::from(physical.is_some());
            let voting =
                self.book.voting + u64::from(matches!(entry.evidence, Evidence::Voting(_)));
            let retained =
                self.book.retained + u64::from(matches!(entry.evidence, Evidence::Retained));
            let matched = self.book.matched
                + u64::from(matches!(&entry.evidence,
                Evidence::Voting(v) if matches!(v.leadership, records::Leadership::Matched { .. })));
            let index = catalog
                .run(deadline.remaining()?, async {
                    self.book.basis.check_current(&catalog).await?;
                    let next = catalog
                        .inner
                        .update(
                            self.book.index,
                            &row.0.key,
                            Some(encode(records::ENTRY_KIND, &entry, records::LIMIT)?),
                            0,
                        )
                        .await?;
                    self.book.basis.check_current(&catalog).await?;
                    Ok(next)
                })
                .await?;
            deadline.remaining()?;
            self.book.index = index;
            self.book.count = count;
            self.book.covered_physical = covered;
            self.book.voting = voting;
            self.book.retained = retained;
            self.book.matched = matched;
            if let Some(sample) = matched_sample {
                self.oldest_matched_sample = Some(
                    self.oldest_matched_sample
                        .map_or(sample, |old| old.min(sample)),
                );
            }
            self.pending = None;
            self.genesis_checked.clear();
            Ok(Some(FleetRestartRuntimeProgress::Verified(progress)))
        })
    }

    /// Require a second complete physical cut and replay the exact quiescence
    /// selector before publishing historical evidence. A cancellation Fence
    /// that synced before a failed root CAS defeats that replay.
    pub fn finish(
        mut self,
        timeout: Duration,
    ) -> BoxFuture<'static, Result<FleetRestartRuntimeCandidate>> {
        Box::pin(async move {
            let catalog = self.catalog.clone();
            let deadline = Deadline::new(&catalog, timeout)?;
            let digest = self.save_candidate(&deadline).await?;
            Ok(FleetRestartRuntimeCandidate {
                digest,
                candidate: self.book,
            })
        })
    }

    async fn save_candidate(&mut self, deadline: &Deadline<'_>) -> Result<[u8; 32]> {
        let catalog = self.catalog.clone();
        self.next_history(deadline.remaining()?).await?;
        if self.pending.is_some()
            || !self.exhausted
            || self.book.count != self.book.basis.histories
            || self.book.covered_physical != self.inventory.histories().len() as u64
            || self.book.matched > self.book.voting
        {
            return Err(FleetCatalogError::InventoryIncomplete);
        }
        let current = proof::physical_locally_quiesced(
            &catalog,
            &self.snapshot,
            &self.book.basis,
            &self.gate,
            &self.observer,
            deadline,
            self.local_quiescence.as_ref(),
        )
        .await?;
        if current.digest() != self.book.physical {
            return Err(FleetCatalogError::Conflict);
        }
        let digest = catalog
            .run(deadline.remaining()?, async {
                self.book.basis.check_current(&catalog).await?;
                let bytes = encode(records::CANDIDATE_KIND, &self.book, records::LIMIT)?;
                let digest = hash(&bytes);
                catalog
                    .inner
                    .create_exact(
                        &format!("restart-candidates/{}", hex::encode(digest)),
                        &bytes,
                        records::LIMIT,
                    )
                    .await?;
                self.book.basis.check_current(&catalog).await?;
                Ok(digest)
            })
            .await?;
        deadline.remaining()?;
        Ok(digest)
    }
}
