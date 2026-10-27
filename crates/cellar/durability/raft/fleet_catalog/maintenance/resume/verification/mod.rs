// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One pending history and a bounded physical cut; no materialization on return.
mod physical;
mod publication;
use super::*;
use serde::Deserialize;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Book {
    pub basis: basis::Basis,
    pub physical: [u8; 32],
    pub index: Option<[u8; 32]>,
    pub count: u64,
    pub covered: u64,
    pub voting: u64,
    pub retained: u64,
    pub replaced: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    format: u8,
    history: [u8; 32],
    member: Option<u64>,
    previous: Option<(u64, bool)>,
    retained: bool,
    replaced: bool,
    proof: Option<runtime_records::Voting>,
}

pub(super) struct VerificationInput<'a> {
    pub catalog: &'a FleetCatalog,
    pub snapshot: &'a FleetSnapshot,
    pub request: &'a FleetRestartResumeRequest,
    pub observer: &'a RaftCells,
    pub cut: Box<ReturnedCut>,
    pub basis: basis::Basis,
    pub local: Option<LocalResume>,
}

pub struct FleetRestartResumeVerification {
    pub(in crate::raft::fleet_catalog::maintenance::resume) catalog: FleetCatalog,
    pub(in crate::raft::fleet_catalog::maintenance::resume) snapshot: FleetSnapshot,
    pub(in crate::raft::fleet_catalog::maintenance::resume) request: FleetRestartResumeRequest,
    pub(in crate::raft::fleet_catalog::maintenance::resume) observer: RaftCells,
    pub(in crate::raft::fleet_catalog::maintenance::resume) cut: Box<ReturnedCut>,
    pub(in crate::raft::fleet_catalog::maintenance::resume) book: Book,
    pub(in crate::raft::fleet_catalog::maintenance::resume) inventory: Box<RaftPhysicalInventory>,
    pub(in crate::raft::fleet_catalog::maintenance::resume) mode: ResumeMode,
    pub(in crate::raft::fleet_catalog::maintenance::resume) cursor: Option<FleetCursor>,
    pub(in crate::raft::fleet_catalog::maintenance::resume) pending:
        Option<(FleetEntry, FleetHistory)>,
    pub(in crate::raft::fleet_catalog::maintenance::resume) exhausted: bool,
    pub(in crate::raft::fleet_catalog::maintenance::resume) oldest: Option<u64>,
    pub(in crate::raft::fleet_catalog::maintenance::resume) local: Option<LocalResume>,
}

impl FleetRestartResumeVerification {
    pub(in crate::raft::fleet_catalog::maintenance::resume) async fn new(
        input: VerificationInput<'_>,
        deadline: &Deadline<'_>,
    ) -> Result<Self> {
        let VerificationInput {
            catalog,
            snapshot,
            request,
            observer,
            cut,
            basis,
            local,
        } = input;
        let observed = physical::Context {
            catalog,
            snapshot,
            request,
            observer,
            cut: &cut,
            basis: &basis,
            local: local.as_ref(),
        }
        .observe(deadline)
        .await?;
        Ok(Self {
            catalog: catalog.clone(),
            snapshot: snapshot.clone(),
            request: request.clone(),
            observer: observer.clone(),
            book: Book {
                basis,
                physical: observed.inventory.digest(),
                index: None,
                count: 0,
                covered: 0,
                voting: 0,
                retained: 0,
                replaced: 0,
            },
            inventory: observed.inventory,
            mode: observed.mode,
            cut,
            cursor: None,
            pending: None,
            exhausted: false,
            oldest: None,
            local,
        })
    }

    pub fn phase(&self) -> FleetRestartResumePhase {
        self.mode.into()
    }

    pub fn verified_histories(&self) -> u64 {
        self.book.count
    }

    pub fn retained_copies(&self) -> u64 {
        self.book.retained
    }

    pub fn replaced_histories(&self) -> u64 {
        self.book.replaced
    }

    pub async fn next_history(&mut self, timeout: Duration) -> Result<Option<FleetHistory>> {
        let catalog = self.catalog.clone();
        let page = catalog
            .run(timeout, async {
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
        if let Some(mut page) = page {
            if page.entries.len() > 1 || page.entries.is_empty() && page.next.is_some() {
                return Err(FleetCatalogError::Corrupt("resume history page"));
            }
            self.pending = page
                .entries
                .pop()
                .map(|entry| {
                    let history = FleetHistory::decode(&entry.value)?;
                    if history
                        .binding()
                        .key()
                        .map_err(|_| FleetCatalogError::Corrupt("resume history key"))?
                        != entry.key
                    {
                        return Err(FleetCatalogError::Corrupt("resume history key"));
                    }
                    Ok((entry, history))
                })
                .transpose()?;
            self.cursor = page.next;
            self.exhausted = self.cursor.is_none();
        }
        Ok(self.pending.as_ref().map(|(_, h)| h.clone()))
    }

    /// A requested handoff retains its history until actual current leadership
    /// and matched progress are observed. No blank or retired copy is revived.
    pub fn reconcile_next(
        &mut self,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<Option<FleetRestartResumeProgress>>> {
        Box::pin(async move {
            let catalog = self.catalog.clone();
            let deadline = Deadline::new(&catalog, timeout)?;
            let Some(history) = self.next_history(deadline.remaining()?).await? else {
                return Ok(None);
            };
            let group = history
                .binding()
                .scope()
                .map_err(|_| FleetCatalogError::Corrupt("resume scope"))?;
            let physical = self
                .inventory
                .history(&group)
                .map_err(|_| FleetCatalogError::Conflict)?
                .copied();
            let target = NodeId::new(self.cut.node.node)
                .map_err(|_| FleetCatalogError::Corrupt("resume target"))?;
            let key = history
                .binding()
                .key()
                .map_err(|_| FleetCatalogError::Corrupt("resume history"))?;
            let (previous, voters) = catalog
                .run(deadline.remaining()?, async {
                    self.book.basis.check_current(&catalog).await?;
                    let previous =
                        super::super::runtime::stopped_slot(&catalog, self.cut.stopped_index, &key)
                            .await?;
                    if let Some(slot) = physical {
                        if slot.birth_digest() != history.birth_digest() {
                            return Err(FleetCatalogError::Conflict);
                        }
                        if !history
                            .voters
                            .iter()
                            .chain(&history.learners)
                            .any(|m| m.node == target.bytes() && m.member == slot.member().get())
                            && !catalog
                                .retained_resource_binding(
                                    &self.snapshot,
                                    &key,
                                    slot.member(),
                                    &self.cut.quiescence.boot,
                                )
                                .await?
                        {
                            return Err(FleetCatalogError::Conflict);
                        }
                    }
                    let voters = if *history.state() == FleetHistoryState::Live {
                        match catalog
                            .restart_voters(&self.snapshot, &history, target)
                            .await?
                        {
                            Some(current) => Some(current),
                            None if physical.is_some() || previous.is_some() => Some(
                                catalog
                                    .settled_group_voters(&self.snapshot, &history)
                                    .await?,
                            ),
                            None => None,
                        }
                    } else {
                        None
                    };
                    self.book.basis.check_current(&catalog).await?;
                    Ok((previous, voters))
                })
                .await?;
            let mut proof_value = None;
            let mut sample = None;
            let retained = physical.is_some_and(|p| !p.in_configuration());
            let mut replaced = false;
            let progress = match history.state() {
                FleetHistoryState::Reserved => {
                    if physical.is_some()
                        || previous.is_some()
                        || history
                            .voters
                            .iter()
                            .chain(&history.learners)
                            .any(|m| m.node == target.bytes())
                    {
                        return Err(FleetCatalogError::InventoryIncomplete);
                    }
                    FleetRestartResumeProgress::Unrelated { group }
                }
                FleetHistoryState::Cancelled { .. } => {
                    if physical.is_some() || previous.is_some() {
                        return Err(FleetCatalogError::Conflict);
                    }
                    FleetRestartResumeProgress::Unrelated { group }
                }
                FleetHistoryState::Live => {
                    if let Some((voters, permit)) = voters {
                        let current_member = voters.get(&target).copied();
                        if let Some(member) = current_member {
                            if physical
                                .is_none_or(|p| p.member() != member || !p.in_configuration())
                            {
                                return Err(FleetCatalogError::InventoryIncomplete);
                            }
                        } else if physical.is_some_and(|p| p.in_configuration()) {
                            return Err(FleetCatalogError::Unconfirmed);
                        }
                        let observed = proof::observe_current_voting(
                            proof::VotingCut {
                                catalog: &catalog,
                                snapshot: &self.snapshot,
                                target,
                                observer: &self.observer,
                                paused: self.mode == ResumeMode::Paused,
                            },
                            &history,
                            &voters,
                            permit.as_ref(),
                            &deadline,
                        )
                        .await?;
                        catalog
                            .run(
                                deadline.remaining()?,
                                self.book.basis.check_current(&catalog),
                            )
                            .await?;
                        if let Some(progress) = super::leadership::reconcile(
                            self,
                            &history,
                            &voters,
                            &observed.voting,
                            &deadline,
                        )
                        .await?
                        {
                            return Ok(Some(progress));
                        }
                        proof::fresh(&catalog, observed.sampled, observed.sampled)?;
                        replaced = previous
                            .is_some_and(|(old, incoming)| incoming && current_member != Some(old));
                        sample = Some(observed.sampled);
                        proof_value = Some(observed.voting);
                        FleetRestartResumeProgress::Verified {
                            group,
                            current_member,
                            retained_member: physical
                                .filter(|p| !p.in_configuration())
                                .map(|p| p.member()),
                            replaced,
                        }
                    } else {
                        if physical.is_some() || previous.is_some() {
                            return Err(FleetCatalogError::Conflict);
                        }
                        FleetRestartResumeProgress::Unrelated { group }
                    }
                }
            };
            let row = self
                .pending
                .as_ref()
                .ok_or(FleetCatalogError::InventoryIncomplete)?;
            let entry = Entry {
                format: 1,
                history: hash(&row.0.value),
                member: physical.map(|p| p.member().get()),
                previous: previous.map(|(m, c)| (m.get(), c)),
                retained,
                replaced,
                proof: proof_value,
            };
            let index = catalog
                .run(deadline.remaining()?, async {
                    self.book.basis.check_current(&catalog).await?;
                    let index = catalog
                        .inner
                        .update(
                            self.book.index,
                            &row.0.key,
                            Some(encode(91, &entry, 4096)?),
                            0,
                        )
                        .await?;
                    self.book.basis.check_current(&catalog).await?;
                    Ok(index)
                })
                .await?;
            self.book.index = index;
            self.book.count = self
                .book
                .count
                .checked_add(1)
                .filter(|n| *n <= self.book.basis.histories)
                .ok_or(FleetCatalogError::Limit)?;
            self.book.covered += u64::from(physical.is_some());
            self.book.voting += u64::from(entry.proof.is_some());
            self.book.retained += u64::from(retained);
            self.book.replaced += u64::from(replaced);
            if let Some(sample) = sample {
                self.oldest = Some(self.oldest.map_or(sample, |old| old.min(sample)));
            }
            self.pending = None;
            deadline.remaining()?;
            Ok(Some(progress))
        })
    }

    pub(in crate::raft::fleet_catalog::maintenance::resume) fn walk_all<'a>(
        &'a mut self,
        deadline: &'a Deadline<'_>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            while let Some(progress) = self.reconcile_next(deadline.remaining()?).await? {
                if matches!(
                    progress,
                    FleetRestartResumeProgress::HandoffRequested { .. }
                ) {
                    return Err(FleetCatalogError::Unconfirmed);
                }
            }
            Ok(())
        })
    }
}
