// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One node-owned physical collection service. Reference adoption is a separate
//! startup proof; turns never select roots or relax the offline engine collector.
use super::resources::CollectibleExtents;
use crate::application::consensus::{
    group_client::{resources::CollectionObserver, ControlServiceKind, ControlServiceLease},
    CellGroupCollectionFailure, RaftCells,
};
use crate::asyncrt::DomainToken;
use crate::raft::budget::{Budget, Permit};
use cellar_ltx::internal::StoreError;
use std::{sync::Arc, time::Duration};

const FILES_PER_TURN: usize = 16;
const METADATA_BYTES: usize = 4096;

pub(super) struct Collection {
    inventories: Vec<CollectibleExtents>,
    working_bytes: usize,
    observer: CollectionObserver,
}

// Field drop order is part of the join: release inventory handles before the
// last registry lease can advertise completed physical ownership.
struct Owner {
    inventories: Vec<CollectibleExtents>,
    _lease: ControlServiceLease,
}

struct Turn {
    _reservation: Permit,
    owner: Arc<Owner>,
    inventory: usize,
}

impl Collection {
    pub(super) fn new(inventories: Vec<CollectibleExtents>) -> anyhow::Result<Option<Self>> {
        if inventories.is_empty() {
            return Ok(None);
        }
        anyhow::ensure!(
            inventories.len() <= 2,
            "collection inventories exceed node roles"
        );
        let working_bytes = inventories.iter().try_fold(0usize, |bound, inventory| {
            inventory
                .store()
                .reclaim_working_bytes(FILES_PER_TURN)
                .map(|n| bound.max(n))
        })?;
        let observer = CollectionObserver::new(inventories.len(), working_bytes);
        Ok(Some(Self {
            inventories,
            working_bytes,
            observer,
        }))
    }

    pub(super) fn observer(&self) -> CollectionObserver {
        self.observer.clone()
    }

    pub(super) fn start(self, service: &RaftCells, poll_ms: u64) -> anyhow::Result<()> {
        let budget = service.control_budget().clone();
        let (bytes, items) = budget.capacity();
        anyhow::ensure!(
            items >= 2
                && self
                    .working_bytes
                    .checked_add(METADATA_BYTES)
                    .is_some_and(|required| required <= bytes),
            "collection lacks control admission backing"
        );
        let lease =
            service.register_control_service(ControlServiceKind::Reclamation, METADATA_BYTES)?;
        let stop = lease.stop_receiver();
        let owner = Arc::new(Owner {
            inventories: self.inventories,
            _lease: lease,
        });
        let domain = service.handle.domain.clone();
        let observer = self.observer.clone();
        self.observer.update(|s| s.running = true);
        let worker = domain.clone();
        match domain.spawn_cleanup(async move {
            run(
                owner,
                budget,
                worker,
                stop,
                self.observer,
                self.working_bytes,
                poll_ms,
            )
            .await;
        }) {
            Ok(task) => {
                task.detach();
                Ok(())
            }
            Err(error) => {
                failed(&observer, CellGroupCollectionFailure::Domain);
                observer.update(|s| s.running = false);
                Err(error.into())
            }
        }
    }
}

fn physical(turn: Turn) -> Result<usize, StoreError> {
    // The turn, its memory and the registry lifetime remain in this physical
    // closure if an async observer disappears while a filesystem call is held.
    turn.owner.inventories[turn.inventory]
        .store()
        .reclaim(FILES_PER_TURN)
}

fn failed(observer: &CollectionObserver, cause: CellGroupCollectionFailure) {
    observer.update(|s| {
        s.failures = s.failures.saturating_add(1);
        s.last_failure = Some(cause);
    });
}

async fn run(
    owner: Arc<Owner>,
    budget: Arc<Budget>,
    domain: DomainToken,
    mut stop: tokio::sync::watch::Receiver<bool>,
    observer: CollectionObserver,
    working_bytes: usize,
    poll_ms: u64,
) {
    'service: loop {
        let mut progress = false;
        for inventory in 0..owner.inventories.len() {
            if *stop.borrow() {
                break 'service;
            }
            match owner.inventories[inventory].store().needs_reclaim() {
                Ok(false) => continue,
                Ok(true) => (),
                Err(error) => {
                    tracing::warn!(%error, "node image collection refused inventory");
                    failed(&observer, CellGroupCollectionFailure::Inventory);
                    break 'service;
                }
            }
            let Some(reservation) = budget.acquire(working_bytes) else {
                observer.update(|s| s.deferrals = s.deferrals.saturating_add(1));
                continue;
            };
            let turn = Turn {
                _reservation: reservation,
                owner: owner.clone(),
                inventory,
            };
            let entered = match domain.blocking(move || physical(turn)) {
                Ok(task) => task,
                Err(_) => {
                    failed(&observer, CellGroupCollectionFailure::Domain);
                    break 'service;
                }
            };
            observer.update(|s| s.turns = s.turns.saturating_add(1));
            // Stop signals never cancel admitted physical work or its memory.
            match entered.await {
                Ok(Ok(files)) => {
                    progress |= files != 0;
                    observer.update(|s| {
                        s.files_reclaimed = s.files_reclaimed.saturating_add(files as u64)
                    });
                }
                Ok(Err(StoreError::Busy)) => {
                    observer.update(|s| s.deferrals = s.deferrals.saturating_add(1))
                }
                Ok(Err(error)) => {
                    tracing::warn!(%error, "node image collection stopped after physical failure");
                    failed(&observer, CellGroupCollectionFailure::Inventory);
                    break 'service;
                }
                Err(_) => {
                    failed(&observer, CellGroupCollectionFailure::Worker);
                    break 'service;
                }
            }
        }
        if *stop.borrow() {
            break;
        }
        // Drain bounded deletion batches promptly, yielding between turns. An
        // idle marked inventory is checked using metadata without file I/O.
        crate::asyncrt::select! {
            _ = stop.changed() => break,
            _ = crate::asyncrt::sleep(Duration::from_millis(if progress { 1 } else { poll_ms })) => (),
        }
    }
    observer.update(|s| s.running = false);
    drop(owner);
}
