// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The receiving Node renews live proof; rooted consent supplies no health lease.
use super::*;
use crate::raft::fleet_stop::{LocalQuiescence, StopSelector};

pub(crate) struct RestartStopCheck {
    pub quiescence: StopSelector,
    pub deadline: u64,
}

impl FleetCatalog {
    pub(crate) fn renew_restart_stop<'a>(
        &'a self,
        selector: &'a StopSelector,
        gateway: &'a RaftCells,
        local: LocalQuiescence,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<RestartStopCheck>> {
        Box::pin(async move {
            let deadline = Deadline::new(self, timeout)?;
            let started = proof::now(self)?;
            let owner_limit = gateway
                .handle
                .domain
                .mono_ms()
                .map_err(|_| FleetCatalogError::Closed)?
                .checked_add(proof::MAX_SAMPLE_AGE_MS)
                .ok_or(FleetCatalogError::Limit)?;
            let (snapshot, gate, basis, physical) = self
                .run(deadline.remaining()?, async {
                    let snapshot = self.inner.current().await?;
                    let (_, gate, ready) = self.checked_restart_stop(&snapshot, selector).await?;
                    let physical = ready.candidate().candidate.physical;
                    let basis = Basis::capture(&snapshot, &gate)?;
                    Ok((snapshot, gate, basis, physical))
                })
                .await?;
            let quiescence = gate.quiesce_request()?.selector;
            local.check(&quiescence)?;
            // The Ready basis pins complete inventory: managed births require
            // prior Root admission and imported birth inventories stay guarded.
            // Any relevant Root drift refuses this consent. Renew all actual
            // physical, quorum and leader evidence instead of restoring samples.
            let inventory = proof::physical_locally_quiesced(
                self,
                &snapshot,
                &basis,
                &gate,
                gateway,
                &deadline,
                Some(&local),
            )
            .await?;
            if inventory.digest() != physical {
                return Err(FleetCatalogError::Conflict);
            }
            let mut walk = FleetRestartVerification {
                catalog: self.clone(),
                snapshot,
                gate,
                observer: gateway.clone(),
                book: Candidate {
                    format: 1,
                    basis,
                    physical,
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
                local_quiescence: Some(local.clone()),
            };
            // No new genesis is initialized while stopping. A regained primary
            // may request the owned handoff, but stop remains unconfirmed until
            // a later exact retry observes surviving leadership and matched cuts.
            while let Some(progress) = walk.verify_one(deadline.remaining()?, true).await? {
                match progress {
                    FleetRestartRuntimeProgress::Verified(_) => (),
                    FleetRestartRuntimeProgress::HandoffRequested { .. } => {
                        return Err(FleetCatalogError::Unconfirmed)
                    }
                    FleetRestartRuntimeProgress::Materialized { .. } => {
                        return Err(FleetCatalogError::Corrupt("stop materialized genesis"))
                    }
                }
            }
            walk.save_candidate(&deadline).await?;
            walk.check_ready_samples(started)?;
            self.run(deadline.remaining()?, walk.book.basis.check_current(self))
                .await?;
            local.check(&quiescence)?;
            let remaining =
                proof::budget(self, started, walk.oldest_matched_sample.unwrap_or(started))?;
            let expires = gateway
                .handle
                .domain
                .mono_ms()
                .map_err(|_| FleetCatalogError::Closed)?
                .checked_add(remaining)
                .ok_or(FleetCatalogError::Limit)?
                .min(owner_limit);
            deadline.remaining()?;
            Ok(RestartStopCheck {
                quiescence,
                deadline: expires,
            })
        })
    }
}
