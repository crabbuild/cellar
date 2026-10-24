// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Stage and verify dead-session bundle windows without retaining their bodies.
//! The caller preserves flush-order windows and uploads only after every bundle
//! in that window verifies, including the final window's preferred follower rows.
use super::{
    bundle_budget, bundle_snapshot, recovery_rows, ClaimBeat, NodeLogManager,
    BUNDLE_TRANSFER_CONCURRENCY,
};
use anyhow::{Context, Result};
use futures_util::{StreamExt, TryStreamExt};

impl NodeLogManager {
    pub(super) async fn gather_recovery_window(
        &self,
        window: Vec<String>,
        mut gathered: recovery_rows::Rows,
        budget: &bundle_budget::Scope,
        dead: &str,
        beat: &mut ClaimBeat,
    ) -> Result<recovery_rows::Rows> {
        budget.hold_rows(&mut gathered);
        let cancellation = gathered.cancellation();
        let mut keys = window.into_iter();
        loop {
            let transfers: Vec<_> = keys.by_ref().take(BUNDLE_TRANSFER_CONCURRENCY).collect();
            if transfers.is_empty() {
                return Ok(gathered);
            }
            let fetches = futures_util::stream::iter(transfers.into_iter().map(|key| {
                let flag = cancellation.clone();
                let scope = budget.clone();
                async move {
                    let file = self
                        .stage_bundle(&key, flag, scope)
                        .await
                        .with_context(|| format!("stage retained recovery bundle {key}"))?
                        .with_context(|| format!("listed recovery bundle {key} disappeared"))?;
                    anyhow::Ok((key, file))
                }
            }))
            .buffer_unordered(BUNDLE_TRANSFER_CONCURRENCY);
            // At most one transfer group retains scratch handles. Poll all
            // transfers through the claim-aware wait: a stalled GET or native
            // write must not let another recoverer steal a live claim.
            let staged: Vec<_> = self
                .recovery_wait(
                    cancellation.clone(),
                    Some((dead, &mut *beat)),
                    fetches.try_collect(),
                )
                .await?;
            for (key, file) in staged {
                let mut rows = self.recovery_rows().with_cancellation(cancellation.clone());
                budget.hold_rows(&mut rows);
                let bundle = self
                    .recovery_io(cancellation.clone(), Some((dead, &mut *beat)), move || {
                        bundle_snapshot::decode(file, rows)
                    })
                    .await
                    .with_context(|| format!("verify retained recovery bundle {key}"))?;
                gathered = self
                    .recovery_io(cancellation.clone(), Some((dead, &mut *beat)), move || {
                        bundle.copy_into(
                            gathered,
                            recovery_rows::Priority::Fallback,
                            recovery_rows::Duplicates::First,
                        )
                    })
                    .await?;
            }
        }
    }
}

#[cfg(test)]
#[path = "recovery_bundles_tests.rs"]
mod tests;
