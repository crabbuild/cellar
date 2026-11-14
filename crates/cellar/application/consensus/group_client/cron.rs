// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One joined bootstrap owner for the node's declared cron namespaces. Due
//! callbacks use the ordinary shared scheduler after enrollment completes.

use super::services::ControlServiceKind;
use super::*;
use cellar_logic::consensus::ProposalId;

impl RaftCells {
    pub(crate) async fn cron_history(
        &self,
        scope: &str,
        input: Vec<u8>,
        timeout: Duration,
    ) -> anyhow::Result<serde_json::Value> {
        use rand::RngCore as _;
        let candidate = self
            .handle
            .core
            .host
            .cron_cells()?
            .into_iter()
            .find(|(cell, _)| cell.cell.as_str() == scope)
            .ok_or_else(|| anyhow::anyhow!("cron namespace is not installed"))?
            .0;
        let started = self.handle.domain.mono_ms()?;
        let options = QueryOptions {
            consistency: QueryConsistency::Linearizable,
            timeout,
            ..Default::default()
        };
        let Some(history) = self
            .resolve_cell_history(
                candidate.cluster,
                candidate.namespace,
                candidate.cell,
                options.clone(),
            )
            .await?
        else {
            return Ok(
                serde_json::json!({"items":[],"next_cursor":null,"observed_at":crate::asyncrt::wall_ms(),"retention_limit":256,"coverage":"retained","active_alarm":null}),
            );
        };
        let remaining = timeout.as_millis().saturating_sub(u128::from(
            self.handle.domain.mono_ms()?.saturating_sub(started),
        ));
        anyhow::ensure!(
            remaining > 0,
            crate::raft::completion::RuntimeError::DeadlineExceeded
        );
        let timeout = Duration::from_millis(u64::try_from(remaining)?);
        let cell = self.cell(history)?;
        let mut random = [0; 16];
        crate::asyncrt::rng("cron-history-query").fill_bytes(&mut random);
        let result = cell
            .query(
                ProposalId::new(random)?,
                &cell.contract("__cronHistory")?,
                input,
                QueryOptions { timeout, ..options },
            )
            .await?;
        Ok(serde_json::from_slice(&result.result)?)
    }

    pub(crate) fn start_cron_bootstrap(&self) -> Result<(), CellGroupControllerError> {
        let cells = self
            .handle
            .core
            .host
            .cron_cells()
            .map_err(|_| CellGroupControllerError::Invalid("cron namespace declarations"))?;
        if cells.is_empty() {
            return Ok(());
        }
        let bytes = cells
            .iter()
            .try_fold(4096usize, |total, (cell, _)| {
                total.checked_add(cell.cell.as_str().len().saturating_add(256))
            })
            .ok_or(CellGroupControllerError::Runtime(
                crate::raft::completion::RuntimeError::Overloaded,
            ))?;
        let lease = self
            .register_control_service(ControlServiceKind::Cron, bytes)
            .map_err(|error| super::startup::catalog_error(error.into()))?;
        let owner = self.internal_observer();
        let domain = self.handle.domain.clone();
        domain.spawn_cleanup(async move {
            let mut stop = lease.stop_receiver();
            let work = async {
                for (cell, creation) in cells {
                    loop {
                        if owner.is_closed() {
                            return;
                        }
                        match owner.enroll_cron(&cell, creation).await {
                            Ok(()) => break,
                            Err(error) => tracing::debug!(%error, "cron enrollment awaits shared authority"),
                        }
                        let Ok(now) = owner.handle.domain.mono_ms() else { return };
                        let Ok(delay) = owner.handle.domain.sleep_until(now.saturating_add(100)) else { return };
                        delay.await;
                    }
                }
            };
            crate::asyncrt::select_biased! {
                "joined cron bootstrap";
                _=stop.changed()=>(),
                _=work=>(),
            }
            drop(lease);
        }).map_err(|_| CellGroupControllerError::Closed)?.detach();
        Ok(())
    }

    async fn enroll_cron(
        &self,
        candidate: &CellHistory,
        creation: ProposalId,
    ) -> anyhow::Result<()> {
        let timeout = Duration::from_secs(5);
        if self
            .resolve_cell_history(
                candidate.cluster,
                candidate.namespace,
                candidate.cell.clone(),
                QueryOptions {
                    consistency: QueryConsistency::Linearizable,
                    timeout,
                    ..Default::default()
                },
            )
            .await?
            .is_some()
        {
            return Ok(());
        }
        // Every boot/node uses the same creation identity. An unknown response
        // can only reconcile that original enrollment, never rearm a new alarm.
        self.cell(candidate.clone())?
            .enroll(
                CellEnrollmentRequest {
                    creation,
                    durability: Durability::Quorum,
                },
                timeout,
            )
            .await?;
        Ok(())
    }
}
