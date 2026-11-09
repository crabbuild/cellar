// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded foreground requests run while the owning fault drive joins restarts.
use super::*;
use cellar::application::consensus::verification::{ExecutionError, RuntimeError, TransportError};
use cellar_logic::consensus::CommandFailure;

#[derive(Default)]
pub(super) struct Traffic {
    pub stop: AtomicBool,
    pub phase: AtomicUsize,
    completed: AtomicUsize,
}

fn retryable(error: &CellDispatchError) -> bool {
    fn runtime(error: &RuntimeError) -> bool {
        matches!(
            error,
            RuntimeError::NotLeader
                | RuntimeError::NotReady
                | RuntimeError::NotFound
                | RuntimeError::DeadlineExceeded
                | RuntimeError::Overloaded
        )
    }
    if error.refusal() == Some(&CommandFailure::Unavailable) {
        return true;
    }
    match error {
        CellDispatchError::Runtime(error) => runtime(error),
        CellDispatchError::Execution(ExecutionError::Busy) => true,
        CellDispatchError::Transport(TransportError::Runtime(error)) => runtime(error),
        CellDispatchError::Transport(
            TransportError::Io(_)
            | TransportError::Timeout
            | TransportError::Overloaded
            | TransportError::UnknownGroup
            | TransportError::StalePeer,
        ) => true,
        _ => false,
    }
}

impl Traffic {
    pub async fn wait_phase(&self, phase: usize) -> Result<()> {
        tokio::time::timeout(TIMEOUT * 16, async {
            while self.completed.load(Ordering::Acquire) & (1 << phase) == 0 {
                ensure!(
                    !self.stop.load(Ordering::Acquire),
                    "foreground requests stopped before phase {phase}"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .with_context(|| {
            format!("foreground quorum writes and strong reads in fault phase {phase}")
        })?
    }

    pub async fn run(&self, host: RaftCells, seed: &SeededCells) -> Result<Vec<CommandReceipt>> {
        let handles = seed
            .cells
            .iter()
            .map(|cell| host.cell(cell.clone()))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut receipts = Vec::new();
        let mut failed_commands = 0usize;
        let mut failed_reads = 0usize;
        'traffic: while !self.stop.load(Ordering::Acquire) {
            let phase = self.phase.load(Ordering::Acquire);
            ensure!(phase < 4, "unknown traffic phase");
            let fresh = self.completed.load(Ordering::Acquire) & (1 << phase) == 0;
            for (index, handle) in handles.iter().enumerate() {
                let id = CommandId::new(ClientId::new([248; 16])?, phase as u64 + 1)?;
                // Retry only this exact offered mutation after uncertainty. A
                // failed invocation is counted separately, never a successful
                // request whose latency was silently extended by the test.
                let receipt = loop {
                    if self.stop.load(Ordering::Acquire) {
                        break 'traffic;
                    }
                    let mut options = CommandOptions::new(id, seed.retry);
                    options.timeout = TIMEOUT;
                    match handle
                        .command(
                            &handle.contract("Add")?,
                            0i64.to_be_bytes().to_vec(),
                            options,
                        )
                        .await
                    {
                        Ok(receipt) => break receipt,
                        Err(error) if retryable(error.cause.as_ref()) => {
                            ensure!(error.command == id);
                            failed_commands += 1;
                            eprintln!("handoff offered command failed: phase={phase}, cell={index}, ambiguous={}, cause={}", error.ambiguous, error.cause);
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        Err(error) => return Err(error.into()),
                    }
                };
                ensure!(
                    receipt.id == id
                        && receipt.durability == Durability::Quorum
                        && receipt.result == seed.receipts[index].result
                );
                ensure!(receipt.version.history == seed.receipts[index].version.history);
                ensure!(
                    receipt.version.sequence.get()
                        == seed.receipts[index].version.sequence.get() + phase as u64 + 1,
                    "command replay applied a mutation twice"
                );
                loop {
                    if self.stop.load(Ordering::Acquire) {
                        break 'traffic;
                    }
                    match handle
                        .query(
                            ProposalId::new([249; 16])?,
                            &handle.contract("Get")?,
                            vec![],
                            QueryOptions {
                                consistency: QueryConsistency::Linearizable,
                                timeout: TIMEOUT,
                                ..Default::default()
                            },
                        )
                        .await
                    {
                        Ok(read) => {
                            ensure!(
                                read.result == receipt.result
                                    && read.version.covers(&receipt.version) == Ok(true)
                            );
                            break;
                        }
                        Err(error) if retryable(error.as_ref()) => {
                            failed_reads += 1;
                            eprintln!("handoff offered strong read failed: phase={phase}, cell={index}, cause={error}");
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                if fresh {
                    receipts.push(receipt);
                }
            }
            self.completed.fetch_or(1 << phase, Ordering::Release);
            // Once the phase's fresh mutations are recorded, keep replaying
            // those exact IDs and reading while the driver stops/reboots hosts.
            // This bounds the receipt bank without making the load go idle.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        eprintln!("handoff foreground: unique acknowledged mutations={}, failed command invocations={failed_commands}, failed read invocations={failed_reads}", receipts.len());
        Ok(receipts)
    }

    pub async fn audit(
        &self,
        host: &RaftCells,
        seed: &SeededCells,
        receipts: &[CommandReceipt],
    ) -> Result<()> {
        ensure!(receipts.len() == 8 && self.completed.load(Ordering::Acquire) == 15);
        for (index, cell) in seed.cells.iter().enumerate() {
            let handle = host.cell(cell.clone())?;
            for phase in 0..4 {
                let original = &receipts[phase * 2 + index];
                let found = handle
                    .receipt(
                        CellReceiptRequest {
                            id: original.id,
                            contract: handle.contract("Add")?,
                            input: 0i64.to_be_bytes().to_vec(),
                            durability: Durability::Quorum,
                        },
                        TIMEOUT,
                    )
                    .await?;
                let receipt = found
                    .receipt()
                    .context("acknowledged load receipt missing after repair")?;
                ensure!(
                    receipt.command() == original.id
                        && receipt.result() == original.result.as_slice()
                );
                ensure!(
                    found.observation().version.sequence.get()
                        == seed.receipts[index].version.sequence.get() + 4
                );
            }
            let replay = handle
                .command(
                    &handle.contract("Add")?,
                    [7i64, 11][index].to_be_bytes().to_vec(),
                    CommandOptions::new(seed.receipts[index].id, seed.retry),
                )
                .await?;
            ensure!(
                replay.id == seed.receipts[index].id
                    && replay.result == seed.receipts[index].result
            );
            let fresh = handle
                .command(
                    &handle.contract("Add")?,
                    1i64.to_be_bytes().to_vec(),
                    CommandOptions::new(CommandId::new(ClientId::new([250; 16])?, 1)?, seed.retry),
                )
                .await?;
            ensure!(
                fresh.durability == Durability::Quorum
                    && fresh.result == ([7i64, 11][index] + 1).to_be_bytes()
            );
            ensure!(
                fresh.version.sequence.get() == seed.receipts[index].version.sequence.get() + 5
            );
            let read = handle
                .query(
                    ProposalId::new([251; 16])?,
                    &handle.contract("Get")?,
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::Linearizable,
                        timeout: TIMEOUT,
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(read.result == fresh.result && read.version.covers(&fresh.version) == Ok(true));
        }
        Ok(())
    }
}
