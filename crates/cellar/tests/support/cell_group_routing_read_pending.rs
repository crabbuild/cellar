// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A later public read arrives while the first follower barrier is still pending.
use super::*;

impl Drill<'_> {
    pub(super) async fn pending_late(
        &self,
        ingress: Ingress,
        expected: &CommandReceipt,
        ordinal: u8,
    ) -> Result<CommandReceipt> {
        self.leader(0).await?;
        let follower = Drill {
            run: self.run.clone(),
            local: 1,
            cluster: self.cluster,
            callbacks: self.callbacks,
            filesystem: self.filesystem,
            client: self.client.clone(),
            queries: self.queries,
            retry: self.retry,
        };
        follower.caught_up(0).await?;
        follower
            .evidence(&format!("pending_late_start_{ingress:?}"))
            .await?;
        let before = follower.sample()?;
        let callbacks = self.callbacks.queries.load(Ordering::Acquire);
        let ordinary = follower.host().resource_usage().host.controller.ordinary;
        ensure!(ordinary.items == 0 && ordinary.max_items == 2);
        // This mutation changes the other cell. Both query images therefore
        // have one exact expected version regardless of their order with it.
        let cell = self.cluster.managed.hosts[0].cell(self.history("two")?)?;
        let command = cell.command(
            &cell.contract("Increment")?,
            1i64.to_be_bytes().to_vec(),
            self.options(ordinal),
        );
        let sync = self.filesystem.arm(storage_fault::HOLD_SYNC);
        let mut writing = tokio::task::JoinSet::new();
        writing.spawn(command);
        tokio::time::timeout(TIMEOUT, self.filesystem.entered.notified()).await?;
        let mut reads = Waiters::default();
        reads.start(0, follower.read(ingress, "one"));
        follower
            .wait(
                "first follower barrier initiated behind leader persistence",
                || {
                    let usage = follower.host().resource_usage();
                    Ok(follower.sample()? == [before[0] + 1, before[1]]
                        && usage.runtime.read_pins.items == 1
                        && usage.host.controller.ordinary.items == 1)
                },
            )
            .await?;
        ensure!(self.callbacks.queries.load(Ordering::Acquire) == callbacks);
        reads.start(1, follower.read(ingress, "one"));
        follower
            .wait(
                "later arrival has a second pending follower barrier",
                || {
                    let usage = follower.host().resource_usage();
                    Ok(follower.sample()? == [before[0] + 2, before[1]]
                        && usage.runtime.read_pins.items == 2
                        && usage.host.controller.ordinary.items == 2)
                },
            )
            .await?;
        ensure!(self.callbacks.queries.load(Ordering::Acquire) == callbacks);
        let after = follower.sample()?;
        ensure!(after == [before[0] + 2, before[1]]);
        evidence::emit(
            serde_json::json!({"event": "pending_barrier_cut", "run": self.run,
            "before": before, "after": after, "callbacks_before": callbacks,
            "callbacks_after": self.callbacks.queries.load(Ordering::Acquire)}),
        );
        follower
            .evidence("pending_late_two_barriers_before_any_callback")
            .await?;
        drop(sync);
        let receipt = writing
            .join_next()
            .await
            .context("joined pending-barrier S3 mutation")???;
        ensure!(receipt.durability == Durability::S3);
        evidence::emit(serde_json::json!({
            "event": "receipt", "run": self.run, "id": format!("{:?}", receipt.id),
            "version": format!("{:?}", receipt.version), "witness": format!("{:?}", receipt.witness),
            "required": "S3", "achieved": format!("{:?}", receipt.durability),
            "value": i64::from_be_bytes(receipt.result.as_slice().try_into()?),
        }));
        follower.verify(reads.finish(0).await?, expected)?;
        follower.verify(reads.finish(1).await?, expected)?;
        follower.drained().await?;
        let current = follower.host().resource_usage().host.controller.ordinary;
        ensure!(current.max_items == ordinary.max_items && current.max_bytes == ordinary.max_bytes);
        ensure!(self.callbacks.queries.load(Ordering::Acquire) == callbacks + 2);
        follower.evidence("pending_late_drained").await?;
        Ok(receipt)
    }
}
