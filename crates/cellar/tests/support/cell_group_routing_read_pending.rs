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
        // Archive and other engine metadata also sync the log. Select a sync
        // while the Raft owner has entered its persistence phase, so reads
        // cannot finish before this fixture releases the leader.
        let (candidates, mut syncing) = tokio::sync::mpsc::unbounded_channel();
        *self.filesystem.sync_when.lock().unwrap() = Some(Arc::new(move || {
            let (reply, answer) = std::sync::mpsc::sync_channel(1);
            if candidates.send(reply).is_err() {
                return false;
            }
            answer.recv().unwrap_or(false)
        }));
        let sync = self.filesystem.arm(storage_fault::HOLD_SYNC);
        let mut writing = tokio::task::JoinSet::new();
        writing.spawn(command);
        let leader = self.cluster.managed.hosts[0].group(self.cluster.managed.history)?;
        tokio::time::timeout(TIMEOUT, async {
            loop {
                cellar::asyncrt::select! {
                    _ = self.filesystem.entered.notified() => return Ok::<_, anyhow::Error>(()),
                    Some(reply) = syncing.recv() => {
                        let status = leader.status().await?;
                        let _ = reply.send(!status.ready && status.leader == Some(1));
                    }
                }
            }
        })
        .await
        .context("hold a sync in the selected leader's Raft persistence phase")??;
        drop(syncing);
        follower.evidence("pending_late_sync_entered").await?;
        let held = leader.status().await?;
        ensure!(
            !held.ready && held.leader == Some(1),
            "the held sync must block the selected leader's Raft phase, held={held:?}"
        );
        let mut reads = Waiters::default();
        reads.start(0, follower.read(ingress, "one"));
        let pending = follower
            .wait(
                "first follower barrier initiated behind leader persistence",
                || {
                    let usage = follower.host().resource_usage();
                    ensure!(
                        reads.0.iter().flatten().all(|(_, task)| !task.is_finished()),
                        "first read completed during leader sync hold, counters={:?}, pins={}, ordinary={}, write_completed={}",
                        follower.sample()?,
                        usage.runtime.read_pins.items,
                        usage.host.controller.ordinary.items,
                        writing.try_join_next().is_some()
                    );
                    Ok(follower.sample()? == [before[0] + 1, before[1]]
                        && usage.runtime.read_pins.items == 1
                        && usage.host.controller.ordinary.items == 1)
                },
            )
            .await;
        if pending.is_err() {
            let _ = follower.evidence("pending_late_first_barrier_failed").await;
        }
        pending?;
        ensure!(self.callbacks.queries.load(Ordering::Acquire) == callbacks);
        follower.evidence("pending_late_first_barrier_held").await?;
        reads.start(1, follower.read(ingress, "one"));
        let pending = follower
            .wait(
                "later arrival has a second pending follower barrier",
                || {
                    let usage = follower.host().resource_usage();
                    ensure!(
                        reads.0.iter().flatten().all(|(_, task)| !task.is_finished()),
                        "read completed during leader sync hold, counters={:?}, pins={}, ordinary={}",
                        follower.sample()?,
                        usage.runtime.read_pins.items,
                        usage.host.controller.ordinary.items
                    );
                    Ok(follower.sample()? == [before[0] + 2, before[1]]
                        && usage.runtime.read_pins.items == 2
                        && usage.host.controller.ordinary.items == 2)
                },
            )
            .await;
        if pending.is_err() {
            let _ = follower
                .evidence("pending_late_second_barrier_failed")
                .await;
        }
        pending?;
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
