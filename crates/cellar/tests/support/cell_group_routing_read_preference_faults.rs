// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual follower persistence, origin metadata refusal and retained SQL callbacks.
use super::*;
use cellar::application::consensus::verification::cell_groups::CellDispatchError;
use cellar::application::consensus::VersionError;

struct MetadataPause(Arc<fault::Store>);

impl MetadataPause {
    fn new(store: Arc<fault::Store>) -> Self {
        assert!(!store
            .deny_routing_metadata_reads
            .swap(true, Ordering::AcqRel));
        Self(store)
    }
}

impl Drop for MetadataPause {
    fn drop(&mut self) {
        self.0
            .deny_routing_metadata_reads
            .store(false, Ordering::Release);
    }
}

impl Drill<'_> {
    pub(super) async fn foreign_minimum(
        &self,
        ingress: Ingress,
        foreign: &CommandReceipt,
    ) -> Result<()> {
        let callbacks = self.callbacks.queries.load(Ordering::Acquire);
        let barriers = self.barriers()?;
        let ordinal = self.queries.fetch_add(1, Ordering::AcqRel) + 1;
        let invocation = ProposalId::new((5000_u128 + u128::from(ordinal)).to_be_bytes())?;
        if matches!(ingress, Ingress::Sdk) {
            let group = self.cluster.managed.history;
            let cell = self.cluster.managed.hosts[0].cell(counter::history(
                group.cluster,
                group.namespace,
                "one",
            )?)?;
            let result = cell
                .query(
                    invocation,
                    &cell.contract("Get")?,
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::AtLeast(foreign.version_token.clone()),
                        timeout: TIMEOUT,
                        ..QueryOptions::default()
                    },
                )
                .await;
            ensure!(matches!(
                result
                    .err()
                    .context("foreign minimum must refuse")?
                    .as_ref(),
                CellDispatchError::Version(VersionError::WrongHistory)
            ));
        } else {
            http::Web::send(
                self.client
                    .get(
                        self.cluster
                            .web()?
                            .url(0, matches!(ingress, Ingress::Axum), "one"),
                    )
                    .header("x-query-id", hex::encode(invocation.bytes()))
                    .header("x-read-consistency", "at-least")
                    .header("x-min-version", foreign.version_token.as_str()),
                format!("foreign minimum {ingress:?}"),
            )
            .await?
            .refused(409, "WrongIncarnation")?;
        }
        self.drained().await?;
        ensure!(
            self.callbacks.queries.load(Ordering::Acquire) == callbacks
                && self.barriers()? == barriers
        );
        emit(
            &self.run_id(),
            serde_json::json!({"event": "foreign_minimum_refused", "ingress": format!("{ingress:?}"),
            "invocation": hex::encode(invocation.bytes()), "callbacks_before": callbacks, "callbacks_after": self.callbacks.queries.load(Ordering::Acquire)}),
        );
        Ok(())
    }

    pub(super) async fn lagged_minimum(
        &self,
        ingress: Ingress,
        ordinal: u8,
        previous: &CommandReceipt,
    ) -> Result<CommandReceipt> {
        self.drained().await?;
        let group = self.cluster.managed.history;
        let follower = &self.cluster.managed.hosts[0];
        let before = follower.group(group)?.observe(TIMEOUT).await?;
        ensure!(before.status.applied.index >= previous.witness.position.index);
        let cell = self.cluster.managed.hosts[1].cell(previous.version.history.clone())?;
        let command = cell.command(
            &cell.contract("Increment")?,
            1_i64.to_be_bytes().to_vec(),
            CommandOptions {
                durability: Durability::S3,
                timeout: TIMEOUT,
                ..CommandOptions::new(
                    CommandId::new(ClientId::new([220 + ordinal; 16])?, 1)?,
                    now()? + 60_000,
                )
            },
        );
        let sync = self.filesystem.arm(storage_fault::HOLD_SYNC);
        let (entered, written) = tokio::join!(
            tokio::time::timeout(TIMEOUT, self.filesystem.entered.notified()),
            command
        );
        entered?;
        let written = written?;
        ensure!(
            written.durability == Durability::S3
                && written.version.sequence > previous.version.sequence
        );
        let lagged_before = follower.group(group)?.observe(TIMEOUT).await?;
        ensure!(lagged_before.status.applied.index < written.witness.position.index);
        let origin = &self.cluster.managed.fixtures[0];
        let store = origin
            .host_store
            .as_ref()
            .context("origin metadata adapter")?;
        let metadata = MetadataPause::new(store.clone());
        let denied = store.denied_routing_metadata_reads.load(Ordering::Acquire);
        let origin_catalog = Fixture::open_catalog(&origin.domain, store.clone(), group.cluster)?;
        ensure!(
            origin_catalog.read(TIMEOUT).await.is_err(),
            "origin metadata fault must refuse a real catalog read"
        );
        ensure!(store.denied_routing_metadata_reads.load(Ordering::Acquire) > denied);
        // The receiver still checks the current directory for every RPC. Only
        // the origin's access to the shared catalog is interrupted here.
        let receiver = &self.cluster.managed.fixtures[1];
        let receiver_catalog_revision = receiver.catalog.read(TIMEOUT).await?.revision();
        let receiver_reads = receiver.store.range_head_reads.load(Ordering::Acquire);
        let before_callbacks = self.callbacks.queries.load(Ordering::Acquire);
        let before_barriers = self.barriers()?;
        let reply = self.query(ingress, Mode::AtLeast, &written).await?;
        self.verify(&reply, &written, 2)?;
        ensure!(
            self.barriers()? == before_barriers,
            "versioned fallback acquired an unrequested quorum proof"
        );
        ensure!(self.callbacks.queries.load(Ordering::Acquire) == before_callbacks + 1);
        // This local attempt never entered SQL. Its cancelled queued intent
        // may drain independently; the follower must still be physically lagged.
        let lagged_at_reply = follower.group(group)?.observe(TIMEOUT).await?;
        ensure!(lagged_at_reply.status.applied.index < written.witness.position.index);
        let receiver_reads_at_reply = receiver.store.range_head_reads.load(Ordering::Acquire);
        ensure!(receiver_reads_at_reply > receiver_reads);
        emit(
            &self.run_id(),
            serde_json::json!({"event": "lagged_minimum_selected", "ingress": format!("{ingress:?}"),
            "invocation": reply["invocation"], "minimum": format!("{:?}", written.version),
            "receipt": format!("{:?}", written.witness), "owners": follower.resource_usage(),
            "applied_before": before.status.applied.index.get(), "required_index": written.witness.position.index.get(),
            "applied_at_reply": lagged_at_reply.status.applied.index.get(), "origin_metadata_denied": true,
            "denied_before": denied,
            "denied_after": store.denied_routing_metadata_reads.load(Ordering::Acquire),
            "receiver_catalog_revision": receiver_catalog_revision,
            "receiver_reads_before": receiver_reads,
            "receiver_reads_after": receiver_reads_at_reply}),
        );
        drop(metadata);
        drop(sync);
        self.drained().await?;
        Ok(written)
    }

    pub(super) async fn cancelled_fallback_pins(
        &self,
        ingress: Ingress,
        expected: &CommandReceipt,
    ) -> Result<()> {
        self.drained().await?;
        let before = self.callbacks.queries.load(Ordering::Acquire);
        let barriers = self.barriers()?;
        let hold = self.callbacks.arm();
        let (invocation, read) = self.read_call(ingress, Mode::Strong, expected);
        let mut reads = tokio::task::JoinSet::new();
        reads.spawn(read);
        self.wait(
            "leader SQL expiry falls back to an independently pinned follower image",
            || {
                let first = self.cluster.managed.hosts[1].resource_usage();
                let second = self.cluster.managed.hosts[0].resource_usage();
                Ok(self.callbacks.queries.load(Ordering::Acquire) == before + 2
                    && first.runtime.read_pins.items == 1
                    && second.runtime.read_pins.items == 1)
            },
        )
        .await?;
        ensure!(
            reads.try_join_next().is_none(),
            "held SQL unexpectedly completed"
        );
        reads.abort_all();
        ensure!(reads
            .join_next()
            .await
            .context("joined cancelled preference caller")?
            .unwrap_err()
            .is_cancelled());
        let origin = self.cluster.managed.hosts[0].resource_usage();
        let leader = self.cluster.managed.hosts[1].resource_usage();
        ensure!(origin.runtime.read_pins.items == 1 && leader.runtime.read_pins.items == 1);
        ensure!(
            origin.host.controller.ordinary.items > 0 && leader.host.controller.ordinary.items > 0
        );
        ensure!(
            origin.host.controller.ordinary.max_items == 2
                && leader.host.controller.ordinary.max_items == 2
        );
        let after = self.barriers()?;
        ensure!(
            after[0] == barriers[0] + 1
                && after[1] == barriers[1] + 1
                && after[2..] == barriers[2..]
        );
        emit(
            &self.run_id(),
            serde_json::json!({"event": "caller_cancelled_physical_pins_retained", "ingress": format!("{ingress:?}"),
            "invocation": hex::encode(invocation.bytes()), "joined": true, "origin": origin, "leader": leader,
            "barriers_before": barriers, "barriers_after": after}),
        );
        drop(hold);
        self.drained().await?;
        let fresh = self.query(ingress, Mode::Strong, expected).await?;
        self.verify(&fresh, expected, 2)?;
        self.drained().await?;
        ensure!(self.callbacks.queries.load(Ordering::Acquire) == before + 3);
        Ok(())
    }
}
