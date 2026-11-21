// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A real learner configuration overtakes reads queued behind follower apply.
use super::*;

fn admitted_data(host: &RaftCells) -> Result<u64> {
    let observation = host.performance();
    ensure!(observation.available && !observation.saturated);
    observation
        .operations
        .into_iter()
        .find(|operation| operation.name == "transport_data_admit")
        .map(|operation| operation.succeeded)
        .context("actual data-lane admission observation")
}

impl Drill<'_> {
    pub(super) async fn configuration(
        &self,
        ingress: [Ingress; 2],
        expected: &[CommandReceipt; 2],
    ) -> Result<()> {
        self.leader(1).await?;
        self.caught_up(1).await?;
        self.evidence(&format!("configuration_start_{ingress:?}"))
            .await?;
        let cluster = &self.cluster.managed;
        // The existing ingress-only node is an eligible empty destination.
        // Canonical capacity and a rooted exact-plan permit precede admission;
        // no fixture invents a membership command or installs a runtime owner.
        admit_capacity(
            &cluster.fixtures[3],
            &cluster.hosts[3],
            &cluster.directory,
            240,
        )
        .await?;
        movement::discover(cluster).await?;
        let (_, execution) = movement::admit_replacement(
            cluster,
            movement::ReplacementTarget {
                source: 1,
                obsolete: 2,
                spare: 3,
            },
            [244, 245],
            false,
        )
        .await?;
        movement::discover(cluster).await?;
        let catalog = &cluster.fixtures[0].catalog;
        let cut = catalog.read(TIMEOUT).await?;
        let selected = catalog
            .pending_movements(&cut, TIMEOUT)
            .await?
            .into_iter()
            .find(|m| m.reservation == execution.reservation)
            .context("rooted learner plan")?;
        let authorization = catalog
            .authorize_group(&cut, &cluster.history, TIMEOUT)
            .await?;
        let permit = catalog
            .authorize_membership(&cut, &cluster.history, TIMEOUT)
            .await?;
        let group = cluster.hosts[1].group(cluster.history)?;
        let before_configuration = self.host().group(cluster.history)?.observe(TIMEOUT).await?;
        ensure!(before_configuration.status.leader == Some(2));
        // A real S3 mutation holds this follower's persistence/apply turn while
        // the other two original voters continue. Reads precede the actual
        // configuration messages in this owner's bounded deferred queue.
        let sync = self.filesystem.arm(storage_fault::HOLD_SYNC);
        let (entered, first_receipt) = tokio::join!(
            tokio::time::timeout(TIMEOUT, self.filesystem.entered.notified()),
            self.write(1, "one", 1, 7)
        );
        entered?;
        let first_receipt = first_receipt?;
        ensure!(first_receipt.version.sequence > expected[0].version.sequence);
        let before_reads = self.sample()?;
        let before_callbacks = self.callbacks.queries.load(Ordering::Acquire);
        let ordinary = self.host().resource_usage().host.controller.ordinary;
        ensure!(ordinary.items == 0 && ordinary.max_items == 2);
        let mut reads = Waiters::default();
        reads.start(0, self.read(ingress[0], "one"));
        reads.start(1, self.read(ingress[1], "two"));
        self.wait(
            "two bounded public reads precede the real configuration cut",
            || {
                let usage = self.host().resource_usage();
                Ok(usage.runtime.read_pins.items == 2 && usage.host.controller.ordinary.items == 2)
            },
        )
        .await?;
        self.host().group(cluster.history)?.status().await?;
        ensure!(self.sample()? == before_reads);
        ensure!(self.callbacks.queries.load(Ordering::Acquire) == before_callbacks);
        self.evidence("configuration_reads_admitted_behind_apply")
            .await?;
        let configured = group
            .begin_managed_repair(authorization, permit, selected.plan, TIMEOUT)
            .await?;
        ensure!(configured.membership().revision() > before_configuration.membership_revision);
        ensure!(configured.configuration().learners == [4]);
        ensure!(configured.configuration().voters == [1, 2, 3]);
        let revision = configured.membership().revision();
        drop(configured);
        // Send a real later entry with the committed configuration floor on
        // the data lane. Wait for physical admission while apply remains held;
        // a membership proposal or an observation alone is not the cut.
        let before_data = admitted_data(self.host())?;
        let second_receipt = self.write(1, "two", 1, 8).await?;
        ensure!(second_receipt.version.sequence > expected[1].version.sequence);
        self.wait(
            "post-configuration data admitted behind the held apply",
            || Ok(admitted_data(self.host())? > before_data),
        )
        .await?;
        ensure!(
            self.host()
                .group(cluster.history)?
                .observe(TIMEOUT)
                .await?
                .membership_revision
                == before_configuration.membership_revision
        );
        self.evidence("configuration_committed_and_data_admitted_before_apply")
            .await?;
        drop(sync);
        let first = reads.finish(0).await?;
        let second = reads.finish(1).await?;
        ensure!(
            matches!(first, Read::GenerationRefused(_)),
            "first pending configuration read: {first:?}"
        );
        ensure!(
            matches!(second, Read::GenerationRefused(_)),
            "second pending configuration read: {second:?}"
        );
        ensure!(self.sample()? == [before_reads[0] + 1, before_reads[1] + 1]);
        ensure!(self.callbacks.queries.load(Ordering::Acquire) == before_callbacks);
        let after = self.host().group(cluster.history)?.observe(TIMEOUT).await?;
        ensure!(
            after.membership_revision == revision
                && after.status.term == before_configuration.status.term
        );
        self.drained().await?;
        self.evidence("configuration_refused_and_drained").await?;
        let current = self.host().resource_usage().host.controller.ordinary;
        ensure!(current.max_items == ordinary.max_items && current.max_bytes == ordinary.max_bytes);
        // These reads were refused under their original generation. A fresh
        // invocation can obtain a fresh proof under the applied configuration.
        self.verify(self.read(ingress[0], "one").1.await?, &first_receipt)?;
        self.drained().await?;
        self.verify(self.read(ingress[1], "two").1.await?, &second_receipt)?;
        self.drained().await?;
        ensure!(self.callbacks.queries.load(Ordering::Acquire) == before_callbacks + 2);
        let cut = catalog.read(TIMEOUT).await?;
        let pending = catalog.pending_reservations(&cut, TIMEOUT).await?;
        ensure!(pending.len() == 1 && pending[0].reservation == execution.reservation);
        self.evidence("configuration_fresh_reads_verified").await?;
        eprintln!("public configuration cut {ingress:?}: original-generation reads refused, no callback, fresh invocation verified; learner=4 revision={revision:?}; exact movement debit retained");
        Ok(())
    }
}
