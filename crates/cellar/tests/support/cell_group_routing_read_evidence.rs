// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public request identities and owner observations for the RO-11 history.
use super::*;

pub(super) fn emit(mut event: serde_json::Value) {
    event["case"] = "read_cohort_has_fresh_barrier".into();
    event["schema"] = 1.into();
    eprintln!("PUBLIC_READ_EVIDENCE {event}");
}

pub(super) fn outcome(run: &str, invocation: ProposalId, result: &Result<Read>) {
    let mut event = serde_json::json!({
        "event": "result", "run": run, "invocation": hex::encode(invocation.bytes()),
    });
    match result {
        Ok(Read::Selected(actual, reply)) => {
            event["disposition"] = "selected".into();
            event["actual_invocation"] = hex::encode(actual.bytes()).into();
            event["cell"] = reply["cell"].clone();
            event["value"] = reply["value"].clone();
            event["sequence"] = reply["sequence"].clone();
        }
        Ok(Read::GenerationRefused(actual)) => {
            event["disposition"] = "not_leader".into();
            event["actual_invocation"] = hex::encode(actual.bytes()).into();
        }
        Err(error) => {
            event["disposition"] = "error".into();
            event["error"] = error.to_string().into();
        }
    }
    emit(event);
}

impl Drill<'_> {
    pub(super) async fn evidence(&self, phase: &str) -> Result<()> {
        let mut nodes = Vec::new();
        for (index, host) in self.cluster.managed.hosts.iter().enumerate() {
            let identity = host.identity();
            let performance = host.performance();
            ensure!(performance.available && !performance.saturated);
            let counters: std::collections::BTreeMap<_, _> = performance
                .counters
                .into_iter()
                .filter(|counter| counter.name.starts_with("cell_"))
                .map(|counter| (counter.name, counter.value))
                .collect();
            let mut node = serde_json::json!({
                "node": hex::encode(identity.node.bytes()),
                "disk": hex::encode(identity.disk.bytes()),
                "process": hex::encode(identity.process.bytes()),
                "fleet_boot": format!("{:?}", host.observe_node(TIMEOUT).await?.fleet_boot),
                "owners": host.resource_usage(), "counters": counters,
            });
            if index < 3 {
                let observed = host
                    .group(self.cluster.managed.history)?
                    .observe(TIMEOUT)
                    .await?;
                node["group"] = serde_json::json!({
                    "history": format!("{:?}", observed.replica.scope),
                    "member": observed.replica.member.get(), "term": observed.status.term.get(),
                    "leader": observed.status.leader, "membership_revision": observed.membership_revision.get(),
                    "applied_index": observed.status.applied.index.get(), "committed_index": observed.status.committed.get(),
                    "persisted_index": observed.persisted.get(),
                });
            }
            nodes.push(node);
        }
        // This is the root adapter underneath the per-node wrappers. Count it
        // once: summing nested wrappers would double-count physical requests.
        let store = &self.cluster.managed.fixtures[0].store;
        emit(serde_json::json!({
            "event": "owners", "run": self.run, "request_node": self.local, "phase": phase, "nodes": nodes,
            "callbacks": {"commands": self.callbacks.commands.load(Ordering::Acquire),
                "queries": self.callbacks.queries.load(Ordering::Acquire), "queries_by_cell": self.callbacks.entries()},
            "metadata_attempts": {"range_head_reads": store.range_head_reads.load(Ordering::Acquire),
                "range_page_reads": store.range_page_reads.load(Ordering::Acquire),
                "range_page_puts": store.range_page_puts.load(Ordering::Acquire),
                "registry_writes": store.registry_writes.load(Ordering::Acquire),
                "denied_routing_reads": store.denied_routing_metadata_reads.load(Ordering::Acquire)},
        }));
        Ok(())
    }

    pub(super) async fn invalid_query_ids(&self) -> Result<()> {
        let before_queries = self.callbacks.queries.load(Ordering::Acquire);
        let before_barriers = self.sample()?;
        for axum in [false, true] {
            let url = self.cluster.web()?.url(0, axum, "one");
            for invalid in [
                "0".repeat(32),
                "f".repeat(31),
                "ff".repeat(17),
                "q".repeat(32),
            ] {
                http::Web::send(
                    self.client.get(&url).header("x-query-id", &invalid),
                    format!("invalid query identity axum={axum}"),
                )
                .await?
                .refused(400, "invalid request or unavailable cell")?;
                emit(
                    serde_json::json!({"event": "invalid_request", "run": self.run, "axum": axum, "method": "GET", "query_ids": [invalid], "status": 400}),
                );
            }
            http::Web::send(
                self.client
                    .get(&url)
                    .header("x-query-id", "01".repeat(16))
                    .header("x-query-id", "02".repeat(16)),
                format!("duplicate query identity axum={axum}"),
            )
            .await?
            .refused(400, "invalid request or unavailable cell")?;
            emit(
                serde_json::json!({"event": "invalid_request", "run": self.run, "axum": axum, "method": "GET", "query_ids": ["01".repeat(16), "02".repeat(16)], "status": 400}),
            );
            http::Web::send(
                self.client
                    .post(format!("{url}?delta=1"))
                    .header("x-query-id", "01".repeat(16)),
                format!("query identity requires GET axum={axum}"),
            )
            .await?
            .refused(400, "invalid request or unavailable cell")?;
            emit(
                serde_json::json!({"event": "invalid_request", "run": self.run, "axum": axum, "method": "POST", "query_ids": ["01".repeat(16)], "status": 400}),
            );
        }
        ensure!(self.callbacks.queries.load(Ordering::Acquire) == before_queries);
        ensure!(self.sample()? == before_barriers);
        Ok(())
    }
}
