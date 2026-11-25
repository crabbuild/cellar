// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Opt-in, request-boundary observations; never read during the timed request.

use super::*;

pub(super) fn before(cluster: &ManagedCluster) -> Option<Vec<RaftPerformanceSnapshot>> {
    (std::env::var("CELLAR_PROFILE_EVENT_CONSUME").as_deref() == Ok("1")).then(|| {
        cluster
            .hosts
            .iter()
            .map(|host| host.performance())
            .collect()
    })
}

pub(super) fn after(
    cluster: &ManagedCluster,
    before: Option<Vec<RaftPerformanceSnapshot>>,
    extents: bool,
    axum: bool,
    rustfs: bool,
) {
    let Some(before) = before else { return };
    for (node, (before, host)) in before.iter().zip(&cluster.hosts).enumerate() {
        let after = host.performance();
        // Other work on this host can contribute. Cross-operation samples are
        // not transactional; sums across operations/nodes are not critical-path
        // time. Reopen, saturation or unavailable samples invalidate deltas.
        let comparable = before.source == after.source
            && before.available
            && after.available
            && !before.saturated
            && !after.saturated;
        let operations = before.operations.iter().zip(&after.operations)
            .filter(|(a, b)| a.started != b.started || a.sum_us != b.sum_us)
            .map(|(a, b)| json!({"name":b.name,"same_operation":a.name==b.name,
                "started":b.started.checked_sub(a.started),"succeeded":b.succeeded.checked_sub(a.succeeded),
                "failed":b.failed.checked_sub(a.failed),"abandoned":b.abandoned.checked_sub(a.abandoned),
                "sum_us":b.sum_us.checked_sub(a.sum_us)})).collect::<Vec<_>>();
        let counters = before.counters.iter().zip(&after.counters)
            .filter(|(a, b)| a.value != b.value)
            .map(|(a, b)| json!({"name":b.name,"same_counter":a.name==b.name,"delta":b.value.checked_sub(a.value)}))
            .collect::<Vec<_>>();
        eprintln!(
            "EventStream Consume profile {}",
            json!({"node":node,"extents":extents,
            "axum":axum,"rustfs":rustfs,"comparable":comparable,
            "operations":operations,"counters":counters})
        );
    }
}
