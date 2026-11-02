// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(clippy::disallowed_methods)]
use super::*;

async fn registry() -> anyhow::Result<(crate::asyncrt::ExecutionDomain, Arc<Registry>)> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let registry = domain
        .run(async { Registry::new(crate::asyncrt::services().domain_token().unwrap()) })
        .await?;
    Ok((domain, registry))
}

#[tokio::test]
async fn observations_preserve_outcomes_histogram_bounds_and_integer_precision(
) -> anyhow::Result<()> {
    let (domain, registry) = registry().await?;
    for us in [0, 50, 51, 1_000_001] {
        registry.record(Operation::EngineFileSync, Some(true), Some(us));
    }
    registry.record(Operation::EngineFileSync, Some(false), None);
    registry.record(Operation::EngineFileSync, None, Some(25));
    registry.increment(Counter::EngineWriteCompletedBytes, u64::MAX - 1);
    let snapshot = registry.snapshot();
    assert!(snapshot.available && !snapshot.saturated);
    let operation = &snapshot.operations[Operation::EngineFileSync as usize];
    assert_eq!(
        (
            operation.succeeded,
            operation.failed,
            operation.abandoned,
            operation.timing_unavailable,
            operation.count,
            operation.sum_us
        ),
        (4, 1, 1, 1, 5, 1_000_127)
    );
    assert_eq!(operation.buckets[0].count, 3);
    assert_eq!(operation.buckets[1].count, 4);
    assert_eq!(operation.buckets.last().unwrap().count, 5);
    let json = serde_json::to_value(&snapshot)?;
    assert_eq!(
        json["counters"][Counter::EngineWriteCompletedBytes as usize]["value"],
        (u64::MAX - 1).to_string()
    );
    let text = snapshot.prometheus();
    assert!(text.contains("cellar_raft_engine_file_sync_duration_seconds_sum 1.000127"));
    assert!(
        text.contains("cellar_raft_engine_file_sync_duration_seconds_bucket{le=\"0.000050\"} 3")
    );
    assert!(!text.contains(&snapshot.source));
    registry.increment(Counter::EngineWriteCompletedBytes, 2);
    assert!(registry.snapshot().saturated);
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn abandoned_work_and_dead_clock_are_not_reported_as_success() -> anyhow::Result<()> {
    let (domain, registry) = registry().await?;
    drop(registry.timer(Operation::SqlStage));
    registry.timer(Operation::SqlStage).finish(false);
    registry.timer(Operation::SqlStage).finish(true);
    let pending = registry.timer(Operation::SqlStage);
    domain.shutdown().await;
    drop(pending);
    let snapshot = registry.snapshot();
    let operation = &snapshot.operations[Operation::SqlStage as usize];
    assert_eq!(
        (
            operation.started,
            operation.succeeded,
            operation.failed,
            operation.abandoned,
            operation.timing_unavailable
        ),
        (4, 1, 1, 2, 1)
    );
    assert_eq!(operation.count, 3);
    assert!(snapshot.sampled_at_us.is_none());
    Ok(())
}

#[tokio::test]
async fn poisoned_observations_are_unavailable_and_never_break_the_work() -> anyhow::Result<()> {
    let (domain, registry) = registry().await?;
    let operation = &registry.operations[0];
    let _ = std::panic::catch_unwind(|| {
        let _lock = operation.lock().unwrap();
        panic!("test poison");
    });
    registry.timer(Operation::EngineQueue).finish(true);
    assert!(!registry.snapshot().available);
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn recovery_observations_have_distinct_phases_and_minute_scale_buckets() -> anyhow::Result<()>
{
    let (domain, registry) = registry().await?;
    let snapshot = registry.snapshot();
    for name in [
        "recovery_authorize",
        "recovery_peer_fetch",
        "recovery_peer_receive",
        "recovery_append_queue",
        "recovery_append",
        "recovery_archive_fetch",
        "recovery_peer_verify",
        "recovery_archive_replay",
        "recovery_install",
        "recovery_archive_wait",
        "snapshot_encode_queue",
        "archive_physical_admission",
        "archive_physical_queue",
    ] {
        assert!(
            snapshot.operations.iter().any(|sample| sample.name == name),
            "missing recovery phase {name}"
        );
    }
    for us in [60_000_000, 120_000_000, 1_383_780_000] {
        registry.record(Operation::SnapshotEncode, Some(true), Some(us));
    }
    let snapshot = registry.snapshot();
    let sample = &snapshot.operations[Operation::SnapshotEncode as usize];
    for (bound, count) in [(60_000_000, 1), (120_000_000, 2), (1_800_000_000, 3)] {
        assert_eq!(
            sample
                .buckets
                .iter()
                .find(|bucket| bucket.upper_us == Some(bound))
                .map(|bucket| bucket.count),
            Some(count)
        );
    }
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn measured_futures_distinguish_failure_from_cancellation() -> anyhow::Result<()> {
    use futures_util::FutureExt as _;
    let (domain, registry) = registry().await?;
    let result = registry
        .measure(Operation::RecoveryPeerFetch, async {
            Err::<(), _>("refused")
        })
        .await;
    assert_eq!(result, Err("refused"));
    let pending = registry.measure(
        Operation::RecoveryPeerFetch,
        std::future::pending::<Result<(), ()>>(),
    );
    assert!(pending.now_or_never().is_none());
    let snapshot = registry.snapshot();
    let sample = &snapshot.operations[Operation::RecoveryPeerFetch as usize];
    assert_eq!(
        (
            sample.started,
            sample.failed,
            sample.abandoned,
            sample.succeeded
        ),
        (2, 1, 1, 0)
    );
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn image_phase_metrics_keep_roles_zero_work_and_actual_primitive_counts() -> anyhow::Result<()>
{
    use cellar_ltx::internal::image_timing::{Phase, Sample, Timings};
    let (domain, registry) = registry().await?;
    let mut work = Timings::default();
    work.phases[Phase::Sql as usize] = Sample {
        calls: 1,
        micros: 3,
        bytes: 0,
    };
    work.phases[Phase::InventoryWait as usize] = Sample {
        calls: 1,
        micros: 17,
        bytes: 0,
    };
    work.phases[Phase::DirectorySync as usize] = Sample {
        calls: 1,
        micros: 11,
        bytes: 0,
    };
    work.phases[Phase::ExtentRead as usize] = Sample {
        calls: 2,
        micros: 7,
        bytes: 8192,
    };
    work.phases[Phase::ExtentWrite as usize] = Sample {
        calls: 1,
        micros: 5,
        bytes: 4096,
    };
    registry.record_image(ImageWork::StageLeader, &work, true);
    registry.record_image(ImageWork::PrepareLeader, &Timings::default(), true);
    registry.record_image(ImageWork::PrepareFollower, &work, true);
    registry
        .timer(Operation::SqlPrepare)
        .finish_as(true, Some(Operation::SqlPrepareLeader));
    let snapshot = registry.snapshot();
    assert!(snapshot.available && !snapshot.saturated);
    assert_eq!(
        snapshot.operations[Operation::ImageStageLeaderDirectorySync as usize].sum_us,
        11
    );
    assert_eq!(
        snapshot.operations[Operation::ImagePrepareFollowerDirectorySync as usize].sum_us,
        11
    );
    let reuse = &snapshot.operations[Operation::ImagePrepareLeaderDirectorySync as usize];
    assert_eq!((reuse.count, reuse.sum_us), (1, 0));
    let all = &snapshot.operations[Operation::SqlPrepare as usize];
    let leader = &snapshot.operations[Operation::SqlPrepareLeader as usize];
    assert_eq!(
        (all.started, all.count, all.sum_us),
        (leader.started, leader.count, leader.sum_us)
    );
    assert_eq!(
        snapshot.counters[Counter::ImageDirectorySyncCalls as usize].value,
        2
    );
    assert_eq!(
        snapshot.counters[Counter::ImageExtentReadBytes as usize].value,
        16384
    );
    assert_eq!(
        snapshot.counters[Counter::ImageExtentWriteBytes as usize].value,
        8192
    );
    work.unavailable = true;
    registry.record_image(ImageWork::PrepareFollower, &work, true);
    assert!(!registry.snapshot().available);
    domain.shutdown().await;
    Ok(())
}
