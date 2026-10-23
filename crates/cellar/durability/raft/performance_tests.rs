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
