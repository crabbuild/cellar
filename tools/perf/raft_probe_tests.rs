use super::*;
use cellar::sdk::ExecutionDomain;

#[tokio::test]
async fn completed_and_failed_awaits_keep_separate_stage_times() -> Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let probes = Probes::new("owner".into());
    domain
        .run(async {
            let probe = probes.start("0:5", 5)?;
            probe.status_finished();
            probe.replication_started();
            probe.replication_finished();
            probe.finish(true);
            drop(probe);
            let failed = probes.start("1:5", 5)?;
            failed.status_finished();
            failed.finish(false);
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    let snapshot = probes.snapshot()?;
    let rows = snapshot["observations"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["outcome"], "success");
    assert_eq!(rows[1]["outcome"], "error");
    let values: Vec<_> = [
        "started_us",
        "status_finished_us",
        "replication_started_us",
        "replication_finished_us",
        "finished_us",
    ]
    .iter()
    .map(|key| rows[0][key].as_u64().unwrap())
    .collect();
    assert!(values.windows(2).all(|pair| pair[0] <= pair[1]));
    assert!(rows[1]["replication_started_us"].is_null());
    assert!(rows[1]["replication_finished_us"].is_null());
    assert!(rows[1]["finished_us"].is_u64());
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn cancellation_outside_the_domain_does_not_fabricate_completion() -> Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let probes = Probes::new("owner".into());
    let pending = domain
        .run(async {
            let probe = probes.start("2:0", 0)?;
            probe.status_finished();
            probe.replication_started();
            Ok::<_, anyhow::Error>(probe)
        })
        .await??;
    assert_eq!(probes.snapshot()?["observations"][0]["outcome"], "pending");
    domain.shutdown().await;
    drop(pending);
    let snapshot = probes.snapshot()?;
    let row = &snapshot["observations"][0];
    assert_eq!(row["outcome"], "cancelled");
    assert!(row["replication_started_us"].is_u64());
    assert!(row["replication_finished_us"].is_null());
    assert!(row["finished_us"].is_null());
    Ok(())
}

#[tokio::test]
async fn bounded_eviction_cannot_update_a_new_probe_with_the_same_id() -> Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let probes = Probes::new("owner".into());
    domain
        .run(async {
            let oldest = probes.start("0:0", 0)?;
            for _ in 0..CAPACITY {
                probes.start("0:0", 0)?.finish(true);
            }
            oldest.finish(false);
            drop(oldest);
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    let snapshot = probes.snapshot()?;
    assert_eq!(snapshot["started"], CAPACITY + 1);
    assert_eq!(snapshot["evicted"], 1);
    assert_eq!(snapshot["source"], "owner");
    let rows = snapshot["observations"].as_array().unwrap();
    assert_eq!(rows.len(), CAPACITY);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row["sequence"], index + 1);
        assert_eq!(row["outcome"], "success");
    }
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn invalid_id_and_poison_refuse_diagnostics() -> Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let probes = Probes::new("owner".into());
    domain
        .run(async {
            for id in ["", "1", "1:", ":2", "1:2:3", "1:a", "1:２", &"1".repeat(42)] {
                assert!(probes.start(id, 0).is_err());
            }
        })
        .await?;
    assert_eq!(probes.snapshot()?["started"], 0);
    assert!(std::panic::catch_unwind(|| {
        let _guard = probes.history.lock().unwrap();
        panic!("poison diagnostic history");
    })
    .is_err());
    assert!(probes.snapshot().is_err());
    domain.shutdown().await;
    Ok(())
}
