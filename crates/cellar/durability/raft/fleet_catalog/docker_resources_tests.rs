// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

pub(super) fn capacity(boot: &BootBinding) -> FleetCapacityAttestation {
    FleetCapacityAttestation {
        boot: boot.clone(),
        profile_revision: 1,
        profile_digest: [1; 32],
        cpu_millis: 1000,
        memory_bytes: 3 * 1024 * 1024 * 1024,
        durable_bytes: 1024 * 1024 * 1024,
        engine_slots: 32,
        memory_swap_disabled: true,
        used_memory_bytes: 0,
        used_durable_bytes: 0,
        evidence_digest: [2; 32],
        evidence_reference: "fixture-capacity".into(),
    }
}

#[tokio::test]
async fn docker_isolation_resources_charge_cache_and_refuse_missing_stats() -> anyhow::Result<()> {
    let f = Fixture::new()?;
    let server = Server::new(&f.boot)?;
    let adapter = f.adapter(&server)?;
    let binding = f.bind(&adapter).await?;
    let capacity = capacity(&f.boot);
    let sample = adapter
        .run(WAIT, adapter.inner.engine.memory(&binding, &capacity))
        .await?;
    ensure!(sample.used == 128 * 1024 * 1024 && sample.limit == capacity.memory_bytes);
    server.state.lock().unwrap().stats["memory_stats"]
        .as_object_mut()
        .unwrap()
        .remove("usage");
    ensure!(matches!(
        adapter
            .run(WAIT, adapter.inner.engine.memory(&binding, &capacity))
            .await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    ensure!(server.mutations().is_empty());
    server.close().await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn docker_isolation_resources_refuse_changed_limits_and_mid_sample_restart(
) -> anyhow::Result<()> {
    let f = Fixture::new()?;
    let server = Server::new(&f.boot)?;
    let adapter = f.adapter(&server)?;
    let binding = f.bind(&adapter).await?;
    let capacity = capacity(&f.boot);
    for field in ["NanoCpus", "Memory", "MemorySwap"] {
        let old = server.state.lock().unwrap().inspect["HostConfig"][field].clone();
        server.state.lock().unwrap().inspect["HostConfig"][field] = json!(0);
        ensure!(matches!(
            adapter
                .run(WAIT, adapter.inner.engine.memory(&binding, &capacity))
                .await,
            Err(FleetCatalogError::Conflict)
        ));
        server.state.lock().unwrap().inspect["HostConfig"][field] = old;
    }
    server.state.lock().unwrap().restart_during_stats = true;
    ensure!(matches!(
        adapter
            .run(WAIT, adapter.inner.engine.memory(&binding, &capacity))
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(server.mutations().is_empty());
    server.close().await?;
    f.domain.shutdown().await;
    Ok(())
}
