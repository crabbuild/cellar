// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Retained diagnostics never supply admission, recovery or capacity proof.
use super::*;
use futures_util::TryStreamExt as _;
use object_store::ObjectStore as _;

pub(crate) async fn observations(cluster: &ManagedCluster) -> Result<()> {
    let timeout = Duration::from_secs(1);
    let observer = cluster
        .hosts
        .first()
        .context("transition diagnostics have no retained source host")?;
    let control = observer.resource_usage().control;
    let catalog = cluster.fixtures[0].catalog.read(timeout).await;
    let nodes = futures_util::future::join_all(cluster.hosts.iter().enumerate().map(
        |(ordinal, host)| async move {
            match host.observe_node(timeout).await {
                Ok(view) => serde_json::json!({
                    "ordinal": ordinal,
                    "available": true,
                    "trust_revision": view.fleet_trust_revision,
                    "startup_enrollment_revision": view.startup_enrollment.as_ref().map(|v| v.revision),
                    "current_enrollment_revision": view.current_enrollment.as_ref().map(|v| v.revision),
                    "current_enrollment_state": view.current_enrollment.as_ref().map(|v| format!("{:?}", v.state)),
                    "engine_groups": view.node.groups,
                    "registered_histories": view.registered_histories,
                    "dormant_histories": view.dormant_histories,
                    "pending_history_registrations": view.pending_history_registrations,
                    "seed_workers": view.seed_workers,
                }),
                Err(_) => serde_json::json!({"ordinal": ordinal, "available": false}),
            }
        },
    ))
    .await;
    let physical = if let Ok(cut) = &catalog {
        let mut rows = Vec::with_capacity(cluster.fixtures.len());
        // Diagnostics must not manufacture a burst of control-lane pressure
        // while observing the failure. Each probe retains its original bound.
        for (ordinal, fixture) in cluster.fixtures.iter().enumerate() {
            rows.push({
                match observer
                    .inspect_peer_physical_inventory(fixture.spec.node, cut.revision(), timeout)
                    .await
                {
                    Ok(view) => serde_json::json!({
                        "ordinal": ordinal,
                        "available": true,
                        "trust_revision": view.view.fleet_trust_revision,
                        "sample_age_ms": view.sample_age_ms,
                        "histories": view.inventory.as_ref().map(|v| v.histories().len()),
                        "initializing": view.inventory.as_ref().map(|v| v.initializing().is_some()),
                    }),
                    Err(error) => serde_json::json!({
                        "ordinal": ordinal,
                        "available": false,
                        // InspectionError has only fixed enum variants.
                        "error_class": format!("{error:?}"),
                    }),
                }
            });
        }
        rows
    } else {
        Vec::new()
    };
    let case = std::thread::current()
        .name()
        .filter(|name| {
            name.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b':' || b == b'-')
        })
        .map(str::to_owned);
    std::fs::write(
        cluster.roots[0]
            .path()
            .join("transition-failure-observations.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "format": 1,
            "transactional": false,
            "case": case,
            "sample_phase": "post_failure_before_close",
            "catalog_revision": catalog.as_ref().ok().map(|cut| cut.revision()),
            "control_before_probes": {
                "bytes": control.bytes,
                "items": control.items,
                "max_bytes": control.max_bytes,
                "max_items": control.max_items,
            },
            "nodes": nodes,
            "physical": physical,
        }))?,
    )?;
    Ok(())
}

/// Retain a nontransactional diagnostic sample of synthetic memory objects.
/// Read memory directly so armed faults cannot hide bytes. External object
/// stores are never listed, read or changed by this diagnostic.
pub(crate) async fn memory(
    store: &fault::Store,
    root: &std::path::Path,
    phase: &'static str,
) -> Result<()> {
    let directory = root.join(format!("transition-memory-{phase}"));
    std::fs::create_dir_all(&directory)?;
    let mut objects = store.inner.list(None).try_collect::<Vec<_>>().await?;
    objects.sort_by(|a, b| a.location.cmp(&b.location));
    let mut manifest = Vec::new();
    for (ordinal, object) in objects.into_iter().enumerate() {
        let response = store.inner.get(&object.location).await?;
        let metadata = response.meta.clone();
        let bytes = response.bytes().await?;
        // Object keys remain metadata, never filesystem paths.
        let file = format!("{ordinal:06}.bin");
        std::fs::write(directory.join(&file), &bytes)?;
        manifest.push(serde_json::json!({
            "object": object.location.as_ref(),
            "file": file,
            "bytes": bytes.len(),
            "e_tag": metadata.e_tag,
            "version": metadata.version,
        }));
    }
    std::fs::write(
        directory.join("manifest.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "format": 1,
            "phase": phase,
            "transactional": false,
            "objects": manifest,
        }))?,
    )?;
    eprintln!(
        "retained managed transition memory: {} objects at {}",
        manifest.len(),
        directory.display()
    );
    Ok(())
}
