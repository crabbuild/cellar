// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Retained event metadata selects history without a redundant name barrier.

use super::*;

fn count(cluster: &ManagedCluster, name: &str) -> Result<u64> {
    cluster.hosts.iter().try_fold(0, |total, host| {
        let sample = host.performance();
        ensure!(sample.available && !sample.saturated);
        let value = sample
            .counters
            .iter()
            .find(|c| c.name == name)
            .with_context(|| format!("missing activation observation {name}"))?
            .value;
        Ok(total + value)
    })
}

pub async fn qualify(real: bool) -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(if real {
            catalog_fault::Store::around(rustfs::store("event-activation-locator", rooted)?)
        } else {
            catalog_fault::Store::default()
        });
        let (app, configuration) = application()?;
        let definitions = [
            "Counter",
            cellar::deploy::EVENT_STREAM_CLASS,
            cellar::deploy::EVENT_CATALOG_CLASS,
        ]
        .into_iter()
        .map(|name| {
            Ok((
                app.resolve_raft_class(name)?,
                if name == "Counter" {
                    Durability::Quorum
                } else {
                    Durability::S3
                },
            ))
        })
        .collect::<Result<Vec<_>>>()?;
        let mut fixture =
            Fixture::with_definition_policies(rooted, definitions, app, store.clone()).await?;
        fixture.configuration = Some(configuration);
        fixture.sql_workers = 1;
        fixture.max_input_bytes = 16 * 1024;
        fixture.effects = Some(RaftEffectOptions {
            callback_timeout_ms: 5000,
            ..Default::default()
        });
        let domain = fixture.domain.clone();
        let mut cluster = Box::pin(ManagedCluster::start_with_applications(
            fixture,
            &[],
            ArchiveSetup::Initialized,
            false,
            None,
            FirstGroupSetup::Normal,
        ))
        .await?;
        let outcome = domain
            .run(async {
                let stream = named_service::activate_at(&cluster, "stream-archive", 0)
                    .await
                    .context("activation fixture stream group")?;
                let catalog = named_service::activate_at(&cluster, "catalog-archive", 1)
                    .await
                    .context("activation fixture catalog group")?;
                for (index, node) in cluster.nodes.iter().enumerate() {
                    node.activate_event_resources().await.with_context(|| {
                        format!("initial retained-metadata activation node={index}")
                    })?;
                }
                named_service::restart(&mut cluster)
                    .await
                    .context("retained metadata restart")?;
                // Settle ordinary post-restart elections before the cost oracle.
                // This warms no namespace range cache or logical-name selection.
                for host in &cluster.hosts {
                    for history in [cluster.history, stream, catalog] {
                        host.group(history)?
                            .quorum_membership(TIMEOUT)
                            .await
                            .context("post-restart metadata group quorum")?;
                    }
                }
                let names = count(&cluster, "cell_name_quorum_selections")?;
                let reads = count(&cluster, "cell_linearizable_read_barriers")?;
                cluster.nodes[0]
                    .activate_event_resources()
                    .await
                    .context("retained metadata activation with fresh quorum")?;
                ensure!(
                    count(&cluster, "cell_name_quorum_selections")? == names,
                    "retained metadata activation forced an independent name ReadIndex"
                );
                ensure!(
                    count(&cluster, "cell_linearizable_read_barriers")? > reads,
                    "retained history replaced the fresh destination quorum"
                );
                // Public name discovery retains its full independent quorum cut.
                let current = cluster.hosts[0]
                    .resolve_cell_history(
                        cluster.history.cluster,
                        cluster.history.namespace,
                        CellId::new("Counter:locator-absent")?,
                        QueryOptions::default(),
                    )
                    .await?;
                ensure!(current.is_none(), "public discovery created an absent name");
                ensure!(
                    count(&cluster, "cell_name_quorum_selections")? > names,
                    "public name lookup lost its quorum selection"
                );
                for node in cluster.nodes.split_off(1) {
                    node.shutdown(cellar::sdk::ShutdownMode::Preserve).await?;
                }
                ensure!(
                    cluster.nodes[0].activate_event_resources().await.is_err(),
                    "a retained local history admitted activation without quorum"
                );
                Ok::<_, anyhow::Error>(())
            })
            .await
            .map_err(anyhow::Error::from)
            .and_then(std::convert::identity);
        let mut cleanup = Vec::new();
        for node in std::mem::take(&mut cluster.nodes) {
            if let Err(error) = node.shutdown(cellar::sdk::ShutdownMode::Preserve).await {
                cleanup.push(error.to_string());
            }
        }
        for host in &cluster.hosts {
            if let Err(error) = host.close().await {
                cleanup.push(error.to_string());
            }
        }
        if outcome.is_err() || !cleanup.is_empty() {
            for root in std::mem::take(&mut cluster.roots) {
                eprintln!(
                    "retained activation locator disk: {}",
                    root.keep().display()
                );
            }
        }
        domain.shutdown().await;
        outcome?;
        ensure!(cleanup.is_empty(), "activation cleanup: {cleanup:?}");
        eprintln!("Retained metadata RustFS={real} extents={rooted}: PASS");
    }
    Ok(())
}
