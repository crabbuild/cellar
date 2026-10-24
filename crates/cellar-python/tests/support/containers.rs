// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exercise container facades through CPython and the original native invocation.
use super::*;
use std::sync::atomic::Ordering;
#[path = "../../../cellar/tests/support/container_peer.rs"]
mod peer;

pub(super) fn child() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let provider = Arc::new(peer::AuditedProvider::default());
    let report = run(
        Options::default(),
        Source::new(include_str!("containers.py"), "containers.py")
            .events([EventKind::Fetch])
            .cell("Counter")
            .container_provider(provider.clone()),
        move |application, monitor| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(async move {
                    tokio::time::timeout(Duration::from_secs(45), async move {
                        let peer = peer::Peer::start().await?;
                        let (node, address) = start_graph(
                            directory.path(),
                            application,
                            "python-containers",
                            None,
                            |config, services, graph| peer.configure(config, services, graph),
                        )
                        .await?;
                        let client = reqwest::Client::builder()
                            .timeout(Duration::from_secs(20))
                            .build()?;
                        for path in ["all", "borrow", "leak", "cancel"] {
                            let started = tokio::time::Instant::now();
                            let response = client.get(format!("{address}/{path}")).send().await?;
                            let status = response.status();
                            let body = response.text().await?;
                            if path == "cancel" {
                                ensure!(!status.is_success(), "pending receive must expire");
                                ensure!(
                                    started.elapsed() >= Duration::from_secs(9),
                                    "receive failed before the configured handler deadline"
                                );
                            } else {
                                ensure!(
                                    status.is_success() && body == "ok",
                                    "{path}: {status}: {body}"
                                );
                            }
                            tokio::time::timeout(Duration::from_secs(3), async {
                                while peer.websockets.load(Ordering::SeqCst) != 0 {
                                    tokio::time::sleep(Duration::from_millis(10)).await;
                                }
                            })
                            .await?;
                        }
                        stop(node, &monitor).await?;
                        peer.check_options();
                        ensure!(provider.calls.load(Ordering::SeqCst) > 20);
                        ensure!(provider.sessions.load(Ordering::SeqCst) > 1);
                        ensure!(peer.engine.containers.lock().unwrap().is_empty());
                        Ok(())
                    })
                    .await?
                })
        },
    )?;
    ensure!(report.complete());
    Ok(())
}
