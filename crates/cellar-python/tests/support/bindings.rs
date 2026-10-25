// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Each binding must preserve its own real invocation/domain authority.
use super::*;

pub(super) fn child() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let code = format!(
        "{}\n{}",
        include_str!("../bridge.py"),
        include_str!("bindings.py")
    );
    let report = run(
        Options::default(),
        Source::new(code, "bindings.py")
            .events([EventKind::Fetch])
            .cell_events("Counter", [EventKind::Fetch, EventKind::Alarm]),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?;
                let mut nodes = Vec::new();
                for name in ["bindings-a", "bindings-b"] {
                    let (node, url) = start_graph(&directory.path().join(name), application.clone(), name, None,
                        |config, _, graph| {
                            config.runtime.handler_budget = Duration::from_secs(5);
                            graph.primary.options.do_classes.extend([
                                cellar::deploy::KV_CLASS.into(), cellar::deploy::SQL_CLASS.into(), cellar::deploy::QUEUE_CLASS.into()]);
                            graph.primary.options.kv_bindings.push(("CACHE".into(), "binding-kv".into()));
                            graph.primary.options.sql_bindings.push(("DB".into(), "binding-sql".into()));
                            graph.primary.options.queue_bindings.push(cellar::engine_api::QueueBinding {
                                environment: "JOBS".into(), queue: "binding-queue".into(), delivery_delay: 0,
                            });
                        }).await?;
                    let marker: String = client.get(format!("{url}/bindings/setup")).send().await?.error_for_status()?.json().await?;
                    ensure!(marker == name);
                    nodes.push((node, url));
                }
                let a = &nodes[0].1;
                let b = &nodes[1].1;
                let held = client.get(format!("{a}/bindings/hold")).send();
                let cross = async {
                    tokio::time::timeout(Duration::from_secs(3), async {
                        loop {
                            if client.get(format!("{b}/bindings/holding")).send().await?.error_for_status()?.json::<bool>().await? { break; }
                            tokio::task::yield_now().await;
                        }
                        Ok::<_, anyhow::Error>(())
                    }).await??;
                    ensure!(client.get(format!("{b}/bindings/cross")).send().await?.error_for_status()?.json::<bool>().await?);
                    client.get(format!("{b}/bindings/release")).send().await?.error_for_status()?;
                    Ok::<_, anyhow::Error>(())
                };
                let (held, crossed) = futures_util::future::join(held, cross).await;
                crossed?;
                ensure!(held?.error_for_status()?.json::<bool>().await?);
                ensure!(client.get(format!("{b}/bindings/expired")).send().await?.error_for_status()?.json::<bool>().await?);
                for (_, url) in &nodes {
                    let state: Value = client.get(format!("{url}/bindings/state")).send().await?.error_for_status()?.json().await?;
                    let name = if url == a { "bindings-a" } else { "bindings-b" };
                    ensure!(state == json!({"authority": name, "forbidden": null, "sql":[{"marker":name}]}),
                        "a retained binding escaped its authority: {state}");
                }
                for (node, _) in nodes.into_iter().rev() { stop(node, &monitor).await?; }
                Ok(())
            })
        },
    )?;
    ensure!(report.complete() && report.native_outcomes.len() == 2);
    Ok(())
}
