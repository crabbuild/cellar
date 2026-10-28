// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Running native host qualification of the catalog, not child compute execution.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{containers::*, *};
use serde_json::json;
use std::sync::{Arc, Mutex};

#[path = "../../cellar-javascript/tests/support/native.rs"]
mod native;

#[derive(Default)]
struct Probe {
    retained: Mutex<Option<SandboxCatalog>>,
    first: Mutex<Option<SandboxAllocation>>,
    parent: Mutex<Option<SandboxParent>>,
}

struct Root(Arc<Probe>);

impl Cell for Root {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            let catalog = SandboxCatalog::open(&context)?;
            let digest = "b".repeat(64);
            match route(&request.url) {
                "reserve" => {
                    ensure!(catalog.parent(&context)?.is_none());
                    ensure!(catalog
                        .reserve(&context, "build", "MISSING", &digest)
                        .await
                        .is_err());
                    ensure!(
                        catalog.parent(&context)?.is_none(),
                        "invalid namespace initialized catalog"
                    );
                    let first = catalog
                        .reserve(&context, "build", "COUNTERS", &digest)
                        .await?;
                    ensure!(first.fresh && first.allocation.generation == 1);
                    let duplicate = catalog
                        .reserve(&context, "build", "COUNTERS", &digest)
                        .await?;
                    ensure!(!duplicate.fresh && duplicate.allocation == first.allocation);
                    // Drop before observing durability. The next event must retain
                    // the reservation, not authorize a second creation attempt.
                    drop(catalog.reserve(&context, "cancelled", "COUNTERS", &digest));
                    *self.0.first.lock().unwrap() = Some(first.allocation);
                    *self.0.parent.lock().unwrap() = catalog.parent(&context)?;
                    *self.0.retained.lock().unwrap() = Some(catalog);
                }
                "stale" | "foreign" => {
                    ensure!(self
                        .0
                        .retained
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .parent(&context)
                        .is_err());
                    if route(&request.url) == "foreign" {
                        let first = self.0.first.lock().unwrap().clone().unwrap();
                        let foreign = catalog
                            .reserve(&context, "build", "COUNTERS", &digest)
                            .await?;
                        ensure!(foreign.allocation.execution_scope != first.execution_scope);
                        ensure!(catalog.begin_delete(&context, &first).await.is_err());
                    }
                }
                "clear" => {
                    let before = catalog.list(&context, None, 256)?;
                    let db = context.storage()?;
                    for query in [
                        "SELECT * FROM __sys_sandbox_parent",
                        "DELETE FROM __sys_sandbox_allocations",
                        "DROP TABLE __sys_sandbox_schema",
                    ] {
                        ensure!(db.sql(query, &[]).is_err());
                    }
                    db.put_json("__sys_sandbox_parent", &json!({"state":"deleted"}))?;
                    db.delete_all()?;
                    ensure!(catalog.list(&context, None, 256)? == before);
                }
                "recovered" => {
                    ensure!(catalog.parent(&context)? == *self.0.parent.lock().unwrap());
                    ensure!(catalog.get(&context, "build")? == *self.0.first.lock().unwrap());
                    let cancelled = catalog
                        .reserve(&context, "cancelled", "COUNTERS", &digest)
                        .await?;
                    ensure!(
                        !cancelled.fresh
                            && cancelled.allocation.state == SandboxAllocationState::Reserved
                    );
                    let first = catalog.get(&context, "build")?.unwrap();
                    let ready = catalog.confirm_ready(&context, &first).await?;
                    ensure!(ready.state == SandboxAllocationState::Ready);
                    ensure!(catalog.confirm_deleted(&context, &ready).await.is_err());
                    let deleting = catalog.begin_delete(&context, &ready).await?;
                    let page = catalog.cleanup(&context, None, 1)?;
                    ensure!(page.allocations == vec![deleting]);
                    ensure!(catalog
                        .reserve(&context, "build", "COUNTERS", &digest)
                        .await
                        .is_err());
                }
                "transactions" => {
                    context.storage()?.transaction_sync(|_| {
                        ensure!(SandboxCatalog::open(&context).is_err());
                        ensure!(catalog.list(&context, None, 256).is_err());
                        Ok(())
                    })?;
                    ensure!(catalog.list(&context, None, 0).is_err());
                    ensure!(catalog.list(&context, None, 257).is_err());
                    ensure!(catalog
                        .reserve(&context, "../build", "COUNTERS", &digest)
                        .await
                        .is_err());
                    ensure!(catalog
                        .reserve(&context, "bad-digest", "COUNTERS", "bad")
                        .await
                        .is_err());
                }
                "replacement" => {
                    let deleting = catalog.get(&context, "build")?.unwrap();
                    ensure!(deleting.state == SandboxAllocationState::Deleting);
                    catalog.confirm_deleted(&context, &deleting).await?;
                    let replacement = catalog
                        .reserve(&context, "build", "COUNTERS", &digest)
                        .await?;
                    ensure!(replacement.fresh && replacement.allocation.generation == 2);
                    ensure!(
                        replacement.allocation.execution_scope
                            != self
                                .0
                                .first
                                .lock()
                                .unwrap()
                                .as_ref()
                                .unwrap()
                                .execution_scope
                    );
                    ensure!(catalog.confirm_deleted(&context, &deleting).await.is_err());
                }
                "close" => {
                    let pending = catalog.reserve(&context, "during-close", "COUNTERS", &digest);
                    let close = catalog.begin_retirement(&context);
                    ensure!(
                        pending.await.is_err(),
                        "superseded reservation acknowledged after close"
                    );
                    let closing = close.await?;
                    ensure!(closing.state == SandboxParentState::Deleting);
                    ensure!(catalog
                        .reserve(&context, "late", "COUNTERS", &digest)
                        .await
                        .is_err());
                    ensure!(catalog.finish_retirement(&context).await.is_err());
                    *self.0.parent.lock().unwrap() = Some(closing);
                }
                "cleanup" => {
                    ensure!(catalog.parent(&context)? == *self.0.parent.lock().unwrap());
                    let mut cursor = None;
                    let mut count = 0;
                    loop {
                        let page = catalog.cleanup(&context, cursor.as_deref(), 1)?;
                        ensure!(page.allocations.len() <= 1);
                        for allocation in page.allocations {
                            ensure!(catalog.confirm_ready(&context, &allocation).await.is_err());
                            let deleting = catalog.begin_delete(&context, &allocation).await?;
                            catalog.confirm_deleted(&context, &deleting).await?;
                            count += 1;
                        }
                        cursor = page.next;
                        if cursor.is_none() {
                            break;
                        }
                    }
                    ensure!(count == 3);
                    ensure!(catalog.cleanup(&context, None, 256)?.allocations.is_empty());
                    ensure!(
                        catalog.finish_retirement(&context).await?.state
                            == SandboxParentState::Deleted
                    );
                    ensure!(catalog
                        .reserve(&context, "after-retirement", "COUNTERS", &digest)
                        .await
                        .is_err());
                }
                "tombstone" => {
                    ensure!(
                        catalog.parent(&context)?.unwrap().state == SandboxParentState::Deleted
                    );
                    ensure!(catalog
                        .list(&context, None, 256)?
                        .allocations
                        .iter()
                        .all(|allocation| allocation.state == SandboxAllocationState::Deleted));
                    ensure!(
                        catalog.begin_retirement(&context).await?.state
                            == SandboxParentState::Deleted
                    );
                    ensure!(catalog
                        .reserve(&context, "build", "COUNTERS", &digest)
                        .await
                        .is_err());
                }
                _ => anyhow::bail!("unknown allocation test route"),
            }
            Ok(Output::Http(
                http::Response::from_json(&json!(true))?.into_http(&context)?,
            ))
        })
    }
}

fn route(url: &str) -> &str {
    url.rsplit('/').next().unwrap()
}

struct App(Arc<Probe>);

impl Application for App {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            ensure!(SandboxCatalog::open(&context).is_err());
            let name = if route(&request.url) == "foreign" {
                "bob"
            } else {
                "alice"
            };
            Ok(Output::Http(
                context
                    .namespace("COUNTERS")?
                    .get_by_name(name)
                    .fetch(&context, request)
                    .await?,
            ))
        })
    }

    fn supports_cell(&self, class: &str) -> bool {
        class == "Counter"
    }

    fn create_cell(&self, _: Context, _: &CellIdentity) -> Result<Arc<dyn Cell>> {
        Ok(Arc::new(Root(self.0.clone())))
    }
}

#[test]
fn named_catalog_recovers_retirement_and_never_reopens_uncertain_or_retired_children() -> Result<()>
{
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(async {
        let directory = tempfile::tempdir()?;
        let probe = Arc::new(Probe::default());
        let client = reqwest::Client::builder().timeout(std::time::Duration::from_secs(10)).build()?;
        for routes in [
            &["reserve","stale","foreign","clear"][..],
            &["recovered","transactions","clear"][..],
            &["replacement","close","clear"][..],
            &["cleanup","clear"][..],
            &["tombstone"][..],
        ] {
            let (node,address) = native::start(directory.path(),Arc::new(App(probe.clone()))).await?;
            let result = async {
                for route in routes {
                    let response = client.get(format!("{address}/{route}")).send().await.with_context(||format!("{route} request"))?;
                    let status = response.status();
                    let body = response.text().await?;
                    ensure!(status.is_success() && body == "true","{route}: {status} {body}");
                }
                Ok::<_,anyhow::Error>(())
            }.await;
            let shutdown = node.shutdown(ShutdownMode::Preserve).await?;
            ensure!(matches!(shutdown,NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out));
            result?;
        }
        Ok(())
    })
}
