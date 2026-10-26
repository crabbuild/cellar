// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native durable provider journal contract, independent of a cloud account.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use cellar::sdk::{containers::*, *};
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

#[path = "../../cellar-javascript/tests/support/native.rs"]
mod native;

#[derive(Default)]
struct Probe {
    retained: Mutex<Option<BindingJournal>>,
    creates: AtomicUsize,
    deletes: AtomicUsize,
}

fn pending() -> BindingRecord {
    BindingRecord {
        provider: "e2b-fixture".into(),
        configuration_digest: "a".repeat(64),
        parameters_digest: "d".repeat(64),
        generation: 1,
        request_id: "b".repeat(32),
        resource_id: None,
        intent: BindingIntent::Running,
        observation: BindingObservation::Pending,
    }
}

struct Root(Arc<Probe>);

impl Cell for Root {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            let journal = BindingJournal::open(&context, "primary")?;
            let route = url_path(&request.url);
            match route {
                "reserve" => {
                    ensure!(journal.load(&context)?.is_none());
                    let checkpoint = journal.compare_exchange(&context, None, pending()).await?;
                    ensure!(
                        checkpoint.revision == 1 && journal.load(&context)? == Some(checkpoint)
                    );
                    // The provider's create can be submitted only after the reservation
                    // future acknowledges durability. Simulate a lost backend reply.
                    self.0.creates.fetch_add(1, Ordering::SeqCst);
                    *self.0.retained.lock().unwrap() = Some(journal);
                }
                "duplicate" => {
                    ensure!(journal
                        .compare_exchange(&context, None, pending())
                        .await
                        .is_err());
                    ensure!(self.0.creates.load(Ordering::SeqCst) == 1);
                }
                "recovery-pending" => {
                    let prior = journal.load(&context)?.unwrap();
                    ensure!(prior.revision == 1 && prior.record == pending());
                    ensure!(self.0.creates.load(Ordering::SeqCst) == 1);
                }
                "transaction" => {
                    let storage = context.storage()?;
                    storage.transaction_sync(|_| {
                        ensure!(BindingJournal::open(&context, "primary").is_err());
                        ensure!(journal.load(&context).is_err());
                        Ok(())
                    })?;
                    ensure!(journal.load(&context)?.unwrap().revision == 2);
                }
                "superseded" => {
                    let competing = BindingJournal::open(&context, "contended")?;
                    let original = competing.compare_exchange(&context, None, pending());
                    let replacement = competing.compare_exchange(&context, Some(1), pending());
                    ensure!(original.await.is_err());
                    let current = replacement.await?;
                    ensure!(current.revision == 2 && competing.load(&context)? == Some(current));
                }
                "reconcile" => {
                    let checkpoint = journal.load(&context)?.unwrap();
                    ensure!(checkpoint.revision == 1 && checkpoint.record == pending());
                    ensure!(self.0.creates.load(Ordering::SeqCst) == 1);
                    let mut observed = checkpoint.record;
                    observed.resource_id = Some("remote-instance".into());
                    observed.observation = BindingObservation::Confirmed;
                    let next = journal
                        .compare_exchange(&context, Some(1), observed.clone())
                        .await?;
                    ensure!(next.revision == 2);
                    ensure!(journal
                        .compare_exchange(&context, Some(1), observed)
                        .await
                        .is_err());
                    ensure!(journal.load(&context)? == Some(next));
                }
                "stale" => {
                    ensure!(self
                        .0
                        .retained
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .load(&context)
                        .is_err());
                }
                "foreign" => {
                    ensure!(journal.load(&context)?.is_none());
                    ensure!(self
                        .0
                        .retained
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .load(&context)
                        .is_err());
                }
                "clear" => {
                    let prior = journal.load(&context)?;
                    let storage = context.storage()?;
                    for sql in [
                        "SELECT record FROM __sys_container_bindings",
                        "DELETE FROM __sys_container_bindings",
                        "DROP TABLE __sys_container_bindings",
                    ] {
                        ensure!(storage.sql(sql, &[]).is_err());
                    }
                    storage
                        .put_json("__sys_container_bindings", &json!({"resource_id":"forged"}))?;
                    storage.delete_all()?;
                    ensure!(journal.load(&context)? == prior);
                }
                "recovery" => {
                    let prior = journal.load(&context)?.unwrap();
                    ensure!(
                        prior.revision == 2
                            && prior.record.resource_id.as_deref() == Some("remote-instance")
                    );
                    ensure!(self.0.creates.load(Ordering::SeqCst) == 1);
                }
                "retire" => {
                    let prior = journal.load(&context)?.unwrap();
                    let mut deleted = prior.record;
                    deleted.intent = BindingIntent::Deleted;
                    deleted.observation = BindingObservation::Pending;
                    let pending = journal
                        .compare_exchange(&context, Some(prior.revision), deleted)
                        .await?;
                    self.0.deletes.fetch_add(1, Ordering::SeqCst);
                    let mut confirmed = pending.record;
                    confirmed.observation = BindingObservation::Confirmed;
                    journal
                        .compare_exchange(&context, Some(pending.revision), confirmed)
                        .await?;
                }
                "replace" => {
                    ensure!(self.0.deletes.load(Ordering::SeqCst) == 1);
                    let prior = journal.load(&context)?.unwrap();
                    let mut next = pending();
                    next.generation = 2;
                    next.request_id = "c".repeat(32);
                    let checkpoint = journal
                        .compare_exchange(&context, Some(prior.revision), next)
                        .await?;
                    ensure!(checkpoint.revision == 5 && checkpoint.record.generation == 2);
                    self.0.creates.fetch_add(1, Ordering::SeqCst);
                }
                _ => anyhow::bail!("unknown journal test route"),
            }
            Ok(Output::Http(
                http::Response::from_json(&json!(true))?.into_http(&context)?,
            ))
        })
    }
}

fn url_path(url: &str) -> &str {
    url.rsplit('/').next().unwrap()
}

struct App(Arc<Probe>);

impl Application for App {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            ensure!(
                BindingJournal::open(&context, "primary").is_err(),
                "stateless journal authority"
            );
            let name = if url_path(&request.url) == "foreign" {
                "bob"
            } else {
                "alice"
            };
            let response = context
                .namespace("COUNTERS")?
                .get_by_name(name)
                .fetch(&context, request)
                .await?;
            Ok(Output::Http(response))
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
fn provider_journals_survive_recovery_and_business_clear_without_replaying_creates() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(async {
        let directory=tempfile::tempdir()?;
        let probe=Arc::new(Probe::default());
        let client=reqwest::Client::builder().timeout(std::time::Duration::from_secs(10)).build()?;
        for routes in [&["reserve","duplicate","stale","foreign","clear"][..], &["recovery-pending","duplicate","superseded","reconcile","transaction","clear"][..], &["recovery","retire","replace"][..]] {
            let (node,address)=native::start(directory.path(),Arc::new(App(probe.clone()))).await?;
            let result=async {
                for route in routes {
                    let response=client.get(format!("{address}/{route}")).send().await?;
                    let status=response.status(); let body=response.text().await?;
                    ensure!(status.is_success() && body=="true","{route}: {status} {body}");
                }
                Ok::<_,anyhow::Error>(())
            }.await;
            let shutdown=node.shutdown(ShutdownMode::Preserve).await?;
            ensure!(matches!(shutdown,NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out));
            result?;
        }
        ensure!(probe.creates.load(Ordering::SeqCst)==2 && probe.deletes.load(Ordering::SeqCst)==1);
        Ok(())
    })
}
