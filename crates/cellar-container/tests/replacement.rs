// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Host configuration changes must respect the durable resource's retirement.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Result};
use cellar::sdk::*;
use cellar_container::{Adapter, Limits, Operation, Provider};
use serde_json::{json, Value};
use std::{path::Path, sync::atomic::Ordering, sync::Arc, time::Duration};

#[path = "support/e2b.rs"]
mod e2b;
#[path = "../../cellar-javascript/tests/support/native.rs"]
mod native;
#[path = "support/vercel.rs"]
mod vercel;

struct Root(Adapter);

impl Cell for Root {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            let session = self.0.session();
            let limits = Limits {
                max_body_bytes: 4096,
            };
            let route = request.url.rsplit('/').next().unwrap();
            let start = json!({"options":{"env":[["HELLO","world"]]}});
            let value = match route {
                "start" => {
                    session
                        .execute(&context, Operation::Start, start, &limits)
                        .await?
                }
                "reconcile" => {
                    session
                        .execute(&context, Operation::Reconcile, json!({}), &limits)
                        .await?
                }
                "running" => {
                    session
                        .execute(&context, Operation::Running, json!({}), &limits)
                        .await?
                }
                "destroy" => {
                    session
                        .execute(&context, Operation::Destroy, json!({}), &limits)
                        .await?
                }
                "refuse" => {
                    for (operation, input) in [
                        (Operation::Start, start),
                        (Operation::Reconcile, json!({})),
                        (Operation::Running, json!({})),
                        (Operation::Destroy, json!({})),
                        (
                            Operation::ExecOutput,
                            json!({"options":{"cmd":["child-exit"]},"max_bytes":64}),
                        ),
                    ] {
                        ensure!(
                            session
                                .execute(&context, operation, input, &limits)
                                .await
                                .is_err(),
                            "{operation:?} adopted an unretired configuration"
                        );
                    }
                    json!(true)
                }
                "pending-delete" => {
                    ensure!(session
                        .execute(&context, Operation::Destroy, json!({}), &limits)
                        .await
                        .is_err());
                    json!(true)
                }
                "lost-create" => {
                    ensure!(session
                        .execute(&context, Operation::Start, start, &limits)
                        .await
                        .is_err());
                    json!(true)
                }
                _ => anyhow::bail!("unknown replacement route"),
            };
            Ok(Output::Http(
                http::Response::from_json(&value)?.into_http(&context)?,
            ))
        })
    }
}

struct App(Arc<Root>);

impl Application for App {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            Ok(Output::Http(
                context
                    .namespace("COUNTERS")?
                    .get_by_name("alice")
                    .fetch(&context, request)
                    .await?,
            ))
        })
    }

    fn supports_cell(&self, class: &str) -> bool {
        class == "Counter"
    }

    fn create_cell(&self, _: Context, _: &CellIdentity) -> Result<Arc<dyn Cell>> {
        Ok(self.0.clone())
    }
}

enum Peer {
    E2b(e2b::Peer),
    Vercel(vercel::Peer),
}

impl Peer {
    async fn start(vercel: bool) -> Result<Self> {
        Ok(if vercel {
            Self::Vercel(vercel::Peer::start().await?)
        } else {
            Self::E2b(e2b::Peer::start().await?)
        })
    }

    fn provider(&self, namespace: &str) -> Result<Arc<dyn Provider>> {
        use cellar_container::Ownership;
        Ok(match self {
            Self::E2b(peer) => Arc::new(cellar_container::e2b::E2b::new(
                cellar_container::e2b::Config::new(
                    "fixture-team",
                    namespace,
                    "control-key",
                    "fixture-template",
                    Ownership::LocalChecksAndExpiry,
                )?
                .api_endpoint(&peer.address)?
                .envd_gateway(&format!("{}envd/", peer.address))?
                .lifetime_seconds(60)?
                .request_timeout_ms(2000)?,
            )),
            Self::Vercel(peer) => Arc::new(cellar_container::vercel::Vercel::new(
                cellar_container::vercel::Config::new(
                    "fixture-team",
                    "fixture-project",
                    namespace,
                    "control-key",
                    "fixture-image",
                    Ownership::LocalChecksAndExpiry,
                )?
                .api_endpoint(&peer.address)?
                .lifetime_ms(60_000)?
                .request_timeout_ms(2000)?,
            )),
        })
    }

    fn requests(&self) -> usize {
        match self {
            Self::E2b(peer) => peer.state.requests.lock().unwrap().len(),
            Self::Vercel(peer) => peer.state.requests.lock().unwrap().len(),
        }
    }

    fn creates(&self) -> usize {
        match self {
            Self::E2b(peer) => peer.state.creates.load(Ordering::SeqCst),
            Self::Vercel(peer) => peer.state.creates.load(Ordering::SeqCst),
        }
    }

    fn lose_create_reply(&self) {
        match self {
            Self::E2b(peer) => peer.state.lost_create.store(true, Ordering::SeqCst),
            Self::Vercel(peer) => peer.state.lost_create.store(true, Ordering::SeqCst),
        }
    }

    fn defer_delete(&self) {
        match self {
            Self::E2b(peer) => peer.state.defer_delete.store(true, Ordering::SeqCst),
            Self::Vercel(peer) => peer.state.defer_delete.store(true, Ordering::SeqCst),
        }
    }

    fn expire(&self) {
        match self {
            Self::E2b(peer) => {
                peer.state.expire();
                peer.state.defer_delete.store(false, Ordering::SeqCst);
            }
            Self::Vercel(peer) => {
                peer.state.expire();
                peer.state.defer_delete.store(false, Ordering::SeqCst);
            }
        }
    }

    async fn stop(self) -> Result<()> {
        match self {
            Self::E2b(peer) => peer.stop().await,
            Self::Vercel(peer) => peer.stop().await,
        }
    }
}

async fn round(path: &Path, provider: Arc<dyn Provider>, cases: &[(&str, Value)]) -> Result<()> {
    let (node, address) = native::start_configured(
        path,
        Arc::new(App(Arc::new(Root(Adapter::new(provider))))),
        |config, _, _| {
            // Match the existing multi-exchange provider protocol campaigns.
            config.runtime.handler_budget = Duration::from_secs(5);
        },
    )
    .await?;
    let result = async {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        for (route, expected) in cases {
            let response = client.get(format!("{address}/{route}")).send().await?;
            let status = response.status();
            let body = response.text().await?;
            ensure!(status.is_success(), "{route}: {status} {body}");
            ensure!(
                serde_json::from_str::<Value>(&body)? == *expected,
                "{route}: {body}"
            );
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let stopped = node.shutdown(ShutdownMode::Preserve).await?;
    ensure!(
        matches!(stopped, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out)
    );
    result
}

async fn qualify(old: &Peer, new: &Peer) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let old_provider = old.provider("original")?;
    let new_provider = new.provider("replacement")?;
    old.lose_create_reply();
    let creates = old.creates();
    round(
        directory.path(),
        old_provider.clone(),
        &[("lost-create", json!(true))],
    )
    .await?;
    ensure!(
        old.creates() == creates + 1,
        "lost reply did not leave exactly one resource"
    );
    let before = (old.requests(), new.requests());
    round(
        directory.path(),
        new_provider.clone(),
        &[("refuse", json!(true))],
    )
    .await?;
    ensure!(
        (old.requests(), new.requests()) == before,
        "unresolved creation allowed replacement effects"
    );
    round(
        directory.path(),
        old_provider.clone(),
        &[("reconcile", json!({"state":"running","generation":"1"}))],
    )
    .await?;
    ensure!(
        old.creates() == creates + 1,
        "recovery replayed the original creation"
    );
    let before = (old.requests(), new.requests());
    round(
        directory.path(),
        new_provider.clone(),
        &[("refuse", json!(true))],
    )
    .await?;
    ensure!(
        (old.requests(), new.requests()) == before,
        "replacement contacted a provider while its predecessor was running"
    );

    old.defer_delete();
    round(
        directory.path(),
        old_provider.clone(),
        &[
            ("pending-delete", json!(true)),
            ("reconcile", json!({"state":"deleting","generation":"1"})),
        ],
    )
    .await?;
    let before = (old.requests(), new.requests());
    round(
        directory.path(),
        new_provider.clone(),
        &[("refuse", json!(true))],
    )
    .await?;
    ensure!(
        (old.requests(), new.requests()) == before,
        "pending deletion allowed replacement effects"
    );

    old.expire();
    round(
        directory.path(),
        old_provider.clone(),
        &[("reconcile", json!({"state":"deleted","generation":"1"}))],
    )
    .await?;
    let before = (old.requests(), new.requests());
    round(
        directory.path(),
        new_provider.clone(),
        &[
            ("reconcile", json!({"state":"deleted","generation":"1"})),
            ("running", json!(false)),
            ("destroy", Value::Null),
        ],
    )
    .await?;
    ensure!(
        (old.requests(), new.requests()) == before,
        "retired observation contacted a provider"
    );

    let creates = new.creates();
    round(
        directory.path(),
        new_provider.clone(),
        &[
            ("start", json!("2")),
            ("reconcile", json!({"state":"running","generation":"2"})),
        ],
    )
    .await?;
    ensure!(
        new.creates() == creates + 1,
        "replacement did not allocate exactly once"
    );
    let before = (old.requests(), new.requests());
    round(directory.path(), old_provider, &[("refuse", json!(true))]).await?;
    ensure!(
        (old.requests(), new.requests()) == before,
        "old configuration touched its successor"
    );
    round(
        directory.path(),
        new_provider,
        &[
            ("destroy", Value::Null),
            ("reconcile", json!({"state":"deleted","generation":"2"})),
        ],
    )
    .await
}

#[test]
fn replacement_requires_confirmed_retirement_across_configuration_and_provider_changes(
) -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let e2b = Peer::start(false).await?;
            let vercel = Peer::start(true).await?;
            let result = async {
                qualify(&e2b, &e2b).await?;
                qualify(&vercel, &vercel).await?;
                qualify(&e2b, &vercel).await?;
                qualify(&vercel, &e2b).await
            }
            .await;
            e2b.stop().await?;
            vercel.stop().await?;
            result
        })
}
