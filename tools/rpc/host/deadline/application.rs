// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native probes run over the ordinary generated service and real peer route.
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{rpc, *};
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use super::super::{
    application::Composite,
    generated::{Count, CounterClient, GetCountRequest, GetCountResponse},
};

#[derive(Clone, Debug)]
pub struct Entry {
    pub node: String,
    pub scope: String,
    pub epoch: u64,
    pub remaining_ms: u64,
}

#[derive(Clone, Debug)]
pub struct Refusal {
    pub child_binding: bool,
    pub new_storage: bool,
    pub cached_write: bool,
    pub commit: bool,
}

pub struct Probe {
    pub armed: AtomicBool,
    pub checkpoint: AtomicBool,
    pub block_ms: u64,
    pub child: Option<&'static str>,
    pub commit_before_cancel: bool,
    pub entries: Mutex<Vec<Entry>>,
    pub refusal: Mutex<Option<Refusal>>,
}

impl Probe {
    pub fn new(block_ms: u64, child: Option<&'static str>) -> Arc<Self> {
        Arc::new(Self {
            armed: AtomicBool::new(false),
            checkpoint: AtomicBool::new(false),
            block_ms,
            child,
            commit_before_cancel: false,
            entries: Mutex::new(Vec::new()),
            refusal: Mutex::new(None),
        })
    }

    pub fn committed(block_ms: u64) -> Arc<Self> {
        let mut probe = Self::new(block_ms, None);
        Arc::get_mut(&mut probe).unwrap().commit_before_cancel = true;
        probe
    }

    fn record(&self, context: &Context) -> Result<()> {
        let identity = context.identity().context("deadline probe needs a cell")?;
        let mut entries = self.entries.lock().unwrap();
        ensure!(entries.len() < 16, "deadline probe entry bound exceeded");
        entries.push(Entry {
            node: context
                .var("DOMAIN_MARKER")
                .context("missing domain marker")?
                .into(),
            scope: identity.scope.clone(),
            epoch: identity.epoch,
            remaining_ms: rpc::BudgetSource::capture(context)?
                .start()?
                .remaining_ms()?,
        });
        Ok(())
    }
}

pub struct App {
    pub inner: Arc<Composite>,
    pub probes: BTreeMap<&'static str, Arc<Probe>>,
}

fn internal(_: impl std::fmt::Display) -> rpc::Failure {
    rpc::Failure::new(rpc::Code::Internal)
}

impl App {
    fn handler(&self, probe: Arc<Probe>) -> Result<Arc<dyn Cell>> {
        let router = rpc::Router::new().method_with_limits(
            self.inner.contract.clone(),
            "example.counter.v1.Counter/GetCount",
            move |context, _, limits| {
                let probe = probe.clone();
                Box::pin(async move {
                    probe.record(&context).map_err(internal)?;
                    let storage = context.storage().map_err(internal)?;
                    if !probe.armed.load(Ordering::Acquire) {
                        let mut writes = 0;
                        for key in [
                            "deadline-transaction",
                            "deadline-late-write",
                            "deadline-committed",
                        ] {
                            writes += i64::from(storage.get_json(key).map_err(internal)?.is_some());
                        }
                        return rpc::encode_message(
                            GetCountResponse {
                                count: Some(Count { value: writes }),
                            },
                            limits,
                        );
                    }
                    if let Some(child) = probe.child {
                        let client = CounterClient::bind(&context, "RUST", child)?;
                        let response = client
                            .get_count_with_options(
                                GetCountRequest {},
                                rpc::CallOptions {
                                    timeout: Some(Duration::from_secs(60)),
                                    ..Default::default()
                                },
                            )
                            .await?;
                        return rpc::encode_message(response, limits);
                    }
                    // Deliberately block an application poll across expiry.
                    // Independent host timers must still enforce the deadline;
                    // cleanup must retain access to this original connection.
                    if probe.commit_before_cancel {
                        storage
                            .put_json("deadline-committed", &json!(true))
                            .map_err(internal)?;
                        storage.sync().await.map_err(internal)?;
                    }
                    let commit = storage
                        .transaction_sync(|transaction| {
                            transaction.put_json("deadline-transaction", &json!(true))?;
                            probe.checkpoint.store(true, Ordering::Release);
                            std::thread::sleep(Duration::from_millis(probe.block_ms));
                            Ok(())
                        })
                        .is_err();
                    let refusal = Refusal {
                        child_binding: CounterClient::bind(&context, "RUST", "never-constructed")
                            .is_err(),
                        new_storage: context.storage().is_err(),
                        cached_write: storage
                            .put_json("deadline-late-write", &json!(true))
                            .is_err(),
                        commit,
                    };
                    let mut observed = probe.refusal.lock().unwrap();
                    if observed.is_some() {
                        return Err(internal("duplicate deadline handler"));
                    }
                    *observed = Some(refusal);
                    Err(internal("deliberately expired handler"))
                })
            },
        )?;
        Ok(Arc::new(router))
    }
}

impl Application for App {
    fn rpc_catalog(&self) -> Option<&rpc::Catalog> {
        self.inner.rpc_catalog()
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.inner.supports_cell(class)
    }

    fn validate_configuration(
        &self,
        options: &cellar::engine_api::WorkerConfigOptions,
    ) -> Result<()> {
        self.inner.validate_configuration(options)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        if identity.class == "RustCounter" {
            if let Some(probe) = identity
                .name
                .as_deref()
                .and_then(|name| self.probes.get(name))
            {
                return self.handler(probe.clone());
            }
        }
        self.inner.create_cell(context, identity)
    }

    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        if matches!(&event, Invocation::Fetch(request) if reqwest::Url::parse(&request.url).is_ok_and(|url| ["/python/", "/v8/", "/node/"].iter().any(|prefix| url.path().starts_with(prefix))))
        {
            let future = self.inner.clone().invoke(context, event);
            return Box::pin(async move {
                let result = future.await;
                if let Err(error) = &result {
                    eprintln!("deadline guest ingress failed: {error:#}");
                }
                result
            });
        }
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                anyhow::bail!("expected deadline ingress");
            };
            let url = reqwest::Url::parse(&request.url)?;
            let query: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
            let name = query.get("name").context("missing deadline cell")?;
            ensure!(
                self.probes.contains_key(name.as_str()),
                "unknown deadline cell"
            );
            let timeout = query
                .get("timeout")
                .map(|value| value.parse::<u64>())
                .transpose()?;
            let client = CounterClient::bind(&context, "RUST", name)?;
            let cancellation = query.get("cancel").map(String::as_str);
            let controller = web::AbortController::new();
            let call = client.get_count_with_options(
                GetCountRequest {},
                rpc::CallOptions {
                    timeout: timeout.map(Duration::from_millis),
                    signal: (cancellation == Some("signal")).then(|| controller.signal()),
                },
            );
            let watched = self.probes[self.probes[name.as_str()].child.unwrap_or(name)].clone();
            let wait_for_checkpoint = async move {
                cellar::asyncrt::timeout(Duration::from_secs(3), async {
                    while !watched.checkpoint.load(Ordering::Acquire) {
                        cellar::asyncrt::sleep(Duration::from_millis(2)).await;
                    }
                })
                .await
                .context("cancelled target never entered its transaction")
            };
            let result = match cancellation {
                Some("signal") => {
                    // Publishing the signal must not require a free application
                    // turn on the caller or target's potentially blocked worker.
                    let cancelling = cellar::asyncrt::spawn(async move {
                        wait_for_checkpoint.await?;
                        controller.abort(None)?;
                        Ok::<_, anyhow::Error>(())
                    });
                    let result = call.await;
                    cancelling.await??;
                    Some(result)
                }
                Some("drop") => {
                    match futures_util::future::select(call, Box::pin(wait_for_checkpoint)).await {
                        futures_util::future::Either::Right((ready, pending)) => {
                            ready?;
                            drop(pending);
                            None
                        }
                        _ => anyhow::bail!("call completed before it could be dropped"),
                    }
                }
                None => Some(call.await),
                _ => anyhow::bail!("unknown cancellation mode"),
            };
            let value = match result {
                Some(Ok(value)) => {
                    json!({"writes":value.count.context("missing probe count")?.value})
                }
                Some(Err(failure)) => json!({"code":failure.code(),"outcome":failure.outcome()}),
                None => json!({"dropped":true}),
            };
            let value = json!({"node":context.var("DOMAIN_MARKER"),"result":value});
            Ok(Output::Http(
                http::Response::from_json(&value)?.into_http(&context)?,
            ))
        })
    }
}
