// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A cold factory can outlive the call that entered it. SDK authority and
//! generated handler admission must expire independently of its native turn.

use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{rpc, *};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};

use super::{counter, generated, native};
use generated::{
    increment_response, CounterClient, CounterHandler, CounterRegistration, GetCountRequest,
    GetCountResponse, IncrementRequest, IncrementResponse,
};

#[derive(Default)]
struct Factory {
    entered: AtomicBool,
    finished: AtomicBool,
    release: Mutex<bool>,
    changed: Condvar,
    refusals: Mutex<Option<Refusals>>,
    increments: AtomicUsize,
    controller: Mutex<Option<web::AbortController>>,
}

#[derive(Clone, Copy, Debug)]
struct Refusals {
    new_storage: bool,
    child_binding: bool,
    cached_write: bool,
    transaction: bool,
}

impl Factory {
    fn release(&self) {
        *self.release.lock().unwrap() = true;
        self.changed.notify_all();
    }

    fn construct(&self, context: &Context) -> Result<()> {
        // This handle is known to work before the original call expires.
        let cached = context.storage()?;
        ensure!(cached.get_json("count")?.is_none());
        self.entered.store(true, Ordering::Release);
        let released = self
            .changed
            .wait_timeout_while(
                self.release.lock().unwrap(),
                Duration::from_secs(5),
                |value| !*value,
            )
            .map_err(|_| anyhow::anyhow!("cold-factory release gate poisoned"))?
            .0;
        ensure!(*released, "cold factory was not released by its observer");
        drop(released);

        let new_storage = context.storage().is_err();
        let child_binding = CounterClient::bind(context, "COUNTERS", "never-created").is_err();
        let cached_write = cached.put_json("late-factory-write", &true.into()).is_err();
        let transaction = cached
            .transaction_sync(|transaction| {
                transaction.put_json("late-factory-transaction", &true.into())
            })
            .is_err();
        *self.refusals.lock().unwrap() = Some(Refusals {
            new_storage,
            child_binding,
            cached_write,
            transaction,
        });
        self.finished.store(true, Ordering::Release);
        Ok(())
    }
}

// Release even if a network/assertion failure exits the observation early.
struct Release(Arc<Factory>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct Counter(Arc<Factory>);

impl CounterHandler for Counter {
    fn increment(
        self: Arc<Self>,
        context: Context,
        request: IncrementRequest,
    ) -> rpc::HandlerFuture<IncrementResponse> {
        self.0.increments.fetch_add(1, Ordering::AcqRel);
        Arc::new(counter::Counter).increment(context, request)
    }

    fn get_count(
        self: Arc<Self>,
        context: Context,
        request: GetCountRequest,
    ) -> rpc::HandlerFuture<GetCountResponse> {
        Box::pin(async move {
            let storage = context
                .storage()
                .map_err(|_| rpc::Failure::new(rpc::Code::Internal))?;
            for key in ["late-factory-write", "late-factory-transaction"] {
                if storage
                    .get_json(key)
                    .map_err(|_| rpc::Failure::new(rpc::Code::Internal))?
                    .is_some()
                {
                    return Err(rpc::Failure::new(rpc::Code::Internal));
                }
            }
            Arc::new(counter::Counter).get_count(context, request).await
        })
    }
}

async fn request(client: &reqwest::Client, base: &str, path: &str) -> Result<serde_json::Value> {
    let response = client.get(format!("{base}{path}")).send().await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(status.is_success(), "cold factory {path}: {status}: {body}");
    Ok(serde_json::from_str(&body)?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadline_during_cold_factory_refuses_late_work() -> Result<()> {
    exercise(Stop::Deadline).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_during_cold_factory_refuses_late_work() -> Result<()> {
    exercise(Stop::Cancel).await
}

enum Stop {
    Deadline,
    Cancel,
}

async fn exercise(stop: Stop) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let factory = Arc::new(Factory::default());
    let factory_for_cell = factory.clone();
    let factory_for_ingress = factory.clone();
    let app = Registry::new(move |context, invocation| {
        let observed = factory_for_ingress.clone();
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                anyhow::bail!("expected HTTP ingress")
            };
            let path = url::Url::parse(&request.url)?;
            let client = CounterClient::bind(&context, "COUNTERS", "cold-factory")?;
            let value = if path.path() == "/read" {
                serde_json::json!({"count":client.get_count(GetCountRequest{}).await?.count.context("missing count")?.value})
            } else {
                let signal = if path.path() == "/cancel" {
                    let controller = web::AbortController::new();
                    let signal = controller.signal();
                    *observed.controller.lock().unwrap() = Some(controller);
                    Some(signal)
                } else {
                    None
                };
                let result = client
                    .increment_with_options(
                        IncrementRequest {
                            delta: Some(1),
                            operation_id: "cold-factory-operation".into(),
                        },
                        rpc::CallOptions {
                            timeout: (path.path() == "/expire")
                                .then_some(Duration::from_millis(200)),
                            signal,
                        },
                    )
                    .await;
                match result {
                    Err(failure) => serde_json::json!({"code":failure.code(),"outcome":failure.outcome()}),
                    Ok(reply) => {
                        let Some(increment_response::Result::Count(count)) = reply.result else {
                            anyhow::bail!("unexpected business result");
                        };
                        serde_json::json!({"count":count.value})
                    }
                }
            };
            Ok(Output::Http(http::Response::from_json(&value)?.into_http(&context)?))
        })
    })
    .cell("Counter", move |context, _| {
        if !factory_for_cell.entered.load(Ordering::Acquire) {
            factory_for_cell.construct(&context)?;
        }
        Ok(Arc::new(CounterRegistration::register(
            rpc::Router::new(),
            Arc::new(Counter(factory_for_cell.clone())),
        )?))
    })?
    .rpc_service(
        "Counter",
        "example.counter.v1.Counter",
        Arc::new(rpc::Contract::from_descriptor(
            include_bytes!("../../../../tools/rpc/fixtures/counter.binpb"),
            rpc::Limits::default(),
        )?),
    )?;
    // This probe has its own explicit parent policy. The child's 200 ms budget
    // must expire first; existing bridge fixtures retain their 250 ms defaults.
    let (node, base) = native::start_configured(directory.path(), Arc::new(app), |config, _, _| {
        config.runtime.handler_budget = Duration::from_secs(3)
    })
    .await?;
    let release = Release(factory.clone());
    let result = async {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        let expired = match stop {
            Stop::Deadline => request(&client, &base, "/expire").await?,
            Stop::Cancel => {
                let cancel = async {
                    tokio::time::timeout(Duration::from_secs(2), async {
                        while !factory.entered.load(Ordering::Acquire) {
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                    })
                    .await
                    .context("cancellation did not reach its cold factory")?;
                    factory
                        .controller
                        .lock()
                        .unwrap()
                        .as_ref()
                        .context("missing cancellation controller")?
                        .abort(None)?;
                    Ok::<_, anyhow::Error>(())
                };
                let (response, cancelled) =
                    tokio::join!(request(&client, &base, "/cancel"), cancel);
                cancelled?;
                response?
            }
        };
        let code = match stop {
            Stop::Deadline => "DeadlineExceeded",
            Stop::Cancel => "Cancelled",
        };
        ensure!(
            factory.entered.load(Ordering::Acquire),
            "call ended before factory entry"
        );
        ensure!(
            !factory.finished.load(Ordering::Acquire),
            "factory escaped its hold"
        );
        ensure!(
            expired == serde_json::json!({"code":code,"outcome":"unknown"}),
            "incorrect cold-factory result: {expired}"
        );
        factory.release();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !factory.finished.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .context("cold factory did not finish")?;
        let refusals = factory
            .refusals
            .lock()
            .unwrap()
            .context("factory did not report its SDK checks")?;
        ensure!(
            refusals.new_storage
                && refusals.child_binding
                && refusals.cached_write
                && refusals.transaction,
            "expired factory retained SDK authority: {refusals:?}"
        );
        ensure!(request(&client, &base, "/read").await? == serde_json::json!({"count":0}));
        ensure!(
            factory.increments.load(Ordering::Acquire) == 0,
            "expired call entered its business handler"
        );
        for _ in 0..2 {
            ensure!(request(&client, &base, "/retry").await? == serde_json::json!({"count":1}));
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    drop(release);
    let outcome = node.shutdown(ShutdownMode::Preserve).await?;
    result?;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("cold-factory node failed during shutdown")
    };
    ensure!(
        report.ingress_drained
            && report.application_work_drained
            && report.cells_drained
            && !report.drain_timed_out
    );
    ensure!(
        factory.increments.load(Ordering::Acquire) == 2,
        "expired invocation ran after a fresh retry"
    );
    Ok(())
}
