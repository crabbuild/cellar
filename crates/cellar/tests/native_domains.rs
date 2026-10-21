//! Ownership boundaries for library hosts sharing an application runtime.
// The surrounding application uses Tokio directly, including its watchdog
// after the Cellar domain has closed and can no longer provide a timer.
#![allow(clippy::disallowed_methods)]
use anyhow::Result;
use cellar::asyncrt::{self, ExecutionDomain};
use cellar::engine_api::{HttpResponse, RequestBody};
use cellar::host_channels::{self, DoCallReq};
use cellar::http_streams::RequestBodyGuard;
use futures_util::FutureExt;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Notify};

#[path = "support/html.rs"]
mod fixture;

fn request() -> DoCallReq {
    let (reply, _) = oneshot::channel::<Result<HttpResponse>>();
    let body = RequestBody::from(Vec::new());
    DoCallReq {
        request_id: None,
        cancel: None,
        deliver_abort_to_handler: false,
        scope: "Account:same-business-id".into(),
        name: None,
        url: "http://application/balance".into(),
        method: "GET".into(),
        body_guard: RequestBodyGuard::of(&body),
        body,
        headers: Vec::new(),
        reply,
        order: None,
        parent: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn domains_route_identical_cell_names_to_their_own_host_across_tasks() -> Result<()> {
    let first = ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    assert!(!Arc::ptr_eq(&first.services(), &second.services()));
    let (first_tx, mut first_rx) = mpsc::unbounded_channel();
    let (second_tx, mut second_rx) = mpsc::unbounded_channel();
    first
        .run(async { host_channels::set_do_call_tx(first_tx) })
        .await?;
    second
        .run(async { host_channels::set_do_call_tx(second_tx) })
        .await?;
    let first_work = first.spawn(async {
        for _ in 0..32 {
            tokio::task::yield_now().await;
            assert!(host_channels::submit_do_call(request()));
        }
        let blocking = asyncrt::blocking(|| host_channels::submit_do_call(request()));
        assert!(blocking.await.unwrap());
        let child = asyncrt::spawn(async { host_channels::submit_do_call(request()) });
        assert!(child.await.unwrap());
    })?;
    let second_work = second.spawn(async {
        for _ in 0..19 {
            tokio::task::yield_now().await;
            assert!(host_channels::submit_do_call(request()));
        }
    })?;
    first_work.await?;
    second_work.await?;
    for _ in 0..34 {
        assert_eq!(first_rx.try_recv()?.scope, "Account:same-business-id");
    }
    for _ in 0..19 {
        assert_eq!(second_rx.try_recv()?.scope, "Account:same-business-id");
    }
    assert!(first_rx.try_recv().is_err());
    assert!(second_rx.try_recv().is_err());
    // Authority is scoped; the surrounding application's task has no sender.
    assert!(!host_channels::submit_do_call(request()));
    first.shutdown().await;
    assert!(
        second
            .run(async { host_channels::submit_do_call(request()) })
            .await?
    );
    assert!(second_rx.try_recv().is_ok());
    second.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn domain_wall_clocks_follow_tasks_without_changing_monotonic_timers() -> Result<()> {
    use std::sync::atomic::AtomicI64;
    let sample = Arc::new(AtomicI64::new(90_000));
    let selected = sample.clone();
    let first = ExecutionDomain::with_wall_clock(
        tokio::runtime::Handle::current(),
        Arc::new(move || selected.load(Ordering::SeqCst)),
    );
    let second =
        ExecutionDomain::with_wall_clock(tokio::runtime::Handle::current(), Arc::new(|| 120_000));
    tokio::time::timeout(
        Duration::from_secs(2),
        first.run(async {
            assert_eq!(asyncrt::wall_ms(), 90_000);
            second
                .run(async {
                    assert_eq!(asyncrt::wall_ms(), 120_000);
                    assert_eq!(asyncrt::spawn(async { asyncrt::wall_ms() }).await?, 120_000);
                    Ok::<_, anyhow::Error>(())
                })
                .await??;
            assert_eq!(asyncrt::wall_ms(), 90_000);
            sample.store(40_000, Ordering::SeqCst);
            assert_eq!(
                asyncrt::spawn(async {
                    tokio::task::yield_now().await;
                    asyncrt::wall_ms()
                })
                .await?,
                40_000
            );
            assert_eq!(asyncrt::blocking(asyncrt::wall_ms).await?, 40_000);
            let mono = asyncrt::mono_ms();
            sample.store(-10, Ordering::SeqCst);
            asyncrt::sleep(Duration::from_millis(10)).await;
            assert_eq!(asyncrt::wall_ms(), 0);
            assert!(asyncrt::mono_ms() >= mono + 10);
            Ok::<_, anyhow::Error>(())
        }),
    )
    .await???;
    assert!(asyncrt::wall_ms() > 1_000_000_000_000);
    first.shutdown().await;
    assert_eq!(second.run(async { asyncrt::wall_ms() }).await?, 120_000);
    second.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_guards_keep_their_owner_when_dropped_in_another_domain() -> Result<()> {
    let first = ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    let (first_id, first_cancelled, first_guard) = first
        .run(async { host_channels::begin_call_cancellation() })
        .await?;
    let (_, mut second_cancelled, second_guard) = second
        .run(async { host_channels::begin_call_cancellation() })
        .await?;
    second
        .run(async {
            assert!(!host_channels::cancel_call(first_id));
            drop(first_guard);
        })
        .await?;
    first_cancelled.await?;
    assert!(second_cancelled.try_recv().is_err());
    // Dropping outside any host also releases the captured registry.
    drop(second_guard);
    second_cancelled.await?;
    first.shutdown().await;
    second.shutdown().await;
    Ok(())
}

struct Cleanup {
    services: Arc<cellar::host_services::HostServices>,
    done: Arc<AtomicBool>,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        assert!(Arc::ptr_eq(&asyncrt::services(), &self.services));
        self.done.store(true, Ordering::Release);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_a_domain_joins_cancelled_work_without_stopping_the_application() -> Result<()> {
    let first = ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    let entered = Arc::new(Notify::new());
    let done = Arc::new(AtomicBool::new(false));
    let cleanup = Cleanup {
        services: first.services(),
        done: done.clone(),
    };
    let started = entered.clone();
    first
        .spawn(async move {
            let _cleanup = cleanup;
            let (_, _cancelled, _guard) = host_channels::begin_call_cancellation();
            started.notify_one();
            std::future::pending::<()>().await;
        })?
        .detach();
    entered.notified().await;
    tokio::time::timeout(Duration::from_secs(2), first.shutdown()).await?;
    assert!(done.load(Ordering::Acquire));
    assert!(first.spawn(async {}).is_err());
    assert!(first.blocking(|| ()).is_err());
    assert!(first.run(async {}).await.is_err());
    assert_eq!(second.spawn(async { 41 + 1 })?.await?, 42);
    assert_eq!(tokio::spawn(async { 42 }).await?, 42);
    second.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closure_wakes_pending_roots_and_nested_polls_restore_authority() -> Result<()> {
    let first = ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    let first_services = first.services();
    let second_services = second.services();
    first
        .run(async {
            assert!(Arc::ptr_eq(&asyncrt::services(), &first_services));
            let failed = std::panic::AssertUnwindSafe(second.run(async {
                assert!(Arc::ptr_eq(&asyncrt::services(), &second_services));
                tokio::task::yield_now().await;
                panic!("application handler panic");
            }))
            .catch_unwind()
            .await;
            assert!(failed.is_err());
            assert!(Arc::ptr_eq(&asyncrt::services(), &first_services));
        })
        .await?;
    let root = first.run(std::future::pending::<()>());
    let close = async {
        tokio::task::yield_now().await;
        first.close();
    };
    let (result, ()) =
        tokio::time::timeout(Duration::from_secs(2), async { tokio::join!(root, close) }).await?;
    assert!(result.is_err());
    assert_eq!(second.spawn(async { 42 })?.await?, 42);
    first.shutdown().await;
    second.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timer_created_during_closure_preserves_cancellation_and_owned_cleanup() -> Result<()> {
    let first = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    let cleaned = Arc::new(AtomicBool::new(false));
    let cleanup = Cleanup {
        services: first.services(),
        done: cleaned.clone(),
    };
    let closing = first.clone();
    let panicked = Arc::new(AtomicBool::new(false));
    let timer_panicked = panicked.clone();
    let task = first.spawn(async move {
        let _cleanup = cleanup;
        // Close during a poll, before Tokio can deliver its pending abort.
        closing.close();
        let result = std::panic::AssertUnwindSafe(async {
            asyncrt::sleep(Duration::from_secs(30)).await;
            panic!("a closed owner resumed its timer");
        })
        .catch_unwind()
        .await;
        timer_panicked.store(result.is_err(), Ordering::Release);
    })?;
    let result = tokio::time::timeout(Duration::from_secs(2), task).await?;
    assert!(
        !panicked.load(Ordering::Acquire),
        "closure became a task panic"
    );
    assert!(result.is_err(), "a closed owner completed its timer");
    tokio::time::timeout(Duration::from_secs(2), first.shutdown()).await?;
    assert!(cleaned.load(Ordering::Acquire));
    assert_eq!(second.spawn(async { 42 })?.await?, 42);
    second.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_waits_for_started_blocking_work_and_owner_drop_cancels_async_work() -> Result<()>
{
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let services = domain.services();
    let blocking = domain.blocking(move || {
        assert!(Arc::ptr_eq(&asyncrt::services(), &services));
        let _ = entered_tx.send(());
        release_rx.recv().unwrap();
        42
    })?;
    entered_rx.await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), domain.shutdown())
            .await
            .is_err()
    );
    release_tx.send(())?;
    assert_eq!(blocking.await?, 42);
    tokio::time::timeout(Duration::from_secs(2), domain.shutdown()).await?;

    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let done = Arc::new(AtomicBool::new(false));
    let cleanup = Cleanup {
        services: domain.services(),
        done: done.clone(),
    };
    let pending = domain.spawn(async move {
        let _cleanup = cleanup;
        std::future::pending::<()>().await;
    })?;
    drop(domain);
    assert!(pending.await.is_err());
    assert!(done.load(Ordering::Acquire));
    Ok(())
}

async fn application(
    domain: &ExecutionDomain,
    path: &std::path::Path,
    label: &'static str,
) -> Result<(cellar::runtime::RuntimeManager, cellar::bucket::Bucket)> {
    use cellar::sdk::{Application, Invocation, Output, Registry};
    domain
        .run(async {
            let services = domain.services();
            let registry = Registry::new(move |context, invocation| {
                let services = services.clone();
                Box::pin(async move {
                    anyhow::ensure!(
                        Arc::ptr_eq(&asyncrt::services(), &services),
                        "handler lost its host domain"
                    );
                    let Invocation::Fetch(request) = invocation else {
                        anyhow::bail!("expected fetch")
                    };
                    context
                        .blob("FILES")?
                        .put(
                            &context,
                            "same-key",
                            label.as_bytes().to_vec(),
                            Default::default(),
                        )
                        .await?;
                    let response = context
                        .namespace("ACCOUNTS")?
                        .get_by_name("same-business-id")
                        .fetch(&context, request)
                        .await?;
                    Ok(Output::Http(response))
                })
            })
            .cell("Account", |_, _| {
                anyhow::bail!("the host routes this test's account")
            })?;
            let mut graph = fixture::graph("embedded-app", "", vec!["Account".into()]);
            graph
                .primary
                .options
                .bindings
                .push(("ACCOUNTS".into(), "Account".into()));
            graph
                .primary
                .options
                .blob_bindings
                .push(("FILES".into(), "files".into()));
            let generation = cellar::generation::Generation::build_native(
                1,
                graph,
                fixture::generation_options(),
                std::collections::HashMap::from([(
                    "embedded-app".into(),
                    Arc::new(registry) as Arc<dyn Application>,
                )]),
            )?;
            let bucket = cellar::bucket::Bucket::open_dev(&path.join("bucket.sqlite"))?;
            host_channels::set_blob_store(bucket.clone());
            let (send, mut calls) = mpsc::unbounded_channel::<DoCallReq>();
            host_channels::set_do_call_tx(send);
            asyncrt::spawn(async move {
                while let Some(call) = calls.recv().await {
                    let _ = call.reply.send(Ok(HttpResponse {
                        status: 200,
                        body: label.as_bytes().to_vec(),
                        stream: None,
                        headers: Vec::new(),
                        websocket: None,
                        write_position: None,
                        observed_position: None,
                    }));
                }
            })
            .detach();
            let runtime = cellar::runtime::RuntimeManager::start(
                generation,
                fixture::runtime_options(path.join("cells")),
            )?;
            Ok((runtime, bucket))
        })
        .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_handlers_keep_routing_and_blob_authority_when_runtime_drivers_spawn() -> Result<()>
{
    let first = ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    let first_directory = tempfile::tempdir()?;
    let second_directory = tempfile::tempdir()?;
    let (first_runtime, first_bucket) =
        application(&first, first_directory.path(), "first-host").await?;
    let (second_runtime, second_bucket) =
        application(&second, second_directory.path(), "second-host").await?;
    let fetch = |runtime: cellar::runtime::RuntimeManager| async move {
        runtime
            .fetch_worker(
                "http://application/balance".into(),
                "GET".into(),
                Vec::new(),
                Vec::new(),
            )
            .await
    };
    let (first_response, second_response) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            first.run(fetch(first_runtime)),
            second.run(fetch(second_runtime.clone()))
        )
    })
    .await?;
    assert_eq!(first_response??.body, b"first-host");
    assert_eq!(second_response??.body, b"second-host");
    let first_object = first
        .run(first_bucket.get("blob/files/same-key"))
        .await??
        .unwrap()
        .0;
    let second_object = second
        .run(second_bucket.get("blob/files/same-key"))
        .await??
        .unwrap()
        .0;
    assert!(first_object.windows(10).any(|bytes| bytes == b"first-host"));
    assert!(!first_object
        .windows(11)
        .any(|bytes| bytes == b"second-host"));
    assert!(second_object
        .windows(11)
        .any(|bytes| bytes == b"second-host"));
    first.shutdown().await;
    assert_eq!(
        second.run(fetch(second_runtime)).await??.body,
        b"second-host"
    );
    second.shutdown().await;
    Ok(())
}

struct PendingCleanup {
    started: Arc<Notify>,
    dropping: Arc<Notify>,
    release: std::sync::mpsc::Receiver<()>,
    done: Arc<AtomicBool>,
}

impl std::future::Future for PendingCleanup {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        self.started.notify_one();
        std::task::Poll::Pending
    }
}

impl Drop for PendingCleanup {
    fn drop(&mut self) {
        self.dropping.notify_one();
        self.release.recv().unwrap();
        self.done.store(true, Ordering::Release);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_completion_waits_for_cancelled_future_destructors() -> Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let started = Arc::new(Notify::new());
    let dropping = Arc::new(Notify::new());
    let done = Arc::new(AtomicBool::new(false));
    let (release, receive) = std::sync::mpsc::channel();
    let task = domain.spawn(PendingCleanup {
        started: started.clone(),
        dropping: dropping.clone(),
        release: receive,
        done: done.clone(),
    })?;
    started.notified().await;
    domain.close();
    dropping.notified().await;
    let early = tokio::time::timeout(Duration::from_millis(20), domain.shutdown()).await;
    // Release even if the contract regresses, so a failed assertion cannot
    // strand a runtime worker in this deliberately delayed destructor.
    release.send(())?;
    assert!(
        early.is_err(),
        "shutdown returned before owned resource cleanup"
    );
    assert!(task.await.is_err());
    tokio::time::timeout(Duration::from_secs(2), domain.shutdown()).await?;
    assert!(done.load(Ordering::Acquire));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_joins_entered_roots_without_aborting_the_application_task() -> Result<()> {
    let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let started = Arc::new(Notify::new());
    let dropping = Arc::new(Notify::new());
    let done = Arc::new(AtomicBool::new(false));
    let (release, receive) = std::sync::mpsc::channel();
    let cleanup = PendingCleanup {
        started: started.clone(),
        dropping: dropping.clone(),
        release: receive,
        done: done.clone(),
    };
    let entered = domain.clone();
    let application_task = tokio::spawn(async move {
        let result = entered.run(cleanup).await;
        // The host closes its root; the surrounding task continues normally.
        assert!(result.is_err());
        42
    });
    started.notified().await;
    domain.close();
    dropping.notified().await;
    let early = tokio::time::timeout(Duration::from_millis(20), domain.shutdown()).await;
    release.send(())?;
    assert!(
        early.is_err(),
        "shutdown returned before an entered root finished dropping"
    );
    assert_eq!(application_task.await?, 42);
    tokio::time::timeout(Duration::from_secs(2), domain.shutdown()).await?;
    assert!(done.load(Ordering::Acquire));
    Ok(())
}

#[derive(Default)]
struct CellSignals {
    started: Notify,
    release: Notify,
    dropped: AtomicBool,
}

struct HoldCleanup(Arc<CellSignals>);

impl Drop for HoldCleanup {
    fn drop(&mut self) {
        self.0.dropped.store(true, Ordering::Release);
    }
}

struct Account {
    storage: cellar::sdk::Storage,
    signals: Arc<CellSignals>,
}

fn cell_response(value: serde_json::Value) -> cellar::sdk::Output {
    cellar::sdk::Output::Http(HttpResponse {
        status: 200,
        body: value.to_string().into_bytes(),
        stream: None,
        headers: Vec::new(),
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}

impl cellar::sdk::Cell for Account {
    fn invoke(
        self: Arc<Self>,
        context: cellar::sdk::Context,
        invocation: cellar::sdk::Invocation,
    ) -> cellar::sdk::HandlerFuture<cellar::sdk::Output> {
        Box::pin(async move {
            let cellar::sdk::Invocation::Fetch(request) = invocation else {
                anyhow::bail!("expected fetch")
            };
            match url::Url::parse(&request.url)?.path() {
                "/hold" => {
                    let signals = self.signals.clone();
                    context
                        .block_concurrency_while(move |context| async move {
                            let _cleanup = HoldCleanup(signals.clone());
                            let storage = context.storage()?;
                            storage.put_json("balance", &serde_json::json!(1))?;
                            signals.started.notify_one();
                            signals.release.notified().await;
                            storage.put_json("balance", &serde_json::json!(2))?;
                            Ok(())
                        })
                        .await?;
                }
                "/increment" => {
                    let balance = self
                        .storage
                        .get_json("balance")?
                        .and_then(|value| value.as_u64())
                        .unwrap_or(0);
                    self.storage
                        .put_json("balance", &serde_json::json!(balance + 1))?;
                }
                "/read" => {}
                path => anyhow::bail!("unexpected account operation {path}"),
            }
            Ok(cell_response(
                self.storage
                    .get_json("balance")?
                    .unwrap_or(serde_json::json!(0)),
            ))
        })
    }
}

async fn cell_application(
    domain: &ExecutionDomain,
    path: &std::path::Path,
    signals: Arc<CellSignals>,
) -> Result<cellar::runtime::RuntimeManager> {
    use cellar::sdk::{Application, Registry};
    domain
        .run(async {
            let registry = Registry::new(|_, _| {
                Box::pin(async { anyhow::bail!("this application only exports cells") })
            })
            .cell("Account", move |context, _| {
                Ok(Arc::new(Account {
                    storage: context.storage()?,
                    signals: signals.clone(),
                }))
            })?;
            let generation = cellar::generation::Generation::build_native(
                1,
                fixture::graph("embedded-accounts", "", vec!["Account".into()]),
                fixture::generation_options(),
                std::collections::HashMap::from([(
                    "embedded-accounts".into(),
                    Arc::new(registry) as Arc<dyn Application>,
                )]),
            )?;
            let runtime = cellar::runtime::RuntimeManager::start(
                generation,
                fixture::runtime_options(path.join("cells")),
            )?;
            runtime
                .restore_cell(
                    "Account:same",
                    &cellar_logic::RestoreSpec {
                        epoch: 1,
                        fresh: true,
                        took_over: false,
                        resume_local: false,
                        prior: None,
                    },
                )
                .await?;
            runtime.start_cell("Account:same".into(), 1, true).await?;
            runtime.publish_cell("Account:same", 1)?;
            // This test isolates input-gate ownership. A local proof adapter
            // acknowledges its SQLite writes; fleet durability is tested separately.
            let (send, mut requests) = mpsc::unbounded_channel();
            host_channels::set_gate_tx(send);
            asyncrt::spawn(async move {
                while let Some(request) = requests.recv().await {
                    let _ = request.reply.send(Ok(()));
                }
            })
            .detach();
            Ok(runtime)
        })
        .await?
}

async fn account_fetch(
    runtime: cellar::runtime::RuntimeManager,
    path: &str,
) -> Result<HttpResponse> {
    runtime
        .fetch_cell(
            "Account:same".into(),
            None,
            cellar::engine_api::RuntimeFetch {
                url: format!("http://application{path}"),
                method: "GET".into(),
                body: Vec::new().into(),
                headers: Vec::new(),
                request_id: Some(cellar::engine_api::next_request_id()),
                order: None,
                parent: None,
            },
            None,
        )
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identical_durable_cells_keep_their_sqlite_state_and_input_gates_in_their_own_host(
) -> Result<()> {
    let first = ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    let first_directory = tempfile::tempdir()?;
    let second_directory = tempfile::tempdir()?;
    let first_signals = Arc::new(CellSignals::default());
    let first_runtime =
        cell_application(&first, first_directory.path(), first_signals.clone()).await?;
    let second_runtime = cell_application(
        &second,
        second_directory.path(),
        Arc::new(CellSignals::default()),
    )
    .await?;
    let hold = first.spawn(account_fetch(first_runtime.clone(), "/hold"))?;
    tokio::time::timeout(Duration::from_secs(2), first_signals.started.notified()).await?;
    let mut first_read = first.spawn(account_fetch(first_runtime, "/read"))?;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut first_read)
            .await
            .is_err()
    );
    let second_write = second.run(account_fetch(second_runtime.clone(), "/increment"));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), second_write)
            .await???
            .body,
        b"1"
    );
    assert_eq!(
        second
            .run(account_fetch(second_runtime.clone(), "/read"))
            .await??
            .body,
        b"1"
    );
    first_signals.release.notify_one();
    assert_eq!(hold.await??.body, b"2");
    assert_eq!(first_read.await??.body, b"2");
    first.shutdown().await;
    assert_eq!(
        second
            .run(account_fetch(second_runtime, "/increment"))
            .await??
            .body,
        b"2"
    );
    second.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_of_one_durable_holder_does_not_release_a_sibling_hosts_gate() -> Result<()> {
    let first = ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    let first_directory = tempfile::tempdir()?;
    let second_directory = tempfile::tempdir()?;
    let first_signals = Arc::new(CellSignals::default());
    let second_signals = Arc::new(CellSignals::default());
    let first_runtime =
        cell_application(&first, first_directory.path(), first_signals.clone()).await?;
    let second_runtime =
        cell_application(&second, second_directory.path(), second_signals.clone()).await?;
    let first_hold = first.spawn(account_fetch(first_runtime, "/hold"))?;
    let second_hold = second.spawn(account_fetch(second_runtime.clone(), "/hold"))?;
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            first_signals.started.notified(),
            second_signals.started.notified()
        );
    })
    .await?;
    let mut second_read = second.spawn(account_fetch(second_runtime, "/read"))?;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut second_read)
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(2), first.shutdown()).await?;
    assert!(first_hold.await.is_err());
    assert!(first_signals.dropped.load(Ordering::Acquire));
    assert!(!second_signals.dropped.load(Ordering::Acquire));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut second_read)
            .await
            .is_err()
    );
    second_signals.release.notify_one();
    assert_eq!(second_hold.await??.body, b"2");
    assert_eq!(second_read.await??.body, b"2");
    second.shutdown().await;
    Ok(())
}

#[derive(Default)]
struct ResourceProbe {
    worker: std::sync::Mutex<Option<cellar::sdk::loader::Worker>>,
    foreign: std::sync::Mutex<Option<cellar::sdk::loader::Worker>>,
    socket: std::sync::Mutex<Option<cellar::sdk::sockets::Socket>>,
    started: Notify,
    release: Notify,
}

fn domain_response(bytes: impl Into<Vec<u8>>) -> cellar::sdk::Output {
    cellar::sdk::Output::Http(HttpResponse {
        status: 200,
        body: bytes.into(),
        stream: None,
        headers: Vec::new(),
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}

async fn resource_application(
    domain: &ExecutionDomain,
    path: &std::path::Path,
    label: &'static str,
    probe: Arc<ResourceProbe>,
    address: Option<String>,
) -> Result<(cellar::runtime::RuntimeManager, cellar::bucket::Bucket)> {
    use cellar::sdk::{Application, Invocation, Registry};
    domain
        .run(async {
            let services = domain.services();
            let registry = Registry::new(move |context, invocation| {
                let services = services.clone();
                let probe = probe.clone();
                let address = address.clone();
                Box::pin(async move {
                    let Invocation::Fetch(request) = invocation else {
                        anyhow::bail!("expected fetch")
                    };
                    let path = url::Url::parse(&request.url)?.path().to_owned();
                    if path == "/socket" {
                        let socket = context.connect(address.unwrap(), Default::default())?;
                        socket.opened().await?;
                        socket
                            .writable
                            .get_writer()?
                            .write(&context, label.as_bytes().to_vec())
                            .await?;
                        *probe.socket.lock().unwrap() = Some(socket.clone());
                        probe.started.notify_one();
                        probe.release.notified().await;
                        let bytes = socket.readable.get_reader()?.read(&context).await?;
                        socket.close(&context).await?;
                        return Ok(domain_response(bytes));
                    }
                    if path == "/foreign" {
                        let foreign = probe.foreign.lock().unwrap().clone().unwrap();
                        anyhow::ensure!(
                            foreign.dispose(&context).is_err(),
                            "another host disposed the worker"
                        );
                        let failure = foreign
                            .entrypoint("default")
                            .fetch(&context, request)
                            .await
                            .err()
                            .expect("a foreign host accepted the loaded worker");
                        return Ok(domain_response(failure.to_string().into_bytes()));
                    }
                    let existing = probe.worker.lock().unwrap().clone();
                    let worker = match existing {
                        Some(worker) => worker,
                        None => {
                            let bucket = context.blob("FILES")?;
                            let child = Registry::new(move |context, _| {
                                let bucket = bucket.clone();
                                let services = services.clone();
                                Box::pin(async move {
                                    anyhow::ensure!(
                                        Arc::ptr_eq(&asyncrt::services(), &services),
                                        "loaded handler lost its host"
                                    );
                                    tokio::task::yield_now().await;
                                    anyhow::ensure!(
                                        Arc::ptr_eq(&asyncrt::services(), &services),
                                        "loaded continuation lost its host"
                                    );
                                    bucket
                                        .put(
                                            &context,
                                            "same-key",
                                            label.as_bytes().to_vec(),
                                            Default::default(),
                                        )
                                        .await?;
                                    Ok(domain_response(label.as_bytes().to_vec()))
                                })
                            });
                            let worker =
                                context
                                    .loader("EXT")?
                                    .get(&context, "same-load", move |_| {
                                        Box::pin(async move {
                                            Ok(cellar::sdk::loader::WorkerCode::new(Arc::new(
                                                child,
                                            )))
                                        })
                                    })?;
                            *probe.worker.lock().unwrap() = Some(worker.clone());
                            worker
                        }
                    };
                    Ok(cellar::sdk::Output::Http(
                        worker
                            .entrypoint("default")
                            .fetch(&context, request)
                            .await?,
                    ))
                })
            });
            let mut graph = fixture::graph("same-resource-app", "", Vec::new());
            graph.primary.loader_bindings.push("EXT".into());
            graph
                .primary
                .options
                .blob_bindings
                .push(("FILES".into(), "files".into()));
            let generation = cellar::generation::Generation::build_native(
                1,
                graph,
                fixture::generation_options(),
                std::collections::HashMap::from([(
                    "same-resource-app".into(),
                    Arc::new(registry) as Arc<dyn Application>,
                )]),
            )?;
            let bucket = cellar::bucket::Bucket::open_dev(&path.join("bucket.sqlite"))?;
            host_channels::set_blob_store(bucket.clone());
            Ok((
                cellar::runtime::RuntimeManager::start(
                    generation,
                    fixture::runtime_options(path.join("cells")),
                )?,
                bucket,
            ))
        })
        .await?
}

async fn resource_fetch(
    runtime: cellar::runtime::RuntimeManager,
    path: &str,
) -> Result<HttpResponse> {
    runtime
        .fetch_worker(
            format!("http://application{path}"),
            "GET".into(),
            Vec::new(),
            Vec::new(),
        )
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loaded_native_applications_keep_their_domain_and_reject_foreign_capabilities() -> Result<()>
{
    let first = ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    let first_dir = tempfile::tempdir()?;
    let second_dir = tempfile::tempdir()?;
    let first_probe = Arc::new(ResourceProbe::default());
    let second_probe = Arc::new(ResourceProbe::default());
    let (first_runtime, first_bucket) = resource_application(
        &first,
        first_dir.path(),
        "first-host",
        first_probe.clone(),
        None,
    )
    .await?;
    let (second_runtime, second_bucket) = resource_application(
        &second,
        second_dir.path(),
        "second-host",
        second_probe.clone(),
        None,
    )
    .await?;
    let (a, b) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            first.run(resource_fetch(first_runtime.clone(), "/loaded")),
            second.run(resource_fetch(second_runtime.clone(), "/loaded"))
        )
    })
    .await?;
    assert_eq!(a??.body, b"first-host");
    assert_eq!(b??.body, b"second-host");
    for (domain, label, bucket) in [
        (&first, b"first-host".as_slice(), &first_bucket),
        (&second, b"second-host".as_slice(), &second_bucket),
    ] {
        let object = domain
            .run(bucket.get("blob/files/same-key"))
            .await??
            .unwrap()
            .0;
        assert!(object.windows(label.len()).any(|bytes| bytes == label));
        assert_eq!(
            domain
                .run(async { cellar::host_loader::loader_registry().lock().unwrap().len() })
                .await?,
            1
        );
    }
    *second_probe.foreign.lock().unwrap() = first_probe.worker.lock().unwrap().clone();
    let refusal = second
        .run(resource_fetch(second_runtime.clone(), "/foreign"))
        .await??;
    assert!(String::from_utf8(refusal.body)?.contains("another execution domain"));
    second_probe.foreign.lock().unwrap().take();
    let owned = first_probe.worker.lock().unwrap().take().unwrap();
    second.run(async { drop(owned) }).await?;
    assert_eq!(
        first
            .run(async { cellar::host_loader::loader_registry().lock().unwrap().len() })
            .await?,
        0
    );
    assert_eq!(
        second
            .run(resource_fetch(second_runtime.clone(), "/loaded"))
            .await??
            .body,
        b"second-host"
    );
    assert_eq!(
        first
            .run(resource_fetch(first_runtime, "/loaded"))
            .await??
            .body,
        b"first-host"
    );
    first.shutdown().await;
    assert!(first.run(async {}).await.is_err());
    assert_eq!(
        second
            .run(resource_fetch(second_runtime, "/loaded"))
            .await??
            .body,
        b"second-host"
    );
    second.shutdown().await;
    // Retained handles may be dropped after their owning runtime has closed.
    drop(first_probe.worker.lock().unwrap().take());
    drop(second_probe.worker.lock().unwrap().take());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_socket_shutdown_closes_one_hosts_peer_and_preserves_the_siblings_transport(
) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let (ready, mut connected) = mpsc::unbounded_channel();
    let (closed, mut peers) = mpsc::unbounded_channel();
    let peer = tokio::spawn(async move {
        let mut connections = Vec::new();
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let ready = ready.clone();
            let closed = closed.clone();
            connections.push(tokio::spawn(async move {
                let mut bytes = [0; 64];
                let mut label = Vec::new();
                while !label.ends_with(b"-host") {
                    let count = stream.read(&mut bytes).await.unwrap();
                    assert_ne!(count, 0, "socket closed before identifying its host");
                    label.extend_from_slice(&bytes[..count]);
                    assert!(label.len() <= bytes.len());
                }
                assert!(label == b"first-host" || label == b"second-host");
                stream.write_all(&label).await.unwrap();
                ready.send(label.clone()).unwrap();
                let mut remaining = Vec::new();
                if let Err(error) = stream.read_to_end(&mut remaining).await {
                    // Cancelling the first host can reset its TCP connection
                    // because the held handler has not read the echoed bytes.
                    assert_eq!(label, b"first-host");
                    assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
                }
                assert!(remaining.is_empty());
                closed.send(label).unwrap();
            }));
        }
        for connection in connections {
            connection.await.unwrap();
        }
    });
    let first = ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    let first_dir = tempfile::tempdir()?;
    let second_dir = tempfile::tempdir()?;
    let first_probe = Arc::new(ResourceProbe::default());
    let second_probe = Arc::new(ResourceProbe::default());
    let (first_runtime, _) = resource_application(
        &first,
        first_dir.path(),
        "first-host",
        first_probe.clone(),
        Some(address.clone()),
    )
    .await?;
    let (second_runtime, _) = resource_application(
        &second,
        second_dir.path(),
        "second-host",
        second_probe.clone(),
        Some(address),
    )
    .await?;
    let first_call = first.spawn(resource_fetch(first_runtime, "/socket"))?;
    let second_call = second.spawn(resource_fetch(second_runtime, "/socket"))?;
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            first_probe.started.notified(),
            second_probe.started.notified()
        );
    })
    .await?;
    let mut labels = Vec::new();
    for _ in 0..2 {
        labels.push(
            tokio::time::timeout(Duration::from_secs(5), connected.recv())
                .await?
                .unwrap(),
        );
    }
    labels.sort();
    assert_eq!(labels, [b"first-host".to_vec(), b"second-host".to_vec()]);
    tokio::time::timeout(Duration::from_secs(5), first.shutdown()).await?;
    if let Ok(response) = first_call.await {
        assert!(
            response.is_err(),
            "a stopped host completed its held socket request"
        );
    }
    let retained = first_probe.socket.lock().unwrap().take().unwrap();
    let _ = retained.closed().await;
    second.run(async { drop(retained) }).await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), peers.recv())
            .await?
            .unwrap(),
        b"first-host"
    );
    assert!(peers.try_recv().is_err());
    second_probe.release.notify_one();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), second_call)
            .await???
            .body,
        b"second-host"
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), peers.recv())
            .await?
            .unwrap(),
        b"second-host"
    );
    peer.await?;
    second.shutdown().await;
    Ok(())
}

#[derive(Default)]
struct FacetDomainProbe {
    exported: std::sync::Mutex<Option<cellar::sdk::facets::Class>>,
    foreign: std::sync::Mutex<Option<cellar::sdk::facets::Class>>,
    scopes: std::sync::Mutex<Vec<String>>,
    retired: std::sync::atomic::AtomicUsize,
}

struct FacetCounter {
    storage: cellar::sdk::Storage,
    label: &'static str,
    probe: Arc<FacetDomainProbe>,
}

impl Drop for FacetCounter {
    fn drop(&mut self) {
        self.probe.retired.fetch_add(1, Ordering::SeqCst);
    }
}

impl cellar::sdk::Cell for FacetCounter {
    fn invoke(
        self: Arc<Self>,
        context: cellar::sdk::Context,
        _: cellar::sdk::Invocation,
    ) -> cellar::sdk::HandlerFuture<cellar::sdk::Output> {
        Box::pin(async move {
            context.storage()?;
            let count = self
                .storage
                .get_json("count")?
                .and_then(|n| n.as_u64())
                .unwrap_or(0)
                + 1;
            self.storage.put_json("count", &serde_json::json!(count))?;
            Ok(cell_response(
                serde_json::json!({"host": self.label, "count": count}),
            ))
        })
    }
}

struct FacetParent {
    facets: cellar::sdk::facets::Facets,
    label: &'static str,
    probe: Arc<FacetDomainProbe>,
}

impl cellar::sdk::Cell for FacetParent {
    fn invoke(
        self: Arc<Self>,
        context: cellar::sdk::Context,
        invocation: cellar::sdk::Invocation,
    ) -> cellar::sdk::HandlerFuture<cellar::sdk::Output> {
        Box::pin(async move {
            let cellar::sdk::Invocation::Fetch(request) = invocation else {
                anyhow::bail!("expected facet parent fetch")
            };
            let foreign = request.url.ends_with("/foreign");
            let class = if foreign {
                self.probe.foreign.lock().unwrap().clone().unwrap()
            } else {
                let label = self.label;
                let probe = self.probe.clone();
                let child = cellar::sdk::Registry::new(|_, _| {
                    Box::pin(async { Ok(cellar::sdk::Output::Done) })
                })
                .cell("Child", move |context, identity| {
                    probe.scopes.lock().unwrap().push(identity.scope.clone());
                    Ok(Arc::new(FacetCounter {
                        storage: context.storage()?,
                        label,
                        probe: probe.clone(),
                    }))
                })?;
                context
                    .loader("EXT")?
                    .get(&context, "same-child", move |_| {
                        Box::pin(async move {
                            Ok(cellar::sdk::loader::WorkerCode::new(Arc::new(child)))
                        })
                    })?
                    .cell_class("Child")
            };
            let name = if foreign { "foreign" } else { "same-child" };
            let facet = self.facets.get(&context, name, move |_| {
                Box::pin(async move { Ok(cellar::sdk::facets::Startup::new(class)) })
            })?;
            let result = facet
                .fetch(
                    &context,
                    cellar::sdk::Request {
                        url: "http://facet/count".into(),
                        method: "POST".into(),
                        body: Vec::new().into(),
                        headers: Vec::new(),
                        entrypoint: None,
                    },
                )
                .await;
            if foreign {
                let error = result
                    .err()
                    .expect("a foreign exported class ran in this host");
                anyhow::ensure!(
                    error.to_string().contains("another execution domain"),
                    "{error}"
                );
                return Ok(domain_response(b"foreign class refused".to_vec()));
            }
            Ok(cellar::sdk::Output::Http(result?))
        })
    }
}

async fn facet_application(
    domain: &ExecutionDomain,
    path: &std::path::Path,
    label: &'static str,
    probe: Arc<FacetDomainProbe>,
) -> Result<cellar::runtime::RuntimeManager> {
    use cellar::sdk::{Application, Registry};
    domain
        .run(async {
            let registry =
                Registry::new(|_, _| Box::pin(async { anyhow::bail!("only cell exports") }))
                    .cell("Account", move |context, _| {
                        *probe.exported.lock().unwrap() =
                            Some(context.exported_cell_class("Local")?);
                        Ok(Arc::new(FacetParent {
                            facets: context.facets()?,
                            label,
                            probe: probe.clone(),
                        }))
                    })?
                    .cell("Local", |_, _| {
                        anyhow::bail!("the foreign class must be refused before construction")
                    })?;
            let mut graph = fixture::graph("embedded-facets", "", vec!["Account".into()]);
            graph.primary.loader_bindings.push("EXT".into());
            let generation = cellar::generation::Generation::build_native(
                1,
                graph,
                fixture::generation_options(),
                std::collections::HashMap::from([(
                    "embedded-facets".into(),
                    Arc::new(registry) as Arc<dyn Application>,
                )]),
            )?;
            let runtime = cellar::runtime::RuntimeManager::start(
                generation,
                fixture::runtime_options(path.join("cells")),
            )?;
            runtime
                .restore_cell(
                    "Account:same",
                    &cellar_logic::RestoreSpec {
                        epoch: 1,
                        fresh: true,
                        took_over: false,
                        resume_local: false,
                        prior: None,
                    },
                )
                .await?;
            runtime.start_cell("Account:same".into(), 1, true).await?;
            runtime.publish_cell("Account:same", 1)?;
            let (send, mut requests) = mpsc::unbounded_channel();
            host_channels::set_gate_tx(send);
            asyncrt::spawn(async move {
                while let Some(request) = requests.recv().await {
                    let _ = request.reply.send(Ok(()));
                }
            })
            .detach();
            Ok(runtime)
        })
        .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identical_facet_roots_keep_loaded_children_and_exported_classes_in_their_host(
) -> Result<()> {
    let first = ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    let first_directory = tempfile::tempdir()?;
    let second_directory = tempfile::tempdir()?;
    let first_probe = Arc::new(FacetDomainProbe::default());
    let second_probe = Arc::new(FacetDomainProbe::default());
    let first_runtime = facet_application(
        &first,
        first_directory.path(),
        "first-host",
        first_probe.clone(),
    )
    .await?;
    let second_runtime = facet_application(
        &second,
        second_directory.path(),
        "second-host",
        second_probe.clone(),
    )
    .await?;
    let (a, b) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            first.run(account_fetch(first_runtime.clone(), "/count")),
            second.run(account_fetch(second_runtime.clone(), "/count"))
        )
    })
    .await?;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&a??.body)?,
        serde_json::json!({"host":"first-host", "count":1})
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&b??.body)?,
        serde_json::json!({"host":"second-host", "count":1})
    );
    assert_eq!(first_probe.scopes.lock().unwrap().len(), 1);
    assert_eq!(
        *first_probe.scopes.lock().unwrap(),
        *second_probe.scopes.lock().unwrap()
    );
    *second_probe.foreign.lock().unwrap() = first_probe.exported.lock().unwrap().clone();
    assert_eq!(
        second
            .run(account_fetch(second_runtime.clone(), "/foreign"))
            .await??
            .body,
        b"foreign class refused"
    );
    first
        .run(async { first_runtime.swap_out_cell("Account:same", 1).await })
        .await??;
    assert_eq!(first_probe.retired.load(Ordering::SeqCst), 1);
    assert_eq!(second_probe.retired.load(Ordering::SeqCst), 0);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &second
                .run(account_fetch(second_runtime.clone(), "/count"))
                .await??
                .body
        )?,
        serde_json::json!({"host":"second-host", "count":2})
    );
    first.shutdown().await;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &second
                .run(account_fetch(second_runtime, "/count"))
                .await??
                .body
        )?,
        serde_json::json!({"host":"second-host", "count":3})
    );
    second.shutdown().await;
    assert_eq!(second_probe.retired.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filesystem_provider_is_owned_across_async_and_blocking_work() -> Result<()> {
    let filesystem: Arc<dyn cellar_ltx::FileSystem> = Arc::new(cellar_ltx::DirectFileSystem);
    let domain = ExecutionDomain::with_filesystem(
        tokio::runtime::Handle::current(),
        filesystem.clone(),
        Some(Arc::new(|| 42)),
    );
    let other = ExecutionDomain::new(tokio::runtime::Handle::current());
    domain
        .run(async {
            assert!(Arc::ptr_eq(&filesystem, &asyncrt::fs()));
            assert_eq!(asyncrt::wall_ms(), 42);
        })
        .await?;
    let inherited = filesystem.clone();
    domain
        .spawn(async move {
            assert!(Arc::ptr_eq(&inherited, &asyncrt::fs()));
            let expected = inherited.clone();
            asyncrt::blocking(move || {
                assert!(Arc::ptr_eq(&expected, &asyncrt::fs()));
                assert_eq!(asyncrt::wall_ms(), 42);
            })
            .await
            .unwrap();
        })?
        .await?;
    other
        .run(async {
            assert!(!Arc::ptr_eq(&filesystem, &asyncrt::fs()));
        })
        .await?;
    domain.shutdown().await;
    other.shutdown().await;
    Ok(())
}
