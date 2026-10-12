//! Public native facet API through production runtime turns, without an engine.
//! This process uses standalone SQLite; fleet replication is a separate gate.

use anyhow::{anyhow, Result};
use cellar::application::facets::{AbortError, Class, CompletionError, Facet, Facets, Startup};
use cellar::application::{Cell, Context, HandlerFuture, Invocation, Output, Registry, Request};
use cellar::engine_api::{Compat, HttpResponse, RuntimeFetch, RuntimeOptions, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::native::clone::{Object, StructuredValue, Value};
use cellar::runtime::RuntimeManager;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

fn graph() -> StructuredValue {
    StructuredValue {
        root: Value::Reference(0),
        objects: vec![
            Object::Object(vec![
                (Value::from("self"), Value::Reference(0)),
                (Value::from("bytes"), Value::Reference(1)),
            ]),
            Object::ArrayBuffer {
                bytes: vec![4, 8, 16],
                max_length: None,
                immutable: false,
            },
        ],
    }
}
fn reason() -> StructuredValue {
    graph()
}
fn request(path: &str) -> Request {
    Request {
        url: format!("http://native-facet/{path}"),
        method: "POST".into(),
        body: Vec::new().into(),
        headers: Vec::new(),
        entrypoint: None,
    }
}
fn response(text: &str) -> Output {
    Output::Http(HttpResponse {
        status: 200,
        body: text.as_bytes().to_vec(),
        stream: None,
        headers: Vec::new(),
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}
#[derive(Default)]
struct Signals {
    callbacks: AtomicUsize,
    failed_callbacks: AtomicUsize,
    constructors: AtomicUsize,
    identities: Mutex<Vec<cellar::application::FacetContext>>,
    manager: Mutex<Option<Facets>>,
    old_stub: Mutex<Option<Facet>>,
    pending: Notify,
    cancel: Notify,
    dropped: Notify,
    pending_drops: AtomicUsize,
}
#[derive(Debug)]
struct StartupFailure(u32);
impl std::fmt::Display for StartupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "startup failure {}", self.0)
    }
}
impl std::error::Error for StartupFailure {}
struct Pending(Arc<Signals>);
impl Drop for Pending {
    fn drop(&mut self) {
        self.0.pending_drops.fetch_add(1, Ordering::SeqCst);
        self.0.dropped.notify_one();
    }
}
struct Supervisor {
    facets: Facets,
    class: Class,
    signals: Arc<Signals>,
}
impl Supervisor {
    fn child(&self, context: &Context) -> Result<Facet> {
        let class = self.class.clone();
        let signals = self.signals.clone();
        self.facets.get(context, "child", move |context| {
            Box::pin(async move {
                context.storage()?.get_alarm()?;
                signals.callbacks.fetch_add(1, Ordering::SeqCst);
                Ok(Startup::new(class))
            })
        })
    }
}
impl Cell for Supervisor {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(incoming) = invocation else {
                unreachable!()
            };
            match incoming.url.rsplit('/').next().unwrap() {
                "failed-start" => {
                    let signals = self.signals.clone();
                    let child = self.facets.get(&context, "failed", move |_| {
                        Box::pin(async move {
                            signals.failed_callbacks.fetch_add(1, Ordering::SeqCst);
                            Err(StartupFailure(42).into())
                        })
                    })?;
                    for _ in 0..2 {
                        let error = child.fetch(&context, request("fetch")).await.err().unwrap();
                        let cause = &error.downcast_ref::<CompletionError>().unwrap().cause;
                        assert_eq!(cause.downcast_ref::<StartupFailure>().unwrap().0, 42);
                    }
                    Ok(response("failed once"))
                }
                "lazy" => {
                    let child = self.child(&context)?;
                    *self.signals.old_stub.lock().unwrap() = Some(child);
                    *self.signals.manager.lock().unwrap() = Some(self.facets.clone());
                    Ok(response("lazy"))
                }
                "fetch" => Ok(Output::Http(
                    self.child(&context)?
                        .fetch(&context, request("fetch"))
                        .await?,
                )),
                "rpc" => {
                    let data = self
                        .child(&context)?
                        .rpc(&context, "echo", cellar::application::encode_rpc(&graph())?)
                        .await?;
                    assert_eq!(cellar::application::decode_rpc(&data)?, graph());
                    Ok(response("rpc"))
                }
                "nested" => Ok(Output::Http(
                    self.child(&context)?
                        .fetch(&context, request("nested"))
                        .await?,
                )),
                "abort" => {
                    let abort = self.facets.abort(&context, "child", reason());
                    let old = self.signals.old_stub.lock().unwrap().clone().unwrap();
                    let error = old.fetch(&context, request("fetch")).await.err().unwrap();
                    assert_eq!(
                        *error.downcast_ref::<AbortError>().unwrap().reason,
                        reason()
                    );
                    abort.await?;
                    Ok(response("aborted"))
                }
                "delete" => {
                    self.facets.delete(&context, "child").await?;
                    Ok(response("deleted"))
                }
                "delete-detached" => {
                    // Dropping the returned wait must still finish deletion.
                    // First use of the replacement waits for the name barrier.
                    drop(self.facets.delete(&context, "child"));
                    Ok(Output::Http(
                        self.child(&context)?
                            .fetch(&context, request("fetch"))
                            .await?,
                    ))
                }
                "foreign" | "stale" => {
                    let old = self.signals.manager.lock().unwrap().clone().unwrap();
                    assert!(old
                        .get(&context, "refused", |_| Box::pin(async { unreachable!() }))
                        .is_err());
                    Ok(response("refused"))
                }
                "block" => Ok(Output::Http(
                    self.child(&context)?
                        .fetch(&context, request("pending"))
                        .await?,
                )),
                "cancel" => {
                    let future = self.child(&context)?.fetch(&context, request("pending"));
                    tokio::pin!(future);
                    cellar::asyncrt::select_biased! {
                        "caller drops a pending native facet operation";
                        _=self.signals.cancel.notified()=>{},
                        _=&mut future=>return Err(anyhow!("pending facet unexpectedly answered")),
                    }
                    Ok(response("cancelled"))
                }
                _ => unreachable!(),
            }
        })
    }
}
struct Leaf {
    class: Class,
    facets: Facets,
    signals: Arc<Signals>,
}
impl Cell for Leaf {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            assert_eq!(context.facet().unwrap().props, graph());
            match invocation {
                Invocation::CellRpc { method, args } => {
                    assert_eq!(method, "echo");
                    assert_eq!(cellar::application::decode_rpc(&args)?, graph());
                    Ok(Output::Rpc(args))
                }
                Invocation::Fetch(incoming) => {
                    if incoming.url.ends_with("pending") {
                        let _pending = Pending(self.signals.clone());
                        self.signals.pending.notify_one();
                        std::future::pending::<()>().await;
                    }
                    if incoming.url.ends_with("nested") {
                        let class = self.class.clone();
                        let child = self.facets.get(&context, "descendant", move |_| {
                            Box::pin(async move { Ok(Startup::new(class)) })
                        })?;
                        return Ok(Output::Http(child.fetch(&context, request("fetch")).await?));
                    }
                    let storage = context.storage()?;
                    let count = storage
                        .get_json("count")?
                        .and_then(|value| value.as_u64())
                        .unwrap_or(0)
                        + 1;
                    storage.put_json("count", &serde_json::json!(count))?;
                    Ok(response(&count.to_string()))
                }
                _ => unreachable!(),
            }
        })
    }
}
fn deployment() -> LoadedDeployment {
    LoadedDeployment {
        native: None,
        options: WorkerConfigOptions {
            src: "this is not JavaScript".into(),
            script_name: "native-facets".into(),
            do_classes: vec!["Supervisor".into()],
            bindings: Vec::new(),
            blob_bindings: Vec::new(),
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: "test".into(),
            modules: Vec::new(),
            compat: Compat::default(),
        },
        script_name: "native-facets".into(),
        version: "test".into(),
        prefix: "test/".into(),
        asset_binding: None,
        loader_bindings: Vec::new(),
        assets: None,
        services: Vec::new(),
        crons: Vec::new(),
        containers: Vec::new(),
        fence_image: None,
        container_images: Vec::new(),
    }
}
async fn call(runtime: &RuntimeManager, scope: &str, path: &str) -> Result<HttpResponse> {
    runtime
        .fetch_cell(
            scope.into(),
            Some("parent-name".into()),
            RuntimeFetch {
                url: format!("http://native/{path}"),
                method: "POST".into(),
                headers: Vec::new(),
                body: Vec::new().into(),
                request_id: Some(cellar::engine_api::next_request_id()),
                order: None,
                parent: None,
            },
            None,
        )
        .await
}
fn facet_files(path: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in cellar::asyncrt::fs().read_dir(path)? {
        let path = entry.path;
        if entry.is_dir {
            files.extend(facet_files(&path)?);
        } else if path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains(".facets-")
            && path
                .extension()
                .is_some_and(|extension| extension == "sqlite")
        {
            files.push(path);
        }
    }
    Ok(files)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_native_facets_preserve_lazy_startup_persistence_barriers_and_retirement(
) -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(30), run()).await??;
    Ok(())
}
async fn run() -> Result<()> {
    let signals = Arc::new(Signals::default());
    let supervisor_signals = signals.clone();
    let leaf_signals = signals.clone();
    let registry = Registry::new(|_, _| Box::pin(async { Ok(response("stateless")) }))
        .cell("Supervisor", move |context, _| {
            assert!(
                context.exported_cell_class("Supervisor").is_err(),
                "migrated classes cannot supply facets"
            );
            let mut props = graph();
            let class = context.exported_cell_class("Leaf")?.with_props(&props)?;
            props.objects.clear(); // The class copied its startup value.
            Ok(Arc::new(Supervisor {
                facets: context.facets()?,
                class,
                signals: supervisor_signals.clone(),
            }))
        })?
        .cell("Leaf", move |context, _| {
            leaf_signals.constructors.fetch_add(1, Ordering::SeqCst);
            leaf_signals
                .identities
                .lock()
                .unwrap()
                .push(context.facet().unwrap().clone());
            Ok(Arc::new(Leaf {
                facets: context.facets()?,
                class: context.exported_cell_class("Leaf")?.with_props(&graph())?,
                signals: leaf_signals.clone(),
            }))
        })?;
    let generation = Generation::build_native(
        1,
        DeploymentGraph::single(deployment()),
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([(
            "native-facets".into(),
            Arc::new(registry) as Arc<dyn cellar::application::Application>,
        )]),
    )?;
    let directory = tempfile::tempdir()?;
    let runtime = Arc::new(RuntimeManager::start(
        generation,
        RuntimeOptions {
            data_dir: directory.path().into(),
            replication: None,
            wake: None,
            alarm_observer: Arc::new(|_, _| {}),
            node: "test".into(),
            region: "test".into(),
            bucket: None,
        },
    )?);
    for scope in ["Supervisor:first", "Supervisor:other"] {
        runtime
            .restore_cell(
                scope,
                &cellar_logic::RestoreSpec {
                    epoch: 1,
                    fresh: true,
                    took_over: false,
                    resume_local: false,
                    prior: None,
                },
            )
            .await?;
        runtime.start_cell(scope.into(), 1, true).await?;
        runtime.publish_cell(scope, 1)?;
    }
    let scope = "Supervisor:first";
    assert_eq!(call(&runtime, scope, "lazy").await?.body, b"lazy");
    assert_eq!(signals.callbacks.load(Ordering::SeqCst), 0);
    assert_eq!(signals.constructors.load(Ordering::SeqCst), 0);
    assert_eq!(
        call(&runtime, scope, "failed-start").await?.body,
        b"failed once"
    );
    assert_eq!(signals.failed_callbacks.load(Ordering::SeqCst), 1);
    assert_eq!(call(&runtime, scope, "fetch").await?.body, b"1");
    assert_eq!(call(&runtime, scope, "fetch").await?.body, b"2");
    assert_eq!(call(&runtime, scope, "rpc").await?.body, b"rpc");
    assert_eq!(signals.callbacks.load(Ordering::SeqCst), 1);
    assert_eq!(signals.constructors.load(Ordering::SeqCst), 1);
    let identity = signals.identities.lock().unwrap()[0].clone();
    assert_eq!(identity.depth, 1);
    assert_eq!(
        identity.id.to_json()?,
        serde_json::json!(["Supervisor", "first", "parent-name"])
    );
    assert_eq!(
        call(&runtime, "Supervisor:other", "foreign").await?.body,
        b"refused"
    );
    assert_eq!(call(&runtime, scope, "abort").await?.body, b"aborted");
    assert_eq!(call(&runtime, scope, "fetch").await?.body, b"3");
    assert_eq!(signals.callbacks.load(Ordering::SeqCst), 2);
    assert_eq!(call(&runtime, scope, "nested").await?.body, b"1");
    assert_eq!(facet_files(directory.path())?.len(), 2);
    {
        let identities = signals.identities.lock().unwrap();
        assert_eq!(identities.last().unwrap().id, identity.id);
        assert_eq!(identities.last().unwrap().depth, 2);
    }
    call(&runtime, scope, "delete").await?;
    assert!(
        facet_files(directory.path())?.is_empty(),
        "delete removes descendant standalone databases"
    );
    assert_eq!(call(&runtime, scope, "fetch").await?.body, b"1");

    assert_eq!(call(&runtime, scope, "delete-detached").await?.body, b"1");
    assert_eq!(facet_files(directory.path())?.len(), 1);

    // A real filesystem failure is retained as the name's barrier. Retry can
    // clear it; a new get cannot silently revive the failed delete's image.
    call(&runtime, scope, "abort").await?;
    let file = facet_files(directory.path())?.pop().unwrap();
    cellar::asyncrt::fs().remove_file(&file)?;
    cellar::asyncrt::fs().create_dir_all(&file)?;
    assert!(call(&runtime, scope, "delete").await.is_err());
    let before = signals.callbacks.load(Ordering::SeqCst);
    assert!(call(&runtime, scope, "fetch").await.is_err());
    assert_eq!(signals.callbacks.load(Ordering::SeqCst), before);
    cellar::asyncrt::fs().remove_dir_all(&file)?;
    call(&runtime, scope, "delete").await?;
    assert_eq!(call(&runtime, scope, "fetch").await?.body, b"1");

    let owner = runtime.clone();
    let cancelled =
        cellar::asyncrt::spawn(async move { call(&owner, "Supervisor:first", "cancel").await });
    signals.pending.notified().await;
    signals.cancel.notify_one();
    assert_eq!(cancelled.await??.body, b"cancelled");
    signals.dropped.notified().await;
    assert_eq!(signals.pending_drops.load(Ordering::SeqCst), 1);
    let owner = runtime.clone();
    let pending =
        cellar::asyncrt::spawn(async move { call(&owner, "Supervisor:first", "block").await });
    signals.pending.notified().await;
    runtime.swap_out_cell(scope, 1).await?;
    assert!(pending.await?.is_err());
    signals.dropped.notified().await;
    assert_eq!(signals.pending_drops.load(Ordering::SeqCst), 2);
    runtime.start_cell(scope.into(), 1, false).await?;
    runtime.publish_cell(scope, 1)?;
    assert_eq!(call(&runtime, scope, "stale").await?.body, b"refused");
    assert_eq!(call(&runtime, scope, "fetch").await?.body, b"2");
    for scope in [scope, "Supervisor:other"] {
        runtime.swap_out_cell(scope, 1).await?;
    }
    Ok(())
}
