// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::{
    application::{Application, Cell, EventState, HandlerFuture, Invocation, Output, Registry},
    engine_api::{Compat, RuntimeFetch, RuntimeOptions, WorkerConfigOptions},
    fleet::LoadedDeployment,
    generation::{DeploymentGraph, Generation, GenerationOptions},
    runtime::RuntimeManager,
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex,
    },
    time::Duration,
};

#[derive(Default)]
struct Signals {
    route: Mutex<Option<Persistent>>,
    event: Mutex<Weak<EventState>>,
    io: Mutex<Weak<crate::host_io::IoContext>>,
    first: AtomicUsize,
    second: AtomicUsize,
}

struct Root(Arc<Signals>);

impl Cell for Root {
    fn invoke(self: Arc<Self>, context: Context, _event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            *self.0.event.lock().unwrap() = Arc::downgrade(&context.state);
            *self.0.io.lock().unwrap() = Arc::downgrade(&context.io);
            let props =
                crate::native::clone::StructuredValue::from_json(serde_json::json!({"phase":1}))?;
            let fetcher = context.exported_entrypoint("gateway")?.with_props(&props)?;
            let body = match fetcher.persistent(&context) {
                Ok(route) => {
                    *self.0.route.lock().unwrap() = Some(route);
                    b"captured".to_vec()
                }
                Err(error) => error.to_string().into_bytes(),
            };
            Ok(Output::Http(response(body)))
        })
    }
}

fn response(body: Vec<u8>) -> HttpResponse {
    HttpResponse {
        status: 200,
        body,
        stream: None,
        headers: Vec::new(),
        websocket: None,
        write_position: None,
        observed_position: None,
    }
}

fn deployment() -> LoadedDeployment {
    LoadedDeployment {
        event_streams: Vec::new(),
        native: None,
        options: WorkerConfigOptions {
            stream_bindings: Vec::new(),
            src: String::new(),
            script_name: "persistent-route".into(),
            do_classes: vec!["Root".into()],
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
        script_name: "persistent-route".into(),
        version: "test".into(),
        prefix: "test/".into(),
        asset_binding: None,
        loader_bindings: Vec::new(),
        assets: None,
        services: Vec::new(),
        crons: Vec::new(),
        cron_revision: None,
        containers: Vec::new(),
        fence_image: None,
        container_images: Vec::new(),
    }
}

fn generation(id: u64, signals: Arc<Signals>) -> Result<Generation> {
    let target = signals.clone();
    let app = Registry::new(move |context, invocation| {
        let signals = target.clone();
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                anyhow::bail!("unexpected service event")
            };
            let entry = request.entrypoint.context("captured entrypoint missing")?;
            assert_eq!(entry.name, "gateway");
            assert_eq!(
                entry.props,
                crate::native::clone::encode(&crate::native::clone::StructuredValue::from_json(
                    serde_json::json!({"phase":1})
                )?)?
            );
            assert_eq!(request.method, "POST");
            assert_eq!(request.body.bytes(), b"payload");
            if context.generation == 1 {
                signals.first.fetch_add(1, Ordering::SeqCst);
            } else {
                signals.second.fetch_add(1, Ordering::SeqCst);
            }
            Ok(Output::Http(response(
                context.generation.to_string().into_bytes(),
            )))
        })
    })
    .cell("Root", move |_, _| Ok(Arc::new(Root(signals.clone()))))?;
    Generation::build_native(
        id,
        DeploymentGraph::single(deployment()),
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        HashMap::from([(
            "persistent-route".into(),
            Arc::new(app) as Arc<dyn Application>,
        )]),
    )
}

fn request() -> Request {
    Request {
        url: "http://gateway/owned".into(),
        method: "POST".into(),
        headers: Vec::new(),
        body: b"payload".to_vec().into(),
        entrypoint: Some(crate::WorkerFetchEntrypoint {
            name: "forged-request-target".into(),
            props: Vec::new(),
        }),
    }
}

fn dispatch(route: &Persistent, live: Arc<AtomicBool>) -> HandlerFuture<HttpResponse> {
    let request = request();
    let guard = RequestBodyGuard::of(&request.body);
    route.dispatch(
        request,
        guard,
        Arc::new(move || {
            ensure!(live.load(Ordering::Acquire), "test run retired");
            Ok(())
        }),
    )
}

fn check_gate(gate: &GateReq) {
    assert_eq!(gate.scope, "Root:retained");
    assert_eq!(
        gate.ticket,
        GateTicket {
            channel: cellar_logic::Channel::Service,
            position: None,
            observed: None,
            epoch: Some(7)
        }
    );
}

async fn install(runtime: &RuntimeManager) -> Result<HttpResponse> {
    runtime
        .restore_cell(
            "Root:retained",
            &cellar_logic::RestoreSpec {
                epoch: 7,
                fresh: true,
                took_over: false,
                resume_local: false,
                prior: None,
            },
        )
        .await?;
    runtime.start_cell("Root:retained".into(), 7, true).await?;
    runtime.publish_cell("Root:retained", 7)?;
    runtime
        .fetch_cell(
            "Root:retained".into(),
            None,
            RuntimeFetch {
                url: "http://root/install".into(),
                method: "GET".into(),
                headers: Vec::new(),
                body: Vec::new().into(),
                request_id: Some(crate::engine_api::next_request_id()),
                order: None,
                parent: None,
            },
            None,
        )
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_routes_use_fresh_epoch_gates_without_pinning_the_installing_event() -> Result<()>
{
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let directory = tempfile::tempdir()?;
    let result = domain
        .run(crate::asyncrt::timeout(Duration::from_secs(45), async {
            let signals = Arc::new(Signals::default());
            let (gate_tx, mut gates) = mpsc::unbounded_channel();
            crate::host_channels::set_gate_tx(gate_tx);
            let (send, mut calls) = mpsc::unbounded_channel();
            crate::host_channels::set_svc_call_tx(send);
            let runtime = Arc::new(RuntimeManager::start(
                generation(1, signals.clone())?,
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
            let target = runtime.clone();
            let forwarding = crate::asyncrt::spawn(async move {
                while let Some(call) = calls.recv().await {
                    crate::host_service::dispatch_fetch(Some(&target), call).await;
                }
            });
            assert_eq!(install(&runtime).await?.body, b"captured");
            let route = signals
                .route
                .lock()
                .unwrap()
                .clone()
                .context("capture missing")?;
            crate::asyncrt::timeout(Duration::from_secs(2), async {
                while signals.event.lock().unwrap().upgrade().is_some()
                    || signals.io.lock().unwrap().upgrade().is_some()
                {
                    crate::asyncrt::sleep(Duration::from_millis(1)).await;
                }
            })
            .await?;
            for admitted in [true, false, true] {
                let live = Arc::new(AtomicBool::new(true));
                let future = dispatch(&route, live);
                let call = crate::asyncrt::spawn(future);
                let gate = crate::asyncrt::timeout(Duration::from_secs(2), gates.recv())
                    .await?
                    .context("fresh gate missing")?;
                check_gate(&gate);
                let before = signals.first.load(Ordering::SeqCst);
                gate.reply
                    .send(if admitted {
                        Ok(())
                    } else {
                        Err(cellar_logic::RequestError::NodeFenced)
                    })
                    .unwrap();
                let reply = call.await?;
                if admitted {
                    assert_eq!(reply?.body, b"1");
                    assert_eq!(signals.first.load(Ordering::SeqCst), before + 1);
                } else {
                    assert!(reply.is_err());
                    assert_eq!(signals.first.load(Ordering::SeqCst), before);
                }
            }
            let call = crate::asyncrt::spawn(dispatch(&route, Arc::new(AtomicBool::new(true))));
            let gate = crate::asyncrt::timeout(Duration::from_secs(2), gates.recv())
                .await?
                .context("cancellation gate missing")?;
            check_gate(&gate);
            call.abort();
            let Err(cancelled) = call.await else {
                anyhow::bail!("cancelled route returned a value")
            };
            assert!(cancelled.message.contains("cancelled"), "{cancelled}");
            assert!(
                gate.reply.is_closed(),
                "cancelled route kept its gate waiter alive"
            );
            let before = signals.first.load(Ordering::SeqCst);
            let live = Arc::new(AtomicBool::new(true));
            let call = crate::asyncrt::spawn(dispatch(&route, live.clone()));
            let gate = crate::asyncrt::timeout(Duration::from_secs(2), gates.recv())
                .await?
                .context("retirement gate missing")?;
            live.store(false, Ordering::Release);
            gate.reply.send(Ok(())).unwrap();
            assert!(call.await?.is_err());
            assert_eq!(signals.first.load(Ordering::SeqCst), before);
            let other = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
            let foreign = other
                .run(dispatch(&route, Arc::new(AtomicBool::new(true))))
                .await?;
            other.shutdown().await;
            assert!(foreign.is_err());
            assert!(gates.try_recv().is_err());
            runtime.adopt(generation(2, signals.clone())?);
            runtime.swap_out_cell("Root:retained", 7).await?;
            // Generation maintenance runs every 30 s in production. Exercise the
            // real reap boundary rather than manufacturing a missing route id.
            crate::asyncrt::timeout(Duration::from_secs(35), async {
                while runtime
                    .draining_generations()
                    .iter()
                    .any(|generation| generation.id() == 1)
                {
                    crate::asyncrt::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
            let call = crate::asyncrt::spawn(dispatch(&route, Arc::new(AtomicBool::new(true))));
            let gate = crate::asyncrt::timeout(Duration::from_secs(2), gates.recv())
                .await?
                .context("expired generation gate missing")?;
            check_gate(&gate);
            gate.reply.send(Ok(())).unwrap();
            assert!(
                call.await?.is_err(),
                "retired route selected the current deployment"
            );
            assert_eq!(signals.second.load(Ordering::SeqCst), 0);
            forwarding.abort();
            let _ = forwarding.await;
            Ok::<_, anyhow::Error>(())
        }))
        .await;
    domain.shutdown().await;
    result??
}

#[tokio::test]
async fn unversioned_routes_refuse_persistent_capture() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let directory = tempfile::tempdir()?;
    let result = domain
        .run(crate::asyncrt::timeout(Duration::from_secs(45), async {
            let signals = Arc::new(Signals::default());
            let runtime = RuntimeManager::start(
                generation(0, signals.clone())?,
                RuntimeOptions {
                    data_dir: directory.path().into(),
                    replication: None,
                    wake: None,
                    alarm_observer: Arc::new(|_, _| {}),
                    node: "test".into(),
                    region: "test".into(),
                    bucket: None,
                },
            )?;
            let reply = install(&runtime).await?;
            assert!(String::from_utf8(reply.body)?.contains("explicit deployment generation"));
            assert!(signals.route.lock().unwrap().is_none());
            runtime.swap_out_cell("Root:retained", 7).await?;
            Ok::<_, anyhow::Error>(())
        }))
        .await;
    domain.shutdown().await;
    result??
}
