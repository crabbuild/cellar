// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use std::{
    future::{poll_fn, Future},
    pin::pin,
    task::Poll,
};

#[tokio::test]
async fn failed_transport_cannot_be_reactivated_by_late_readiness() -> Result<()> {
    let domain = asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let run = Run::new(1)?;
            let mut guard = ProtocolGuard {
                run: run.clone(),
                engine: Weak::new(),
                cell: Weak::new(),
                finished: false,
            };
            guard.finish(Err(anyhow!("injected transport failure")));
            // Readiness may already have completed in a concurrently resumed start.
            run.ready.store(true, Ordering::Release);
            assert!(
                !run.active(),
                "late startup restored failed network authority"
            );
            assert!(run.done.borrow().as_ref().is_some_and(Result::is_err));
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result?
}

#[tokio::test]
async fn abandoning_start_before_helper_creation_completes_its_monitor() -> Result<()> {
    let domain = asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let directory = tempfile::tempdir()?;
    let result = domain
        .run(async {
            let (engine, cell) = fixture(directory.path())?;
            let number = cell.begin_run();
            let monitor = engine.monitor(&cell, number);
            drop(StartGuard::new(&engine, &cell, number));
            assert!(!cell.running() && cell.address(8080).is_err());
            let error = asyncrt::timeout(Duration::from_secs(1), monitor)
                .await?
                .unwrap_err();
            assert!(error.contains("startup was abandoned"));
            assert_eq!(engine.reserved_memory_bytes(), 0);
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result?
}

#[tokio::test]
async fn cancelled_join_preserves_the_task_for_the_next_cleanup() -> Result<()> {
    let domain = asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let run = Run::new(1)?;
            let (release, released) = tokio::sync::oneshot::channel();
            let done = run.done.clone();
            let task = asyncrt::spawn(async move {
                let _ = released.await;
                done.send_replace(Some(Ok(())));
            });
            run.started.store(true, Ordering::Release);
            *run.task.lock().unwrap() = Some(task);
            {
                let mut joining = pin!(run.join(Duration::from_secs(1)));
                assert!(poll_fn(|cx| Poll::Ready(joining.as_mut().poll(cx)))
                    .await
                    .is_pending());
                assert!(run.task.lock().unwrap().is_none());
            }
            assert!(
                run.task.lock().unwrap().is_some(),
                "cancelled cleanup detached its task"
            );
            release
                .send(())
                .map_err(|_| anyhow!("network task ended early"))?;
            run.join(Duration::from_secs(1)).await?;
            assert!(run.task.lock().unwrap().is_none());
            run.join(Duration::from_secs(1)).await?;
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result?
}

#[tokio::test]
async fn timed_out_join_aborts_and_joins_the_owned_task() -> Result<()> {
    struct Exited(Arc<AtomicBool>);

    impl Drop for Exited {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let domain = asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let run = Run::new(1)?;
            let exited = Arc::new(AtomicBool::new(false));
            let guard = Exited(exited.clone());
            *run.task.lock().unwrap() = Some(asyncrt::spawn(async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            }));
            run.started.store(true, Ordering::Release);
            assert!(run.join(Duration::from_millis(20)).await.is_err());
            assert!(
                exited.load(Ordering::Acquire),
                "timeout returned before the task was joined"
            );
            assert!(run.task.lock().unwrap().is_none());
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result?
}

#[tokio::test]
async fn ownership_release_retires_network_before_sdk_control_is_available() -> Result<()> {
    let domain = asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let directory = tempfile::tempdir()?;
    let result = domain
        .run(async {
            let (engine, cell) = fixture(directory.path())?;
            let number = cell.begin_run();
            let network = Run::new(number)?;
            network.ready.store(true, Ordering::Release);
            {
                let mut state = cell.state.lock().unwrap();
                state.network = Some(network.clone());
                state.address = Address::Ip("127.0.0.1".into());
                state.resource = Some(EngineResource {
                    run: number,
                    id: ContainerId::new(&"a".repeat(64))?,
                });
            }
            assert!(cell.address(8080).is_ok() && cell.engine_id(number).is_ok());
            let _held = cell.sdk_control.lock().await;
            {
                let mut release = pin!(engine.release(&cell.scope, Release::Destroy));
                let polled = poll_fn(|cx| Poll::Ready(release.as_mut().poll(cx))).await;
                assert!(
                    polled.is_pending(),
                    "ownership release bypassed SDK serialization"
                );
                assert!(
                    !network.active()
                        && cell.address(8080).is_err()
                        && cell.engine_id(number).is_err(),
                    "ownership loss left usable network authority while waiting for SDK control"
                );
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result?
}

fn fixture(directory: &std::path::Path) -> Result<(Arc<ContainerEngine>, Arc<CellContainer>)> {
    let service = asyncrt::services().containers();
    let engine = Arc::new(ContainerEngine {
        service: Arc::downgrade(&service),
        names: ResourceNames::new("unit", directory, Some("network-revocation")),
        runtime: Some("runc".into()),
        dns: Vec::new(),
        network_helper: Some(NetworkHelperOptions::new(
            format!("sha256:{}", "b".repeat(64)),
            "/bin/helper".into(),
            "127.0.0.11:53".parse()?,
        )),
        resolv_conf: None,
        docker: Docker::new(directory.join("unused.sock")),
        node: "unit".into(),
        bucket: None,
        images: Mutex::new(HashMap::new()),
        cells: Mutex::new(HashMap::new()),
        networks: tokio::sync::Mutex::new(None),
        fenced: tokio::sync::Mutex::new(false),
    });
    let cell = Arc::new(CellContainer {
        sdk_control: tokio::sync::Mutex::new(()),
        scope: "Workspace:owned".into(),
        name: "owned".into(),
        spec: Arc::new(ContainerSpec {
            class_name: "Workspace".into(),
            image: "unused".into(),
            instance_type: None,
            max_instances: None,
            runtime: None,
            exposed_ports: Vec::new(),
        }),
        state: Mutex::new(CellState::default()),
        processes: Mutex::new(Vec::new()),
        process_registry: Arc::downgrade(&service.processes),
        exits: Mutex::new(RunExits::default()),
    });
    engine
        .cells
        .lock()
        .unwrap()
        .insert(cell.scope.clone(), cell.clone());
    Ok((engine, cell))
}

#[tokio::test]
async fn empty_inventory_cannot_discharge_an_unresolved_create() -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let domain = asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let directory = tempfile::tempdir()?;
    let listener = tokio::net::UnixListener::bind(directory.path().join("unused.sock"))?;
    let result = domain
        .run(async {
            let peer = asyncrt::spawn(async move {
                for _ in 0..2 {
                    let (mut stream, _) = listener.accept().await?;
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        anyhow::ensure!(request.len() < 8192, "unbounded inventory request");
                        request.push(stream.read_u8().await?);
                    }
                    anyhow::ensure!(
                        request.starts_with(b"GET /containers/json?all=true&filters="),
                        "unexpected Engine request"
                    );
                    stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n[]",
                        )
                        .await?;
                }
                Ok::<_, anyhow::Error>(())
            });
            for submitted in [false, true] {
                let (engine, cell) = fixture(directory.path())?;
                let number = cell.begin_run();
                let network = Run::new(number)?;
                network
                    .creation_submitted
                    .store(submitted, Ordering::Release);
                {
                    let mut state = cell.state.lock().unwrap();
                    state.running = false;
                    state.network = Some(network.clone());
                }
                let result = asyncrt::timeout(
                    Duration::from_secs(2),
                    engine.cleanup_network(&cell, &network),
                )
                .await?;
                if submitted {
                    assert!(result
                        .unwrap_err()
                        .to_string()
                        .contains("creation outcome is unknown"));
                    assert!(cell
                        .state
                        .lock()
                        .unwrap()
                        .network
                        .as_ref()
                        .is_some_and(|current| Arc::ptr_eq(current, &network)));
                    assert_eq!(engine.reserved_memory_bytes(), MEMORY_BYTES);
                } else {
                    result?;
                    assert!(cell.state.lock().unwrap().network.is_none());
                    assert_eq!(engine.reserved_memory_bytes(), 0);
                }
            }
            peer.await??;
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result?
}
