// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::container::{CellContainer, CellState, ContainerSpec, ResourceNames, RunExits};
use crate::{asyncrt, docker::Docker};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simultaneous_cells_share_atomic_admission_and_include_helper_memory() -> Result<()> {
    for (limits, class_limit, admitted) in [
        (
            Some(CapacityLimits::new(
                16,
                2 * (1024 * 1024 * 1024 + network::MEMORY_BYTES),
            )?),
            None,
            2,
        ),
        (Some(CapacityLimits::new(3, u64::MAX)?), None, 3),
        (None, Some(4), 4),
    ] {
        let domain = asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let directory = tempfile::tempdir()?;
        let result = domain
            .run(async {
                let service = asyncrt::services().containers();
                let engine = Arc::new(ContainerEngine {
                    service: Arc::downgrade(&service),
                    names: ResourceNames::new("admission", directory.path(), None),
                    runtime: Some("runc".into()),
                    dns: Vec::new(),
                    network_helper: Some(crate::container::NetworkHelperOptions::new(
                        format!("sha256:{}", "b".repeat(64)),
                        "/bin/helper".into(),
                        "127.0.0.11:53".parse()?,
                    )),
                    capacity: limits,
                    resolv_conf: None,
                    docker: Docker::new(directory.path().join("unused.sock")),
                    node: "admission".into(),
                    bucket: None,
                    images: Mutex::new(HashMap::new()),
                    cells: Mutex::new(HashMap::new()),
                    networks: tokio::sync::Mutex::new(None),
                    fenced: tokio::sync::Mutex::new(false),
                });
                let mut candidates = Vec::new();
                for index in 0..16 {
                    let scope = format!("Sandbox:{index}");
                    let cell = Arc::new(CellContainer {
                        sdk_control: tokio::sync::Mutex::new(()),
                        scope: scope.clone(),
                        name: scope.clone(),
                        spec: Arc::new(ContainerSpec {
                            class_name: "Sandbox".into(),
                            image: "unused".into(),
                            instance_type: Some("basic".into()),
                            max_instances: class_limit,
                            runtime: None,
                            exposed_ports: Vec::new(),
                        }),
                        state: Mutex::new(CellState::default()),
                        processes: Mutex::new(Vec::new()),
                        process_registry: Arc::downgrade(&service.processes),
                        exits: Mutex::new(RunExits::default()),
                    });
                    engine.cells.lock().unwrap().insert(scope, cell.clone());
                    candidates.push(cell);
                }
                let barrier = Arc::new(tokio::sync::Barrier::new(candidates.len()));
                let tasks = candidates
                    .into_iter()
                    .map(|cell| {
                        let engine = engine.clone();
                        let barrier = barrier.clone();
                        asyncrt::spawn(async move {
                            let run = cell.begin_run();
                            barrier.wait().await;
                            match engine.admit_start(&cell, run) {
                                Ok(()) => true,
                                Err(error) => {
                                    assert!(error.is::<CapacityExceeded>());
                                    false
                                }
                            }
                        })
                    })
                    .collect::<Vec<_>>();
                let mut successful = 0;
                for task in tasks {
                    successful += u64::from(task.await?);
                }
                assert_eq!(successful, admitted);
                assert_eq!(
                    engine.reserved_memory_bytes(),
                    admitted * (1024 * 1024 * 1024 + network::MEMORY_BYTES)
                );
                assert_eq!(engine.running_instances_by_class()["Sandbox"], admitted);
                // Published fleet hints use the same uncertain reservations and
                // helper memory as hard admission, not just running processes.
                let sample = engine
                    .cells
                    .lock()
                    .unwrap()
                    .values()
                    .next()
                    .unwrap()
                    .clone();
                service.settings.lock().unwrap().options = Some(crate::container::EngineOptions {
                    capacity: limits,
                    socket: directory.path().join("unused.sock"),
                    namespace: None,
                    runtime: None,
                    dns: Vec::new(),
                    network_helper: engine.network_helper.clone(),
                });
                service.deployment.write().unwrap().specs = vec![sample.spec.clone()];
                *service.engine.lock().unwrap() = Some(engine.clone());
                assert_eq!(available_instances_by_class()["Sandbox"], 0);
                let occupied = engine
                    .cells
                    .lock()
                    .unwrap()
                    .values()
                    .find(|cell| cell.state.lock().unwrap().capacity_reserved)
                    .unwrap()
                    .clone();
                occupied.state.lock().unwrap().release_unused_capacity();
                assert_eq!(available_instances_by_class()["Sandbox"], 1);

                Ok::<_, anyhow::Error>(())
            })
            .await;
        domain.shutdown().await;
        result??;
    }
    Ok(())
}
