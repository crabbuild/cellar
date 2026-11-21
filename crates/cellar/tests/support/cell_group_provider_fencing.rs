// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Execution-claim protocol qualification. Docker runs real revocable processes;
//! this fixture controller is not a production controller or native adapter.
use super::*;
use cellar::sdk::containers::{BackendIdentity, ExecutionClaim, ExecutionFenceAuthority};
use std::collections::BTreeMap;

struct Controller {
    active: tokio::sync::Mutex<BTreeMap<[u8; 32], ExecutionClaim>>,
    calls: AtomicUsize,
    refuse: std::sync::atomic::AtomicBool,
    docker: Option<Arc<Docker>>,
}

impl ExecutionFenceAuthority for Controller {
    fn identity(&self) -> BackendIdentity {
        BackendIdentity {
            binding: "TOOL".into(),
            provider: "docker".into(),
            configuration_digest: hex::encode([19; 32]),
        }
    }

    fn activate(self: Arc<Self>, claim: ExecutionClaim) -> HandlerFuture<()> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::AcqRel);
            ensure!(
                !self.refuse.load(Ordering::Acquire),
                "controller refused execution"
            );
            let mut active = self.active.lock().await;
            if let Some(previous) = active.get(&claim.resource_key()) {
                ensure!(
                    claim.sequence() >= previous.sequence(),
                    "stale controller claim"
                );
                if claim.sequence() == previous.sequence() {
                    ensure!(
                        claim.owner_digest() == previous.owner_digest()
                            && claim.checkpoint() == previous.checkpoint(),
                        "conflicting controller claim"
                    );
                    return Ok(());
                }
                if let Some(docker) = &self.docker {
                    docker.stop(previous).await?;
                }
            }
            if let Some(docker) = &self.docker {
                docker.start(&claim).await?;
            }
            active.insert(claim.resource_key(), claim);
            Ok(())
        })
    }
}

impl Controller {
    async fn submit(&self, claim: &ExecutionClaim) -> Result<()> {
        // Submission and grant replacement share the same controller lock.
        // A delayed request cannot race the old-process stop/new-grant barrier.
        let active = self.active.lock().await;
        let current = active
            .get(&claim.resource_key())
            .context("no active grant")?;
        ensure!(
            current.sequence() == claim.sequence()
                && current.owner_digest() == claim.owner_digest()
                && current.checkpoint().record.generation == claim.checkpoint().record.generation,
            "stale provider execution grant"
        );
        if let Some(docker) = &self.docker {
            docker.effect(claim).await?;
        }
        Ok(())
    }
}

struct Docker {
    engine: cellar::docker::Docker,
    image: String,
    owner: String,
}

impl Docker {
    fn configured() -> Result<Self> {
        let socket = std::env::var("CELLAR_CG17_FENCE_DOCKER_SOCKET")?;
        let image = std::env::var("CELLAR_CG17_FENCE_DOCKER_IMAGE")?;
        let owner = std::env::var("CELLAR_CG17_FENCE_DOCKER_OWNER")?;
        ensure!(
            image.contains("@sha256:")
                && !owner.is_empty()
                && owner.len() <= 80
                && owner
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
            "unowned/unpinned Docker fixture"
        );
        Ok(Self {
            engine: cellar::docker::Docker::new(socket),
            image,
            owner,
        })
    }

    fn name(&self, claim: &ExecutionClaim) -> String {
        format!(
            "{}-{}-{}",
            self.owner,
            hex::encode(claim.resource_key()),
            claim.sequence()
        )
    }

    async fn start(&self, claim: &ExecutionClaim) -> Result<()> {
        let name = self.name(claim);
        let created = self.engine.expect("POST", &format!("/containers/create?name={name}"), Some(json!({
            "Image": self.image, "Cmd": ["/bin/sh", "-c", "while :; do sleep 1; done"],
            "Labels": {"cellar.cg17.owner": self.owner, "cellar.cg17.claim": claim.owner_digest()},
            "NetworkDisabled": true,
            "HostConfig": {"Memory": 67108864, "MemorySwap": 67108864, "NanoCpus": 100000000, "RestartPolicy": {"Name": "no"}}
        })), "create fenced fixture process").await?.json()?;
        let id =
            cellar::docker::ContainerId::new(created["Id"].as_str().context("created Docker ID")?)?;
        self.engine
            .expect(
                "POST",
                &format!("/containers/{id}/start"),
                None,
                "start fenced fixture process",
            )
            .await?;
        ensure!(
            self.running(claim).await?,
            "new grant has no running process"
        );
        eprintln!("retained Docker execution grant: {name} id={id}");
        Ok(())
    }

    async fn inspect(&self, claim: &ExecutionClaim) -> Result<serde_json::Value> {
        let inspected = self
            .engine
            .expect(
                "GET",
                &format!("/containers/{}/json", self.name(claim)),
                None,
                "inspect fenced fixture",
            )
            .await?
            .json()?;
        ensure!(
            inspected["Config"]["Labels"]["cellar.cg17.owner"].as_str()
                == Some(self.owner.as_str())
                && inspected["Config"]["Labels"]["cellar.cg17.claim"].as_str()
                    == Some(claim.owner_digest()),
            "foreign Docker process"
        );
        Ok(inspected)
    }

    async fn running(&self, claim: &ExecutionClaim) -> Result<bool> {
        Ok(self.inspect(claim).await?["State"]["Running"].as_bool() == Some(true))
    }

    async fn stop(&self, claim: &ExecutionClaim) -> Result<()> {
        let inspected = self.inspect(claim).await?;
        ensure!(
            inspected["State"]["Running"].as_bool() == Some(true),
            "old process disappeared before controller fence"
        );
        let id = cellar::docker::ContainerId::new(
            inspected["Id"].as_str().context("inspected Docker ID")?,
        )?;
        self.engine
            .expect(
                "POST",
                &format!("/containers/{id}/kill?signal=SIGKILL"),
                None,
                "fence old fixture process",
            )
            .await?;
        let exit = self
            .engine
            .expect(
                "POST",
                &format!("/containers/{id}/wait?condition=not-running"),
                None,
                "observe fenced process exit",
            )
            .await?
            .json()?;
        ensure!(
            exit["StatusCode"].as_i64() == Some(137),
            "old executor did not exit from SIGKILL"
        );
        ensure!(
            !self.running(claim).await?,
            "old grant retained a live process"
        );
        Ok(())
    }

    async fn effect(&self, claim: &ExecutionClaim) -> Result<()> {
        let inspected = self.inspect(claim).await?;
        let id = cellar::docker::ContainerId::new(
            inspected["Id"].as_str().context("inspected Docker ID")?,
        )?;
        let exec = self
            .engine
            .expect(
                "POST",
                &format!("/containers/{id}/exec"),
                Some(json!({
                    "AttachStdout": true, "AttachStderr": true, "Tty": true,
                    "Cmd": ["/bin/sh", "-c", "printf execution-grant >> /effect; cat /effect"]
                })),
                "create fixture effect",
            )
            .await?
            .json()?;
        let exec = exec["Id"].as_str().context("fixture exec ID")?;
        ensure!(
            exec.len() == 64 && exec.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid exec ID"
        );
        let reply = self
            .engine
            .expect(
                "POST",
                &format!("/exec/{exec}/start"),
                Some(json!({"Detach": false, "Tty": true})),
                "run fixture effect",
            )
            .await?;
        let state = self
            .engine
            .expect(
                "GET",
                &format!("/exec/{exec}/json"),
                None,
                "inspect fixture effect",
            )
            .await?
            .json()?;
        ensure!(
            state["ExitCode"].as_i64() == Some(0) && reply.body.as_ref() == b"execution-grant",
            "provider effect bytes/exit status mismatch"
        );
        Ok(())
    }
}

pub(super) async fn qualify(real_rustfs: bool, docker: bool) -> Result<()> {
    for rooted in [false, true] {
        for minimum in [Durability::Quorum, Durability::S3] {
            let docker = if docker {
                let mut configured = Docker::configured()?;
                configured.owner.push_str(&format!(
                    "-{}-{}",
                    u8::from(rooted),
                    if minimum == Durability::S3 {
                        "s3"
                    } else {
                        "quorum"
                    }
                ));
                Some(Arc::new(configured))
            } else {
                None
            };
            let backend = Arc::new(if real_rustfs {
                catalog_fault::Store::around(rustfs::store(
                    if minimum == Durability::S3 {
                        "execution-fence-s3"
                    } else {
                        "execution-fence-quorum"
                    },
                    rooted,
                )?)
            } else {
                catalog_fault::Store::default()
            });
            let controller = Arc::new(Controller {
                active: Default::default(),
                calls: AtomicUsize::new(0),
                refuse: false.into(),
                docker: docker.clone(),
            });
            let probe = Arc::new(Probe::default());
            *probe.fence.lock().unwrap() = Some(controller.clone());
            let executor = controller.clone();
            *probe.fenced_effect.lock().unwrap() = Some(Arc::new(move |claim| {
                let executor = executor.clone();
                Box::pin(async move { executor.submit(&claim).await })
            }));
            let mut cluster = start(rooted, minimum, backend, probe.clone(), 64 * 1024).await?;
            let domain = cluster.fixtures[0].domain.clone();
            let work = domain
                .run(async {
                    let group = cluster.history;
                    let history = CellHistory {
                        cluster: group.cluster,
                        namespace: group.namespace,
                        cell: CellId::new("Counter:execution-fence")?,
                        incarnation: IncarnationId::new([224; 16])?,
                    };
                    let leader = managed_fixture::leader(&cluster.hosts, group).await?;
                    cluster.hosts[leader]
                        .cell(history.clone())?
                        .enroll(
                            CellEnrollmentRequest {
                                creation: ProposalId::new([225; 16])?,
                                durability: minimum,
                            },
                            TIMEOUT,
                        )
                        .await?;
                    call(&cluster, &history, minimum, 1, "Create").await?;
                    let first = call(&cluster, &history, minimum, 2, "Fence").await?;
                    let old = probe
                        .claims
                        .lock()
                        .unwrap()
                        .last()
                        .cloned()
                        .context("first execution claim")?;
                    ensure!(
                        old.sequence() == 1 && old.history() == &history && old.binding() == "TOOL"
                    );
                    let replay = call(&cluster, &history, minimum, 2, "Fence").await?;
                    ensure!(
                        first.result == replay.result
                            && controller.calls.load(Ordering::Acquire) == 1
                    );
                    let same = call(&cluster, &history, minimum, 3, "Fence").await?;
                    ensure!(
                        same.result == first.result,
                        "same tenure minted another grant"
                    );
                    if let Some(docker) = &docker {
                        ensure!(docker.running(&old).await?);
                    }
                    bindings::restart(&mut cluster).await?;
                    call(&cluster, &history, minimum, 4, "Fence").await?;
                    let new = probe
                        .claims
                        .lock()
                        .unwrap()
                        .last()
                        .cloned()
                        .context("new execution claim")?;
                    ensure!(
                        new.sequence() == 2
                            && new.resource_key() == old.resource_key()
                            && new.owner_digest() != old.owner_digest()
                            && new.checkpoint().record == old.checkpoint().record,
                        "retained restart changed resource identity or failed to fence ownership"
                    );
                    ensure!(
                        controller.clone().activate(old.clone()).await.is_err(),
                        "delayed old claim regained execution"
                    );
                    ensure!(
                        controller.submit(&old).await.is_err(),
                        "old execution token admitted provider work"
                    );
                    call(&cluster, &history, minimum, 5, "FencedExec").await?;
                    if let Some(docker) = &docker {
                        ensure!(!docker.running(&old).await? && docker.running(&new).await?);
                        docker.stop(&new).await?;
                    }
                    controller.refuse.store(true, Ordering::Release);
                    let refused = call(&cluster, &history, minimum, 6, "Fence")
                        .await
                        .err()
                        .context("unavailable controller admitted a grant")?;
                    let refused = refused
                        .downcast_ref::<Arc<CellCommandError>>()
                        .context("controller failure lost its typed command error")?;
                    ensure!(
                        refused.ambiguous && refused.command == bindings::options(6, minimum)?.id,
                        "controller failure erased the original command uncertainty"
                    );
                    ensure!(
                        probe.claims.lock().unwrap().len() == 4,
                        "failed fence published a grant"
                    );
                    eprintln!("retained refused fencing command: {:?}", refused.command);
                    Ok::<_, anyhow::Error>(())
                })
                .await
                .map_err(anyhow::Error::from)
                .and_then(std::convert::identity);
            let closed = managed_fixture::close_group_nodes(
                std::mem::take(&mut cluster.nodes),
                &cluster.hosts,
            )
            .await;
            for root in std::mem::take(&mut cluster.roots) {
                eprintln!("retained provider fence disk: {}", root.keep().display());
            }
            domain.shutdown().await;
            work.with_context(|| {
                format!(
                    "provider execution fencing extents={rooted} minimum={minimum:?} docker={}",
                    docker.is_some()
                )
            })?;
            closed?;
        }
    }
    Ok(())
}
