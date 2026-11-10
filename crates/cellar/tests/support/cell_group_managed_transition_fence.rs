// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fence and erase only the disks owned by the managed host test.
use super::*;
use ed25519_dalek::{Signer as _, SigningKey};
use futures_util::future::BoxFuture;
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeSet, path::PathBuf, sync::atomic::AtomicUsize};

type PhysicalVoter = (NodeId, DiskLineage, IncarnationId);

/// A new executable process opens only the real RustFS roots, exact serialized
/// selectors and pinned public key. No in-memory authority or replay pins cross
/// that boundary; its key-only adapter cannot initiate a physical fence.
pub(super) async fn verify_rustfs_operator(
    fixture: &Fixture,
    selected: &cellar::sdk::fleet_operations::RaftFleetTransitionRecoverySelection,
    principal: &str,
    cohort: &str,
    rooted: bool,
    receipt: [u8; 32],
) -> Result<()> {
    use serde_json::json;
    let config = json!({
        "rooted": rooted,
        "endpoint": std::env::var("CELLAR_CELL_GROUPS_RUSTFS_ENDPOINT")?,
        "bucket": std::env::var("CELLAR_CELL_GROUPS_RUSTFS_BUCKET")?,
        "prefix": format!("{}/{cohort}/{}", std::env::var("CELLAR_CELL_GROUPS_RUSTFS_PREFIX")?,
            if rooted {"extents"} else {"materialized"}),
        "access": std::env::var("AWS_ACCESS_KEY_ID")?,
        "secret": std::env::var("AWS_SECRET_ACCESS_KEY")?,
        "bucket_prefix": "tenant/",
        "connection": {
            "cluster_id":hex::encode(fixture.declaration.namespace.cluster.bytes()),
            "incarnation_id":hex::encode([80;16]),
            "coordination_prefix":"hosts/",
            "namespaces":[{
                "namespace_id":selected.namespace_id,
                "schema_digest":hex::encode(fixture.declaration.namespace.schema_digest),
                "catalog_prefix":"managed/", "scope":"store_root", "managed":true,
                "image_storage":if rooted {"extents"} else {"materialized"},
                "group_directory":DirectoryLimits::default(),
            }]
        },
        "principal":principal,
        "namespace_id":selected.namespace_id,
        "operation_id":selected.operation_id,
        "intent_digest":selected.intent_digest,
        "selection":selected,
        "fence_public":SigningKey::from_bytes(&[147;32]).verifying_key().to_bytes(),
        "certificate":[],
    });
    let expected = selected.clone();
    fixture
        .domain
        .blocking(move || -> Result<()> {
            let root = tempfile::tempdir()?;
            for variant in ["valid", "foreign-key", "changed-selection"] {
                let mut config = config.clone();
                if variant == "foreign-key" {
                    config["fence_public"] = json!(SigningKey::from_bytes(&[148; 32])
                        .verifying_key()
                        .to_bytes());
                } else if variant == "changed-selection" {
                    config["selection"]["fence_digest"] = json!(hex::encode([99; 32]));
                }
                let path = root.path().join(format!("{variant}.json"));
                use std::{fs::OpenOptions, io::Write as _};
                let mut options = OpenOptions::new();
                options.write(true).create_new(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt as _;
                    options.mode(0o600);
                }
                options
                    .open(&path)?
                    .write_all(&serde_json::to_vec(&config)?)?;
                let result =
                    std::process::Command::new(env!("CARGO_BIN_EXE_cellar-perf-cell-group"))
                        .env("CELLAR_GROUP_ACTION", "managed-reopen")
                        .env("CELLAR_GROUP_CONFIG", path)
                        .env("CELLAR_GROUP_DATA", root.path().join(variant))
                        .env("CELLAR_GROUP_GENERATION", "0")
                        .env("CELLAR_GROUP_NODE", "0")
                        .env("CELLAR_GROUP_RETAINED", "false")
                        .output()?;
                if variant == "valid" {
                    ensure!(
                        result.status.success(),
                        "cold managed operator failed: {}",
                        String::from_utf8_lossy(&result.stderr)
                    );
                    let report: serde_json::Value = serde_json::from_slice(&result.stdout)?;
                    ensure!(
                        report["selection"] == serde_json::to_value(&expected)?
                            && report["receipt_digest"] == hex::encode(receipt)
                            && report["ranges"].as_u64().is_some_and(|n| n > 0),
                        "different cold recovery proof"
                    );
                } else {
                    ensure!(
                        !result.status.success() && result.stdout.is_empty(),
                        "invalid cold recovery inputs accepted"
                    );
                }
            }
            Ok(())
        })?
        .await?
}

struct Sources {
    owners: BTreeSet<PhysicalVoter>,
    nodes: Vec<Node>,
    hosts: Vec<RaftCells>,
    disks: Vec<PathBuf>,
}

impl Sources {
    async fn erase(self, domain: Arc<ExecutionDomain>) -> Result<Vec<PathBuf>> {
        let Self {
            nodes,
            hosts,
            disks,
            ..
        } = self;
        // Stop and join every process before removing any source disk.
        Box::pin(super::close(nodes, &hosts, None)).await?;
        ensure!(hosts.iter().all(RaftCells::is_closed));
        drop(hosts);
        domain
            .blocking(move || {
                for disk in &disks {
                    ensure!(disk.join("raft").is_dir(), "owned voter disk is missing");
                    ensure!(!std::fs::symlink_metadata(disk)?.is_symlink());
                }
                for disk in &disks {
                    std::fs::remove_dir_all(disk).context("erase owned old voter disk")?;
                    ensure!(!disk.try_exists()?);
                }
                Ok(disks)
            })?
            .await?
    }
}

async fn verify_controller_challenge(
    domain: &ExecutionDomain,
    digest: [u8; 32],
    challenge: Vec<u8>,
) -> Result<BTreeSet<PhysicalVoter>> {
    let output = domain
        .blocking(move || -> Result<std::process::Output> {
            use std::{
                io::Write as _,
                process::{Command, Stdio},
            };
            let controller = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../tools/multi-raft/managed_group_fence.py");
            let mut child = Command::new("python3")
                .arg(controller)
                .args(["--expected-digest", &hex::encode(digest)])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()?;
            let written = match child.stdin.take() {
                Some(mut input) => input.write_all(&challenge),
                None => Err(std::io::Error::other("controller stdin missing")),
            };
            if written.is_err() {
                let _ = child.kill();
            }
            let output = child.wait_with_output()?;
            written?;
            Ok(output)
        })?
        .await??;
    ensure!(
        output.status.success(),
        "managed controller rejected the SDK challenge: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let writers: Vec<[String; 3]> = serde_json::from_value(report["writers"].clone())?;
    let bytes = |text: &str| -> Result<[u8; 16]> {
        hex::decode(text)?
            .try_into()
            .map_err(|_| anyhow::anyhow!("controller writer identity size"))
    };
    writers
        .into_iter()
        .map(|row| {
            Ok((
                NodeId::new(bytes(&row[0])?)?,
                DiskLineage::new(bytes(&row[1])?)?,
                IncarnationId::new(bytes(&row[2])?)?,
            ))
        })
        .collect()
}

#[derive(Default)]
struct FenceState {
    sources: Option<Sources>,
    digest: Option<[u8; 32]>,
    erasing: Option<BoxFuture<'static, Result<Vec<PathBuf>>>>,
    erased: Vec<PathBuf>,
    challenge: Option<Vec<u8>>,
}

#[derive(Default)]
pub(super) struct TransitionFence {
    domain: Option<Arc<ExecutionDomain>>,
    state: tokio::sync::Mutex<FenceState>,
    pub(super) fences: AtomicUsize,
    pub(super) verifications: AtomicUsize,
}

impl TransitionFence {
    pub(super) fn own(
        domain: Arc<ExecutionDomain>,
        fixtures: &[Fixture],
        roots: &mut Vec<tempfile::TempDir>,
        nodes: &mut Vec<Node>,
        hosts: &mut Vec<RaftCells>,
    ) -> Result<Self> {
        let voters = fixtures.len();
        ensure!(
            voters > 0 && roots.len() == voters && nodes.len() == voters && hosts.len() == voters
        );
        let mut owners = BTreeSet::new();
        let mut disks = BTreeSet::new();
        for (((fixture, root), node), host) in
            fixtures.iter().zip(&*roots).zip(&*nodes).zip(&*hosts)
        {
            let spec = &fixture.spec;
            let identity = host.identity();
            ensure!(
                identity.node == spec.node && identity.disk == spec.disk,
                "foreign test voter for {}",
                spec.label
            );
            // NodeDirectory canonicalizes its path when taking ownership.
            let owned_root = root.path().canonicalize()?;
            ensure!(
                node.info().data_dir == owned_root,
                "foreign test disk owner for {}",
                spec.label
            );
            ensure!(owners.insert((spec.node, spec.disk, spec.physical_host)));
            ensure!(disks.insert(root.path()));
        }
        // Keep paths if the fixture fails or a fence call is cancelled.
        // Only the joined erasure job may delete them.
        let disks = std::mem::take(roots)
            .into_iter()
            .map(tempfile::TempDir::keep)
            .collect();
        Ok(Self {
            domain: Some(domain),
            state: tokio::sync::Mutex::new(FenceState {
                sources: Some(Sources {
                    owners,
                    nodes: std::mem::take(nodes),
                    hosts: std::mem::take(hosts),
                    disks,
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    pub(super) async fn erased_disks(&self) -> usize {
        self.state.lock().await.erased.len()
    }
}

pub(super) async fn check_old_boot_refused(fixture: &Fixture) -> Result<()> {
    let root = tempfile::tempdir()?;
    let node = start_node(
        root.path(),
        fixture.application.clone(),
        &fixture.spec.label,
    )
    .await?;
    let config = fixture.config(&node, LogOpenMode::Create, 211).await;
    // Poll startup and its error capture outside the split/merge scenario stack.
    let boot = fixture.domain.spawn(Box::pin(async move {
        let attempted = match config {
            Ok((config, services)) => Ok(Box::pin(node.start_raft_cells(config, services)).await),
            Err(error) => Err(error),
        };
        ensure!(matches!(
            node.shutdown(ShutdownMode::Preserve).await?,
            NodeOutcome::Stopped(_)
        ));
        match attempted.context("prepare the old voter restart probe")? {
            Err(error) => {
                ensure!(
                    error.chain().any(|cause| matches!(
                        cause.downcast_ref::<cellar::application::consensus::CatalogError>(),
                        Some(cellar::application::consensus::CatalogError::LostStableState)
                    )),
                    "expected the permanent engine pin to refuse an old voter on empty storage: {error:#}"
                );
                Ok(())
            }
            Ok(host) => {
                host.close().await?;
                anyhow::bail!("a new engine booted with an erased voter's identity");
            }
        }
    }))?;
    boot.await?
}

impl DisasterFenceAuthority<FleetTransitionRecoveryFenceRequest> for TransitionFence {
    fn fence<'a>(
        &'a self,
        request: &'a FleetTransitionRecoveryFenceRequest,
    ) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            self.fences.fetch_add(1, Ordering::AcqRel);
            let mut state = self.state.lock().await;
            let digest = request.digest();
            let challenge = request.encode_challenge()?;
            if let Some(encoded) = &state.challenge {
                ensure!(encoded == &challenge, "different canonical fence challenge");
            } else {
                // Exercise the real SDK -> Python wire boundary and compare its
                // independently derived minimum to the actual owned voters.
                let physical = verify_controller_challenge(
                    self.domain.as_ref().context("fence domain")?,
                    digest,
                    challenge.clone(),
                )
                .await?;
                ensure!(
                    state.sources.as_ref().context("fence owners")?.owners == physical,
                    "exported fence challenge omitted physical owners"
                );
                state.challenge = Some(challenge);
            }
            if let Some(selected) = state.digest {
                ensure!(selected == digest, "different fence scope");
            }
            if let Some(sources) = &state.sources {
                let required = request
                    .intent()
                    .required_voter_lineages()
                    .iter()
                    .map(|lineage| (lineage.node, lineage.disk, lineage.physical_host))
                    .collect::<BTreeSet<_>>();
                ensure!(
                    sources.owners == required,
                    "incomplete physical writer inventory"
                );
                let domain = self.domain.as_ref().context("fence execution domain")?;
                let sources = state.sources.take().context("owned voters already taken")?;
                let job = domain.spawn(Box::pin(sources.erase(domain.clone())))?;
                state.digest = Some(digest);
                // Keep the task handle in the authority across a lost or
                // cancelled caller. A retry joins the same physical operation.
                state.erasing = Some(Box::pin(async move { job.await? }));
            }
            if let Some(erasing) = &mut state.erasing {
                let erased = erasing.await;
                state.erasing = None;
                state.erased = erased?;
            }
            ensure!(!state.erased.is_empty(), "source erasure is unconfirmed");
            // A fixture controller key is pinned independently of archive
            // objects. Exercise the production managed receipt verifier after
            // joined erasure; this does not claim Docker launch revocation.
            let key = SigningKey::from_bytes(&[147; 32]);
            let inventory = Sha256::digest(serde_json::to_vec(&state.erased)?);
            let mut certificate = vec![1];
            certificate.extend_from_slice(&digest);
            certificate.extend_from_slice(&inventory);
            let mut message = CellGroupPermanentFenceAuthority::PURPOSE.to_vec();
            message.extend_from_slice(&certificate);
            certificate.extend_from_slice(&key.sign(&message).to_bytes());
            let authority = CellGroupPermanentFenceAuthority::new(key.verifying_key().to_bytes())?
                .with_certificate(certificate)?;
            authority.fence(request).await
        })
    }

    fn verify<'a>(
        &'a self,
        request: &'a FleetTransitionRecoveryFenceRequest,
        evidence: &'a [u8],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.verifications.fetch_add(1, Ordering::AcqRel);
            let key = SigningKey::from_bytes(&[147; 32]);
            CellGroupPermanentFenceAuthority::new(key.verifying_key().to_bytes())?
                .verify(request, evidence)
                .await?;
            let digest = request.digest();
            let disks = {
                let state = self.state.lock().await;
                ensure!(
                    state.digest == Some(digest) && !state.erased.is_empty(),
                    "fence not effective"
                );
                state.erased.clone()
            };
            self.domain
                .as_ref()
                .context("fence execution domain")?
                .blocking(move || {
                    for disk in disks {
                        ensure!(!disk.try_exists()?, "erased voter disk reappeared");
                    }
                    Ok(())
                })?
                .await?
        })
    }
}
