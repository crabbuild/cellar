// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Separate worker processes, SIGKILL, erased owner storage and a fresh S3 learner.
//! The controller's files carry test requests only; Raft uses real TLS sockets.

use super::{full_capacity as fixture, public_node, *};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    fs::File,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};

#[cfg(unix)]
mod controller;
mod worker;

const CHILD_TEST: &str = "process_recovery::worker_process";
const CHILD_CONFIG: &str = "CELLAR_PROCESS_RECOVERY_CHILD";
const CELL: &str = "Counter:process-recovery";
const START_TIMEOUT: Duration = Duration::from_secs(1800);

#[derive(Clone, Serialize, Deserialize)]
struct Peer {
    addresses: [SocketAddr; 3],
}

impl Peer {
    fn native(&self, index: usize) -> Result<RaftPeerNode> {
        Ok(RaftPeerNode {
            node: NodeId::new([index as u8 + 1; 16])?,
            disk: DiskLineage::new([index as u8 + 10; 16])?,
            process: ProcessGeneration::new([index as u8 + 70; 16])?,
            label: format!("process-recovery-{index}"),
            server_name: "localhost".into(),
            control_address: self.addresses[0],
            data_address: self.addresses[1],
            query_address: self.addresses[2],
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Config {
    index: usize,
    quota: u64,
    prefix: String,
    directory: PathBuf,
    control: PathBuf,
    peers: Vec<Peer>,
    source: Option<usize>,
    payload_bytes: u64,
    sample_ids: [u64; 3],
    sample_hashes: [[u8; 32]; 3],
}

#[derive(Debug, Serialize, Deserialize)]
enum Action {
    Status,
    Command {
        client: u8,
        retry_until_ms: u64,
    },
    Read {
        expected: u64,
        token: Option<String>,
        full: bool,
    },
    BeginRepair {
        obsolete: u64,
    },
    AdvanceRepair {
        finish: bool,
    },
    Transfer,
    Stop,
}

#[derive(Serialize, Deserialize)]
struct Request {
    sequence: u64,
    action: Action,
}

fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let temporary = path.with_extension("pending");
    let mut file = File::create(&temporary)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

fn store(prefix: &str) -> Result<Arc<dyn object_store::ObjectStore>> {
    let store = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(std::env::var("CELLAR_LARGE_IMAGE_BUCKET")?)
        .with_endpoint(std::env::var("CELLAR_LARGE_IMAGE_ENDPOINT")?)
        .with_allow_http(true)
        .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
        .build()?;
    Ok(Arc::new(object_store::prefix::PrefixStore::new(
        store, prefix,
    )))
}

fn registration(schema: [u8; 32]) -> Result<RaftCellRegistration> {
    let mut namespace = NamespaceConfig::new(
        ClusterId::new([81; 16])?,
        NamespaceId::new([82; 16])?,
        NamespaceMode::Raft,
        schema,
    );
    namespace.image_storage = RaftImageStorage::Extents;
    Ok(RaftCellRegistration {
        placement: RaftCellPlacement::Replica,
        class: "Counter".into(),
        birth: BirthProposal {
            scope: GroupScope {
                cluster: namespace.cluster,
                namespace: namespace.namespace,
                group: GroupId::new([83; 16])?,
                incarnation: IncarnationId::new([84; 16])?,
                cell: CELL.into(),
            },
            voters: (0..3)
                .map(|i| {
                    Ok(InitialMember {
                        member: MemberId::new(i + 1)?,
                        node: NodeId::new([i as u8 + 1; 16])?,
                    })
                })
                .collect::<Result<_>>()?,
            learners: vec![],
        },
        namespace,
    })
}

fn repair_id() -> Result<cellar_logic::consensus::ProposalId> {
    Ok(cellar_logic::consensus::ProposalId::new([85; 16])?)
}

/// Prepare the immutable input separately from builds and timed recovery.
#[test]
#[ignore = "requires an empty fixture directory and 10 GB of admitted disk space"]
fn prepare_decimal_ten_gb_fixture() -> Result<()> {
    let directory = PathBuf::from(
        std::env::var_os("CELLAR_PROCESS_RECOVERY_FIXTURE_DIR")
            .context("empty fixture directory required")?,
    );
    ensure!(
        directory.is_absolute() && std::fs::read_dir(&directory)?.next().is_none(),
        "fixture directory must be absolute and empty"
    );
    let source = fixture::make_template(&directory, 10_000_000_000, true)?;
    let file = File::options().read(true).write(true).open(&source)?;
    ensure!(
        file.metadata()?.len() == 9_999_998_976,
        "fixture size mismatch"
    );
    file.sync_all()?;
    let mut permissions = file.metadata()?.permissions();
    permissions.set_readonly(true);
    file.set_permissions(permissions)?;
    file.sync_all()?;
    eprintln!("process-recovery fixture={}", source.display());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "child entry point owned and joined by the process-recovery controller"]
async fn worker_process() -> Result<()> {
    let path = std::env::var_os(CHILD_CONFIG).context("missing child configuration")?;
    worker::run(serde_json::from_slice(&std::fs::read(path)?)?).await
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable RustFS credentials and an admitted worker directory"]
async fn rustfs_272_mib_after_sigkill_restores_empty_process() -> Result<()> {
    controller::run(272 * 1024 * 1024).await
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the retained 10 GB fixture, RustFS and admitted worker/store capacity"]
async fn rustfs_decimal_ten_gb_after_sigkill_restores_empty_process() -> Result<()> {
    controller::run(10_000_000_000).await
}
