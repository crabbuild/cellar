// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! Container engine effects bound to durable cell ownership.
//!
//! A cell of a class named in the deployment's `containers` config owns at
//! most one container, named after the cell, on the node that owns the
//! cell. Rust callers use this module to perform effects against the
//! container engine, which is the Docker Engine API on a unix socket
//! (Podman serves the same API).
//!
//! The container is bound to the cell's ownership on this node, not to its
//! residency: an idle eviction leaves it running under an inactivity timer
//! and the next activation of the same cell reconnects to it by name, which
//! is what Cloudflare does and what the `@cloudflare/containers` class's
//! `sleepAfter` alarm relies on. Every other stop destroys it. The
//! container's disk is ephemeral on Cloudflare too, so a takeover that
//! starts fresh is conformant.
//!
//! Images travel through the bucket. `cellar deploy` saves the image the
//! config names as a tar at `deploy/images/<id>.tar`, and a node loads it
//! into its engine the first time a cell of that class starts, so a node
//! never talks to a registry.

use crate::asyncrt;
use crate::docker::{frame_header, Docker, Stream};
use anyhow::{anyhow, Context};
use bytes::Bytes;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, watch};

/// One `containers[]` entry of a deployment, as the node sees it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ContainerSpec {
    pub class_name: String,
    /// The image reference the engine starts, `cellar-image:<content key>`,
    /// so two deployments of one image share one tar and one load.
    pub image: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_instances: Option<u64>,
    /// The OCI runtime this class runs under, such as `runsc` (gVisor) for
    /// untrusted code. Overrides the node's `CELLAR_CONTAINER_RUNTIME` for
    /// this class; `None` takes the node default. A node whose daemon does
    /// not have the runtime fails every start of the class, so the class
    /// runs only where its isolation is available. cellar extends the
    /// Cloudflare config here, which has no per-class runtime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<String>,
}

/// An immutable image archive carried by a native package. The image names
/// its config/layer content; the archive digest verifies its serialized bytes.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImageArchive {
    pub image: String,
    pub sha256: String,
    pub bytes: u64,
}

impl ImageArchive {
    pub fn validate(&self) -> anyhow::Result<()> {
        let key = self.image.strip_prefix("cellar-image:").unwrap_or_default();
        let digest = |s: &str| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        anyhow::ensure!(
            digest(key) && digest(&self.sha256) && self.bytes > 0,
            "invalid native container image archive"
        );
        Ok(())
    }

    pub fn key(&self) -> anyhow::Result<String> {
        self.validate()?;
        Ok(format!("deploy/images/{}.tar", self.sha256))
    }

    pub fn verify(&self, bytes: &[u8]) -> anyhow::Result<()> {
        use sha2::Digest;
        self.validate()?;
        anyhow::ensure!(
            bytes.len() as u64 == self.bytes
                && format!("{:x}", sha2::Sha256::digest(bytes)) == self.sha256,
            "native container image archive checksum mismatch"
        );
        Ok(())
    }
}

/// The reference every engine holds an image under, from its content key.
/// The key hashes the layer digests and the image config, so an engine
/// that rebuilds identical content under a new id still resolves it.
pub fn image_reference(key: &str) -> String {
    format!("cellar-image:{key}")
}

/// The bucket key of one saved image, from its reference.
pub fn image_key(image: &str) -> String {
    let key = image.strip_prefix("cellar-image:").unwrap_or(image);
    format!("deploy/images/{key}.tar")
}

/// The resources an instance type reserves. Cloudflare's published table;
/// a name outside it is refused at deploy.
pub fn instance_resources(instance_type: &str) -> Option<(f64, u64)> {
    let gib = 1024 * 1024 * 1024;
    Some(match instance_type {
        "lite" | "dev" => (1.0 / 16.0, 256 * 1024 * 1024),
        "basic" => (0.25, gib),
        "standard-1" | "standard" => (0.5, 4 * gib),
        "standard-2" => (1.0, 6 * gib),
        "standard-3" => (2.0, 8 * gib),
        "standard-4" => (4.0, 12 * gib),
        _ => return None,
    })
}

/// The instance type of a class that declares none, as on Cloudflare.
const DEFAULT_INSTANCE_TYPE: &str = "dev";

/// The resolvers a container gets when it runs under a non-default runtime
/// and the operator set none. gVisor's network stack does not reach
/// Docker's embedded resolver at `127.0.0.11`, so a container under it
/// cannot resolve a hostname though it reaches the Internet by address; a
/// public resolver, which the fence permits, restores name resolution.
/// `CELLAR_CONTAINER_DNS` overrides this.
const DEFAULT_CONTAINER_DNS: &[&str] = &["1.1.1.1", "1.0.0.1"];

/// The memory a container of this instance type reserves on the node. A
/// class that declares none gets the default type, as `create_and_start`
/// does. An unknown name is refused at deploy, so a running container
/// always maps; this returns 0 for a name that somehow does not, which
/// undercounts rather than blocks a sample.
pub fn instance_memory_bytes(instance_type: Option<&str>) -> u64 {
    instance_resources(instance_type.unwrap_or(DEFAULT_INSTANCE_TYPE))
        .map(|(_, memory)| memory)
        .unwrap_or(0)
}

/// The memory this node commits to its running containers, for the node's
/// capacity accounting. Zero when no engine has connected. See
/// `cellar_logic::pressure::Load::container_reserved_bytes` for why the node
/// counts the cap rather than the container's live usage.
pub fn reserved_memory_bytes() -> u64 {
    engine_if_ready().map_or(0, |engine| engine.reserved_memory_bytes())
}

/// This node's running containers per class, published in the node lease so
/// peers can sum a class's instances across the fleet for `max_instances`.
/// Empty when no engine has connected.
pub fn running_instances_by_class() -> std::collections::BTreeMap<String, u64> {
    engine_if_ready().map_or_else(Default::default, |engine| {
        engine.running_instances_by_class()
    })
}

/// Processes one container can hold. Cloudflare publishes no number; this
/// is room for a build tool's process tree and far short of a fork bomb.
const PIDS_LIMIT: u64 = 1024;
/// The host interfaces of the two bridges, named so the fence can address
/// them; Docker's default `br-<id>` changes with every recreation.
const BRIDGE_NAME_OPTION: &str = "com.docker.network.bridge.name";
/// The fence, installed on the node from a one-shot privileged container of
/// the `cellar-fence` image: a container may reach the Internet and nothing
/// of the node's own. Hooks before Docker's own chains (priority filter -
/// 10) so a verdict here is final. Input: no new connection from a bridge
/// to the node itself, which is where the internal listener and the public
/// listener bind; replies to connections the node opened still pass.
/// Forward: nothing to the private ranges, where the fleet, the VPC, and
/// the metadata service live. Rules on the host side of the bridge cover
/// every runtime, gVisor included, and no process inside a container can
/// see them, let alone remove them.
const FENCE_RULES: &str = r#"table inet __TABLE__
delete table inet __TABLE__
table inet __TABLE__ {
  chain input {
    type filter hook input priority -10; policy accept;
    iifname { "__OPEN__", "__INTERNAL__" } ct state established,related accept
    iifname { "__OPEN__", "__INTERNAL__" } reject
  }
  chain forward {
    type filter hook forward priority -10; policy accept;
    iifname { "__OPEN__", "__INTERNAL__" } ip daddr { 169.254.0.0/16, 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 100.64.0.0/10 } reject
    iifname { "__OPEN__", "__INTERNAL__" } ip6 daddr { fe80::/10, fc00::/7 } reject
  }
}
"#;

/// Docker's bridge option that forbids traffic between two containers on
/// the same bridge. The node still reaches every container, because it
/// speaks from the host side of the bridge.
const ICC_OPTION: &str = "com.docker.network.bridge.enable_icc";

/// How a cell stop treats its container.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Release {
    /// An idle eviction: keep the container under its inactivity timer.
    Keep,
    /// Ownership leaves this node, or the cell is reset: destroy it.
    Destroy,
}

/// The default inactivity window after an idle eviction when the object
/// never set one. The `@cloudflare/containers` alarm wakes the object well
/// inside this to enforce `sleepAfter`, so this is the backstop for an
/// object that never wakes, not the policy.
const DEFAULT_INACTIVITY: Duration = Duration::from_secs(10 * 60);

/// Environment every container sees. The values mirror workerd's local
/// engine: applications read the names, never the values.
const DEFAULT_ENV: &[&str] = &[
    "CLOUDFLARE_COUNTRY_A2=XX",
    "CLOUDFLARE_DEPLOYMENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "CLOUDFLARE_LOCATION=loc01",
    "CLOUDFLARE_REGION=REGN",
    "CLOUDFLARE_APPLICATION_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
];

pub struct StartParams {
    pub entrypoint: Option<Vec<String>>,
    pub env: Vec<(String, String)>,
    pub enable_internet: bool,
    pub labels: Vec<(String, String)>,
}

#[derive(Default)]
struct CellState {
    running: bool,
    /// Where the node dials the container: the bridge address on Linux,
    /// the published loopback ports elsewhere.
    address: Address,
    inactivity: Option<Duration>,
    /// The pending destroy after an idle eviction. Dropping it cancels.
    sweeper: Option<asyncrt::TaskHandle<()>>,
    /// Bumped per start so a wait from a previous run cannot report for
    /// this one.
    run: u64,
}

#[derive(Clone, Debug, Default)]
enum Address {
    #[default]
    None,
    Ip(String),
    Published(HashMap<u16, u16>),
}

pub struct CellContainer {
    pub(crate) sdk_control: tokio::sync::Mutex<()>,
    scope: String,
    name: String,
    spec: Arc<ContainerSpec>,
    state: Mutex<CellState>,
    /// Processes `exec()` started in this container. An object drops one
    /// after `output()`; the rest go with the container.
    processes: Mutex<Vec<u64>>,
    process_registry: Weak<ProcessRegistry>,
    /// The exit of the current run, `None` while it runs. `Some(Err)` is
    /// an engine failure the wait could not attribute to the process.
    /// Written with `send_replace`: a plain `send` discards the value
    /// while nobody subscribes, and `monitor()` usually subscribes late.
    exit: watch::Sender<Option<(u64, Result<i64, String>)>>,
}

impl CellContainer {
    fn publish_exit(&self, run: u64, result: Result<i64, String>) {
        self.exit.send_if_modified(|current| {
            if current.as_ref().is_some_and(|(newer, _)| *newer > run) {
                return false;
            }
            *current = Some((run, result));
            true
        });
    }

    pub(crate) fn forget_process(&self, id: u64) {
        self.processes
            .lock()
            .unwrap()
            .retain(|current| *current != id);
        if let Some(registry) = self.process_registry.upgrade() {
            registry.remove(id);
        }
    }

    pub fn running(&self) -> bool {
        self.state.lock().unwrap().running
    }

    /// `host:port` for `getTcpPort(port)`.
    pub fn address(&self, port: u16) -> Result<String, String> {
        let state = self.state.lock().unwrap();
        if !state.running {
            return Err("the container is not running".to_string());
        }
        match &state.address {
            Address::Ip(ip) => Ok(format!("{ip}:{port}")),
            Address::Published(ports) => ports
                .get(&port)
                .map(|host| format!("127.0.0.1:{host}"))
                .ok_or_else(|| {
                    format!(
                        "The container is not listening on port {port}: on this platform a port \
                         must be declared with EXPOSE in the image"
                    )
                }),
            Address::None => Err(format!("The container is not listening on port {port}")),
        }
    }

    pub fn set_inactivity(&self, duration: Duration) {
        self.state.lock().unwrap().inactivity = Some(duration);
    }

    /// Open the next run, synchronously, before the start is even queued.
    /// The object's `start()` returns at once and its `monitor()` follows
    /// immediately, so the run they both mean must exist before either
    /// effect runs, or the monitor would find the previous run's exit and
    /// report the new container dead on arrival.
    pub fn begin_run(&self) -> u64 {
        let mut state = self.state.lock().unwrap();
        state.run += 1;
        state.running = true;
        state.address = Address::None;
        let run = state.run;
        drop(state);
        self.exit.send_replace(None);
        run
    }

    /// The run `monitor()` waits on: the newest one.
    pub fn current_run(&self) -> u64 {
        self.state.lock().unwrap().run
    }
}

pub struct ContainerEngine {
    service: Weak<ContainerService>,
    names: ResourceNames,
    /// See `Config::runtime`.
    runtime: Option<String>,
    /// See `Config::dns`.
    dns: Vec<String>,
    /// The absolute path of the container resolv.conf this engine wrote and
    /// bind-mounts; `None` when it could not be written.
    resolv_conf: Option<PathBuf>,
    docker: Docker,
    node: String,
    bucket: Option<crate::bucket::Bucket>,
    /// Per-image load lock, so two cells of one class starting together
    /// load the tar once.
    images: Mutex<HashMap<String, Arc<tokio::sync::Mutex<bool>>>>,
    cells: Mutex<HashMap<String, Arc<CellContainer>>>,
    networks: tokio::sync::Mutex<Option<(String, String)>>,
    /// Whether this process has installed the fence on the node's bridges.
    fenced: tokio::sync::Mutex<bool>,
}

/// What the runtime told this module about the node: set once at start.
#[derive(Clone)]
struct Config {
    docker: Option<Docker>,
    namespace: Option<String>,
    node: String,
    bucket: Option<crate::bucket::Bucket>,
    /// `CELLAR_CONTAINER_RUNTIME`: the OCI runtime every container starts
    /// under, such as `runsc` (gVisor) or `kata` (a VM). `None` is the
    /// daemon's default, which is `runc` unless the operator changed it. A
    /// class can override it, see `ContainerSpec::runtime`.
    runtime: Option<String>,
    /// `CELLAR_CONTAINER_DNS`: the resolvers a container gets, overriding
    /// `DEFAULT_CONTAINER_DNS`. Empty takes the default when a runtime is in
    /// use and the daemon's otherwise.
    dns: Vec<String>,
    /// The node's state directory. cellar writes the container resolv.conf
    /// here and bind-mounts it, so the path must be the same on the host as
    /// cellar sees it; a self-hosted node runs cellar as a host process, and
    /// the lab mounts the directory one-to-one.
    data_dir: PathBuf,
}

/// Infrastructure for one embedded host. An explicit socket avoids process
/// environment discovery. A namespace is stable across restarts and must be
/// unique among hosts sharing a daemon; when omitted it uses node and state path.
#[derive(Clone, Debug)]
pub struct EngineOptions {
    /// Docker or Podman Engine API Unix socket owned by the application.
    pub socket: PathBuf,
    /// Stable identity for restart cleanup. Omission derives it from the node
    /// ID and absolute state path. Distinct hosts must have distinct identities.
    pub namespace: Option<String>,
    /// Default OCI runtime, overridden by a class's explicit runtime.
    pub runtime: Option<String>,
    /// Resolvers; an empty list uses the daemon's default or the runtime default.
    pub dns: Vec<String>,
}

#[derive(Default)]
struct Settings {
    options: Option<EngineOptions>,
    config: Option<Config>,
    sealed: bool,
}

#[derive(Default)]
struct Deployment {
    specs: Vec<Arc<ContainerSpec>>,
    fence_image: Option<String>,
    archives: HashMap<String, ImageArchive>,
}

#[derive(Default)]
pub(crate) struct ContainerService {
    domain: OnceLock<asyncrt::DomainToken>,
    closed: AtomicBool,
    settings: Mutex<Settings>,
    deployment: RwLock<Deployment>,
    engine: Mutex<Option<Arc<ContainerEngine>>>,
    initialize: tokio::sync::Mutex<()>,
    processes: Arc<ProcessRegistry>,
}

impl ContainerService {
    pub(crate) fn bind_domain(&self, domain: asyncrt::DomainToken) {
        if let Err(candidate) = self.domain.set(domain) {
            assert!(
                self.domain
                    .get()
                    .is_some_and(|bound| bound.same_owner(&candidate)),
                "container services cannot belong to more than one execution domain"
            );
        }
    }

    fn check_open(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "container host is closed"
        );
        Ok(())
    }

    pub(crate) fn configure_options(&self, options: EngineOptions) -> anyhow::Result<()> {
        let mut settings = self.settings.lock().unwrap();
        self.check_open()?;
        anyhow::ensure!(
            settings.config.is_none(),
            "configure container infrastructure before starting the host"
        );
        anyhow::ensure!(
            !options.socket.as_os_str().is_empty(),
            "container socket is empty"
        );
        anyhow::ensure!(
            options
                .namespace
                .as_ref()
                .is_none_or(|namespace| !namespace.is_empty()),
            "container namespace is empty"
        );
        settings.options = Some(options);
        Ok(())
    }

    fn configure(
        &self,
        node: String,
        bucket: Option<crate::bucket::Bucket>,
        data_dir: PathBuf,
    ) -> anyhow::Result<()> {
        let mut settings = self.settings.lock().unwrap();
        self.check_open()?;
        anyhow::ensure!(
            !settings.sealed,
            "container host configuration is already in use"
        );
        let (docker, namespace, runtime, dns) = match &settings.options {
            Some(options) => (
                Some(Docker::new(&options.socket)),
                options.namespace.clone(),
                options.runtime.clone(),
                options.dns.clone(),
            ),
            None => {
                let runtime = std::env::var("CELLAR_CONTAINER_RUNTIME")
                    .ok()
                    .filter(|runtime| !runtime.is_empty());
                let dns = std::env::var("CELLAR_CONTAINER_DNS")
                    .unwrap_or_default()
                    .split(',')
                    .map(|resolver| resolver.trim().to_string())
                    .filter(|resolver| !resolver.is_empty())
                    .collect();
                (None, None, runtime, dns)
            }
        };
        settings.config = Some(Config {
            docker,
            namespace,
            node,
            bucket,
            runtime,
            dns,
            data_dir: std::path::absolute(data_dir)?,
        });
        Ok(())
    }

    async fn engine(self: &Arc<Self>) -> anyhow::Result<Arc<ContainerEngine>> {
        self.check_open()?;
        let _initializing = self.initialize.lock().await;
        self.check_open()?;
        if let Some(engine) = self.engine.lock().unwrap().clone() {
            return Ok(engine);
        }
        let config = {
            let mut settings = self.settings.lock().unwrap();
            let config = settings
                .config
                .clone()
                .ok_or_else(|| anyhow!("the container engine is not configured on this host"))?;
            settings.sealed = true;
            config
        };
        let docker = config
            .docker
            .clone()
            .or_else(Docker::discover)
            .ok_or_else(|| {
                anyhow!(
                    "no container engine: configure a unix socket or run a Docker or Podman daemon"
                )
            })?;
        let engine = Arc::new(ContainerEngine::connect(self, docker, config).await?);
        let mut cache = self.engine.lock().unwrap();
        self.check_open()?;
        *cache = Some(engine.clone());
        Ok(engine)
    }

    #[allow(clippy::disallowed_methods)] // Forced cleanup uses the application runtime after admission closes.
    pub(crate) fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.processes.close();
        let engine = self.engine.lock().unwrap().take();
        self.settings.lock().unwrap().config = None;
        *self.deployment.write().unwrap() = Deployment::default();
        let Some(engine) = engine else {
            return;
        };
        engine.close_cells();
        if let Some(domain) = self.domain.get() {
            let cleanup_domain = domain.clone();
            let cleanup = domain.spawn_cleanup(async move {
                cleanup_domain.wait_for_work().await;
                // Infrastructure may be unavailable during forced shutdown.
                // Bound cleanup without consuming the surrounding runtime.
                match tokio::time::timeout(Duration::from_secs(10), engine.reap()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => tracing::warn!(event = "container_shutdown_reap_failed", %error, "containers of this host may still be running"),
                    Err(_) => tracing::warn!(event = "container_shutdown_reap_timeout", "containers of this host may still be running"),
                }
            });
            if let Ok(task) = cleanup {
                task.detach();
            }
        }
    }
}

/// Configure the current host. Embedded applications can first call
/// [`crate::host_services::HostServices::configure_containers`] to supply their
/// infrastructure directly; the process runner otherwise reads its environment.
pub fn configure(
    node: String,
    bucket: Option<crate::bucket::Bucket>,
    data_dir: PathBuf,
) -> anyhow::Result<()> {
    asyncrt::services()
        .containers()
        .configure(node, bucket, data_dir)
}

pub fn install_specs(
    specs: Vec<ContainerSpec>,
    fence_image: Option<String>,
    archives: Vec<ImageArchive>,
) {
    let service = asyncrt::services().containers();
    let mut deployment = service.deployment.write().unwrap();
    if service.closed.load(Ordering::Acquire) {
        return;
    }
    *deployment = Deployment {
        specs: specs.into_iter().map(Arc::new).collect(),
        fence_image,
        archives: archives
            .into_iter()
            .map(|archive| (archive.image.clone(), archive))
            .collect(),
    };
}

pub fn spec(class: &str) -> Option<Arc<ContainerSpec>> {
    let service = asyncrt::services().containers();
    if service.closed.load(Ordering::Acquire) {
        return None;
    }
    let deployment = service.deployment.read().unwrap();
    deployment
        .specs
        .iter()
        .find(|spec| spec.class_name == class)
        .cloned()
}

/// Connect the current host on first use. Connection failures are retryable.
pub async fn engine() -> anyhow::Result<Arc<ContainerEngine>> {
    asyncrt::services().containers().engine().await
}

/// The current host's engine, if already connected.
pub fn engine_if_ready() -> Option<Arc<ContainerEngine>> {
    let service = asyncrt::services().containers();
    if service.closed.load(Ordering::Acquire) {
        return None;
    }
    let engine = service.engine.lock().unwrap().clone();
    engine
}

/// Destroy every container of this node at process exit. A preserve
/// shutdown (`cellar dev` on Ctrl-C) keeps its cells resident and stops
/// none of them, and a handoff cut by the deadline leaves cells behind
/// too; without this their containers ran on until the next start of the
/// same node reaped them, invisible to the object that owned them.
pub async fn shutdown() {
    let Some(engine) = engine_if_ready() else {
        return;
    };
    if let Err(error) = engine.reap().await {
        tracing::warn!(
            event = "container_shutdown_reap_failed",
            error = %format!("{error:#}"),
            "containers of this node may still be running"
        );
    }
}

/// Connect and load every image of the installed specs, ahead of the
/// first cell that needs one. Failures are logged: a node without an
/// engine still serves every other class, and the container class fails
/// at its first `start()` with the same message.
pub async fn prewarm() {
    let engine = match engine().await {
        Ok(engine) => engine,
        Err(error) => {
            tracing::warn!(error = %format!("{error:#}"), "containers are unavailable on this node");
            return;
        }
    };
    if let Err(error) = engine.ensure_fence().await {
        tracing::warn!(
            error = %format!("{error:#}"),
            "the container bridges are not fenced; no container starts on this node"
        );
    }
    let specs = asyncrt::services()
        .containers()
        .deployment
        .read()
        .unwrap()
        .specs
        .clone();
    for spec in specs {
        if let Err(error) = engine.ensure_image(&spec.image).await {
            tracing::warn!(
                class = %spec.class_name,
                image = %spec.image,
                error = %format!("{error:#}"),
                "container image is unavailable"
            );
        }
    }
}

impl ContainerEngine {
    /// Connect to the engine and reap every container a previous process
    /// of this node left behind. A restarted node cannot know which of its
    /// containers still belong to cells it will own again, and a container
    /// whose object has lost track of it is a leak, so a restart starts
    /// clean. The next `start()` of each object creates a fresh one.
    async fn connect(
        service: &Arc<ContainerService>,
        docker: Docker,
        config: Config,
    ) -> anyhow::Result<Self> {
        let Config {
            node,
            bucket,
            runtime,
            dns,
            data_dir,
            namespace,
            ..
        } = config;
        let names = ResourceNames::new(&node, &data_dir, namespace.as_deref());
        // Write the resolv.conf once, to bind into every container that runs
        // under a non-default runtime. gVisor cannot reach Docker's embedded
        // resolver, so a bind-mounted file with a public resolver, which the
        // fence permits, is the only resolv.conf the container can use.
        let resolvers: Vec<String> = if dns.is_empty() {
            DEFAULT_CONTAINER_DNS
                .iter()
                .map(|r| r.to_string())
                .collect()
        } else {
            dns.clone()
        };
        let resolv_conf = {
            let path = data_dir.join(format!("container-resolv-{}.conf", names.tag));
            let body: String = resolvers
                .iter()
                .map(|resolver| format!("nameserver {resolver}\n"))
                .collect();
            let fs = asyncrt::fs();
            match fs
                .create_dir_all(&data_dir)
                .and_then(|()| fs.write(&path, body.as_bytes()))
            {
                Ok(()) => Some(path),
                Err(error) => {
                    tracing::warn!(event = "container_resolv_write_failed", %error);
                    None
                }
            }
        };
        let engine = Self {
            service: Arc::downgrade(service),
            names,
            docker,
            node,
            bucket,
            runtime,
            dns,
            resolv_conf,
            images: Mutex::new(HashMap::new()),
            cells: Mutex::new(HashMap::new()),
            networks: tokio::sync::Mutex::new(None),
            fenced: tokio::sync::Mutex::new(false),
        };
        engine.reap().await?;
        Ok(engine)
    }

    pub fn socket(&self) -> &std::path::Path {
        self.docker.socket()
    }

    async fn reap(&self) -> anyhow::Result<()> {
        let filters = json!({ "label": [format!("cellar.node={}", self.node), format!("cellar.host={}", self.names.key)] }).to_string();
        let reply = self
            .docker
            .expect(
                "GET",
                &format!(
                    "/containers/json?all=true&filters={}",
                    percent_encoding::utf8_percent_encode(
                        &filters,
                        percent_encoding::NON_ALPHANUMERIC
                    )
                ),
                None,
                "list containers",
            )
            .await?;
        let list = reply.json()?;
        for entry in list.as_array().into_iter().flatten() {
            if let Some(id) = entry.get("Id").and_then(Value::as_str) {
                let removed = self
                    .docker
                    .call("DELETE", &format!("/containers/{id}?force=true"), None)
                    .await?;
                anyhow::ensure!(
                    removed.status.is_success() || removed.status.as_u16() == 404,
                    "reap container failed with [{}] {}",
                    removed.status.as_u16(),
                    removed.message()
                );
            }
        }
        Ok(())
    }

    pub(crate) fn check_owner(&self) -> anyhow::Result<Arc<ContainerService>> {
        let service = self
            .service
            .upgrade()
            .ok_or_else(|| anyhow!("container host is gone"))?;
        service.check_open()?;
        anyhow::ensure!(
            Arc::ptr_eq(&service, &asyncrt::services().containers()),
            "container belongs to another host"
        );
        Ok(service)
    }

    fn check_cell(&self, cell: &Arc<CellContainer>) -> anyhow::Result<()> {
        self.check_owner()?;
        anyhow::ensure!(
            self.cell(&cell.scope)
                .is_some_and(|current| Arc::ptr_eq(&current, cell)),
            "container belongs to another or retired host activation"
        );
        Ok(())
    }

    fn close_cells(&self) {
        let cells = std::mem::take(&mut *self.cells.lock().unwrap());
        for cell in cells.into_values() {
            let mut state = cell.state.lock().unwrap();
            state.running = false;
            state.address = Address::None;
            state.sweeper = None;
            let run = state.run;
            drop(state);
            cell.publish_exit(run, Err("container host is closed".into()));
        }
    }

    /// Make the class's image present in the engine, loading it from the
    /// bucket when it is not. Idempotent and serialized per image.
    pub async fn ensure_image(&self, image: &str) -> anyhow::Result<()> {
        let service = self.check_owner()?;
        let lock = self
            .images
            .lock()
            .unwrap()
            .entry(image.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(false)))
            .clone();
        let mut loaded = lock.lock().await;
        if *loaded {
            return Ok(());
        }
        let reply = self
            .docker
            .call("GET", &format!("/images/{image}/json"), None)
            .await?;
        if reply.status.is_success() {
            tracing::info!(image, "container image present in the engine");
            *loaded = true;
            return Ok(());
        }
        let bucket = self
            .bucket
            .as_ref()
            .ok_or_else(|| anyhow!("image {image} is not present in the container engine"))?;
        let archive = service
            .deployment
            .read()
            .unwrap()
            .archives
            .get(image)
            .cloned();
        let key = match &archive {
            Some(archive) => archive.key()?,
            None => image_key(image),
        };
        let (tar, _) = bucket.get(&key).await?.ok_or_else(|| {
            anyhow!("image {image} is not in the bucket at {key}; run `cellar deploy`")
        })?;
        if let Some(archive) = archive {
            archive.verify(&tar)?;
        }
        let bytes = tar.len();
        let reply = self
            .docker
            .post_octets("/images/load?quiet=true", tar, "load image")
            .await?;
        tracing::info!(
            image,
            bytes,
            answer = %String::from_utf8_lossy(&reply.body).trim_end(),
            "container image loaded from the bucket"
        );
        *loaded = true;
        Ok(())
    }

    /// The two bridges every container joins: one with egress, one
    /// without. Docker implements an internal network by omitting the
    /// masquerade rule and the default route; the node still reaches the
    /// bridge address, which is all ingress needs.
    async fn networks(&self) -> anyhow::Result<(String, String)> {
        let mut guard = self.networks.lock().await;
        if let Some(names) = guard.as_ref() {
            return Ok(names.clone());
        }
        for (name, internal) in [
            (&self.names.open_network, false),
            (&self.names.internal_network, true),
        ] {
            // Replace only resources carrying this host's namespace. An
            // unusable bridge fails startup rather than bypassing isolation.
            let current = self
                .docker
                .call("GET", &format!("/networks/{name}"), None)
                .await?;
            if current.status.is_success() {
                let bridge = if internal {
                    &self.names.internal_bridge
                } else {
                    &self.names.open_bridge
                };
                let network = current.json()?;
                anyhow::ensure!(
                    network
                        .pointer("/Labels/cellar.host")
                        .and_then(Value::as_str)
                        == Some(&self.names.key),
                    "container network {name} belongs to another host"
                );
                let current_options = network.get("Options").cloned().unwrap_or(Value::Null);
                let option = |key: &str| current_options.get(key).and_then(Value::as_str);
                if network.get("Internal").and_then(Value::as_bool) == Some(internal)
                    && option(ICC_OPTION) == Some("false")
                    && option(BRIDGE_NAME_OPTION) == Some(bridge)
                {
                    continue;
                }
                let removed = self
                    .docker
                    .call("DELETE", &format!("/networks/{name}"), None)
                    .await?;
                if !removed.status.is_success() {
                    tracing::warn!(
                        network = name,
                        error = %removed.message(),
                        "the container bridge cannot be replaced while in use"
                    );
                    return Err(anyhow!(
                        "replace container network {name}: {}",
                        removed.message()
                    ));
                }
            }
            let reply = self
                .docker
                .call(
                    "POST",
                    "/networks/create",
                    Some(json!({
                        "Name": name,
                        "Driver": "bridge",
                        "Internal": internal,
                        "Labels": { "cellar.host": self.names.key },
                        "Options": {
                            ICC_OPTION: "false",
                            BRIDGE_NAME_OPTION: if internal { &self.names.internal_bridge } else { &self.names.open_bridge },
                        },
                    })),
                )
                .await?;
            if reply.status.as_u16() == 409 {
                // A competing creator must not bypass ownership or isolation.
                let network = self
                    .docker
                    .expect(
                        "GET",
                        &format!("/networks/{name}"),
                        None,
                        "inspect container network",
                    )
                    .await?
                    .json()?;
                let bridge = if internal {
                    &self.names.internal_bridge
                } else {
                    &self.names.open_bridge
                };
                anyhow::ensure!(
                    network
                        .pointer("/Labels/cellar.host")
                        .and_then(Value::as_str)
                        == Some(&self.names.key)
                        && network.get("Internal").and_then(Value::as_bool) == Some(internal)
                        && network
                            .get("Options")
                            .and_then(|options| options.get(ICC_OPTION))
                            .and_then(Value::as_str)
                            == Some("false")
                        && network
                            .get("Options")
                            .and_then(|options| options.get(BRIDGE_NAME_OPTION))
                            .and_then(Value::as_str)
                            == Some(bridge),
                    "container network {name} has foreign ownership or incompatible isolation"
                );
            }
            if !reply.status.is_success() && reply.status.as_u16() != 409 {
                return Err(anyhow!(
                    "create network {name} failed with [{}] {}",
                    reply.status.as_u16(),
                    reply.message()
                ));
            }
        }
        let names = (
            self.names.open_network.clone(),
            self.names.internal_network.clone(),
        );
        *guard = Some(names.clone());
        Ok(names)
    }

    /// Install the fence on the node's bridges, once per process. Every
    /// container start waits on this and fails when it fails: a node that
    /// cannot fence its bridges runs no container, because an unfenced
    /// container can reach the node's internal listener and the cloud's
    /// metadata service. The rules live in the kernel and outlive this
    /// process; a restart re-applies them, which is idempotent.
    pub async fn ensure_fence(&self) -> anyhow::Result<()> {
        let service = self.check_owner()?;
        let mut fenced = self.fenced.lock().await;
        if *fenced {
            return Ok(());
        }
        let image = service
            .deployment
            .read()
            .unwrap()
            .fence_image
            .clone()
            .ok_or_else(|| {
                anyhow!(
                    "this deployment has no fence image; deploy it again with this cellar, which \
                 saves the cellar-fence image beside the container images"
                )
            })?;
        self.ensure_image(&image).await?;
        self.networks().await?;
        let rules = self.names.fence_rules();
        let body = json!({
            "Image": image,
            "Env": [format!("CELLAR_NFT={rules}")],
            "Cmd": ["sh", "-c", "printf '%s' \"$CELLAR_NFT\" | nft -f -"],
            "Labels": { "cellar.node": self.node, "cellar.host": self.names.key, "cellar.fence": "1" },
            "HostConfig": { "NetworkMode": "host", "CapAdd": ["NET_ADMIN"],
                "NanoCpus": 62500000u64, "Memory": 16777216u64, "MemorySwap": 16777216u64 },
        });
        let created = self
            .docker
            .expect(
                "POST",
                "/containers/create",
                Some(body),
                "create fence container",
            )
            .await?
            .json()?;
        let id = created
            .get("Id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("create fence container answered without an id"))?
            .to_string();
        let outcome = async {
            self.docker
                .expect(
                    "POST",
                    &format!("/containers/{id}/start"),
                    None,
                    "start fence container",
                )
                .await?;
            let waited = self
                .docker
                .expect(
                    "POST",
                    &format!("/containers/{id}/wait"),
                    None,
                    "wait fence container",
                )
                .await?
                .json()?;
            let code = waited
                .get("StatusCode")
                .and_then(Value::as_i64)
                .unwrap_or(-1);
            if code != 0 {
                let logs = self.logs(&id).await.unwrap_or_default();
                return Err(anyhow!(
                    "the fence container exited with {code}: {}",
                    logs.trim()
                ));
            }
            Ok(())
        }
        .await;
        let _ = self
            .docker
            .call("DELETE", &format!("/containers/{id}?force=true"), None)
            .await;
        outcome.context("fence the container bridges")?;
        tracing::info!(
            event = "container_bridges_fenced",
            bridges = format!("{},{}", self.names.open_bridge, self.names.internal_bridge),
            "containers can reach the Internet and nothing of the node's own"
        );
        *fenced = true;
        Ok(())
    }

    /// Both output streams of a stopped container, for an error message.
    async fn logs(&self, id: &str) -> anyhow::Result<String> {
        let reply = self
            .docker
            .expect(
                "GET",
                &format!("/containers/{id}/logs?stdout=true&stderr=true"),
                None,
                "container logs",
            )
            .await?;
        let mut text = Vec::new();
        let mut rest: &[u8] = &reply.body;
        while rest.len() >= 8 {
            let (_, length) = frame_header(rest[..8].try_into().unwrap());
            let end = (8 + length).min(rest.len());
            text.extend_from_slice(&rest[8..end]);
            rest = &rest[end..];
        }
        Ok(String::from_utf8_lossy(&text).into_owned())
    }

    /// The cell's container handle, adopting a container a previous
    /// activation on this node left running. Called at cell start for a
    /// class with a spec; the object's `running` reads the answer.
    pub async fn attach(&self, scope: &str, class: &str) -> anyhow::Result<Arc<CellContainer>> {
        let service = self.check_owner()?;
        let spec = service
            .deployment
            .read()
            .unwrap()
            .specs
            .iter()
            .find(|spec| spec.class_name == class)
            .cloned()
            .ok_or_else(|| anyhow!("class {class} has no container"))?;
        let previous = self.cell(scope);
        let _previous_control = match &previous {
            Some(cell) => Some(cell.sdk_control.lock().await),
            None => None,
        };
        if let Some(cell) = &previous {
            // Cancel idle cleanup only after any cleanup already executing has
            // finished. Cancelling it halfway through removal loses that fence.
            cell.state.lock().unwrap().sweeper = None;
            if cell.spec.as_ref() != spec.as_ref() {
                self.forget_locked(cell).await?;
            }
        }
        let cell = {
            let mut cells = self.cells.lock().unwrap();
            let cell = cells.entry(scope.to_string()).or_insert_with(|| {
                Arc::new(CellContainer {
                    sdk_control: tokio::sync::Mutex::new(()),
                    scope: scope.to_string(),
                    name: container_name(&self.names.key, scope),
                    spec,
                    state: Mutex::new(CellState::default()),
                    processes: Mutex::new(Vec::new()),
                    process_registry: Arc::downgrade(&service.processes),
                    exit: watch::channel(None).0,
                })
            });
            // A returning cell cancels the destroy its eviction armed.
            cell.state.lock().unwrap().sweeper = None;
            cell.clone()
        };
        let _new_control = if previous
            .as_ref()
            .is_some_and(|prior| Arc::ptr_eq(prior, &cell))
        {
            None
        } else {
            Some(cell.sdk_control.lock().await)
        };
        let running = self.inspect_running(&cell).await?;
        // A container this handle did not start, left running by an
        // earlier owner of the name: give it a run so `monitor()` has an
        // exit to wait for. The run opens before the address lands, because
        // opening one clears it.
        let adopted = running.is_some() && !cell.running();
        if adopted {
            let run = cell.begin_run();
            self.watch_exit(&cell, run);
        }
        let mut state = cell.state.lock().unwrap();
        state.running = running.is_some();
        state.address = running.unwrap_or_default();
        drop(state);
        drop(_new_control);
        Ok(cell)
    }

    /// The cell's handle, if the cell started on this node.
    pub fn cell(&self, scope: &str) -> Option<Arc<CellContainer>> {
        if self
            .service
            .upgrade()
            .is_none_or(|service| service.closed.load(Ordering::Acquire))
        {
            return None;
        }
        self.cells.lock().unwrap().get(scope).cloned()
    }

    /// The memory the node's running containers reserve, summed over their
    /// instance-type caps. A cell whose container is not running reserves
    /// nothing: a stopped container holds no memory, and an idle eviction
    /// stops it before the sample would count it.
    pub fn reserved_memory_bytes(&self) -> u64 {
        self.cells
            .lock()
            .unwrap()
            .values()
            .filter(|cell| cell.running())
            .map(|cell| instance_memory_bytes(cell.spec.instance_type.as_deref()))
            .sum()
    }

    /// This node's running containers per class.
    fn running_instances_by_class(&self) -> std::collections::BTreeMap<String, u64> {
        let mut counts = std::collections::BTreeMap::new();
        for cell in self.cells.lock().unwrap().values() {
            if cell.running() {
                *counts.entry(cell.spec.class_name.clone()).or_insert(0) += 1;
            }
        }
        counts
    }

    /// Refuse a start that would put the class over its fleet-wide
    /// `max_instances`. The fleet count is this node's own running
    /// containers of the class, read live, plus every peer's count from the
    /// shared capacity sample; `begin_run` already marked the starting cell
    /// running, so the live count includes it. A class with no
    /// `max_instances`, or a node with no bucket to read the sample from as
    /// under `cellar dev`, has no fleet ceiling. See
    /// [`crate::ownership_store::fleet_class_instances`] for the staleness
    /// the sample admits.
    async fn enforce_instance_ceiling(&self, cell: &CellContainer) -> anyhow::Result<()> {
        let Some(max) = cell.spec.max_instances else {
            return Ok(());
        };
        let class = &cell.spec.class_name;
        let here = self
            .cells
            .lock()
            .unwrap()
            .values()
            .filter(|other| other.running() && other.spec.class_name == *class)
            .count() as u64;
        let elsewhere = match &self.bucket {
            Some(bucket) => {
                crate::ownership_store::fleet_class_instances(bucket, class, &self.node).await
            }
            None => 0,
        };
        anyhow::ensure!(
            here + elsewhere <= max,
            "container class {class} is at its max_instances limit of {max}: \
             {here} on this node and {elsewhere} on other nodes",
        );
        Ok(())
    }

    async fn inspect_running(&self, cell: &CellContainer) -> anyhow::Result<Option<Address>> {
        let reply = self
            .docker
            .call("GET", &format!("/containers/{}/json", cell.name), None)
            .await?;
        if reply.status.as_u16() == 404 {
            return Ok(None);
        }
        if !reply.status.is_success() {
            return Err(anyhow!(
                "inspect container failed with [{}] {}",
                reply.status.as_u16(),
                reply.message()
            ));
        }
        let info = reply.json()?;
        let running = info
            .pointer("/State/Running")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        Ok(running.then(|| address_of(&info)))
    }

    /// Start the cell's container. The object's `start()` returns before
    /// this completes, as on Cloudflare; a failure surfaces through
    /// `monitor()`.
    /// Start the cell's container for the run `begin_run` opened. The
    /// object's `start()` returns before this completes, as on Cloudflare;
    /// a failure surfaces through `monitor()`.
    pub async fn start(
        &self,
        cell: &Arc<CellContainer>,
        run: u64,
        params: StartParams,
    ) -> anyhow::Result<()> {
        self.check_cell(cell)?;
        let started = self.create_and_start(cell, params).await;
        let address = match started {
            Ok(address) => address,
            Err(error) => {
                // `start()` is fire-and-forget for the object, and its
                // `monitor()` sees this error only if it was already
                // waiting; say it here too, or a start that fails fast
                // leaves no trace anywhere.
                tracing::warn!(
                    event = "container_start_failed",
                    cell = %cell.scope,
                    error = %format!("{error:#}"),
                    "the container did not start"
                );
                let mut state = cell.state.lock().unwrap();
                state.running = false;
                drop(state);
                cell.publish_exit(run, Err(format!("{error:#}")));
                return Err(error);
            }
        };
        {
            let mut state = cell.state.lock().unwrap();
            state.running = true;
            state.address = address;
        }
        self.watch_exit(cell, run);
        Ok(())
    }

    /// Wait for the container's root process to end and publish the exit
    /// for `run`; a later run's start has already replaced the state.
    fn watch_exit(&self, cell: &Arc<CellContainer>, run: u64) {
        let docker = self.docker.clone();
        let cell_ = cell.clone();
        asyncrt::spawn(async move {
            let result = docker
                .call("POST", &format!("/containers/{}/wait", cell_.name), None)
                .await
                .and_then(|reply| {
                    if !reply.status.is_success() {
                        return Err(anyhow!(
                            "wait failed with [{}] {}",
                            reply.status.as_u16(),
                            reply.message()
                        ));
                    }
                    reply
                        .json()?
                        .get("StatusCode")
                        .and_then(Value::as_i64)
                        .ok_or_else(|| anyhow!("wait answered without a status code"))
                })
                .map_err(|error| format!("{error:#}"));
            let mut state = cell_.state.lock().unwrap();
            if state.run == run {
                state.running = false;
                state.address = Address::None;
            }
            drop(state);
            cell_.publish_exit(run, result);
        })
        .detach();
    }

    async fn create_and_start(
        &self,
        cell: &CellContainer,
        params: StartParams,
    ) -> anyhow::Result<Address> {
        self.ensure_fence().await?;
        self.enforce_instance_ceiling(cell).await?;
        self.ensure_image(&cell.spec.image).await?;
        let (open, internal) = self.networks().await?;
        // A previous run's container may still be named: the wait task
        // observed its exit but nothing removed it, or a restart adopted a
        // stopped one. The name is the cell's, so it is ours to remove.
        let _ = self
            .docker
            .call(
                "DELETE",
                &format!("/containers/{}?force=true", cell.name),
                None,
            )
            .await;
        let mut env: Vec<String> = DEFAULT_ENV.iter().map(|entry| entry.to_string()).collect();
        env.push(format!("CELLAR_CELL_ID={}", cell.scope));
        env.push(format!("CELLAR_CELL_CLASS={}", cell.spec.class_name));
        env.push(format!("CELLAR_NODE_ID={}", self.node));
        env.push(format!("CLOUDFLARE_DURABLE_OBJECT_ID={}", cell.scope));
        env.extend(
            params
                .env
                .iter()
                .map(|(name, value)| format!("{name}={value}")),
        );
        let mut labels = serde_json::Map::new();
        labels.insert("cellar.node".into(), json!(self.node));
        labels.insert("cellar.host".into(), json!(self.names.key));
        labels.insert("cellar.cell".into(), json!(cell.scope));
        labels.insert("cellar.class".into(), json!(cell.spec.class_name));
        for (name, value) in &params.labels {
            labels.insert(format!("cellar.user.{name}"), json!(value));
        }
        // Off Linux the node reaches a container only through published
        // ports, and Docker publishes nothing on an internal network, so
        // `enableInternet: false` would make the container unreachable.
        // Development there keeps egress and reports the limitation below.
        let offline = !params.enable_internet && cfg!(target_os = "linux");
        if !params.enable_internet && !offline {
            tracing::warn!(
                cell = %cell.scope,
                "enableInternet: false is not enforced on this platform; the container keeps egress"
            );
        }
        // A container is a tenant's process tree, not the operator's: no
        // capability, no privilege gain through setuid binaries, the
        // daemon's seccomp profile, and a process ceiling. What a class
        // legitimately needs beyond this is a config question for later,
        // not a default. The kernel boundary itself is the runtime's.
        let mut host_config = json!({
            "NetworkMode": if offline { &internal } else { &open },
            "PublishAllPorts": !cfg!(target_os = "linux"),
            "CapDrop": ["ALL"],
            "SecurityOpt": ["no-new-privileges"],
            "PidsLimit": PIDS_LIMIT,
            // An init as PID 1 reaps orphaned children. Without it a process
            // whose parent exits becomes a zombie the container holds until
            // it stops, and enough of them exhaust the PID ceiling; a fleet
            // fork bomb left exactly these behind.
            "Init": true,
        });
        // A class can name its own runtime, else the node default. A class
        // that needs isolation names `runsc`, and the node's daemon must
        // have it or the start fails, so the class runs only where its
        // isolation is real.
        let runtime = cell
            .spec
            .runtime
            .as_deref()
            .or(self.runtime.as_deref())
            .filter(|runtime| !runtime.is_empty());
        if let Some(runtime) = runtime {
            host_config["Runtime"] = json!(runtime);
        }
        // A container under a non-default runtime gets an explicit resolver
        // through a bind-mounted resolv.conf: gVisor cannot reach Docker's
        // embedded resolver at `127.0.0.11`, and on a user bridge Docker
        // keeps that address in resolv.conf whatever `--dns` says, so the
        // file itself must name a reachable resolver. An operator who set
        // `CELLAR_CONTAINER_DNS` gets it for every container.
        let wants_resolver = runtime.is_some() || !self.dns.is_empty();
        if wants_resolver {
            if let Some(resolv) = &self.resolv_conf {
                let bind = format!("{}:/etc/resolv.conf:ro", resolv.display());
                host_config["Binds"] = json!([bind]);
            }
        }
        // Every container has a limit: a class without an instance type
        // gets Cloudflare's default type rather than the node.
        let instance_type = cell
            .spec
            .instance_type
            .as_deref()
            .unwrap_or(DEFAULT_INSTANCE_TYPE);
        let (vcpu, memory) =
            instance_resources(instance_type).expect("the deploy refused this instance type");
        host_config["NanoCpus"] = json!((vcpu * 1e9) as u64);
        host_config["Memory"] = json!(memory);
        host_config["MemorySwap"] = json!(memory);
        let mut body = json!({
            "Image": cell.spec.image,
            "Env": env,
            "Labels": labels,
            "HostConfig": host_config,
        });
        if let Some(entrypoint) = params.entrypoint {
            body["Cmd"] = json!(entrypoint);
        }
        // The daemon can answer 409 for a name whose previous container is
        // still being removed, so a fresh start after `destroy()` retries
        // briefly, as workerd's engine does.
        let mut reply = self
            .docker
            .call(
                "POST",
                &format!("/containers/create?name={}", cell.name),
                Some(body.clone()),
            )
            .await?;
        for _ in 0..20 {
            if reply.status.as_u16() != 409 {
                break;
            }
            asyncrt::sleep(Duration::from_millis(100)).await;
            let _ = self
                .docker
                .call(
                    "DELETE",
                    &format!("/containers/{}?force=true", cell.name),
                    None,
                )
                .await;
            reply = self
                .docker
                .call(
                    "POST",
                    &format!("/containers/create?name={}", cell.name),
                    Some(body.clone()),
                )
                .await?;
        }
        if reply.status.as_u16() == 404 {
            return Err(anyhow!("No such image available named {}", cell.spec.image));
        }
        if !reply.status.is_success() {
            return Err(anyhow!(
                "Create container failed with [{}] {}",
                reply.status.as_u16(),
                reply.message()
            ));
        }
        self.docker
            .expect(
                "POST",
                &format!("/containers/{}/start", cell.name),
                None,
                "start container",
            )
            .await?;
        let info = self
            .docker
            .expect(
                "GET",
                &format!("/containers/{}/json", cell.name),
                None,
                "inspect container",
            )
            .await?
            .json()?;
        Ok(address_of(&info))
    }

    /// Wait for the current run to end. `Ok(code)` is the root process's
    /// exit code; a destroyed container reports 137.
    pub async fn monitor(&self, cell: &Arc<CellContainer>, run: u64) -> Result<i64, String> {
        self.check_cell(cell).map_err(|error| error.to_string())?;
        let mut receiver = cell.exit.subscribe();
        loop {
            if let Some((ended, result)) = receiver.borrow_and_update().clone() {
                if ended >= run {
                    return result;
                }
            }
            if receiver.changed().await.is_err() {
                return Err("the container engine went away".to_string());
            }
        }
    }

    pub async fn destroy(&self, cell: &Arc<CellContainer>) -> anyhow::Result<()> {
        self.check_cell(cell)?;
        // Kill first so the wait task reports 137 before the removal makes
        // the name disappear under it.
        let _ = self
            .docker
            .call(
                "POST",
                &format!("/containers/{}/kill?signal=SIGKILL", cell.name),
                None,
            )
            .await;
        let removed = self
            .docker
            .call(
                "DELETE",
                &format!("/containers/{}?force=true", cell.name),
                None,
            )
            .await?;
        anyhow::ensure!(
            removed.status.is_success() || removed.status.as_u16() == 404,
            "destroy container failed with [{}] {}",
            removed.status.as_u16(),
            removed.message()
        );
        let mut state = cell.state.lock().unwrap();
        state.running = false;
        state.address = Address::None;
        Ok(())
    }

    pub async fn signal(&self, cell: &Arc<CellContainer>, signal: u32) -> anyhow::Result<()> {
        self.check_cell(cell)?;
        self.docker
            .expect(
                "POST",
                &format!("/containers/{}/kill?signal={signal}", cell.name),
                None,
                "signal container",
            )
            .await?;
        Ok(())
    }

    /// The cell left this node's runtime. An idle eviction keeps the
    /// container for its inactivity window; anything else destroys it, and
    /// the stop waits for that: a drain ends the process right after its
    /// last stop, and a destroy left to a detached task did not always get
    /// its two daemon calls in before the exit.
    pub async fn release(self: &Arc<Self>, scope: &str, release: Release) -> anyhow::Result<()> {
        self.check_owner()?;
        let Some(cell) = self.cell(scope) else {
            return Ok(());
        };
        match release {
            Release::Keep => {
                let window = cell
                    .state
                    .lock()
                    .unwrap()
                    .inactivity
                    .unwrap_or(DEFAULT_INACTIVITY);
                let engine = self.clone();
                let cell_ = cell.clone();
                let sweeper = asyncrt::spawn(async move {
                    asyncrt::sleep(window).await;
                    loop {
                        match engine.forget(&cell_).await {
                            Ok(()) => break,
                            Err(error) => {
                                tracing::warn!(cell = %cell_.scope, %error, "idle container removal failed; retrying")
                            }
                        }
                        asyncrt::sleep(Duration::from_secs(30)).await;
                    }
                });
                cell.state.lock().unwrap().sweeper = Some(sweeper);
            }
            Release::Destroy => self.forget(&cell).await?,
        }
        Ok(())
    }

    async fn forget(&self, cell: &Arc<CellContainer>) -> anyhow::Result<()> {
        let _control = cell.sdk_control.lock().await;
        self.forget_locked(cell).await
    }

    async fn forget_locked(&self, cell: &Arc<CellContainer>) -> anyhow::Result<()> {
        if !self
            .cell(&cell.scope)
            .is_some_and(|current| Arc::ptr_eq(&current, cell))
        {
            return Ok(());
        }
        self.destroy(cell).await?;
        let ids = std::mem::take(&mut *cell.processes.lock().unwrap());
        if let Some(registry) = cell.process_registry.upgrade() {
            for id in ids {
                registry.remove(id);
            }
        }
        let mut cells = self.cells.lock().unwrap();
        if cells
            .get(&cell.scope)
            .is_some_and(|current| Arc::ptr_eq(current, cell))
        {
            cells.remove(&cell.scope);
        }
        Ok(())
    }

    pub async fn exec(
        &self,
        cell: &Arc<CellContainer>,
        params: ExecParams,
    ) -> anyhow::Result<Arc<ExecProcess>> {
        self.check_cell(cell)?;
        let service = self.check_owner()?;
        if !cell.running() {
            return Err(anyhow!("exec() requires a running container."));
        }
        let mut body = json!({
            "AttachStdin": true,
            "AttachStdout": true,
            "AttachStderr": true,
            "Tty": false,
            "Cmd": params.cmd,
        });
        if !params.env.is_empty() {
            body["Env"] = json!(params
                .env
                .iter()
                .map(|(name, value)| format!("{name}={value}"))
                .collect::<Vec<_>>());
        }
        if let Some(cwd) = &params.cwd {
            body["WorkingDir"] = json!(cwd);
        }
        if let Some(user) = &params.user {
            body["User"] = json!(user);
        }
        let created = self
            .docker
            .expect(
                "POST",
                &format!("/containers/{}/exec", cell.name),
                Some(body),
                "create exec",
            )
            .await?
            .json()?;
        let exec_id = created
            .get("Id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("exec create answered without an id"))?
            .to_string();
        let stream = self
            .docker
            .hijack(
                &format!("/exec/{exec_id}/start"),
                json!({ "Detach": false, "Tty": false }),
            )
            .await?;
        // Docker reports `Running: false` with no pid before it has spawned
        // the process, and a finished exec keeps its pid, so a pid of zero
        // is the one answer that means "not yet": retry it briefly, as
        // workerd does. Breaking on `Running: false` here lost the pid of a
        // short command whenever the inspect landed in that window.
        let mut pid = 0;
        for _ in 0..20 {
            let info = self
                .docker
                .expect(
                    "GET",
                    &format!("/exec/{exec_id}/json"),
                    None,
                    "inspect exec",
                )
                .await?
                .json()?;
            pid = info.get("Pid").and_then(Value::as_i64).unwrap_or(0);
            if pid != 0 {
                break;
            }
            asyncrt::sleep(Duration::from_millis(50)).await;
        }
        let (read, write) = tokio::io::split(stream);
        let (stdout_tx, stdout_rx) = mpsc::channel(16);
        let (stderr_tx, stderr_rx) = mpsc::channel(16);
        let combined = params.combined;
        let ended = watch::channel(false).0;
        let completion = ExecCompletion(ended.clone());
        let reader = asyncrt::spawn(async move {
            let _completion = completion;
            demux(read, stdout_tx, stderr_tx, combined).await;
        });
        let process = Arc::new(ExecProcess {
            id: service.processes.next.fetch_add(1, Ordering::Relaxed),
            service: self.service.clone(),
            closed: AtomicBool::new(false),
            reader: Mutex::new(Some(reader)),
            exec_id,
            container: cell.name.clone(),
            pid,
            docker: self.docker.clone(),
            stdin: tokio::sync::Mutex::new(Some(write)),
            stdout: tokio::sync::Mutex::new(stdout_rx),
            stderr: tokio::sync::Mutex::new(stderr_rx),
            ended,
            exit_code: tokio::sync::Mutex::new(None),
        });
        service.processes.insert(process.clone())?;
        cell.processes.lock().unwrap().push(process.id);
        Ok(process)
    }
}

pub struct ExecParams {
    pub cmd: Vec<String>,
    pub env: Vec<(String, String)>,
    pub cwd: Option<String>,
    pub user: Option<String>,
    /// stderr folded into stdout.
    pub combined: bool,
}

type WriteHalf = tokio::io::WriteHalf<Box<dyn Stream>>;

pub struct ExecProcess {
    pub id: u64,
    service: Weak<ContainerService>,
    closed: AtomicBool,
    reader: Mutex<Option<asyncrt::TaskHandle<()>>>,
    exec_id: String,
    container: String,
    pub pid: i64,
    docker: Docker,
    stdin: tokio::sync::Mutex<Option<WriteHalf>>,
    stdout: tokio::sync::Mutex<mpsc::Receiver<Bytes>>,
    stderr: tokio::sync::Mutex<mpsc::Receiver<Bytes>>,
    /// True once the hijacked stream reached EOF, which the daemon sends
    /// when the process exits.
    ended: watch::Sender<bool>,
    exit_code: tokio::sync::Mutex<Option<i64>>,
}

struct ProcessRegistry {
    closed: AtomicBool,
    next: AtomicU64,
    entries: Mutex<HashMap<u64, Arc<ExecProcess>>>,
}

impl Default for ProcessRegistry {
    fn default() -> Self {
        Self {
            closed: AtomicBool::new(false),
            next: AtomicU64::new(1),
            entries: Mutex::new(HashMap::new()),
        }
    }
}

impl ProcessRegistry {
    fn insert(&self, process: Arc<ExecProcess>) -> anyhow::Result<()> {
        let mut entries = self.entries.lock().unwrap();
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "container host is closed"
        );
        entries.insert(process.id, process);
        Ok(())
    }

    fn remove(&self, id: u64) {
        let process = self.entries.lock().unwrap().remove(&id);
        if let Some(process) = process {
            process.close();
        }
    }

    fn close(&self) {
        let entries = {
            let mut entries = self.entries.lock().unwrap();
            self.closed.store(true, Ordering::Release);
            std::mem::take(&mut *entries)
        };
        for process in entries.into_values() {
            process.close();
        }
    }
}

pub fn process(id: u64) -> Option<Arc<ExecProcess>> {
    asyncrt::services()
        .containers()
        .processes
        .entries
        .lock()
        .unwrap()
        .get(&id)
        .cloned()
}

pub fn drop_process(id: u64) {
    asyncrt::services().containers().processes.remove(id);
}

struct ExecCompletion(watch::Sender<bool>);

impl Drop for ExecCompletion {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

struct StdinLease<'a> {
    closed: &'a AtomicBool,
    stdin: tokio::sync::MutexGuard<'a, Option<WriteHalf>>,
}

impl Drop for StdinLease<'_> {
    fn drop(&mut self) {
        if self.closed.load(Ordering::Acquire) {
            self.stdin.take();
        }
    }
}

impl Drop for ExecProcess {
    fn drop(&mut self) {
        self.close();
    }
}

impl ExecProcess {
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.ended.send_replace(true);
        let reader = self.reader.lock().unwrap().take();
        drop(reader);
        if let Ok(mut stdin) = self.stdin.try_lock() {
            stdin.take();
        }
    }

    fn check_open(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "container host is closed"
        );
        let service = self
            .service
            .upgrade()
            .ok_or_else(|| anyhow!("container host is gone"))?;
        service.check_open()?;
        anyhow::ensure!(
            Arc::ptr_eq(&service, &asyncrt::services().containers()),
            "exec process belongs to another host"
        );
        Ok(())
    }

    /// One chunk of stdout (`1`) or stderr (`2`); empty at end of stream.
    pub async fn read(&self, which: u8) -> Bytes {
        if self.check_open().is_err() {
            return Bytes::new();
        }
        let mut receiver = match which {
            1 => self.stdout.lock().await,
            _ => self.stderr.lock().await,
        };
        receiver.recv().await.unwrap_or_default()
    }

    pub async fn write(&self, bytes: &[u8]) -> Result<(), String> {
        let mut guard = StdinLease {
            closed: &self.closed,
            stdin: self.stdin.lock().await,
        };
        self.check_open().map_err(|error| error.to_string())?;
        let stdin = guard.stdin.as_mut().ok_or("stdin is closed")?;
        stdin
            .write_all(bytes)
            .await
            .map_err(|error| format!("stdin write failed: {error}"))?;
        stdin
            .flush()
            .await
            .map_err(|error| format!("stdin flush failed: {error}"))
    }

    /// End stdin. Half-closing the hijacked connection is how the daemon
    /// learns the process's stdin reached EOF.
    pub async fn close_stdin(&self) {
        if self.check_open().is_err() {
            return;
        }
        if let Some(mut stdin) = self.stdin.lock().await.take() {
            let _ = stdin.shutdown().await;
        }
    }

    pub async fn wait(&self) -> anyhow::Result<i64> {
        self.check_open()?;
        let mut exit = self.exit_code.lock().await;
        if let Some(code) = *exit {
            return Ok(code);
        }
        let mut ended = self.ended.subscribe();
        while !*ended.borrow_and_update() {
            if ended.changed().await.is_err() {
                break;
            }
        }
        self.check_open()?;
        // The stream closes when the process exits, but the daemon records
        // the exit code a moment later.
        let mut code = None;
        for _ in 0..40 {
            let info = self
                .docker
                .expect(
                    "GET",
                    &format!("/exec/{}/json", self.exec_id),
                    None,
                    "inspect exec",
                )
                .await?
                .json()?;
            if !info
                .get("Running")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                code = info.get("ExitCode").and_then(Value::as_i64);
                break;
            }
            asyncrt::sleep(Duration::from_millis(50)).await;
        }
        let code = code.ok_or_else(|| anyhow!("the process did not report an exit code"))?;
        *exit = Some(code);
        Ok(code)
    }

    /// The Engine API has no exec kill, so the signal is delivered by a
    /// second exec of `kill`, as workerd does.
    pub async fn kill(&self, signal: u32) -> anyhow::Result<()> {
        self.check_open()?;
        if *self.ended.borrow() {
            return Ok(());
        }
        anyhow::ensure!(self.pid > 0, "exec process has no live PID");
        let body = json!({
            "AttachStdin": false, "AttachStdout": false, "AttachStderr": false,
            "Cmd": ["kill", format!("-{signal}"), self.pid.to_string()],
        });
        let created = self
            .docker
            .expect(
                "POST",
                &format!("/containers/{}/exec", self.container),
                Some(body),
                "create exec",
            )
            .await?
            .json()?;
        let id = created
            .get("Id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("exec create answered without an id"))?;
        self.docker
            .expect(
                "POST",
                &format!("/exec/{id}/start"),
                Some(json!({ "Detach": true })),
                "start exec",
            )
            .await?;
        Ok(())
    }
}

/// Split Docker's multiplexed stream into stdout and stderr chunks until
/// the daemon closes it.
async fn demux(
    mut read: tokio::io::ReadHalf<Box<dyn Stream>>,
    stdout: mpsc::Sender<Bytes>,
    stderr: mpsc::Sender<Bytes>,
    combined: bool,
) {
    let mut header = [0u8; 8];
    loop {
        if read.read_exact(&mut header).await.is_err() {
            return;
        }
        let (stream, length) = frame_header(&header);
        let mut payload = vec![0u8; length];
        if read.read_exact(&mut payload).await.is_err() {
            return;
        }
        let target = if stream == 2 && !combined {
            &stderr
        } else {
            &stdout
        };
        if target.send(Bytes::from(payload)).await.is_err() {
            // The reader went away; keep draining so the process is not
            // blocked on a full pipe.
            continue;
        }
    }
}

fn address_of(info: &Value) -> Address {
    if cfg!(target_os = "linux") {
        let ip = info
            .pointer("/NetworkSettings/Networks")
            .and_then(Value::as_object)
            .and_then(|networks| networks.values().next())
            .and_then(|network| network.get("IPAddress"))
            .and_then(Value::as_str)
            .filter(|ip| !ip.is_empty());
        return ip.map_or(Address::None, |ip| Address::Ip(ip.to_string()));
    }
    let mut ports = HashMap::new();
    if let Some(map) = info
        .pointer("/NetworkSettings/Ports")
        .and_then(Value::as_object)
    {
        for (key, bindings) in map {
            let Some(port) = key
                .strip_suffix("/tcp")
                .and_then(|port| port.parse::<u16>().ok())
            else {
                continue;
            };
            let host = bindings
                .as_array()
                .into_iter()
                .flatten()
                .find_map(|binding| binding.get("HostPort")?.as_str()?.parse::<u16>().ok());
            if let Some(host) = host {
                ports.insert(port, host);
            }
        }
    }
    Address::Published(ports)
}

/// Stable infrastructure names belonging to one host, independent of the
/// process ID. Bridges fit Linux's 15-character interface-name limit.
struct ResourceNames {
    key: String,
    tag: String,
    open_network: String,
    internal_network: String,
    open_bridge: String,
    internal_bridge: String,
    table: String,
}

impl ResourceNames {
    fn new(node: &str, data_dir: &std::path::Path, namespace: Option<&str>) -> Self {
        use base64::Engine;
        use sha2::Digest;
        let mut hash = sha2::Sha256::new();
        hash.update(node.as_bytes());
        hash.update([0]);
        match namespace {
            Some(namespace) => {
                hash.update(b"namespace:");
                hash.update(namespace.as_bytes());
            }
            None => {
                hash.update(b"state:");
                hash.update(data_dir.as_os_str().as_encoded_bytes());
            }
        }
        let digest = hash.finalize();
        let key = format!("{digest:x}");
        let tag = key[..24].to_string();
        let bridge = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&digest[..9]);
        Self {
            open_network: format!("cellar-{tag}"),
            internal_network: format!("cellar-{tag}-internal"),
            open_bridge: format!("cl{bridge}0"),
            internal_bridge: format!("cl{bridge}1"),
            table: format!("cellar_{tag}"),
            key,
            tag,
        }
    }

    fn fence_rules(&self) -> String {
        FENCE_RULES
            .replace("__TABLE__", &self.table)
            .replace("__OPEN__", &self.open_bridge)
            .replace("__INTERNAL__", &self.internal_bridge)
    }
}

/// A Docker name from a host namespace and cell scope. The full identities
/// live in labels; names use a content digest with legal daemon characters.
fn container_name(host: &str, scope: &str) -> String {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(format!("{host}\n{scope}").as_bytes());
    format!("cellar-{:x}", digest)[..30].to_string()
}

#[cfg(test)]
mod exit_tests {
    use super::*;

    #[test]
    fn delayed_old_exit_cannot_hide_the_latest_run_completion() {
        let cell = CellContainer {
            sdk_control: tokio::sync::Mutex::new(()),
            scope: "Sandbox:test".into(),
            name: "test".into(),
            spec: Arc::new(ContainerSpec {
                class_name: "Sandbox".into(),
                image: image_reference(&"a".repeat(64)),
                instance_type: None,
                max_instances: None,
                runtime: None,
            }),
            state: Mutex::new(CellState::default()),
            processes: Mutex::new(Vec::new()),
            process_registry: Weak::new(),
            exit: watch::channel(None).0,
        };
        let old = cell.begin_run();
        let current = cell.begin_run();
        cell.publish_exit(current, Ok(0));
        cell.publish_exit(old, Err("late engine wait".into()));
        assert_eq!(*cell.exit.borrow(), Some((current, Ok(0))));
    }
}
