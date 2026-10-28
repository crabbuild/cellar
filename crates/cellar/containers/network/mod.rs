// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A helper namespace and its private transport belong to one container run.

use super::*;
use crate::application::service::{HttpHandler, Persistent};
use anyhow::{ensure, Result};
use cellar_egress::{control, docker_io::DockerIo, transport};
use rand::RngCore;
mod http;
mod https;

pub(super) const MEMORY_BYTES: u64 = 256 * 1024 * 1024;
const RUN_LABEL: &str = "cellar.network.run";
const ROLE_LABEL: &str = "cellar.network.role";

pub(super) struct Pending {
    target: cellar_egress::HttpTarget,
    handler: Box<dyn HttpHandler>,
}

pub(super) struct Run {
    pub run: u64,
    token: String,
    session: [u8; 32],
    pub id: OnceLock<ContainerId>,
    retired: AtomicBool,
    ready: AtomicBool,
    established: AtomicBool,
    started: AtomicBool,
    creation_submitted: AtomicBool,
    removed: AtomicBool,
    control: Mutex<Option<control::Routes<Box<dyn HttpHandler>>>>,
    installer: Mutex<Option<control::Installer<Box<dyn HttpHandler>>>>,
    task: Mutex<Option<asyncrt::TaskHandle<()>>>,
    cleanup: tokio::sync::Mutex<()>,
    done: watch::Sender<Option<Result<(), String>>>,
    tls: OnceLock<Tls>,
}

struct Tls {
    authority: Arc<https::Authority>,
    upstream: Arc<rustls::ClientConfig>,
}

impl Run {
    fn new(run: u64) -> Result<Arc<Self>> {
        let mut session = [0; 32];
        asyncrt::rng("container-network-session").try_fill_bytes(&mut session)?;
        Ok(Arc::new(Self {
            run,
            token: hex::encode(session),
            session,
            id: OnceLock::new(),
            retired: AtomicBool::new(false),
            ready: AtomicBool::new(false),
            established: AtomicBool::new(false),
            started: AtomicBool::new(false),
            creation_submitted: AtomicBool::new(false),
            removed: AtomicBool::new(false),
            control: Mutex::new(None),
            installer: Mutex::new(None),
            task: Mutex::new(None),
            cleanup: tokio::sync::Mutex::new(()),
            done: watch::channel(None).0,
            tls: OnceLock::new(),
        }))
    }

    pub fn retire(&self) {
        self.retired.store(true, Ordering::Release);
        self.close_transport();
    }

    fn close_transport(&self) {
        self.ready.store(false, Ordering::Release);
        if let Some(routes) = self.control.lock().unwrap().as_ref() {
            routes.retire();
        }
    }

    pub fn active(&self) -> bool {
        self.ready.load(Ordering::Acquire) && !self.retired.load(Ordering::Acquire)
    }

    async fn join(&self, timeout: Duration) -> Result<()> {
        if !self.started.load(Ordering::Acquire) {
            return Ok(());
        }
        let task = self.task.lock().unwrap().take();
        let mut lease = JoinLease {
            slot: &self.task,
            task,
        };
        if let Some(task) = lease.task.as_mut() {
            match asyncrt::timeout(timeout, &mut *task).await {
                Ok(joined) => {
                    lease.task.take();
                    joined.context("network transport task failed")?;
                }
                Err(error) => {
                    task.abort();
                    let _ = task.await;
                    lease.task.take();
                    return Err(error).context("network transport retirement timed out");
                }
            }
        }
        let mut done = self.done.subscribe();
        let outcome = asyncrt::timeout(timeout, async {
            let observed = done.wait_for(Option::is_some).await?;
            Ok::<_, anyhow::Error>(observed.as_ref().unwrap().clone())
        })
        .await
        .context("network transport retirement timed out")??;
        outcome.map_err(anyhow::Error::msg)
    }

    fn labels(&self, engine: &ContainerEngine, cell: &CellContainer, role: &str) -> Value {
        let mut labels = json!({"cellar.node":engine.node, "cellar.host":engine.names.key,
            "cellar.cell":cell.scope, "cellar.class":cell.spec.class_name});
        labels[RUN_LABEL] = json!(self.token);
        labels[ROLE_LABEL] = json!(role);
        labels
    }
}

/// A cancelled cleanup waiter returns the join handle to its run.
struct JoinLease<'a> {
    slot: &'a Mutex<Option<asyncrt::TaskHandle<()>>>,
    task: Option<asyncrt::TaskHandle<()>>,
}

impl Drop for JoinLease<'_> {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            *self.slot.lock().unwrap() = Some(task);
        }
    }
}

struct ProtocolGuard {
    run: Arc<Run>,
    engine: Weak<ContainerEngine>,
    cell: Weak<CellContainer>,
    finished: bool,
}

impl ProtocolGuard {
    fn finish(&mut self, outcome: Result<()>) {
        self.finished = true;
        let expected = self.run.retired.load(Ordering::Acquire);
        self.run.retire();
        let outcome = if expected {
            Ok(())
        } else {
            outcome.and_then(|_| Err(anyhow!("container network transport ended unexpectedly")))
        };
        let failure = outcome.as_ref().err().map(|error| format!("{error:#}"));
        self.run
            .done
            .send_replace(Some(outcome.map_err(|error| format!("{error:#}"))));
        if let (Some(failure), Some(engine), Some(cell)) =
            (failure, self.engine.upgrade(), self.cell.upgrade())
        {
            let mut state = cell.state.lock().unwrap();
            if state.run == self.run.run {
                state.running = false;
                state.address = Address::None;
            }
            drop(state);
            cell.publish_exit(self.run.run, Err(failure));
            let run = self.run.clone();
            if let Ok(task) = asyncrt::try_spawn(async move {
                let _control = cell.sdk_control.lock().await;
                if let Err(error) = engine.cleanup_network(&cell, &run).await {
                    tracing::warn!(cell = %cell.scope, %error, "failed network run requires owned cleanup");
                }
            }) {
                task.detach();
            }
        }
    }
}

impl Drop for ProtocolGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.finish(Err(anyhow!("container network task stopped")));
        }
    }
}

/// Dropping an accepted start retires transport immediately and submits owned
/// cleanup behind SDK control. Domain shutdown also reaps all owned labels.
pub(super) struct StartGuard {
    engine: Arc<ContainerEngine>,
    cell: Arc<CellContainer>,
    run: u64,
    pub armed: bool,
}

impl StartGuard {
    pub fn new(engine: &Arc<ContainerEngine>, cell: &Arc<CellContainer>, run: u64) -> Self {
        Self {
            engine: engine.clone(),
            cell: cell.clone(),
            run,
            armed: true,
        }
    }
}

impl Drop for StartGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let network = {
            let mut state = self.cell.state.lock().unwrap();
            if state.run == self.run {
                state.running = false;
                state.address = Address::None;
            }
            state
                .network
                .clone()
                .filter(|network| network.run == self.run)
        };
        self.cell
            .publish_exit(self.run, Err("container startup was abandoned".into()));
        let Some(network) = network else {
            return;
        };
        network.retire();
        let engine = self.engine.clone();
        let cell = self.cell.clone();
        if let Ok(task) = asyncrt::try_spawn(async move {
            let _control = cell.sdk_control.lock().await;
            if let Err(error) = engine.cleanup_network(&cell, &network).await {
                tracing::warn!(cell = %cell.scope, %error, "cancelled network startup requires owned cleanup");
            }
        }) {
            task.detach();
        }
    }
}

impl ContainerEngine {
    pub(super) async fn install_https_ca(
        &self,
        cell: &CellContainer,
        id: &ContainerId,
        run: &Arc<Run>,
    ) -> Result<()> {
        let Some(tls) = run.tls.get() else {
            return Ok(());
        };
        self.check_cell(cell)?;
        ensure!(
            cell.current_run() == run.run && run.active(),
            "CA installation belongs to a retired run"
        );
        let body = Bytes::from(tls.authority.archive()?);
        let (reply, _) = self
            .docker
            .archive(
                "PUT",
                &format!("/containers/{id}/archive?path=%2F&noOverwriteDirNonDir=true"),
                body,
                16384,
            )
            .await?;
        ensure!(
            reply.status.is_success(),
            "install run-owned public CA: {}",
            reply.message()
        );
        self.check_cell(cell)?;
        ensure!(
            cell.current_run() == run.run && run.active(),
            "CA installation completed after retirement"
        );
        Ok(())
    }

    pub(super) async fn network_ports(&self, cell: &CellContainer) -> Result<Vec<u16>> {
        let info = self
            .docker
            .expect(
                "GET",
                &format!("/images/{}/json", cell.spec.image),
                None,
                "inspect network application ports",
            )
            .await?
            .json()?;
        let mut ports: std::collections::BTreeSet<u16> =
            cell.spec.exposed_ports.iter().copied().collect();
        if let Some(exposed) = info
            .pointer("/Config/ExposedPorts")
            .filter(|value| !value.is_null())
        {
            for key in exposed
                .as_object()
                .context("image ports are not an object")?
                .keys()
            {
                let (number, protocol) =
                    key.split_once('/').context("image omitted port protocol")?;
                let port: u16 = number.parse().context("invalid image port")?;
                ensure!(
                    port != 0 && matches!(protocol, "tcp" | "udp"),
                    "invalid image port declaration"
                );
                if protocol == "tcp" {
                    ports.insert(port);
                }
            }
        }
        let ports = ports.into_iter().collect::<Vec<_>>();
        ports::validate_exposed_ports(&ports)?;
        ensure!(
            !ports.iter().any(|port| matches!(port, 39189 | 39190)),
            "application ports collide with network helper listeners"
        );
        Ok(ports)
    }

    pub(super) async fn network_address(&self, network: &Run) -> Result<Address> {
        ensure!(network.active(), "container network has retired");
        let id = network
            .id
            .get()
            .context("network helper has no recorded ID")?;
        let info = self
            .docker
            .expect(
                "GET",
                &format!("/containers/{id}/json"),
                None,
                "inspect network address",
            )
            .await?
            .json()?;
        ensure!(
            info["Id"].as_str() == Some(id.as_str())
                && info.pointer("/State/Running").and_then(Value::as_bool) == Some(true),
            "network helper is no longer running"
        );
        ensure!(network.active(), "container network has retired");
        Ok(address_of(&info))
    }

    pub(super) fn validate_network_runtime(&self, cell: &CellContainer) -> Result<()> {
        if self.network_helper.is_some() {
            ensure!(
                cell.spec.runtime.as_deref().or(self.runtime.as_deref()) == Some("runc"),
                "the native network-helper profile requires explicit runc"
            );
        }
        Ok(())
    }

    pub(super) async fn prepare_network(
        self: &Arc<Self>,
        cell: &Arc<CellContainer>,
        run: u64,
        internet: bool,
        network: &str,
        ports: &[u16],
    ) -> Result<Option<Arc<Run>>> {
        let Some(options) = &self.network_helper else {
            return Ok(None);
        };
        self.ensure_image(&options.image).await?;
        let attempt = Run::new(run)?;
        if let Some(https) = &options.https {
            attempt
                .tls
                .set(Tls {
                    authority: https::Authority::new().await?,
                    upstream: https.upstream.clone(),
                })
                .map_err(|_| anyhow!("TLS run already initialized"))?;
        }
        {
            let mut state = cell.state.lock().unwrap();
            ensure!(
                state.run == run && state.network.is_none(),
                "network startup belongs to a superseded run"
            );
            state.network = Some(attempt.clone());
        }
        let mut body = json!({
            "Image":options.image, "Entrypoint":[options.executable],
            "Cmd":["--session",attempt.token,"--internet",internet.to_string(),
                "--dns",options.upstream_dns.to_string(),"--nft-path",options.nft_path],
            "Labels":attempt.labels(self, cell, "helper"),
            "OpenStdin":true,"StdinOnce":true,"AttachStdin":true,
            "AttachStdout":true,"AttachStderr":true,"Tty":false,
            "HostConfig":{"NetworkMode":network,"Runtime":"runc",
                "CapDrop":["ALL"],"CapAdd":["NET_ADMIN"],
                "SecurityOpt":["no-new-privileges"],"PidsLimit":128,
                "Memory":MEMORY_BYTES,"MemorySwap":MEMORY_BYTES,"NanoCpus":62500000,
                "Sysctls":{"net.ipv4.ip_unprivileged_port_start":"0",
                    "net.ipv6.conf.all.disable_ipv6":"0","net.ipv6.conf.lo.disable_ipv6":"0"}}
        });
        if options.https.is_some() {
            body["Cmd"]
                .as_array_mut()
                .unwrap()
                .extend([json!("--https"), json!("true")]);
        }
        ports::configure(&mut body, ports, !cfg!(target_os = "linux"))?;
        let reservations = if cfg!(target_os = "linux") {
            Vec::new()
        } else {
            ports::reserve_host_bindings(&mut body, ports).await?
        };
        attempt.creation_submitted.store(true, Ordering::Release);
        let reply = self
            .docker
            .expect(
                "POST",
                &format!("/containers/create?name={}-network", cell.name),
                Some(body),
                "create network helper",
            )
            .await?
            .json()?;
        let id = ContainerId::new(
            reply["Id"]
                .as_str()
                .context("network helper omitted its immutable ID")?,
        )?;
        attempt
            .id
            .set(id.clone())
            .map_err(|_| anyhow!("network helper ID already recorded"))?;
        let io = self.docker.attach_io(id.as_str()).await?;
        let (link, mut incoming, driver) =
            transport::session(DockerIo::new(io), transport::Role::Host);
        let (installer, routes, controller) =
            control::channel::<Box<dyn HttpHandler>>(link.clone(), attempt.session);
        *attempt.control.lock().unwrap() = Some(routes.clone());
        if attempt.retired.load(Ordering::Acquire) {
            routes.retire();
        }
        let mut guard = ProtocolGuard {
            run: attempt.clone(),
            engine: Arc::downgrade(self),
            cell: Arc::downgrade(cell),
            finished: false,
        };
        attempt.started.store(true, Ordering::Release);
        let serving = attempt.clone();
        let task = asyncrt::try_spawn(async move {
            let native = async {
                let control = incoming
                    .next()
                    .await
                    .context("network helper omitted control readiness")?;
                tokio::try_join!(
                    controller.run(control),
                    http::serve(incoming, serving, routes, link)
                )?;
                Ok::<_, anyhow::Error>(())
            };
            let result = tokio::try_join!(driver.run(), native).map(|_| ());
            guard.finish(result);
        })?;
        *attempt.task.lock().unwrap() = Some(task);
        self.check_cell(cell)?;
        ensure!(
            cell.current_run() == run && !attempt.retired.load(Ordering::Acquire),
            "network startup belongs to a retired run"
        );
        drop(reservations);
        self.docker
            .expect(
                "POST",
                &format!("/containers/{id}/start"),
                None,
                "start network helper",
            )
            .await?;
        let readiness = async {
            installer.ready().await?;
            attempt.established.store(true, Ordering::Release);
            let pending = {
                let mut state = cell.state.lock().unwrap();
                ensure!(
                    state.run == run,
                    "pending intercepts belong to a superseded run"
                );
                std::mem::take(&mut state.pending_http)
            };
            for rule in pending {
                ensure!(
                    !attempt.retired.load(Ordering::Acquire),
                    "network helper retired during startup"
                );
                installer.install(rule.target, rule.handler).await?;
            }
            Ok::<_, anyhow::Error>(())
        };
        asyncrt::timeout(options.startup_timeout, readiness)
            .await
            .context("network helper readiness timed out")??;
        ensure!(
            !attempt.retired.load(Ordering::Acquire),
            "network helper retired during startup"
        );
        attempt.ready.store(true, Ordering::Release);
        // Keep the command sender alive with the run; dropping it would retire
        // an otherwise healthy controller.
        *attempt.installer.lock().unwrap() = Some(installer);
        Ok(Some(attempt))
    }

    pub(super) async fn cleanup_network(&self, cell: &CellContainer, run: &Arc<Run>) -> Result<()> {
        run.retire();
        let _cleanup = run.cleanup.lock().await;
        if run.removed.load(Ordering::Acquire) {
            return Ok(());
        }
        let ready = run.established.load(Ordering::Acquire);
        let filters = json!({"label":[format!("cellar.node={}",self.node),
            format!("cellar.host={}",self.names.key),format!("cellar.cell={}",cell.scope),
            format!("{RUN_LABEL}={}",run.token)]})
        .to_string();
        let list = self
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
                "discover owned network run",
            )
            .await?
            .json()?;
        let mut applications = Vec::new();
        let mut helpers = Vec::new();
        for entry in list
            .as_array()
            .context("network resource list is not an array")?
        {
            let labels = &entry["Labels"];
            for (key, expected) in [
                ("cellar.node", self.node.as_str()),
                ("cellar.host", self.names.key.as_str()),
                ("cellar.cell", cell.scope.as_str()),
                ("cellar.class", cell.spec.class_name.as_str()),
                (RUN_LABEL, run.token.as_str()),
            ] {
                ensure!(
                    labels[key].as_str() == Some(expected),
                    "network cleanup received a foreign resource"
                );
            }
            let id = ContainerId::new(
                entry["Id"]
                    .as_str()
                    .context("network resource omitted its immutable ID")?,
            )?;
            match labels[ROLE_LABEL].as_str() {
                Some("application") => applications.push(id),
                Some("helper") => helpers.push(id),
                _ => anyhow::bail!("network resource omitted its owned role"),
            }
        }
        if let Some(resource) = cell
            .state
            .lock()
            .unwrap()
            .resource
            .as_ref()
            .filter(|resource| resource.run == run.run)
        {
            if !applications.contains(&resource.id) {
                applications.push(resource.id.clone());
            }
        }
        if let Some(id) = run.id.get() {
            if !helpers.contains(id) {
                helpers.push(id.clone());
            }
        }
        ensure!(
            !run.creation_submitted.load(Ordering::Acquire) || !helpers.is_empty(),
            "network helper creation outcome is unknown; retain owned cleanup obligation"
        );
        for id in applications {
            let _ = self
                .docker
                .call(
                    "POST",
                    &format!("/containers/{id}/kill?signal=SIGKILL"),
                    None,
                )
                .await;
            self.remove_id(&id).await?;
        }
        let timeout = self.network_helper.as_ref().unwrap().shutdown_timeout;
        let joined = run.join(timeout).await;
        let mut graceful = Ok(());
        if ready {
            for id in &helpers {
                graceful = async {
                    let reply = asyncrt::timeout(
                        timeout,
                        self.docker
                            .call("POST", &format!("/containers/{id}/wait"), None),
                    )
                    .await
                    .context("network helper shutdown timed out")??;
                    ensure!(
                        reply.status.is_success() && reply.json()?["StatusCode"] == 0,
                        "network helper did not exit successfully"
                    );
                    Ok(())
                }
                .await;
                if graceful.is_err() {
                    break;
                }
            }
        }
        for id in helpers {
            self.remove_id(&id).await?;
        }
        // A process waiter and an explicit release can both submit cleanup.
        // Once every recorded resource is confirmed absent, later waiters
        // must not treat an already removed helper's /wait 404 as a failure.
        run.removed.store(true, Ordering::Release);
        let mut state = cell.state.lock().unwrap();
        if state
            .network
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, run))
        {
            state.network = None;
            if state
                .resource
                .as_ref()
                .is_some_and(|resource| resource.run == run.run)
            {
                state.resource = None;
            }
            if state.run == run.run {
                state.running = false;
                state.address = Address::None;
            }
        }
        drop(state);
        joined.and(graceful)
    }

    pub(super) fn network_application_labels(&self, body: &mut Value, network: &Run) {
        body["Labels"][RUN_LABEL] = json!(network.token);
        body["Labels"][ROLE_LABEL] = json!("application");
    }

    pub(crate) fn http_interception_configured(&self, cell: &CellContainer) -> bool {
        self.network_helper.is_some() && self.validate_network_runtime(cell).is_ok()
    }

    pub(crate) fn https_interception_configured(&self, cell: &CellContainer) -> bool {
        self.http_interception_configured(cell)
            && self
                .network_helper
                .as_ref()
                .is_some_and(|options| options.https.is_some())
    }

    pub(crate) async fn intercept_http(
        &self,
        cell: &Arc<CellContainer>,
        target: cellar_egress::HttpTarget,
        handler: Persistent,
    ) -> Result<()> {
        self.check_cell(cell)?;
        ensure!(
            self.http_interception_configured(cell),
            "HTTP interception requires the explicit native runc network-helper profile"
        );
        ensure!(
            target.protocol() != cellar_egress::Protocol::Https
                || self.https_interception_configured(cell),
            "native HTTPS interception is not configured"
        );
        let handler: Box<dyn HttpHandler> = Box::new(handler);
        let network = {
            let mut state = cell.state.lock().unwrap();
            if state.network.is_none() && !state.running {
                let existing = state
                    .pending_http
                    .iter()
                    .position(|rule| rule.target == target);
                let entries = state
                    .pending_http
                    .iter()
                    .map(|rule| rule.target.entries())
                    .sum::<usize>()
                    + existing.map_or(target.entries(), |_| 0);
                ensure!(
                    entries <= cellar_egress::MAX_INTERCEPT_ENTRIES,
                    "You can't configure more than 128 egress interceptors"
                );
                let rule = Pending { target, handler };
                let previous = if let Some(index) = existing {
                    Some(std::mem::replace(&mut state.pending_http[index], rule))
                } else {
                    state.pending_http.push(rule);
                    None
                };
                drop(state);
                drop(previous);
                return Ok(());
            }
            state
                .network
                .clone()
                .context("container has no ready network helper")?
        };
        ensure!(
            network.run == cell.current_run() && network.active(),
            "container network has retired"
        );
        let installer = network
            .installer
            .lock()
            .unwrap()
            .clone()
            .context("network helper is not ready")?;
        installer.install(target, handler).await?;
        self.check_cell(cell)?;
        ensure!(
            network.run == cell.current_run() && network.active(),
            "container network retired during installation"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests;
