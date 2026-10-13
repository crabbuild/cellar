// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld: crates/celld/js/tcp.rs.
//! Event-owned outbound TCP/TLS on an instance-owned transport registry.

use crate::host_gates::{await_egress_gate, EgressGate};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

pub(crate) trait Stream: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin> Stream for T {}

pub(crate) type Duplex = Box<dyn Stream>;
type ReadHalf = tokio::io::ReadHalf<Duplex>;
type WriteHalf = tokio::io::WriteHalf<Duplex>;

struct Transport {
    read: Arc<tokio::sync::Mutex<Option<ReadHalf>>>,
    write: Arc<tokio::sync::Mutex<Option<WriteHalf>>>,
}

impl Transport {
    fn new(stream: Duplex) -> Self {
        let (read, write) = tokio::io::split(stream);
        Self {
            read: Arc::new(tokio::sync::Mutex::new(Some(read))),
            write: Arc::new(tokio::sync::Mutex::new(Some(write))),
        }
    }
}

pub(crate) struct Lifetime {
    operations: CancellationToken,
    closed: CancellationToken,
    outcome: Mutex<Option<Result<(), String>>>,
}

impl Lifetime {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            operations: CancellationToken::new(),
            closed: CancellationToken::new(),
            outcome: Mutex::new(None),
        })
    }

    fn finish(&self, result: Result<(), String>) {
        {
            let mut outcome = self.outcome.lock().unwrap();
            if outcome.is_some() {
                return;
            }
            *outcome = Some(result);
        }
        // Wakers and transport destructors must run outside registry locks.
        self.operations.cancel();
        self.closed.cancel();
    }

    pub(crate) async fn closed(&self) -> Result<(), String> {
        self.closed.cancelled().await;
        self.outcome.lock().unwrap().as_ref().unwrap().clone()
    }

    async fn run<T>(
        &self,
        future: impl std::future::Future<Output = Result<T, String>>,
    ) -> Result<T, String> {
        crate::asyncrt::select_biased! { "socket closure cancels undelivered transport work";
            _ = self.operations.cancelled() => Err("socket is closed".into()),
            result = future => result,
        }
    }
}

pub(crate) struct Entry {
    lifetime: Arc<Lifetime>,
    transport: Option<Transport>,
}

pub(crate) struct TcpService {
    registry: Arc<Mutex<HashMap<u64, Entry>>>,
    next: AtomicU64,
    closed: AtomicBool,
    #[cfg(any(test, cellar_internal_tests))]
    root: Arc<Mutex<Option<Vec<u8>>>>,
    connector: OnceLock<tokio_rustls::TlsConnector>,
}

impl Default for TcpService {
    fn default() -> Self {
        Self {
            registry: Arc::new(Mutex::new(HashMap::new())),
            next: AtomicU64::new(1),
            closed: AtomicBool::new(false),
            #[cfg(any(test, cellar_internal_tests))]
            root: Arc::new(Mutex::new(None)),
            connector: OnceLock::new(),
        }
    }
}

impl TcpService {
    pub(crate) fn next_id(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn close(&self) {
        let entries = {
            let mut registry = self.registry.lock().unwrap();
            self.closed.store(true, Ordering::Release);
            std::mem::take(&mut *registry)
        };
        for entry in entries.into_values() {
            entry
                .lifetime
                .finish(Err("execution domain is closed".into()));
        }
    }
}

/// Reserve before suspension. Cleanup can then prevent a late connection or
/// TLS handshake from publishing a transport into an already retired event.
pub(crate) fn reserve_in(service: &TcpService, id: u64) -> Arc<Lifetime> {
    let lifetime = Lifetime::new();
    let mut registry = service.registry.lock().unwrap();
    if service.closed.load(Ordering::Acquire) {
        drop(registry);
        lifetime.finish(Err("execution domain is closed".into()));
        return lifetime;
    }
    let replaced = registry.insert(
        id,
        Entry {
            lifetime: lifetime.clone(),
            transport: None,
        },
    );
    drop(registry);
    if let Some(replaced) = replaced {
        replaced.lifetime.finish(Ok(()));
    }
    lifetime
}

fn remove(service: &TcpService, id: u64, outcome: Result<(), String>) {
    let removed = service.registry.lock().unwrap().remove(&id);
    if let Some(removed) = removed {
        removed.lifetime.finish(outcome);
    }
}

pub(crate) fn free_in(service: &TcpService, id: u64) {
    remove(service, id, Ok(()));
}

pub(crate) fn fail_in(service: &TcpService, id: u64, error: String) {
    remove(service, id, Err(error));
}

fn lifetime_in(service: &TcpService, id: u64) -> Result<Arc<Lifetime>, String> {
    service
        .registry
        .lock()
        .unwrap()
        .get(&id)
        .map(|entry| entry.lifetime.clone())
        .ok_or_else(|| "socket is closed".into())
}

fn publish_in(
    service: &TcpService,
    id: u64,
    owner: &Arc<Lifetime>,
    stream: Duplex,
) -> Result<(), String> {
    let mut transport = Some(Transport::new(stream));
    let published = {
        let mut registry = service.registry.lock().unwrap();
        if let Some(entry) = registry.get_mut(&id) {
            if !service.closed.load(Ordering::Acquire)
                && Arc::ptr_eq(owner, &entry.lifetime)
                && entry.transport.is_none()
            {
                entry.transport = transport.take();
                true
            } else {
                false
            }
        } else {
            false
        }
    };
    drop(transport);
    if published {
        Ok(())
    } else {
        Err("socket is closed".into())
    }
}

struct Opening {
    service: Arc<TcpService>,
    id: u64,
    completed: bool,
}

impl Drop for Opening {
    fn drop(&mut self) {
        if !self.completed {
            fail_in(
                &self.service,
                self.id,
                "socket opening was cancelled".into(),
            );
        }
    }
}

struct Upgrading(Arc<Lifetime>);

impl Drop for Upgrading {
    fn drop(&mut self) {
        self.0.finish(Err("TLS upgrade was cancelled".into()));
    }
}

struct UpgradeReservation {
    service: Arc<TcpService>,
    completed: bool,
    old: u64,
    new: u64,
}

impl Drop for UpgradeReservation {
    fn drop(&mut self) {
        if !self.completed {
            fail_in(&self.service, self.old, "TLS upgrade was cancelled".into());
            fail_in(
                &self.service,
                self.new,
                "socket opening was cancelled".into(),
            );
        }
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, Eq, PartialEq)]
pub(crate) struct SocketInfo {
    pub id: u64,
    #[serde(rename = "remoteAddress")]
    pub remote_address: Option<String>,
    #[serde(rename = "localAddress")]
    pub local_address: Option<String>,
}

#[derive(serde::Deserialize)]
pub(crate) struct ConnectArgs {
    pub hostname: String,
    pub port: u16,
    pub secure: bool,
    #[serde(default)]
    pub expected_host: Option<String>,
}

// Internal source conformance can inject a DER root without changing the
// production trust configuration or giving applications a verification bypass.
#[cfg(any(test, cellar_internal_tests))]
fn test_root() -> Arc<Mutex<Option<Vec<u8>>>> {
    crate::asyncrt::services().tcp().root.clone()
}

#[cfg(cellar_internal_tests)]
pub fn test_extra_tls_root(der: Vec<u8>) {
    *test_root().lock().unwrap() = Some(der);
}

fn tls_connector(service: &TcpService) -> tokio_rustls::TlsConnector {
    #[cfg(any(test, cellar_internal_tests))]
    {
        build_tls_connector(service.root.lock().unwrap().clone())
    }
    #[cfg(not(any(test, cellar_internal_tests)))]
    {
        service
            .connector
            .get_or_init(|| build_tls_connector(None))
            .clone()
    }
}

fn build_tls_connector(extra: Option<Vec<u8>>) -> tokio_rustls::TlsConnector {
    let mut roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    if let Some(extra) = extra {
        let _ = roots.add(rustls::pki_types::CertificateDer::from(extra));
    }
    tokio_rustls::TlsConnector::from(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}

fn unbracket(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host)
}

async fn handshake(service: &TcpService, stream: Duplex, host: String) -> Result<Duplex, String> {
    let name = rustls::pki_types::ServerName::try_from(unbracket(&host).to_owned())
        .map_err(|error| format!("invalid TLS server name: {error}"))?;
    let tls = tls_connector(service)
        .connect(name, stream)
        .await
        .map_err(|error| format!("TLS handshake failed: {error}"))?;
    Ok(Box::new(tls))
}

pub(crate) async fn connect_in(
    service: Arc<TcpService>,
    id: u64,
    request: ConnectArgs,
    gate: EgressGate,
) -> Result<SocketInfo, String> {
    let mut guard = Opening {
        service: service.clone(),
        id,
        completed: false,
    };
    let owner = lifetime_in(&service, id)?;
    let result = owner
        .run(async {
            await_egress_gate(gate).await?;
            let stream =
                tokio::net::TcpStream::connect((unbracket(&request.hostname), request.port))
                    .await
                    .map_err(|error| {
                        format!(
                            "connection to {}:{} failed: {error}",
                            request.hostname, request.port
                        )
                    })?;
            let info = SocketInfo {
                id,
                remote_address: stream.peer_addr().map(|address| address.to_string()).ok(),
                local_address: stream.local_addr().map(|address| address.to_string()).ok(),
            };
            let stream: Duplex = Box::new(stream);
            let stream = if request.secure {
                handshake(
                    &service,
                    stream,
                    request.expected_host.unwrap_or(request.hostname),
                )
                .await?
            } else {
                stream
            };
            publish_in(&service, id, &owner, stream)?;
            Ok(info)
        })
        .await;
    if let Err(error) = &result {
        fail_in(&service, id, error.clone());
    }
    if result.is_ok() {
        guard.completed = true;
    }
    result
}

pub(crate) async fn read_in(
    service: &TcpService,
    id: u64,
    limit: usize,
) -> Result<Vec<u8>, String> {
    let (owner, half) = {
        let registry = service.registry.lock().unwrap();
        let entry = registry.get(&id).ok_or("socket is closed")?;
        let transport = entry.transport.as_ref().ok_or("socket is not open")?;
        (entry.lifetime.clone(), transport.read.clone())
    };
    owner
        .run(async move {
            let mut guard = half.lock().await;
            let read = guard.as_mut().ok_or("socket is closed")?;
            let mut buffer = vec![0; limit.min(16 * 1024)];
            if buffer.is_empty() {
                return Ok(buffer);
            }
            let count = read
                .read(&mut buffer)
                .await
                .map_err(|error| format!("read failed: {error}"))?;
            buffer.truncate(count);
            Ok(buffer)
        })
        .await
}

pub(crate) async fn write_in(
    service: &TcpService,
    id: u64,
    bytes: Vec<u8>,
    gate: EgressGate,
) -> Result<(), String> {
    let (owner, half) = {
        let registry = service.registry.lock().unwrap();
        let entry = registry.get(&id).ok_or("socket is closed")?;
        let transport = entry.transport.as_ref().ok_or("socket is not open")?;
        (entry.lifetime.clone(), transport.write.clone())
    };
    owner
        .run(async move {
            await_egress_gate(gate).await?;
            let mut guard = half.lock().await;
            let write = guard.as_mut().ok_or("socket is closed")?;
            write
                .write_all(&bytes)
                .await
                .map_err(|error| format!("write failed: {error}"))?;
            write
                .flush()
                .await
                .map_err(|error| format!("flush failed: {error}"))
        })
        .await
}

pub(crate) async fn shutdown_in(
    service: &TcpService,
    id: u64,
    gate: EgressGate,
) -> Result<(), String> {
    let (owner, half) = {
        let registry = service.registry.lock().unwrap();
        let entry = registry.get(&id).ok_or("socket is closed")?;
        let transport = entry.transport.as_ref().ok_or("socket is not open")?;
        (entry.lifetime.clone(), transport.write.clone())
    };
    owner
        .run(async move {
            await_egress_gate(gate).await?;
            let mut guard = half.lock().await;
            guard
                .as_mut()
                .ok_or("socket is closed")?
                .shutdown()
                .await
                .map_err(|error| format!("shutdown failed: {error}"))
        })
        .await
}

pub(crate) async fn start_tls_in(
    service: Arc<TcpService>,
    old_id: u64,
    new_id: u64,
    host: String,
    gate: EgressGate,
) -> Result<SocketInfo, String> {
    let mut guard = UpgradeReservation {
        service: service.clone(),
        completed: false,
        old: old_id,
        new: new_id,
    };
    let owner = lifetime_in(&service, new_id)?;
    let result = owner
        .run(async {
            await_egress_gate(gate).await?;
            let old = service
                .registry
                .lock()
                .unwrap()
                .remove(&old_id)
                .ok_or("socket is closed")?;
            // Interrupt parked plaintext reads/writes before reuniting the halves.
            // No speculative plaintext read can block the TLS handshake.
            let upgrade = Upgrading(old.lifetime.clone());
            old.lifetime.operations.cancel();
            let outcome = async {
                let transport = old.transport.as_ref().ok_or("socket is not open")?;
                let read = transport
                    .read
                    .lock()
                    .await
                    .take()
                    .ok_or("socket is closed")?;
                let write = transport
                    .write
                    .lock()
                    .await
                    .take()
                    .ok_or("socket is closed")?;
                let stream = handshake(&service, read.unsplit(write), host).await?;
                publish_in(&service, new_id, &owner, stream)?;
                Ok(SocketInfo {
                    id: new_id,
                    remote_address: None,
                    local_address: None,
                })
            }
            .await;
            old.lifetime
                .finish(outcome.as_ref().map(|_| ()).map_err(Clone::clone));
            drop(upgrade);
            outcome
        })
        .await;
    if let Err(error) = &result {
        // STARTTLS consumes plaintext authority at call time. A refused proof
        // before the handshake must still settle and close the old socket.
        fail_in(&service, old_id, error.clone());
        fail_in(&service, new_id, error.clone());
    }
    if result.is_ok() {
        guard.completed = true;
    }
    result
}

#[cfg(test)]
pub(crate) mod tests;

#[cfg(any(test, cellar_internal_tests))]
fn registry() -> Arc<Mutex<HashMap<u64, Entry>>> {
    crate::asyncrt::services().tcp().registry.clone()
}

#[cfg(any(test, cellar_internal_tests))]
fn next_id() -> u64 {
    crate::asyncrt::services().tcp().next_id()
}

#[cfg(any(test, cellar_internal_tests))]
fn reserve(id: u64) -> Arc<Lifetime> {
    reserve_in(&crate::asyncrt::services().tcp(), id)
}

#[cfg(any(test, cellar_internal_tests))]
fn free(id: u64) {
    free_in(&crate::asyncrt::services().tcp(), id);
}

#[cfg(any(test, cellar_internal_tests))]
fn publish(id: u64, owner: &Arc<Lifetime>, stream: Duplex) -> Result<(), String> {
    publish_in(&crate::asyncrt::services().tcp(), id, owner, stream)
}

#[cfg(test)]
fn insert(id: u64, stream: Duplex) {
    let owner = reserve(id);
    publish(id, &owner, stream).unwrap();
}

#[cfg(any(test, cellar_internal_tests))]
fn connect(
    id: u64,
    args: ConnectArgs,
    gate: EgressGate,
) -> impl std::future::Future<Output = Result<SocketInfo, String>> {
    connect_in(crate::asyncrt::services().tcp(), id, args, gate)
}

#[cfg(any(test, cellar_internal_tests))]
fn start_tls(
    old: u64,
    new: u64,
    host: String,
    gate: EgressGate,
) -> impl std::future::Future<Output = Result<SocketInfo, String>> {
    start_tls_in(crate::asyncrt::services().tcp(), old, new, host, gate)
}

#[cfg(any(test, cellar_internal_tests))]
fn read(id: u64, limit: usize) -> impl std::future::Future<Output = Result<Vec<u8>, String>> {
    let service = crate::asyncrt::services().tcp();
    async move { read_in(&service, id, limit).await }
}

#[cfg(any(test, cellar_internal_tests))]
fn write(
    id: u64,
    bytes: Vec<u8>,
    gate: EgressGate,
) -> impl std::future::Future<Output = Result<(), String>> {
    let service = crate::asyncrt::services().tcp();
    async move { write_in(&service, id, bytes, gate).await }
}

#[cfg(any(test, cellar_internal_tests))]
fn shutdown(id: u64, gate: EgressGate) -> impl std::future::Future<Output = Result<(), String>> {
    let service = crate::asyncrt::services().tcp();
    async move { shutdown_in(&service, id, gate).await }
}
