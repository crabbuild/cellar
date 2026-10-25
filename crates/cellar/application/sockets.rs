// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Event-owned TCP/TLS sockets with leased byte endpoints and STARTTLS.
//!
//! An echo exchange with a peer that replies after the write side closes:
//! ```no_run
//! use cellar::application::{Context, sockets::SocketOptions};
//! async fn exchange(context: &Context, address: &str) -> anyhow::Result<Vec<u8>> {
//!     let socket = context.connect(address, SocketOptions::default())?;
//!     socket.opened().await?;
//!     let writer = socket.writable.get_writer()?;
//!     writer.write(context, b"hello".to_vec()).await?;
//!     writer.close(context).await?;
//!     writer.release_lock();
//!     let reader = socket.readable.get_reader()?;
//!     let mut response = Vec::new();
//!     loop {
//!         let chunk = reader.read(context).await?;
//!         if chunk.is_empty() { break; }
//!         response.extend(chunk);
//!     }
//!     socket.closed().await?;
//!     Ok(response)
//! }
//! ```
use super::{Context, HandlerFuture};
use crate::{host_gates, host_tcp as host, worker_config::EgressPolicy};
use anyhow::{anyhow, Result};
use futures_util::future::{BoxFuture, FutureExt, Shared};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, Weak,
};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SocketAddress {
    pub hostname: String,
    pub port: u16,
}

impl SocketAddress {
    pub fn new(hostname: impl Into<String>, port: u16) -> Result<Self> {
        let address = Self {
            hostname: hostname.into(),
            port,
        };
        address.validate()?;
        Ok(address)
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            !self.hostname.is_empty(),
            "Specified address is missing hostname."
        );
        validate_joined(&format!("{}:{}", self.hostname, self.port))
    }
}

fn validate_joined(address: &str) -> Result<()> {
    anyhow::ensure!(
        !address.is_empty()
            && address.len() <= 255
            && address
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b".-_[]:".contains(&byte)),
        "Specified address is empty string, contains unsupported characters or is too long."
    );
    Ok(())
}

impl TryFrom<&str> for SocketAddress {
    type Error = anyhow::Error;

    fn try_from(address: &str) -> Result<Self> {
        let parsed = url::Url::parse(&format!("fake://{address}"))
            .map_err(|_| anyhow!("Specified address could not be parsed."))?;
        let hostname = parsed
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or_else(|| anyhow!("Specified address is missing hostname."))?;
        let port = parsed
            .port()
            .ok_or_else(|| anyhow!("Specified address is missing port."))?;
        validate_joined(address)?;
        Ok(Self {
            hostname: hostname.to_owned(),
            port,
        })
    }
}

impl TryFrom<String> for SocketAddress {
    type Error = anyhow::Error;

    fn try_from(address: String) -> Result<Self> {
        Self::try_from(address.as_str())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SecureTransport {
    #[default]
    Off,
    On,
    StartTls,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SocketOptions {
    pub secure_transport: SecureTransport,
    pub allow_half_open: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SocketInfo {
    pub remote_address: Option<String>,
    pub local_address: Option<String>,
}

type Opening = Shared<BoxFuture<'static, std::result::Result<host::SocketInfo, String>>>;

#[derive(Default)]
struct Flags {
    closing: bool,
    detached: bool,
    reader: bool,
    writer: bool,
    read_ended: bool,
    write_ended: bool,
    read_error: Option<String>,
    write_error: Option<String>,
}

struct State {
    service: Arc<host::TcpService>,
    id: u64,
    owner: Weak<crate::host_io::IoContext>,
    opening: Opening,
    lifetime: Arc<host::Lifetime>,
    address: SocketAddress,
    options: SocketOptions,
    flags: Mutex<Flags>,
    read_end: CancellationToken,
    write_end: CancellationToken,
    read_tail: Mutex<Option<Shared<BoxFuture<'static, ()>>>>,
    write_tail: Mutex<Option<Shared<BoxFuture<'static, ()>>>>,
}

impl State {
    fn end_read(&self, error: Option<String>) {
        {
            let mut flags = self.flags.lock().unwrap();
            flags.read_ended = true;
            if flags.read_error.is_none() {
                flags.read_error = error;
            }
        }
        self.read_end.cancel();
    }

    fn check(&self, context: &Context) -> Result<()> {
        context.check_turn()?;
        anyhow::ensure!(
            self.owner
                .upgrade()
                .is_some_and(|owner| Arc::ptr_eq(&owner, &context.io)),
            "TCP socket belongs to another or retired event"
        );
        Ok(())
    }
}

impl Drop for State {
    fn drop(&mut self) {
        host::free_in(&self.service, self.id);
    }
}

#[derive(Clone)]
pub struct Socket {
    state: Arc<State>,
    pub readable: Readable,
    pub writable: Writable,
}

#[derive(Clone)]
pub struct Readable(Arc<State>);

#[derive(Clone)]
pub struct Writable(Arc<State>);

struct ReadLease {
    state: Weak<State>,
    released: CancellationToken,
    released_once: AtomicBool,
}

impl Drop for ReadLease {
    fn drop(&mut self) {
        self.release();
    }
}

impl ReadLease {
    fn release(&self) {
        if self.released_once.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(state) = self.state.upgrade() {
            state.flags.lock().unwrap().reader = false;
        }
        self.released.cancel();
    }
}

pub struct Reader {
    state: Arc<State>,
    lease: Arc<ReadLease>,
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.lease.release();
    }
}

pub struct Writer {
    state: Arc<State>,
    released: AtomicBool,
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.release_lock();
    }
}

struct Done(Option<oneshot::Sender<()>>);

impl Drop for Done {
    fn drop(&mut self) {
        if let Some(done) = self.0.take() {
            let _ = done.send(());
        }
    }
}

fn ready<T: Send + 'static>(error: anyhow::Error) -> HandlerFuture<T> {
    Box::pin(async move { Err(error) })
}

/// Operations are driven by the original event without retaining residency.
/// The answer retains its handle, and abandoning the event drops pending I/O.
fn schedule<T: Send + 'static>(
    context: &Context,
    state: Arc<State>,
    future: impl std::future::Future<Output = Result<T>> + Send + 'static,
) -> HandlerFuture<T> {
    let (answer, receive) = oneshot::channel();
    context
        .state
        .unrefed
        .lock()
        .unwrap()
        .push(Box::pin(async move {
            let result = future.await;
            let _ = answer.send(result);
            Ok(())
        }));
    let context = context.clone();
    Box::pin(async move {
        state.check(&context)?;
        let result = receive
            .await
            .map_err(|_| anyhow!("TCP operation was cancelled"))?;
        state.check(&context)?;
        result
    })
}

fn socket(
    context: &Context,
    id: u64,
    address: SocketAddress,
    options: SocketOptions,
    lifetime: Arc<host::Lifetime>,
    operation: BoxFuture<'static, std::result::Result<host::SocketInfo, String>>,
) -> Socket {
    // The event owns transport work. Handles share only its result receiver;
    // retaining a handle must not retain a suspended dial/TLS handshake after
    // the event retires and drops its pending operations.
    let (answer, receive) = oneshot::channel();
    let opening = async move {
        receive
            .await
            .unwrap_or_else(|_| Err("socket opening was cancelled".into()))
    }
    .boxed()
    .shared();
    let state = Arc::new(State {
        service: context.io.host_services().tcp(),
        id,
        owner: Arc::downgrade(&context.io),
        opening,
        lifetime,
        address,
        options,
        flags: Mutex::new(Flags::default()),
        read_end: CancellationToken::new(),
        write_end: CancellationToken::new(),
        read_tail: Mutex::new(None),
        write_tail: Mutex::new(None),
    });
    context
        .state
        .unrefed
        .lock()
        .unwrap()
        .push(Box::pin(async move {
            let _ = answer.send(operation.await);
            Ok(())
        }));
    Socket {
        readable: Readable(state.clone()),
        writable: Writable(state.clone()),
        state,
    }
}

impl Context {
    /// Open an event-owned socket through the configured outbound authority.
    /// The opening begins without an explicit `opened` wait; no reads run ahead
    /// of the application. This policy does not sandbox arbitrary Rust I/O.
    pub fn connect<A>(&self, address: A, options: SocketOptions) -> Result<Socket>
    where
        A: TryInto<SocketAddress>,
        A::Error: std::fmt::Display,
    {
        self.check_turn()?;
        let address = address
            .try_into()
            .map_err(|error| anyhow!(error.to_string()))?;
        address.validate()?;
        match self.worker_config.egress {
            EgressPolicy::Deny => anyhow::bail!(
                "This worker is not permitted to access the internet via global functions."
            ),
            EgressPolicy::Broker(_) => {
                anyhow::bail!("A globalOutbound Fetcher cannot broker connect() in cellar.")
            }
            EgressPolicy::Allow => {}
        }
        anyhow::ensure!(
            self.worker_config.native_outbound_broker.is_none(),
            "A globalOutbound Fetcher cannot broker connect() in cellar."
        );
        self.connect_direct(address, options)
    }

    // Only a validated, cell-owned container capability may bypass global egress.
    pub(super) fn connect_direct(
        &self,
        address: SocketAddress,
        options: SocketOptions,
    ) -> Result<Socket> {
        self.check_turn()?;
        address.validate()?;
        let service = self.io.host_services().tcp();
        let id = service.next_id();
        let lifetime = host::reserve_in(&service, id);
        self.io.tcp_sockets.lock().unwrap().push(id);
        let gate = host_gates::egress_gate_request(&self.io, cellar_logic::Channel::Tcp);
        let request = host::ConnectArgs {
            hostname: address.hostname.clone(),
            port: address.port,
            secure: options.secure_transport == SecureTransport::On,
            expected_host: None,
        };
        let opening = host::connect_in(service, id, request, gate).boxed();
        Ok(socket(self, id, address, options, lifetime, opening))
    }
}

impl Socket {
    pub fn secure_transport(&self) -> SecureTransport {
        self.state.options.secure_transport
    }

    /// Observe the opening result without borrowing transport authority.
    /// The result remains available after the original event retires.
    pub fn opened(&self) -> HandlerFuture<SocketInfo> {
        let state = self.state.clone();
        Box::pin(async move {
            let info = state.opening.clone().await.map_err(anyhow::Error::msg)?;
            Ok(SocketInfo {
                remote_address: info.remote_address,
                local_address: info.local_address,
            })
        })
    }

    /// A completion value, with no transport authority or event retention.
    pub fn closed(&self) -> HandlerFuture<()> {
        let lifetime = self.state.lifetime.clone();
        Box::pin(async move { lifetime.closed().await.map_err(anyhow::Error::msg) })
    }

    pub fn close(&self, context: &Context) -> HandlerFuture<()> {
        if let Err(error) = self.state.check(context) {
            return ready(error);
        }
        self.state.flags.lock().unwrap().closing = true;
        let state = self.state.clone();
        let gate = host_gates::egress_gate_request(&context.io, cellar_logic::Channel::Tcp);
        schedule(context, state.clone(), async move {
            let result = host_gates::await_egress_gate(gate).await;
            host::free_in(&state.service, state.id);
            result.map_err(anyhow::Error::msg)
        })
    }

    /// Consume plaintext access once. The upgraded socket is a new handle;
    /// queued/pending plaintext operations are interrupted by the shared host.
    pub fn start_tls(
        &self,
        context: &Context,
        expected_server_hostname: Option<String>,
    ) -> Result<Socket> {
        self.state.check(context)?;
        {
            let mut flags = self.state.flags.lock().unwrap();
            anyhow::ensure!(
                self.secure_transport() != SecureTransport::On,
                "Cannot startTls on a TLS socket."
            );
            anyhow::ensure!(self.secure_transport() == SecureTransport::StartTls, "The `secureTransport` socket option must be set to 'starttls' for startTls to be used.");
            anyhow::ensure!(
                !flags.closing,
                "The connection was closed before startTls could be started."
            );
            anyhow::ensure!(!flags.detached, "startTls can only be called once.");
            flags.detached = true;
        }
        let service = self.state.service.clone();
        let id = service.next_id();
        let lifetime = host::reserve_in(&service, id);
        context.io.tcp_sockets.lock().unwrap().push(id);
        let gate = host_gates::egress_gate_request(&context.io, cellar_logic::Channel::Tcp);
        let old = self.state.clone();
        let host_name = expected_server_hostname.unwrap_or_else(|| old.address.hostname.clone());
        let opening = async move {
            let result = async {
                old.opening.clone().await?;
                host::start_tls_in(service.clone(), old.id, id, host_name, gate).await
            }
            .await;
            if let Err(error) = &result {
                host::fail_in(&service, id, error.clone());
            }
            result
        }
        .boxed();
        Ok(socket(
            context,
            id,
            self.state.address.clone(),
            SocketOptions {
                secure_transport: SecureTransport::On,
                ..self.state.options
            },
            lifetime,
            opening,
        ))
    }
}

impl Readable {
    pub fn get_reader(&self) -> Result<Reader> {
        let mut flags = self.0.flags.lock().unwrap();
        anyhow::ensure!(!flags.reader, "TCP readable is locked");
        flags.reader = true;
        Ok(Reader {
            state: self.0.clone(),
            lease: Arc::new(ReadLease {
                state: Arc::downgrade(&self.0),
                released: CancellationToken::new(),
                released_once: AtomicBool::new(false),
            }),
        })
    }
}

impl Reader {
    pub fn release_lock(&self) {
        self.lease.release();
    }

    /// Accept an ordered read of at most 16 KiB. Empty bytes indicate EOF.
    /// Multiple reads may be queued and their answers awaited in any order.
    pub fn read(&self, context: &Context) -> HandlerFuture<Vec<u8>> {
        self.read_limit(context, 16 * 1024)
    }

    fn read_limit(&self, context: &Context, limit: usize) -> HandlerFuture<Vec<u8>> {
        let prepared = (|| -> Result<_> {
            self.state.check(context)?;
            anyhow::ensure!(
                !self.lease.released.is_cancelled(),
                "TCP reader lock was released"
            );
            let (done, receive) = oneshot::channel();
            let next = async {
                let _ = receive.await;
            }
            .boxed()
            .shared();
            let previous = self.state.read_tail.lock().unwrap().replace(next);
            Ok((previous, Done(Some(done))))
        })();
        let (previous, done) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => return ready(error),
        };
        let state = self.state.clone();
        let lease = self.lease.clone();
        let caller = context.clone();
        schedule(context, state.clone(), async move {
            let _done = done;
            let read = async {
                if let Some(previous) = previous {
                    previous.await;
                }
                state.opening.clone().await.map_err(anyhow::Error::msg)?;
                state.check(&caller)?;
                let (ended, error) = {
                    let flags = state.flags.lock().unwrap();
                    (
                        flags.read_ended || flags.closing || flags.detached,
                        flags.read_error.clone(),
                    )
                };
                if let Some(error) = error {
                    return Err(anyhow!(error));
                }
                if ended {
                    state.end_read(None);
                    return Ok(Vec::new());
                }
                let bytes = match host::read_in(&state.service, state.id, limit).await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        let ending = {
                            let flags = state.flags.lock().unwrap();
                            flags.closing || flags.detached
                        };
                        if ending {
                            state.end_read(None);
                            return Ok(Vec::new());
                        }
                        state.end_read(Some(error.clone()));
                        host::fail_in(&state.service, state.id, error.clone());
                        return Err(anyhow!(error));
                    }
                };
                if bytes.is_empty() && limit != 0 {
                    state.end_read(None);
                    if !state.options.allow_half_open {
                        if !state.flags.lock().unwrap().writer {
                            let gate = host_gates::egress_gate_request(
                                &caller.io,
                                cellar_logic::Channel::Tcp,
                            );
                            let _ = host::shutdown_in(&state.service, state.id, gate).await;
                        }
                        state.flags.lock().unwrap().closing = true;
                        host::free_in(&state.service, state.id);
                    }
                }
                Ok(bytes)
            };
            crate::asyncrt::select_biased! { "releasing a reader refuses its undelivered read";
                _ = lease.released.cancelled() => Err(anyhow!("TCP reader lock was released")),
                result = read => result,
            }
        })
    }

    /// Copy at most 16 KiB into caller storage; an empty buffer does not signal EOF.
    pub async fn read_into(&self, context: &Context, buffer: &mut [u8]) -> Result<usize> {
        let bytes = self.read_limit(context, buffer.len()).await?;
        buffer[..bytes.len()].copy_from_slice(&bytes);
        Ok(bytes.len())
    }

    pub fn closed(&self) -> HandlerFuture<()> {
        let state = self.state.clone();
        let lease = self.lease.clone();
        Box::pin(async move {
            crate::asyncrt::select_biased! { "reader release wins over an undelivered stream closure";
                _ = lease.released.cancelled() => Err(anyhow!("TCP reader lock was released")),
                _ = state.read_end.cancelled() => match &state.flags.lock().unwrap().read_error {
                    Some(error) => Err(anyhow!(error.clone())),
                    None => Ok(()),
                },
                result = state.lifetime.closed() => result.map_err(anyhow::Error::msg),
            }
        })
    }
}

impl Writable {
    pub fn get_writer(&self) -> Result<Writer> {
        let mut flags = self.0.flags.lock().unwrap();
        anyhow::ensure!(!flags.writer, "TCP writable is locked");
        flags.writer = true;
        Ok(Writer {
            state: self.0.clone(),
            released: AtomicBool::new(false),
        })
    }
}

impl Writer {
    pub fn release_lock(&self) {
        if !self.released.swap(true, Ordering::AcqRel) {
            self.state.flags.lock().unwrap().writer = false;
        }
    }

    fn enqueue(&self, context: &Context, bytes: Option<Vec<u8>>) -> HandlerFuture<()> {
        let prepared = (|| -> Result<_> {
            self.state.check(context)?;
            anyhow::ensure!(
                !self.released.load(Ordering::Acquire),
                "TCP writer lock was released"
            );
            let mut flags = self.state.flags.lock().unwrap();
            anyhow::ensure!(!flags.detached, "This socket was upgraded.");
            anyhow::ensure!(
                !flags.closing && !flags.write_ended,
                "TCP writable is closed"
            );
            if bytes.is_none() {
                flags.write_ended = true;
            }
            if let Some(error) = &flags.write_error {
                anyhow::bail!(error.clone());
            }
            let gate = host_gates::egress_gate_request(&context.io, cellar_logic::Channel::Tcp);
            let (done, receive) = oneshot::channel();
            let next = async {
                let _ = receive.await;
            }
            .boxed()
            .shared();
            let previous = self.state.write_tail.lock().unwrap().replace(next);
            Ok((gate, previous, Done(Some(done))))
        })();
        let (gate, previous, done) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => return ready(error),
        };
        let state = self.state.clone();
        let caller = context.clone();
        schedule(context, state.clone(), async move {
            let _done = done;
            if let Some(previous) = previous {
                previous.await;
            }
            state.opening.clone().await.map_err(anyhow::Error::msg)?;
            state.check(&caller)?;
            anyhow::ensure!(
                !state.flags.lock().unwrap().detached,
                "This socket was upgraded."
            );
            if let Some(error) = &state.flags.lock().unwrap().write_error {
                anyhow::bail!(error.clone());
            }
            let result = match bytes {
                Some(bytes) => host::write_in(&state.service, state.id, bytes, gate)
                    .await
                    .map_err(anyhow::Error::msg),
                None => {
                    let result = host::shutdown_in(&state.service, state.id, gate)
                        .await
                        .map_err(anyhow::Error::msg);
                    if result.is_ok() {
                        state.write_end.cancel();
                    }
                    result
                }
            };
            if let Err(error) = &result {
                state.flags.lock().unwrap().write_error = Some(error.to_string());
                state.write_end.cancel();
                host::fail_in(&state.service, state.id, error.to_string());
            }
            result
        })
    }

    /// Writes are accepted and ordered at call time, even when their answers
    /// are awaited in a different order or a writer lock is released.
    pub fn write(&self, context: &Context, bytes: impl Into<Vec<u8>>) -> HandlerFuture<()> {
        self.enqueue(context, Some(bytes.into()))
    }

    pub fn close(&self, context: &Context) -> HandlerFuture<()> {
        self.enqueue(context, None)
    }

    pub fn closed(&self) -> HandlerFuture<()> {
        let state = self.state.clone();
        Box::pin(async move {
            crate::asyncrt::select_biased! { "stream shutdown settles before event socket cleanup";
                _ = state.write_end.cancelled() => match &state.flags.lock().unwrap().write_error { Some(error) => Err(anyhow!(error.clone())), None => Ok(()) },
                result = state.lifetime.closed() => result.map_err(anyhow::Error::msg),
            }
        })
    }
}
