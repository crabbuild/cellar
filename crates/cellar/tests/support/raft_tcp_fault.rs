// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fault injection between embedded nodes' real TLS listeners.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};

#[derive(Clone, Copy, Default)]
pub(super) struct Fault {
    pub(super) blocked: bool,
    pub(super) stopped: bool,
    pub(super) hold_response: bool,
    pub(super) hold_after: Option<usize>,
    pub(super) hold_once: bool,
}

pub(super) struct Proxy {
    pub(super) address: SocketAddr,
    pub(super) state: watch::Sender<Fault>,
    pub(super) active: Arc<AtomicUsize>,
    pub(super) attempts: Arc<AtomicUsize>,
    pub(super) held_response_bytes: Arc<AtomicUsize>,
    pub(super) held_connection: Arc<AtomicUsize>,
    task: Option<JoinHandle<Result<()>>>,
}

// This shared fixture is also compiled by targets which only cut connections.
#[allow(dead_code)]
pub struct HeldResponses(pub(super) watch::Sender<Fault>);

impl Drop for HeldResponses {
    fn drop(&mut self) {
        self.0.send_modify(|fault| {
            fault.hold_response = false;
            fault.hold_after = None;
            fault.hold_once = false;
        });
    }
}

async fn connection_cut(mut faults: watch::Receiver<Fault>) {
    loop {
        let fault = *faults.borrow_and_update();
        if fault.blocked || fault.stopped || faults.changed().await.is_err() {
            return;
        }
    }
}

async fn forward_responses(
    mut reader: impl AsyncRead + Unpin,
    mut writer: impl AsyncWrite + Unpin,
    mut faults: watch::Receiver<Fault>,
    held: Arc<AtomicUsize>,
    selected: Arc<AtomicUsize>,
    connection: usize,
) -> std::io::Result<()> {
    let mut buffer = [0; 16 * 1024];
    let mut forwarded = 0usize;
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            return Ok(());
        }
        let mut offset = 0;
        let mut counted = false;
        while offset < count {
            let mut fault = *faults.borrow_and_update();
            let chosen = selected.load(Ordering::SeqCst);
            if fault.hold_once && chosen != 0 && chosen != connection {
                fault.hold_response = false;
                fault.hold_after = None;
            }
            let allowance = if fault.hold_response {
                0
            } else {
                fault
                    .hold_after
                    .map_or(count - offset, |after| after.saturating_sub(forwarded))
            };
            if allowance == 0 {
                if fault.hold_once
                    && selected
                        .compare_exchange(0, connection, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err_and(|chosen| chosen != connection)
                {
                    continue;
                }
                if !counted {
                    held.fetch_add(count - offset, Ordering::SeqCst);
                    counted = true;
                }
                if faults.changed().await.is_err() {
                    return Ok(());
                }
                continue;
            }
            let end = count.min(offset.saturating_add(allowance));
            writer.write_all(&buffer[offset..end]).await?;
            forwarded += end - offset;
            offset = end;
        }
    }
}

struct Active(Arc<AtomicUsize>);

impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Proxy {
    pub(super) async fn start(destination: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (state, mut changes) = watch::channel(Fault::default());
        let active = Arc::new(AtomicUsize::new(0));
        let attempts = Arc::new(AtomicUsize::new(0));
        let held_response_bytes = Arc::new(AtomicUsize::new(0));
        let held_connection = Arc::new(AtomicUsize::new(0));
        let held = held_response_bytes.clone();
        let selected = held_connection.clone();
        let counted = active.clone();
        let attempted = attempts.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                cellar::asyncrt::select! {
                    changed = changes.changed() => {
                        if changed.is_err() || changes.borrow().stopped { break; }
                    }
                    done = connections.join_next(), if !connections.is_empty() => { if let Some(done) = done { done?; } }
                    accepted = listener.accept(), if connections.len() < 8 => {
                        let (mut source, _) = accepted?;
                        // Match the native transport: fault injection must not
                        // add Nagle/delayed-ACK stalls to small snapshot chunks.
                        source.set_nodelay(true)?;
                        let connection=attempted.fetch_add(1, Ordering::SeqCst)+1;
                        if changes.borrow().blocked { continue; }
                        let cut = changes.clone();
                        let held = held.clone();
                        let selected = selected.clone();
                        let active = counted.clone(); active.fetch_add(1, Ordering::SeqCst);
                        let guard = Active(active);
                        connections.spawn(async move {
                            let _guard = guard;
                            let fault = *cut.borrow();
                            if fault.blocked || fault.stopped { return; }
                            // A cut drops both sockets. A response hold instead
                            // retains one bounded ciphertext chunk until release
                            // or either connection direction closes.
                            cellar::asyncrt::select_biased! { "test fault transitions close sockets before more bytes are forwarded";
                                _ = connection_cut(cut.clone()) => (),
                                _ = async {
                                    let target = TcpStream::connect(destination).await?;
                                    target.set_nodelay(true)?;
                                    let (mut from_source, to_source) = source.split();
                                    let (from_target, mut to_target) = target.into_split();
                                    cellar::asyncrt::select! {
                                        _ = tokio::io::copy(&mut from_source, &mut to_target) => (),
                                        _ = forward_responses(from_target, to_source, cut, held, selected, connection) => (),
                                    }
                                    Ok::<_, std::io::Error>(())
                                } => (),
                            }
                        });
                    }
                }
            }
            connections.abort_all();
            while let Some(result) = connections.join_next().await {
                if let Err(error) = result {
                    ensure!(error.is_cancelled(), "proxy connection panicked: {error}");
                }
            }
            Ok(())
        });
        Ok(Self {
            address,
            state,
            active,
            attempts,
            held_response_bytes,
            held_connection,
            task: Some(task),
        })
    }

    pub(super) fn block(&self, blocked: bool) {
        self.state.send_if_modified(|fault| {
            let changed = fault.blocked != blocked;
            fault.blocked = blocked;
            changed
        });
    }

    pub(super) async fn close(&mut self) -> Result<()> {
        self.state.send_modify(|fault| fault.stopped = true);
        if let Some(task) = self.task.take() {
            task.await??;
        }
        Ok(())
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.state.send_modify(|fault| fault.stopped = true);
    }
}
