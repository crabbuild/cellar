// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Receiving RPC tunnels and request buffers share one execution-domain budget.
use crate::{rpc_reservation::Reservation, runtime_policy::RuntimePolicy};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const CONTROL_BYTES: usize = 64 * 1024;

/// Independently sampled receiving RPC quotas, not process/guest heap usage.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Usage {
    /// Authenticated receiving RPC connections still owned by I/O or native work.
    pub open_tunnels: usize,
    /// Peer names with at least one receiving connection; empty entries retire.
    pub peers: usize,
    /// Requests retained by upload, routing, a native driver or a reply body.
    pub active_requests: usize,
    /// Dense input capacity, one possible copy and fixed control allowance.
    pub reserved_request_bytes: usize,
}

pub(crate) struct Budget(Arc<Inner>);

struct Inner {
    policy: RuntimePolicy,
    calls: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    tunnels: Arc<Semaphore>,
    peers: Mutex<HashMap<String, usize>>,
}

#[derive(Clone)]
pub(crate) struct Connection(Arc<Connected>);

struct Connected {
    inner: Arc<Inner>,
    peer: String,
    _tunnel: OwnedSemaphorePermit,
    active: AtomicUsize,
    touched: AtomicU64,
}

#[derive(Clone)]
pub(crate) struct Request(Arc<Admitted>);

struct Admitted {
    connection: Connection,
    _call: OwnedSemaphorePermit,
    bytes: Mutex<OwnedSemaphorePermit>,
}

impl Budget {
    pub(crate) fn new(policy: RuntimePolicy) -> Self {
        Self(Arc::new(Inner {
            policy,
            calls: Arc::new(Semaphore::new(policy.max_inbound_rpc_requests)),
            bytes: Arc::new(Semaphore::new(policy.max_inbound_rpc_bytes)),
            tunnels: Arc::new(Semaphore::new(policy.max_inbound_rpc_tunnels)),
            peers: Mutex::new(HashMap::new()),
        }))
    }

    pub(crate) fn connect(&self, peer: &str) -> Option<Connection> {
        let tunnel = self.0.tunnels.clone().try_acquire_owned().ok()?;
        let mut peers = self.0.peers.lock().unwrap();
        if peers.get(peer).copied().unwrap_or(0) >= self.0.policy.max_rpc_tunnels_per_peer {
            return None;
        }
        *peers.entry(peer.into()).or_default() += 1;
        Some(Connection(Arc::new(Connected {
            inner: self.0.clone(),
            peer: peer.into(),
            _tunnel: tunnel,
            active: AtomicUsize::new(0),
            touched: AtomicU64::new(crate::asyncrt::mono_ms()),
        })))
    }

    pub(crate) fn usage(&self) -> Usage {
        Usage {
            open_tunnels: self.0.policy.max_inbound_rpc_tunnels
                - self.0.tunnels.available_permits(),
            peers: self.0.peers.lock().unwrap().len(),
            active_requests: self.0.policy.max_inbound_rpc_requests
                - self.0.calls.available_permits(),
            reserved_request_bytes: self.0.policy.max_inbound_rpc_bytes
                - self.0.bytes.available_permits(),
        }
    }
}

impl Connection {
    pub(crate) fn touch(&self) {
        self.0
            .touched
            .store(crate::asyncrt::mono_ms(), Ordering::SeqCst);
    }

    pub(crate) fn request(&self) -> Option<Request> {
        let call = self.0.inner.calls.clone().try_acquire_owned().ok()?;
        let bytes = self
            .0
            .inner
            .bytes
            .clone()
            .try_acquire_many_owned(CONTROL_BYTES as u32)
            .ok()?;
        self.0.active.fetch_add(1, Ordering::SeqCst);
        self.touch();
        Some(Request(Arc::new(Admitted {
            connection: self.clone(),
            _call: call,
            bytes: Mutex::new(bytes),
        })))
    }

    pub(crate) fn idle_timeout(&self) -> Duration {
        self.0.inner.policy.rpc_tunnel_idle_timeout
    }

    /// Active uploads, native work and replies retain their connection. An idle
    /// peer cannot pin its slots indefinitely by leaving pooled tunnels open.
    pub(crate) async fn idle(&self) {
        let idle_ms = self.idle_timeout().as_millis() as u64;
        loop {
            let remaining = if self.0.active.load(Ordering::SeqCst) == 0 {
                self.0
                    .touched
                    .load(Ordering::SeqCst)
                    .saturating_add(idle_ms)
                    .saturating_sub(crate::asyncrt::mono_ms())
            } else {
                idle_ms
            };
            if remaining == 0 {
                return;
            }
            crate::asyncrt::sleep(Duration::from_millis(remaining.min(1_000))).await;
        }
    }
}

impl Request {
    /// Reserve dense input capacity plus its possible native/JSON copy before
    /// growing the buffer. One merged permit avoids per-frame bookkeeping lists.
    pub(crate) fn buffer(&self, capacity: usize) -> Option<()> {
        let target = capacity.checked_mul(2)?.checked_add(CONTROL_BYTES)?;
        let mut held = self.0.bytes.lock().unwrap();
        if target > held.num_permits() {
            let additional = u32::try_from(target - held.num_permits()).ok()?;
            held.merge(
                self.0
                    .connection
                    .0
                    .inner
                    .bytes
                    .clone()
                    .try_acquire_many_owned(additional)
                    .ok()?,
            );
        }
        Some(())
    }

    pub(crate) fn ticket(&self) -> Reservation {
        Reservation::shared(self.0.clone())
    }
}

impl Drop for Admitted {
    fn drop(&mut self) {
        self.connection.touch();
        self.connection.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Drop for Connected {
    fn drop(&mut self) {
        let mut peers = self.inner.peers.lock().unwrap();
        if let Some(count) = peers.get_mut(&self.peer) {
            *count -= 1;
            if *count == 0 {
                peers.remove(&self.peer);
            }
        }
    }
}

#[cfg(test)]
#[path = "inbound_rpc_tests.rs"]
mod tests;
