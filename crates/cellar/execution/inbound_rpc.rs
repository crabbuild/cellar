// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Receiving RPC tunnels and request buffers share one execution-domain budget.
use crate::{application::rpc::Code, rpc_reservation::Reservation, runtime_policy::RuntimePolicy};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

const CONTROL_BYTES: usize = 64 * 1024;

/// Independently sampled receiving RPC quotas, not process/guest heap usage.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Usage {
    /// Authenticated receiving RPC connections still owned by I/O or native work.
    pub open_tunnels: usize,
    /// Peer names with at least one receiving connection; empty entries retire.
    pub peers: usize,
    /// Requests retained by upload, routing, native work or transport reply bytes.
    pub active_requests: usize,
    /// Dense input capacity, one possible copy and fixed control allowance.
    pub reserved_request_bytes: usize,
}

pub(crate) struct Budget(Arc<Inner>);

struct Inner {
    policy: RuntimePolicy,
    domain: crate::asyncrt::DomainToken,
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
    replies: Mutex<HashMap<usize, u64>>,
    changed: Notify,
}

#[derive(Clone)]
pub(crate) struct Request(Arc<Admitted>);

struct Admitted {
    connection: Connection,
    _call: OwnedSemaphorePermit,
    bytes: Mutex<OwnedSemaphorePermit>,
}

/// Shared ownership of a published reply, independent of native driver cleanup.
#[derive(Clone)]
pub(crate) struct Reply {
    _held: Arc<Published>,
}

struct Published {
    request: Request,
    key: usize,
}

impl Budget {
    pub(crate) fn new(policy: RuntimePolicy, domain: crate::asyncrt::DomainToken) -> Self {
        Self(Arc::new(Inner {
            policy,
            domain,
            calls: Arc::new(Semaphore::new(policy.max_inbound_rpc_requests)),
            bytes: Arc::new(Semaphore::new(policy.max_inbound_rpc_bytes)),
            tunnels: Arc::new(Semaphore::new(policy.max_inbound_rpc_tunnels)),
            peers: Mutex::new(HashMap::new()),
        }))
    }

    pub(crate) fn connect(&self, peer: &str) -> Option<Connection> {
        let now = self.0.domain.mono_ms().ok()?;
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
            touched: AtomicU64::new(now),
            replies: Mutex::new(HashMap::new()),
            changed: Notify::new(),
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
        // Transport buffers can retire outside their server's poll or after
        // shutdown. Use the original clock, never an ambient execution domain.
        // A closed owner has no live idle timer; reservation refund must still
        // complete without requiring that domain to be installed.
        if let Ok(now) = self.0.inner.domain.mono_ms() {
            self.0.touched.fetch_max(now, Ordering::SeqCst);
        }
        self.0.changed.notify_one();
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

    /// Active upload/native work keeps its own deadline. Published replies have
    /// a bounded write interval even after their bytes leave the Body object.
    pub(crate) async fn idle(&self) {
        let idle_ms = self.idle_timeout().as_millis() as u64;
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let Ok(now) = self.0.inner.domain.mono_ms() else {
                return;
            };
            let mut remaining = if self.0.active.load(Ordering::SeqCst) == 0 {
                self.0
                    .touched
                    .load(Ordering::SeqCst)
                    .saturating_add(idle_ms)
                    .saturating_sub(now)
            } else {
                idle_ms
            };
            if let Some(deadline) = self.0.replies.lock().unwrap().values().min().copied() {
                remaining = remaining.min(deadline.saturating_sub(now));
            }
            if remaining == 0 {
                return;
            }
            let Ok(wait) = self
                .0
                .inner
                .domain
                .sleep_until(now.saturating_add(remaining.min(1_000)))
            else {
                return;
            };
            crate::asyncrt::select_biased! { "a newly published reply can shorten the current transport wait";
                _ = &mut changed => {},
                _ = wait => {},
            }
        }
    }
}

impl Request {
    /// Start delivery only after the handler/proof result is ready. The map is
    /// bounded by request admission: each entry owns one distinct Request Arc.
    pub(crate) fn begin_reply(&self, caller_deadline: Option<u64>) -> Result<Reply, Code> {
        let connection = &self.0.connection;
        let now = connection
            .0
            .inner
            .domain
            .mono_ms()
            .map_err(|_| Code::Unavailable)?;
        let limit = now.saturating_add(connection.idle_timeout().as_millis() as u64);
        let deadline = caller_deadline.map_or(limit, |caller| caller.min(limit));
        if deadline <= now {
            return Err(Code::DeadlineExceeded);
        }
        // The published lease holds this Arc until its key is removed, so its
        // private allocation identity cannot be reused by another live reply.
        let key = Arc::as_ptr(&self.0) as usize;
        {
            let mut replies = connection.0.replies.lock().unwrap();
            let Entry::Vacant(entry) = replies.entry(key) else {
                return Err(Code::Unavailable);
            };
            entry.insert(deadline);
        }
        connection.0.changed.notify_one();
        Ok(Reply {
            _held: Arc::new(Published {
                request: self.clone(),
                key,
            }),
        })
    }

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

impl Drop for Published {
    fn drop(&mut self) {
        let connection = &self.request.0.connection;
        connection.0.replies.lock().unwrap().remove(&self.key);
        connection.0.changed.notify_one();
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
