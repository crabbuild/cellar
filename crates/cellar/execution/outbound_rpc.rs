// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Finite SDK submission ownership across gates, queues and native execution.
use crate::{engine_api::RpcData, runtime_policy::RuntimePolicy};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// Fixed transport control/frame allowance, charged separately from payload and
// identifiers. Response buffers and adapter/guest allocations have other budgets.
const CONTROL_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
/// Independently sampled SDK submission counters, not a process memory sample.
pub struct Usage {
    /// Requests whose last caller, dispatch or native-driver owner has not retired.
    pub active_requests: usize,
    /// Length-weighted request/copy/control allowance retained by those requests.
    pub reserved_request_bytes: usize,
}

pub(crate) struct Budget {
    calls: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    max_calls: usize,
    max_bytes: usize,
}

/// Opaque ownership carried by host dispatch and its local native driver.
/// Only the host can construct it; clones retain the same reservation.
#[doc(hidden)]
#[derive(Clone)]
pub struct Reservation {
    _held: Arc<Held>,
}

struct Held {
    _call: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

impl Budget {
    pub(crate) fn new(policy: RuntimePolicy) -> Self {
        Self {
            calls: Arc::new(Semaphore::new(policy.max_outbound_rpc_requests)),
            bytes: Arc::new(Semaphore::new(policy.max_outbound_rpc_bytes)),
            max_calls: policy.max_outbound_rpc_requests,
            max_bytes: policy.max_outbound_rpc_bytes,
        }
    }

    pub(crate) fn reserve(
        &self,
        scope: &str,
        name: Option<&str>,
        method: &str,
        args: &RpcData,
    ) -> Option<Reservation> {
        let metadata = scope
            .len()
            .checked_add(name.map_or(0, str::len))?
            .checked_add(method.len())?;
        let mut value = args;
        // Leased values are bounded in the wire protocol. Do not recursively
        // inspect a native caller's manually constructed nested value here.
        let mut depth = 0;
        let payload = loop {
            match value {
                RpcData::Json(value) => break value.len(),
                RpcData::Structured(value) => break value.len(),
                RpcData::Leased { value: inner, .. } => {
                    depth += 1;
                    if depth > 1 {
                        return None;
                    }
                    value = inner;
                }
            }
        };
        let weight = payload
            .checked_mul(2)?
            .checked_add(metadata.checked_mul(4)?)?
            .checked_add(CONTROL_BYTES)?;
        let call = self.calls.clone().try_acquire_owned().ok()?;
        let bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(u32::try_from(weight).ok()?)
            .ok()?;
        Some(Reservation {
            _held: Arc::new(Held {
                _call: call,
                _bytes: bytes,
            }),
        })
    }

    pub(crate) fn usage(&self) -> Usage {
        Usage {
            active_requests: self.max_calls - self.calls.available_permits(),
            reserved_request_bytes: self.max_bytes - self.bytes.available_permits(),
        }
    }
}
