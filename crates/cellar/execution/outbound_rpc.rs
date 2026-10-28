// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Finite SDK submission ownership across gates, queues and native execution.
use crate::rpc_reservation::Reservation;
use crate::{engine_api::RpcData, runtime_policy::RuntimePolicy};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// Fixed transport control/frame allowance, charged separately from payload and
// identifiers. Response buffers and adapter/guest allocations have other budgets.
const CONTROL_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
/// Independently sampled SDK submission counters, not a process memory sample.
pub struct Usage {
    /// Requests whose last caller, dispatch, native-driver or input-byte owner remains.
    pub active_requests: usize,
    /// Dense request/copy/control allowance retained by those requests/byte aliases.
    pub reserved_request_bytes: usize,
}

pub(crate) struct Budget {
    calls: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    max_calls: usize,
    max_bytes: usize,
}

struct Held {
    _call: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

/// One admitted submission has known dense backing before any gate/queue wait.
pub(crate) struct Admitted {
    pub(crate) reservation: Reservation,
    pub(crate) scope: String,
    pub(crate) name: Option<String>,
    pub(crate) method: String,
    pub(crate) args: RpcData,
}

fn text(value: &str) -> Option<String> {
    let mut dense = String::new();
    dense.try_reserve_exact(value.len()).ok()?;
    dense.push_str(value);
    Some(dense)
}

fn dense_value(args: &RpcData, reservation: &Reservation) -> Option<RpcData> {
    match args {
        RpcData::Json(json) => Some(RpcData::Json(text(json)?)),
        RpcData::Structured(bytes) => {
            let mut data = Vec::new();
            data.try_reserve_exact(bytes.len()).ok()?;
            data.extend_from_slice(bytes);
            Some(RpcData::Structured(reservation.retain_bytes(data.into())))
        }
        RpcData::Leased { .. } => None,
    }
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

    pub(crate) fn admit(
        &self,
        scope: &str,
        name: Option<&str>,
        method: &str,
        args: &RpcData,
    ) -> Option<Admitted> {
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
        let reservation = Reservation::shared(Arc::new(Held {
            _call: call,
            _bytes: bytes,
        }));
        // Reserve before allocating any dense copies. Caller-owned unknown
        // backing is relinquished by the submission boundary before it waits.
        let args = match args {
            RpcData::Leased {
                value: inner,
                lease,
            } => RpcData::Leased {
                value: Box::new(dense_value(inner, &reservation)?),
                lease: lease.clone(),
            },
            args => dense_value(args, &reservation)?,
        };
        Some(Admitted {
            scope: text(scope)?,
            name: match name {
                Some(name) => Some(text(name)?),
                None => None,
            },
            method: text(method)?,
            args,
            reservation,
        })
    }

    pub(crate) fn usage(&self) -> Usage {
        Usage {
            active_requests: self.max_calls - self.calls.available_permits(),
            reserved_request_bytes: self.max_bytes - self.bytes.available_permits(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::rpc::Lease;

    #[test]
    fn normalization_preserves_lease_revocation_and_refuses_nested_leases() {
        let budget = Budget::new(RuntimePolicy::default());
        let lease = Lease::new();
        let args = RpcData::Json("null".into())
            .with_lease(lease.clone())
            .unwrap();
        let admitted = budget.admit("scope", None, "method", &args).unwrap();
        let RpcData::Leased {
            value,
            lease: normalized,
        } = &admitted.args
        else {
            panic!("source normalization lost its lease");
        };
        assert!(matches!(value.as_ref(), RpcData::Json(value) if value == "null"));
        assert!(!normalized.is_closed());
        lease.close();
        assert!(
            normalized.is_closed(),
            "normalization minted another lease lifetime"
        );
        drop(admitted);
        assert_eq!(budget.usage(), Usage::default());
        let nested = RpcData::Leased {
            value: Box::new(args),
            lease: Lease::new(),
        };
        assert!(budget.admit("scope", None, "method", &nested).is_none());
        assert_eq!(budget.usage(), Usage::default());
    }
}
