// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Host-owned RPC response buffers, including encoding and output-proof waits.
use crate::application::rpc::{Code, Failure};
use bytes::Bytes;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// Fixed owner/codec control allowance, separate from payload capacity. These
// counters describe owned allocation layouts/allowances, not malloc size or RSS.
const CONTROL_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Usage {
    /// Encoding/collection owners, including buffers retained by their aliases.
    pub active_buffers: usize,
    /// Payload capacity, possible copies/reallocation overlap and fixed controls.
    pub reserved_response_bytes: usize,
}

pub(crate) struct Budget(Arc<Inner>);

struct Inner {
    bytes: Arc<Semaphore>,
    maximum: usize,
    active: AtomicUsize,
}

pub(crate) struct Admission {
    pool: Arc<Inner>,
    bytes: OwnedSemaphorePermit,
}

/// Dense ownership releases unknown external backing before a proof/queue wait.
/// The permit lives in Bytes' owner, so a one-byte slice retains its full charge.
struct Owned {
    data: Vec<u8>,
    _admission: Admission,
}

impl AsRef<[u8]> for Owned {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl Budget {
    pub(crate) fn new(maximum: usize) -> Self {
        Self(Arc::new(Inner {
            bytes: Arc::new(Semaphore::new(maximum)),
            maximum,
            active: AtomicUsize::new(0),
        }))
    }

    pub(crate) fn reserve(&self, weight: usize) -> Result<Admission, Failure> {
        let bytes = acquire(&self.0, weight)?;
        self.0.active.fetch_add(1, Ordering::SeqCst);
        Ok(Admission {
            pool: self.0.clone(),
            bytes,
        })
    }

    pub(crate) fn usage(&self) -> Usage {
        Usage {
            active_buffers: self.0.active.load(Ordering::SeqCst),
            reserved_response_bytes: self.0.maximum - self.0.bytes.available_permits(),
        }
    }
}

fn exhausted() -> Failure {
    // A response can follow committed work. This never establishes rollback or
    // carries the transport tag which permits automatic activation retries.
    Failure::new(Code::ResourceExhausted)
}

fn acquire(pool: &Inner, weight: usize) -> Result<OwnedSemaphorePermit, Failure> {
    pool.bytes
        .clone()
        .try_acquire_many_owned(u32::try_from(weight).map_err(|_| exhausted())?)
        .map_err(|_| exhausted())
}

pub(crate) fn encoding_weight(limit: usize) -> Result<usize, Failure> {
    // Native response bytes, Protobuf control, clone encoding and dense output
    // normalization. Guest heaps/bridge copies retain their adapter admission.
    limit
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(CONTROL_BYTES))
        .ok_or_else(exhausted)
}

/// Ordinary JSON results use their collector's one-copy allowance for valid
/// UTF-8. Invalid bytes can expand threefold: reserve that additional ownership
/// before allocating an exact-capacity lossy conversion.
pub(crate) fn json(data: Bytes) -> Result<String, Failure> {
    if let Ok(text) = std::str::from_utf8(&data) {
        let mut output = String::new();
        output
            .try_reserve_exact(text.len())
            .map_err(|_| exhausted())?;
        output.push_str(text);
        return Ok(output);
    }
    let extra = data
        .len()
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(CONTROL_BYTES))
        .ok_or_else(exhausted)?;
    let _conversion = crate::asyncrt::services().reserve_rpc_response(extra)?;
    let maximum = data.len().checked_mul(3).ok_or_else(exhausted)?;
    let mut output = String::new();
    output.try_reserve_exact(maximum).map_err(|_| exhausted())?;
    let mut remaining = data.as_ref();
    loop {
        match std::str::from_utf8(remaining) {
            Ok(text) => {
                output.push_str(text);
                break;
            }
            Err(error) => {
                let valid = error.valid_up_to();
                // The UTF-8 validator proved this prefix; avoid unsafe indexing
                // or assuming an incomplete suffix has a fixed byte count.
                output.push_str(std::str::from_utf8(&remaining[..valid]).map_err(|_| exhausted())?);
                output.push('\u{fffd}');
                match error.error_len() {
                    Some(length) => remaining = &remaining[valid + length..],
                    None => break,
                }
            }
        }
    }
    Ok(output)
}

fn retained_weight(capacity: usize) -> Result<usize, Failure> {
    // Keep room for one business/transport copy while the original is retained.
    capacity
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(CONTROL_BYTES))
        .ok_or_else(exhausted)
}

impl Admission {
    fn resize(&mut self, weight: usize) -> Result<(), Failure> {
        let held = self.bytes.num_permits();
        if weight > held {
            self.bytes.merge(acquire(&self.pool, weight - held)?);
        } else if weight < held {
            // Refund only after the corresponding allocation/overlap retired.
            drop(self.bytes.split(held - weight));
        }
        Ok(())
    }

    fn finish(mut self, data: Vec<u8>) -> Result<Bytes, Failure> {
        self.resize(retained_weight(data.capacity())?)?;
        Ok(Bytes::from_owner(Owned {
            data,
            _admission: self,
        }))
    }

    /// Copy an application-owned view into known dense backing before retaining
    /// it as a host reply. A tiny slice must not pin an arbitrary large owner.
    pub(crate) fn normalize(mut self, data: Bytes) -> Result<Bytes, Failure> {
        self.resize(retained_weight(data.len())?)?;
        let mut dense = Vec::new();
        dense
            .try_reserve_exact(data.len())
            .map_err(|_| exhausted())?;
        dense.extend_from_slice(&data);
        drop(data);
        self.finish(dense)
    }
}

impl Drop for Admission {
    fn drop(&mut self) {
        self.pool.active.fetch_sub(1, Ordering::SeqCst);
        // OwnedSemaphorePermit refunds on any thread, even after domain close.
    }
}

/// Collector admission grows before allocations and accounts for realloc's
/// simultaneous old/new buffers. It never queues behind another response.
pub(crate) struct Buffer {
    data: Vec<u8>,
    admission: Admission,
    floor: usize,
}

impl Buffer {
    pub(crate) fn new(admission: Admission) -> Self {
        Self {
            data: Vec::new(),
            floor: admission.bytes.num_permits(),
            admission,
        }
    }

    pub(crate) fn initial_weight() -> usize {
        CONTROL_BYTES
    }

    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    /// Refuse a valid declared minimum before polling the body, without eagerly
    /// allocating. Keep its allowance through fragmented collection.
    pub(crate) fn anticipate(&mut self, length: usize) -> Result<(), Failure> {
        let weight = retained_weight(length)?.max(self.floor);
        self.admission.resize(weight)?;
        self.floor = weight;
        Ok(())
    }

    pub(crate) fn extend(&mut self, data: &[u8], limit: usize) -> Result<(), Failure> {
        let length = self
            .data
            .len()
            .checked_add(data.len())
            .ok_or_else(exhausted)?;
        if length > limit {
            return Err(exhausted());
        }
        if length > self.data.capacity() {
            let capacity = length
                .max(self.data.capacity().saturating_mul(2))
                .max(4096)
                .min(limit);
            let overlap = retained_weight(capacity)?
                .checked_add(self.data.capacity())
                .ok_or_else(exhausted)?;
            self.admission.resize(overlap.max(self.floor))?;
            self.data
                .try_reserve_exact(capacity - self.data.len())
                .map_err(|_| exhausted())?;
            self.admission
                .resize(retained_weight(self.data.capacity())?.max(self.floor))?;
        }
        self.data.extend_from_slice(data);
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Bytes, Failure> {
        self.admission.finish(self.data)
    }
}

#[cfg(test)]
#[path = "rpc_response_tests.rs"]
mod tests;
