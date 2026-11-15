// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Invocation-owned staged parts and host-wide anonymous-file capacity.

use super::{Context, UploadedPart};
use anyhow::{ensure, Result};
use std::sync::{Arc, Mutex};

pub const PART_CHUNK_BYTES: usize = 64 * 1024;

/// Proven refusal before a part allocates scratch storage or submits I/O.
#[derive(Debug)]
pub struct PartRefused(pub(super) &'static str);

impl std::fmt::Display for PartRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for PartRefused {}

#[derive(Default)]
pub(crate) struct Budget(Mutex<Usage>);

#[derive(Default)]
struct Usage {
    bytes: usize,
    parts: usize,
}

impl Budget {
    pub(crate) fn reserve(
        self: &Arc<Self>,
        bytes: usize,
        maximum: usize,
        max_parts: usize,
    ) -> Result<Arc<dyn Send + Sync>> {
        let mut usage = self.0.lock().unwrap();
        let total = usage
            .bytes
            .checked_add(bytes)
            .filter(|total| *total <= maximum)
            .ok_or(PartRefused("Blob scratch capacity exhausted"))?;
        if usage.parts >= max_parts {
            return Err(PartRefused("Blob scratch file limit exhausted").into());
        }
        usage.bytes = total;
        usage.parts += 1;
        Ok(Arc::new(Reservation {
            budget: self.clone(),
            bytes,
        }))
    }
}

struct Reservation {
    budget: Arc<Budget>,
    bytes: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let mut usage = self.budget.0.lock().unwrap();
        usage.bytes -= self.bytes;
        usage.parts -= 1;
    }
}

/// A temporary part belongs to its original invocation. Its uploaded receipt
/// becomes visible only after exact-length completion. Cancellation or an
/// error during a write poisons the stage; discard it rather than replaying.
pub struct ResumablePart {
    inner: Option<Box<dyn crate::bucket::StreamedPart>>,
    authority: Option<crate::application::InvocationAuthority>,
    raft_authority: Option<crate::application::storage::RaftStorageAuthority>,
    number: u32,
    interrupted: bool,
}

impl ResumablePart {
    pub(super) fn new(
        context: &Context,
        number: u32,
        inner: Box<dyn crate::bucket::StreamedPart>,
    ) -> Result<Self> {
        Ok(Self {
            inner: Some(inner),
            authority: if context.raft_storage.is_none() {
                Some(context.invocation_authority()?)
            } else {
                None
            },
            raft_authority: context.raft_storage.clone(),
            number,
            interrupted: false,
        })
    }

    fn check(&self, context: &Context) -> Result<()> {
        if let Some(authority) = &self.authority {
            authority.check(context)?;
        }
        if let Some(authority) = &self.raft_authority {
            context.check_binding_turn()?;
            crate::application::Storage::raft_command(authority.clone()).check_raft_turn()?;
        }
        Ok(())
    }

    pub async fn write(&mut self, context: &Context, bytes: bytes::Bytes) -> Result<()> {
        self.check(context)?;
        ensure!(!self.interrupted, "part stage was interrupted; discard it");
        ensure!(bytes.len() <= PART_CHUNK_BYTES, "part chunk exceeds 64 KiB");
        if context.raft_storage.is_none() {
            crate::native_worker::blob_gate(context).await?;
        }
        self.check(context)?;
        self.interrupted = true;
        let mut inner = self
            .inner
            .take()
            .ok_or(PartRefused("part stage was interrupted"))?;
        let (inner, result) = super::raft::io(context, async move {
            let result = inner.write(bytes).await;
            Ok((inner, result))
        })
        .await?;
        self.inner = Some(inner);
        result?;
        self.check(context)?;
        self.interrupted = false;
        Ok(())
    }

    pub async fn complete(mut self, context: &Context) -> Result<UploadedPart> {
        self.check(context)?;
        ensure!(!self.interrupted, "part stage was interrupted; discard it");
        if context.raft_storage.is_none() {
            crate::native_worker::blob_gate(context).await?;
        }
        self.check(context)?;
        let inner = self
            .inner
            .take()
            .ok_or(PartRefused("part stage was interrupted"))?;
        let receipt = super::raft::io(context, async move { Ok(inner.complete().await?) }).await?;
        self.check(context)?;
        ensure!(
            !receipt.content_id.is_empty() && receipt.content_id.len() <= 16_384,
            "invalid part receipt"
        );
        Ok(UploadedPart {
            part_number: self.number,
            etag: receipt.content_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_capacity_follows_its_last_owner_and_rejects_overflow() -> Result<()> {
        let a = Arc::new(Budget::default());
        let b = Arc::new(Budget::default());
        let lease = a.reserve(3, 8, 1)?;
        let entered = lease.clone();
        drop(lease);
        assert!(a.reserve(6, 8, 1).err().unwrap().is::<PartRefused>());
        assert!(a.reserve(0, 8, 1).err().unwrap().is::<PartRefused>());
        let independent = b.reserve(8, 8, 1)?;
        drop(entered);
        let full = a.reserve(8, 8, 1)?;
        assert!(a.reserve(usize::MAX, usize::MAX, usize::MAX).is_err());
        drop((full, independent));
        assert_eq!(a.0.lock().unwrap().bytes, 0);
        assert_eq!(a.0.lock().unwrap().parts, 0);
        Ok(())
    }
}
