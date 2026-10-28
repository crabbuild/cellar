// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An opaque native-driver lifetime, independent of its source or peer quota.
use std::sync::Arc;

/// Shared admission ownership. Only host adapters can mint these tickets.
#[doc(hidden)]
#[derive(Clone)]
pub struct Reservation {
    _held: Arc<dyn Send + Sync>,
    // Only the native typed client supplies this. Never derive caller limits
    // from untrusted response metadata or apply them to ordinary SDK values.
    reply_limit: Option<usize>,
}

struct RetainedBytes {
    data: bytes::Bytes,
    _reservation: Reservation,
}

impl AsRef<[u8]> for RetainedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl Reservation {
    pub(crate) fn shared<T: Send + Sync + 'static>(held: Arc<T>) -> Self {
        Self {
            _held: held,
            reply_limit: None,
        }
    }

    pub(crate) fn with_reply_limit(mut self, limit: Option<usize>) -> Self {
        self.reply_limit = limit;
        self
    }

    pub(crate) fn reply_limit(&self) -> Option<usize> {
        self.reply_limit
    }

    /// Transport copies and slices can outlive their request/control future.
    /// Retain admission until their last byte owner drops, even off-domain.
    pub(crate) fn retain_bytes(&self, data: bytes::Bytes) -> bytes::Bytes {
        bytes::Bytes::from_owner(RetainedBytes {
            data,
            _reservation: self.clone(),
        })
    }
}
