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
}
