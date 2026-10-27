// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An opaque native-driver lifetime, independent of its source or peer quota.
use std::sync::Arc;

/// Shared admission ownership. Only host adapters can mint these tickets.
#[doc(hidden)]
#[derive(Clone)]
pub struct Reservation {
    _held: Arc<dyn Send + Sync>,
}

impl Reservation {
    pub(crate) fn shared<T: Send + Sync + 'static>(held: Arc<T>) -> Self {
        Self { _held: held }
    }
}
