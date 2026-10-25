// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Local receipt composition. Its guarantee lasts while receipts are retained.

use super::*;
use crate::application::event_stream::Record;

pub struct Inbox {
    storage: Storage,
    processing_scope: String,
}

impl Storage {
    pub fn inbox(&self, processing_scope: &str) -> Result<Inbox> {
        self.scope()?;
        anyhow::ensure!(
            !processing_scope.is_empty() && processing_scope.len() <= 128,
            "invalid inbox processing scope"
        );
        Ok(Inbox {
            storage: self.clone(),
            processing_scope: processing_scope.into(),
        })
    }
}

impl Inbox {
    /// Record the original event bytes in the caller's business transaction.
    /// A matching retained receipt returns false; conflicting bytes fail.
    pub fn accept(&self, event: &Record) -> Result<bool> {
        let scope = self.storage.scope()?;
        anyhow::ensure!(
            crate::storage::facet_opening(scope).is_none(),
            "facets cannot retain durable inbox receipts"
        );
        anyhow::ensure!(
            serde_json::from_slice::<serde_json::Value>(event.encoded())?
                == serde_json::to_value(&event.event)?,
            "inbox event was modified after delivery"
        );
        crate::storage::inbox::accept(
            scope,
            &self.processing_scope,
            &event.event.id,
            event.encoded(),
            crate::application::limits::current()
                .events
                .max_inbox_receipts,
        )
    }

    /// Explicitly end deduplication for a bounded set of old receipts. Replaying
    /// those events afterwards may repeat effects. This never runs on a timer.
    pub fn prune_before(&self, processed_before_ms: i64, max_receipts: usize) -> Result<usize> {
        let scope = self.storage.scope()?;
        anyhow::ensure!(
            crate::storage::facet_opening(scope).is_none(),
            "facets cannot prune durable inbox receipts"
        );
        crate::storage::inbox::prune(
            scope,
            &self.processing_scope,
            processed_before_ms,
            max_receipts,
        )
    }
}
