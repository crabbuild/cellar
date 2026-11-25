// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Sealed, SQL-only expiry on the actual KV activation and protocol lock.
use super::*;

pub(crate) trait ReplicatedExpiry: Send + Sync {
    /// False preserves the ordinary proven callback path. A successful call
    /// changes only the installed SQL transaction; it grants no durable result.
    fn try_expire(&self, at_ms: i64) -> Result<bool>;
}

pub(crate) fn create(context: &Context) -> Result<(Arc<dyn Cell>, Arc<dyn ReplicatedExpiry>)> {
    let storage = context.storage()?;
    ensure!(
        storage.is_raft(),
        "replicated KV expiry requires shared storage"
    );
    let cell = namespace_cell(storage);
    Ok((cell.clone(), cell))
}

impl ReplicatedExpiry for NamespaceCell {
    fn try_expire(&self, at_ms: i64) -> Result<bool> {
        ensure!(
            self.storage.is_raft() && at_ms >= 0,
            "invalid replicated KV expiry"
        );
        // Never await another invocation while holding an unproven claim.
        // Busy namespaces retain the original entry proof and callback path.
        let Ok(_protocol) = self.protocol.try_lock() else {
            return Ok(false);
        };
        self.open()?;
        if self.blob_due()? || crate::kv_blob::limits()["raceSweepPut"] == true {
            return Ok(false);
        }
        // Raft payload collection has a separate verified-closure collector.
        // Expiry removes SQL references only, as the ordinary Raft sweep does.
        let removed = self.sweep(at_ms, cellar_logic::sweep::BATCH_ROWS as i64)?;
        if removed >= cellar_logic::sweep::BATCH_ROWS {
            self.storage.set_alarm(at_ms.saturating_add(1))?;
        } else {
            self.rearm_at(at_ms)?;
        }
        Ok(true)
    }
}

#[cfg(test)]
#[path = "raft_expiry_tests.rs"]
mod tests;
