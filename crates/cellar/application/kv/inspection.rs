// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Conditional operator edits share the namespace's revision and transaction.
use super::*;

impl NamespaceCell {
    pub(super) fn revision(&self, revision: i64) -> Result<String> {
        if !self.storage.is_raft() {
            return Ok(revision.to_string());
        }
        let history = crate::storage::capture_raft_dispatch()?
            .ok_or(super::super::consensus::CommandFailure::UnsupportedCapability)?
            .source_history()?;
        Ok(format!(
            "cg1:{}:{revision}",
            hex::encode(history.incarnation.bytes())
        ))
    }

    pub(super) fn observed_revision(
        &self,
        key: &str,
        at: i64,
    ) -> Result<(Option<String>, Option<i64>)> {
        let row = self
            .exec(
                "SELECT revision,expires_at FROM __sys_kv WHERE name=? AND (expires_at IS NULL OR expires_at>?)",
                &[key.to_string().into(), at.into()],
            )?
            .into_iter()
            .next();
        match row {
            Some(row) => Ok((
                Some(self.revision(integer(&row, "revision")?)?),
                optional_integer(&row, "expires_at")?,
            )),
            None => Ok((None, None)),
        }
    }

    pub(super) fn next_revision(&self) -> Result<i64> {
        let row = self
            .exec(
                "INSERT INTO __sys_kv_meta(name,value) VALUES ('revision',1) ON CONFLICT(name) DO UPDATE SET value=value+1 WHERE value<9223372036854775807 RETURNING value",
                &[],
            )?
            .into_iter()
            .next()
            .ok_or_else(|| error("namespace revision range exhausted"))?;
        integer(&row, "value")
    }
}
