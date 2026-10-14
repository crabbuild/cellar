//! Conditional operator edits share the namespace's revision and transaction.
use super::*;
use serde::{Deserialize, Serialize};

/// A revision observed while reading a key. `None` requires an absent/live-expired key.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RevisionCondition {
    pub revision: Option<String>,
}

impl RevisionCondition {
    pub fn validate(&self) -> Result<()> {
        if let Some(revision) = &self.revision {
            let number = revision
                .parse::<i64>()
                .map_err(|_| error("revision must be a nonnegative decimal integer"))?;
            ensure!(
                number >= 0 && number.to_string() == *revision,
                "KV_ERROR: invalid revision"
            );
        }
        Ok(())
    }
}

/// Absolute expiration is milliseconds since the Unix epoch, matching inspection.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum EditExpiration {
    Preserve,
    Never,
    At { timestamp_ms: u64 },
}

impl NamespaceCell {
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
                Some(integer(&row, "revision")?.to_string()),
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
