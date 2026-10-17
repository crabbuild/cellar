//! KV options, conditional edit validation and structured-value codec.
use anyhow::{anyhow, ensure, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub mod validation;
pub mod wire;

fn error(message: impl std::fmt::Display) -> anyhow::Error {
    anyhow!("KV_ERROR: {message}")
}

#[derive(Clone, Debug, Default)]
pub struct PutOptions {
    /// Unix seconds, as in the original binding. TTL takes precedence.
    pub expiration: Option<f64>,
    pub expiration_ttl: Option<f64>,
    pub metadata: Option<Value>,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum ReadType {
    #[default]
    Text,
    Json,
    Bytes,
    Stream,
}

#[derive(Clone, Debug, Default)]
pub struct ListOptions {
    pub prefix: String,
    pub limit: Option<i64>,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListedKey {
    pub name: String,
    pub metadata: Option<Value>,
    pub expiration: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListResult {
    pub keys: Vec<ListedKey>,
    pub list_complete: bool,
    pub cursor: Option<String>,
    pub cache_status: Option<String>,
}

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
