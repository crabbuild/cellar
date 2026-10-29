// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use serde::Deserialize;
use sha2::{Digest, Sha256};

pub(super) const LIMIT: usize = 16 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Binding {
    pub format: u8,
    pub boot: BootBinding,
    pub owner: String,
    pub engine: String,
    pub container: String,
    pub started: String,
    pub restarts: u64,
}

impl Binding {
    pub fn validate(&self) -> Result<()> {
        self.boot
            .validate()
            .map_err(|_| FleetCatalogError::Invalid("Docker boot binding"))?;
        if self.format != 1
            || !identifier(&self.owner)
            || !identifier(&self.engine)
            || !timestamp(&self.started)
            || ContainerId::new(&self.container).is_err()
        {
            return Err(FleetCatalogError::Invalid("Docker container binding"));
        }
        Ok(())
    }

    pub fn key_for_boot(boot: &BootBinding) -> Result<String> {
        boot.validate()
            .map_err(|_| FleetCatalogError::Invalid("Docker boot binding"))?;
        Ok(format!("boots/{}", hex::encode(digest(boot)?)))
    }

    pub fn boot_key(&self) -> Result<String> {
        Self::key_for_boot(&self.boot)
    }

    pub fn container_key(&self) -> String {
        format!(
            "containers/{}/{}",
            hex::encode(Sha256::digest(self.engine.as_bytes())),
            self.container
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Intent {
    pub format: u8,
    pub operation: [u8; 16],
    pub binding: Binding,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Terminal {
    pub finished: String,
    pub exit_code: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Receipt {
    pub format: u8,
    pub intent: Intent,
    pub terminal: Terminal,
}

pub(super) fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
}

pub(super) fn timestamp(value: &str) -> bool {
    !value.starts_with("0001-")
        && value.len() >= 20
        && value.len() <= 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || b"-:TZ.+".contains(&b))
}

pub(super) fn bytes(value: &impl Serialize) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value)
        .map_err(|_| FleetCatalogError::Invalid("Docker isolation encoding"))?;
    if bytes.len() > LIMIT {
        return Err(FleetCatalogError::Limit);
    }
    Ok(bytes)
}

pub(super) fn digest(value: &impl Serialize) -> Result<[u8; 32]> {
    Ok(Sha256::digest(bytes(value)?).into())
}
