//! Revisioned schedule overrides are scoped to an immutable deployment prefix.
//! Publication of a different prefix restores that deployment's declarations.
use crate::bucket::Bucket;
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Schedule {
    pub version: u32,
    pub application: String,
    pub prefix: String,
    pub expressions: Vec<String>,
}

pub(crate) fn key(prefix: &str, declared: &[String]) -> String {
    let mut hash = Sha256::new();
    hash.update(prefix.as_bytes());
    for expression in declared {
        hash.update([0]);
        hash.update(expression.as_bytes());
    }
    format!("config/cron/{:x}.json", hash.finalize())
}

pub(crate) fn validate(expressions: &[String]) -> Result<()> {
    ensure!(
        expressions.len() <= 64,
        "at most 64 cron expressions are supported"
    );
    let mut seen = std::collections::BTreeSet::new();
    for expression in expressions {
        ensure!(expression.len() <= 256, "cron expression is too long");
        ensure!(seen.insert(expression), "duplicate cron expression");
        cellar_logic::cron::parse(expression).map_err(|error| anyhow::anyhow!("{error}"))?;
    }
    Ok(())
}

pub(crate) async fn read(
    bucket: &Bucket,
    application: &str,
    prefix: &str,
    declared: &[String],
) -> Result<Option<(Vec<String>, String)>> {
    let Some((bytes, revision)) = bucket.get(&key(prefix, declared)).await? else {
        return Ok(None);
    };
    ensure!(
        bytes.len() <= 32 * 1024,
        "cron configuration exceeds 32 KiB"
    );
    let schedule: Schedule = serde_json::from_slice(&bytes)?;
    ensure!(
        schedule.version == 1 && schedule.application == application && schedule.prefix == prefix,
        "cron configuration identity mismatch"
    );
    validate(&schedule.expressions)?;
    Ok(Some((schedule.expressions, revision)))
}

/// Resolve the same schedule used by the deployment loader, without building a generation.
pub(crate) async fn desired(
    bucket: &Bucket,
    pointer: &crate::protocol::DeployPointer,
) -> Result<(Vec<String>, Option<String>)> {
    let (bytes, _) = bucket
        .get(&format!("{}/manifest.json", pointer.prefix))
        .await?
        .ok_or_else(|| anyhow::anyhow!("manifest unavailable"))?;
    let manifest: crate::protocol::Manifest = serde_json::from_slice(&bytes)?;
    ensure!(
        manifest.version == pointer.version
            && pointer
                .script_name
                .as_deref()
                .is_none_or(|name| name == manifest.script_name),
        "manifest identity mismatch"
    );
    Ok(
        match read(
            bucket,
            &manifest.script_name,
            &pointer.prefix,
            &manifest.crons,
        )
        .await?
        {
            Some((expressions, revision)) => (expressions, Some(revision)),
            None => (manifest.crons, None),
        },
    )
}
