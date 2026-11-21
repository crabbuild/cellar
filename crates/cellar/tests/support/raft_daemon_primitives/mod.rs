// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Normal primitive bindings over two managed RF3 groups in daemon processes.
use super::*;
mod application;
mod qualification;
mod startup;

pub(crate) const ENABLED: &str = "CELLAR_RAFT_DAEMON_PRIMITIVES";
pub(crate) const MINIMUM: &str = "CELLAR_RAFT_PRIMITIVE_MINIMUM";
pub(crate) const BOOT_TOKEN: &str = "CELLAR_RAFT_PRIMITIVE_BOOT_TOKEN";
pub(crate) use qualification::qualify;
pub(crate) use startup::child;

pub(crate) fn enabled() -> bool {
    std::env::var_os(ENABLED).is_some()
}

fn minimum() -> Result<Durability> {
    match std::env::var(MINIMUM)?.as_str() {
        "quorum" => Ok(Durability::Quorum),
        "s3" => Ok(Durability::S3),
        _ => anyhow::bail!("invalid primitive namespace minimum"),
    }
}

fn group(index: usize) -> Result<GroupHistory> {
    ensure!(index < 2, "primitive group index");
    let mut history = configuration::group(
        ClusterId::new([1; 16])?,
        NamespaceId::new([u8::try_from(index)? + 2; 16])?,
    )?;
    history.group = GroupId::new([u8::try_from(index)? + 5; 16])?;
    Ok(history)
}
