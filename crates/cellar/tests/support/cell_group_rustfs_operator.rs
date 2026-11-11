// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Captured image profile and cold-operator verification for recovery fixtures.
use anyhow::{Context as _, Result};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Clone)]
pub(crate) struct OperatorStoreConfig {
    pub(crate) rooted: bool,
    pub(crate) connection: crate::rustfs::StoreConfig,
}

tokio::task_local! {
    pub(crate) static OPERATOR: (OperatorStoreConfig, Arc<AtomicUsize>);
}

pub(crate) fn config() -> Option<OperatorStoreConfig> {
    OPERATOR.try_with(|(config, _)| config.clone()).ok()
}

pub(crate) fn verified() -> Result<()> {
    OPERATOR
        .try_with(|(_, verified)| verified.fetch_add(1, Ordering::AcqRel))
        .context("explicit cold operator store scope")?;
    Ok(())
}
