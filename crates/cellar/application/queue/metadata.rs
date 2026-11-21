// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Queue inspection over a caller's already selected committed image.
//! This view never installs schema, advances clocks, expires leases or rearms work.

use crate::application::Storage;
use anyhow::{ensure, Context as _, Result};
use serde_json::Value;

pub(super) struct Metadata<'a> {
    storage: &'a Storage,
    retention_ms: Option<i64>,
    max_admin_batch_size: usize,
}

impl<'a> Metadata<'a> {
    pub(super) fn new(
        storage: &'a Storage,
        retention_ms: Option<i64>,
        max_admin_batch_size: usize,
    ) -> Self {
        Self {
            storage,
            retention_ms,
            max_admin_batch_size,
        }
    }

    fn retention_cutoff(&self, at: i64) -> i64 {
        self.retention_ms
            .map_or(i64::MIN, |ms| at.saturating_sub(ms))
    }

    pub(super) fn info(&self, at: i64) -> Result<Value> {
        super::read::info(self.storage, self.retention_cutoff(at), at)
    }

    pub(super) fn read(&self, body: &Value, at: i64) -> Result<Value> {
        match body["op"].as_str() {
            Some("info") => self.info(at),
            Some("peek") => self.peek(
                at,
                limit(body, 10, self.max_admin_batch_size)?,
                body["console"].as_bool().unwrap_or(false),
            ),
            _ => {
                anyhow::bail!(crate::application::consensus::CommandFailure::UnsupportedCapability)
            }
        }
    }

    pub(super) fn metrics(&self, at: i64, waiting: bool) -> Result<Value> {
        super::read::metrics(self.storage, self.retention_cutoff(at), at, waiting)
    }

    pub(super) fn peek(&self, at: i64, requested: i64, console: bool) -> Result<Value> {
        super::read::peek(
            self.storage,
            self.retention_cutoff(at),
            at,
            requested,
            console,
        )
    }
}

pub(super) fn limit(body: &Value, default: i64, maximum: usize) -> Result<i64> {
    let limit = match body.get("limit").filter(|value| !value.is_null()) {
        None => default,
        Some(value) => value.as_i64().context("limit must be a positive integer")?,
    };
    ensure!(
        limit > 0 && limit as u64 <= maximum as u64,
        "QUEUE_ERROR: limit must be from 1 through {}",
        maximum
    );
    Ok(limit)
}

pub(crate) fn read(storage: &Storage, body: &Value) -> Result<Value> {
    let (retention, maximum) = crate::queue_policy::inspection_limits();
    Metadata::new(storage, retention, maximum).read(body, super::now())
}

pub(crate) fn metrics(storage: &Storage) -> Result<Value> {
    let (retention, maximum) = crate::queue_policy::inspection_limits();
    Metadata::new(storage, retention, maximum).metrics(super::now(), false)
}
