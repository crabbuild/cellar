// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Original operation budget across catalog turns and external physical work.
use super::*;

pub(super) struct Deadline<'a> {
    catalog: &'a FleetCatalog,
    end: u64,
}

impl<'a> Deadline<'a> {
    pub(super) fn new(catalog: &'a FleetCatalog, timeout: Duration) -> Result<Self> {
        if timeout.as_millis() == 0 || timeout > Duration::from_secs(60) {
            return Err(FleetCatalogError::Invalid("timeout"));
        }
        let now = catalog
            .inner
            .domain
            .mono_ms()
            .map_err(|_| FleetCatalogError::Closed)?;
        Ok(Self {
            catalog,
            end: now
                .checked_add(timeout.as_millis() as u64)
                .ok_or(FleetCatalogError::Limit)?,
        })
    }

    pub(super) fn remaining(&self) -> Result<Duration> {
        let now = self
            .catalog
            .inner
            .domain
            .mono_ms()
            .map_err(|_| FleetCatalogError::Closed)?;
        self.end
            .checked_sub(now)
            .filter(|ms| *ms > 0)
            .map(Duration::from_millis)
            .ok_or(FleetCatalogError::Unconfirmed)
    }
}
