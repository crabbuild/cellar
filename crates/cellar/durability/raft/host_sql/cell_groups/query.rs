// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable cell queries on the existing node SQL workers.

use super::*;
use crate::raft::cell_groups::{CellImagePin, CellReadAuthority};
use cellar_logic::cell_group::CellVersion;
use std::panic::AssertUnwindSafe;

type Callback = Box<
    dyn FnOnce(&crate::storage::ImmutableQuery, &CellVersion) -> anyhow::Result<Vec<u8>> + Send,
>;

pub(super) struct Query {
    options: CellSqlOptions,
    authority: CellReadAuthority,
    callback: Callback,
    cancelled: Arc<AtomicBool>,
    closing: Arc<AtomicBool>,
    bytes: Arc<Bytes>,
}

pub struct CellSqlQueryResult {
    result: Vec<u8>,
    version: CellVersion,
    _bytes: Arc<Bytes>,
}

impl CellSqlQueryResult {
    pub fn result(&self) -> &[u8] {
        &self.result
    }

    pub fn version(&self) -> &CellVersion {
        &self.version
    }
}

struct Cancel(Arc<AtomicBool>);

impl Drop for Cancel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

impl CellGroupSqlPool {
    /// Consume an invocation's coherent pin. Queries can coexist with an
    /// unresolved private writer but read only the selected committed image.
    /// The registered dispatcher must enforce operation contracts before here.
    pub async fn query(
        &self,
        options: CellSqlOptions,
        authority: CellReadAuthority,
        callback: impl FnOnce(&crate::storage::ImmutableQuery, &CellVersion) -> anyhow::Result<Vec<u8>>
            + Send
            + 'static,
    ) -> Result<CellSqlQueryResult, ExecutionError> {
        self.admit()?;
        let domain = &self.inner.workers[0].domain;
        if !authority.belongs_to_engine(&self.inner.engine_owner)
            || options.application_digest() != authority.route().schema_digest
        {
            return Err(ExecutionError::Invalid("cell query engine/schema"));
        }
        authority
            .check(domain.mono_ms().map_err(|_| unavailable())?)
            .map_err(crate::raft::execution::authority_failure)?;
        options.limits().validate()?;
        let bytes = self.inner.admission.reserve_read(
            authority.route().history,
            options
                .limits()
                .max_result_bytes
                .checked_add(4096)
                .ok_or_else(overloaded)?,
        )?;
        let cell = authority.cell().version.history.clone();
        let deadline = authority.deadline();
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = Cancel(cancelled.clone());
        let query = Query {
            options,
            authority,
            callback: Box::new(callback),
            cancelled,
            closing: self.inner.closing.clone(),
            bytes,
        };
        let timer = domain.sleep_until(deadline).map_err(|_| unavailable())?;
        crate::asyncrt::select! {
            result = self.call(cell, worker::Action::Query(Box::new(query))) => {
                match result? {
                    CellSqlOutcome::Queried(output) => {
                        if domain.mono_ms().map_err(|_| unavailable())? >= deadline {
                            return Err(crate::raft::execution::storage(RuntimeError::DeadlineExceeded));
                        }
                        Ok(output)
                    }
                    _ => Err(ExecutionError::Invalid("cell query reply")),
                }
            }
            _ = timer => Err(crate::raft::execution::storage(RuntimeError::DeadlineExceeded)),
        }
    }
}

// The SQLite wrapper catches progress-hook panics. This wrapper allows the
// domain clock to be captured without claiming that arbitrary clock backends
// themselves implement unwind-safety; a clock failure interrupts the query.
struct QueryClock(AssertUnwindSafe<crate::asyncrt::DomainToken>);

impl QueryClock {
    fn expired(&self, deadline: u64) -> bool {
        self.0.mono_ms().map_or(true, |now| now >= deadline)
    }
}

pub(super) fn run(
    mut query: Query,
    domain: &crate::asyncrt::DomainToken,
) -> Result<CellSqlOutcome, ExecutionError> {
    let check = |query: &Query| {
        if query.cancelled.load(Ordering::Acquire) || query.closing.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        query
            .authority
            .check(domain.mono_ms().map_err(|_| unavailable())?)
            .map_err(crate::raft::execution::authority_failure)
    };
    check(&query)?;
    let database = match (&query.options, query.authority.image()) {
        (CellSqlOptions::Materialized(options), CellImagePin::Materialized(pin)) => {
            crate::storage::open_immutable_generation(
                pin.clone(),
                crate::storage::ImmutableQueryOptions {
                    services: options.query_services.clone(),
                    sqlite_vec: false,
                },
            )
        }
        (CellSqlOptions::Extents(options), CellImagePin::Extents(pin)) => {
            let image = pin
                .view()
                .image(
                    options.cache_directory.clone(),
                    options.cache_pool.clone(),
                    options.cache,
                )
                .map_err(crate::raft::execution::storage)?;
            crate::storage::open_immutable_extent_image(
                image,
                crate::storage::ImmutableQueryOptions {
                    services: options.query_services.clone(),
                    sqlite_vec: false,
                },
            )
        }
        _ => return Err(ExecutionError::Invalid("cell query image storage")),
    }
    .map_err(crate::raft::execution::storage)?;
    let clock = QueryClock(AssertUnwindSafe(domain.clone()));
    let deadline = query.authority.deadline();
    let cancelled = query.cancelled.clone();
    let closing = query.closing.clone();
    database
        .connection()
        .progress_handler(
            1000,
            Some(move || {
                cancelled.load(Ordering::Acquire)
                    || closing.load(Ordering::Acquire)
                    || clock.expired(deadline)
            }),
        )
        .map_err(crate::raft::execution::storage)?;
    check(&query)?;
    let version = query.authority.cell().version.clone();
    let result =
        std::panic::catch_unwind(AssertUnwindSafe(|| (query.callback)(&database, &version)))
            .unwrap_or_else(|panic| Err(crate::native_worker::panic_error(panic)))
            .map_err(crate::raft::execution::storage)?;
    // The callback has been consumed, so check the remaining owners explicitly.
    if query.cancelled.load(Ordering::Acquire) || query.closing.load(Ordering::Acquire) {
        return Err(unavailable());
    }
    query
        .authority
        .check(domain.mono_ms().map_err(|_| unavailable())?)
        .map_err(crate::raft::execution::authority_failure)?;
    if result.len() > query.options.limits().max_result_bytes {
        return Err(overloaded());
    }
    drop(database);
    drop(query.authority);
    Bytes::shrink(&mut query.bytes, result.len() + 4096)?;
    Ok(CellSqlOutcome::Queried(CellSqlQueryResult {
        result,
        version,
        _bytes: query.bytes,
    }))
}
