// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Observe a guest batch's real SQLite statements in the disposable owner.
//!
//! PROFILE fires when a statement finishes. Unlike an update hook, it lets us
//! certify the first completed write rather than assume when a row changes:
//! https://sqlite.org/c3ref/c_trace.html
//! No SQL is executed from a callback and no production storage hook is added.
use super::{Gate, Point};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::Context;
use rusqlite::ffi;
use serde_json::{json, Value};
use std::{
    cell::RefCell,
    ffi::{c_char, c_int, c_uint, c_void, CStr},
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{Arc, Mutex},
    time::Duration,
};

// These are the owning storage component's actual KV statements. A change in
// that implementation must fail this checkpoint, never silently certify a
// different SQL boundary. Only these small statements are expanded below.
const READ: &str = "SELECT v FROM __sys_cell_kv WHERE scope=?1 AND k=?2";
const PUT: &str = "INSERT INTO __sys_cell_kv(scope,k,v) VALUES(?1,?2,?3) ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v";

thread_local! {
    static CURRENT: RefCell<Option<Arc<Watch>>> = const { RefCell::new(None) };
}

/// Restored at the end of each native poll, including cancellation/unwinding.
pub(crate) struct Turn(Option<Arc<Watch>>);

impl Turn {
    pub(crate) fn enter(watch: Option<Arc<Watch>>) -> Self {
        Self(CURRENT.with(|current| current.replace(watch)))
    }
}

impl Drop for Turn {
    fn drop(&mut self) {
        CURRENT.with(|current| current.replace(self.0.take()));
    }
}

#[derive(Default)]
struct State {
    database: Option<usize>,
    receipt: Option<(usize, usize)>,
    receipt_absent: bool,
    write: Option<(usize, i64)>,
    reached: bool,
}

pub(crate) struct Watch {
    gate: Arc<Gate>,
    evidence: Value,
    receipt_sql: String,
    count_sql: String,
    state: Mutex<State>,
    #[cfg(test)]
    observe_only: bool,
}

fn quoted(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

impl Watch {
    pub(crate) fn new(
        gate: Arc<Gate>,
        context: &Context,
        operation_id: &str,
        delta: i64,
    ) -> Result<Arc<Self>> {
        ensure!(
            delta > 0,
            "transaction checkpoint requires a positive delta"
        );
        let identity = context.identity().context("checkpoint requires a cell")?;
        let sample = cellar::storage::sync_sample(&identity.scope)?
            .context("checkpoint has no native SQLite connection")?;
        ensure!(
            !sample.in_transaction && sample.epoch == identity.epoch,
            "checkpoint starts in a transaction or at the wrong epoch"
        );
        let count = delta.to_string();
        let evidence = json!({"ready":true,"point":Point::AfterCountBeforeReceipt.name(),
            "node":context.var("DOMAIN_MARKER").context("checkpoint has no domain marker")?,
            "class":identity.class,"scope":identity.scope,"epoch":sample.epoch,
            "count":count,"receipt":null,"typed_reply_held":false,
            "transaction_open":true,"transaction_observer":"sqlite_trace_profile"});
        Ok(Arc::new(Self::build(
            gate,
            evidence,
            &identity.scope,
            operation_id,
            &count,
        )))
    }

    fn build(gate: Arc<Gate>, evidence: Value, scope: &str, id: &str, count: &str) -> Self {
        let scope = quoted(scope);
        let receipt = quoted(&format!("receipt:{id}"));
        let value = quoted(&json!(count).to_string());
        Self {
            gate,
            evidence,
            receipt_sql: format!("SELECT v FROM __sys_cell_kv WHERE scope={scope} AND k={receipt}"),
            count_sql: format!("INSERT INTO __sys_cell_kv(scope,k,v) VALUES({scope},'count',{value}) ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v"),
            state: Mutex::default(),
            #[cfg(test)]
            observe_only: false,
        }
    }

    /// SQLite owns `statement` throughout this callback. Read only metadata;
    /// calling the SDK or preparing/stepping another statement here is invalid.
    unsafe fn event(&self, event: c_uint, statement: *mut ffi::sqlite3_stmt) -> Result<()> {
        // SAFETY: the trace mask contains only events carrying a live statement.
        let raw = unsafe { ffi::sqlite3_sql(statement) };
        ensure!(!raw.is_null(), "checkpoint statement has no SQL text");
        // SAFETY: SQLite retains this NUL-terminated text with its statement.
        let raw = unsafe { CStr::from_ptr(raw) }.to_bytes();
        if raw != READ.as_bytes() && raw != PUT.as_bytes() {
            return Ok(());
        }
        // SAFETY: both metadata queries use the callback's live statement. The
        // expanded string is a SQLite allocation, freed before any pause.
        let (database, expanded) = unsafe {
            (
                ffi::sqlite3_db_handle(statement),
                ffi::sqlite3_expanded_sql(statement),
            )
        };
        ensure!(
            !database.is_null() && !expanded.is_null(),
            "checkpoint expansion failed"
        );
        // SAFETY: expanded_sql returns owned, NUL-terminated UTF-8 SQL.
        let expanded_text = unsafe { CStr::from_ptr(expanded) }
            .to_string_lossy()
            .into_owned();
        // SAFETY: SQLite allocated this string and no reference to it remains.
        unsafe { ffi::sqlite3_free(expanded.cast()) };
        let mut state = self.state.lock().unwrap();
        if state.reached {
            return Ok(());
        }
        if let Some(previous) = state.database {
            ensure!(
                previous == database as usize,
                "checkpoint crossed SQLite connections"
            );
        } else {
            state.database = Some(database as usize);
        }
        if expanded_text == self.receipt_sql {
            match event {
                ffi::SQLITE_TRACE_STMT => state.receipt = Some((statement as usize, 0)),
                ffi::SQLITE_TRACE_ROW => {
                    let read = state
                        .receipt
                        .as_mut()
                        .context("receipt row has no started read")?;
                    ensure!(read.0 == statement as usize, "receipt statement changed");
                    read.1 += 1;
                }
                ffi::SQLITE_TRACE_PROFILE => {
                    let (read, rows) =
                        state.receipt.take().context("receipt read did not start")?;
                    ensure!(
                        read == statement as usize && rows == 0,
                        "checkpoint receipt already exists"
                    );
                    state.receipt_absent = true;
                }
                _ => anyhow::bail!("unexpected SQLite trace event"),
            }
        } else if raw == PUT.as_bytes() {
            ensure!(
                expanded_text == self.count_sql,
                "batch wrote another key before its count checkpoint"
            );
            match event {
                ffi::SQLITE_TRACE_STMT => {
                    ensure!(state.write.is_none(), "count write already started");
                    // SAFETY: the callback's database remains open and on its owning thread.
                    state.write = Some((statement as usize, unsafe {
                        ffi::sqlite3_total_changes64(database)
                    }));
                }
                ffi::SQLITE_TRACE_PROFILE => {
                    let (write, before) =
                        state.write.take().context("count write did not start")?;
                    // SAFETY: these read-only status calls use that same live connection.
                    let (after, changed, autocommit) = unsafe {
                        (
                            ffi::sqlite3_total_changes64(database),
                            ffi::sqlite3_changes64(database),
                            ffi::sqlite3_get_autocommit(database),
                        )
                    };
                    ensure!(
                        write == statement as usize && state.receipt_absent,
                        "count write has no completed absent-receipt read"
                    );
                    ensure!(
                        autocommit == 0 && after == before + 1 && changed == 1,
                        "count statement did not finish one write inside its transaction"
                    );
                    state.reached = true;
                    let mut evidence = self.evidence.clone();
                    evidence["sqlite_checkpoint"] = json!({"statement_finished":true,
                        "receipt_read_rows":0,"count_changes":changed,
                        "total_changes_before":before,"total_changes_after":after});
                    self.gate.publish(evidence)?;
                    drop(state);
                    #[cfg(test)]
                    if self.observe_only {
                        return Ok(());
                    }
                    // The coordinator reads an atomically published file and
                    // SIGKILLs this owner. If it fails, exit before SQLite can
                    // return to the receipt write or commit the partial batch.
                    std::thread::sleep(Duration::from_secs(10));
                    std::process::exit(70);
                }
                _ => anyhow::bail!("count statement unexpectedly returned a row"),
            }
        }
        Ok(())
    }

    fn failed(&self, reason: &str) {
        let _ = self
            .gate
            .publish(json!({"ready":false,"observer_failed":true,"reason":reason}));
        #[cfg(test)]
        if self.observe_only {
            return;
        }
        // Never let an invalid observer certify or finish a partial batch.
        std::process::exit(70);
    }
}

unsafe extern "C" fn trace(
    event: c_uint,
    _: *mut c_void,
    statement: *mut c_void,
    _: *mut c_void,
) -> c_int {
    let watch = CURRENT.with(|current| current.borrow().clone());
    let Some(watch) = watch else {
        return 0;
    };
    if statement.is_null() {
        watch.failed("SQLite trace supplied a null statement");
        return 0;
    }
    // Do not unwind through SQLite's C frames, even if a qualification assertion
    // or poisoned observer mutex panics. Only this disposable owner may exit.
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: registration requests only STMT, ROW and PROFILE, all of
        // which pass a live prepared-statement pointer for this callback.
        unsafe { watch.event(event, statement.cast()) }
    }));
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(error)) => watch.failed(&error.to_string()),
        Err(_) => watch.failed("SQLite checkpoint observer panicked"),
    }
    0
}

unsafe extern "C" fn attach(
    database: *mut ffi::sqlite3,
    _: *mut *mut c_char,
    _: *const ffi::sqlite3_api_routines,
) -> c_int {
    // SAFETY: SQLite calls this extension with a newly opened live connection.
    // The callback has static lifetime and stores no owning/raw context pointer.
    unsafe {
        ffi::sqlite3_trace_v2(
            database,
            ffi::SQLITE_TRACE_STMT | ffi::SQLITE_TRACE_ROW | ffi::SQLITE_TRACE_PROFILE,
            Some(trace),
            std::ptr::null_mut(),
        )
    }
}

pub(crate) fn install() -> Result<()> {
    // SAFETY: the statically linked callback has SQLite's documented extension
    // ABI and process lifetime. Registration is idempotent for this entry point.
    let result = unsafe { ffi::sqlite3_auto_extension(Some(attach)) };
    ensure!(
        result == ffi::SQLITE_OK,
        "SQLite checkpoint installation failed: {result}"
    );
    Ok(())
}

#[cfg(test)]
#[path = "sqlite_tests.rs"]
mod tests;
