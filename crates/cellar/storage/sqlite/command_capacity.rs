// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical receipt/control accounting inside the private command transaction.
//! Unused quota stays virtual: a new cell does not preallocate its allowance.

use anyhow::{ensure, Context as _, Result};
use rusqlite::{ffi, Connection};

#[cfg(test)]
#[path = "command_capacity_tests.rs"]
mod tests;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Usage {
    pub application_bytes: u64,
    pub receipt_bytes: u64,
    pub structural_reserve_bytes: u64,
}

/// Install the native dbstat module under a private, protected temporary name
/// before the command authorizer is installed. Application tables named dbstat
/// cannot substitute their rows for physical allocation observations.
pub(super) fn initialize(connection: &Connection) -> Result<()> {
    connection.execute_batch("CREATE VIRTUAL TABLE temp.__sys_raft_capacity USING dbstat(main)")?;
    Ok(())
}

pub(super) fn measure(connection: &Connection, quota: u64) -> Result<Usage> {
    ensure!(
        !connection.is_autocommit(),
        "capacity inspection requires a private transaction"
    );
    let page_size: u64 = connection.query_row("PRAGMA page_size", [], |r| r.get(0))?;
    let pages: u64 = connection.query_row("PRAGMA page_count", [], |r| r.get(0))?;
    let free: u64 = connection.query_row("PRAGMA freelist_count", [], |r| r.get(0))?;
    ensure!(
        cellar_ltx::ltx::is_valid_page_size(u32::try_from(page_size)?),
        "invalid capacity page size"
    );
    let auto_vacuum: u32 = connection.query_row("PRAGMA auto_vacuum", [], |r| r.get(0))?;
    ensure!(auto_vacuum <= 2, "invalid auto-vacuum mode");
    let mut reserved = -1_i32;
    // SAFETY: the framework exclusively owns this live connection and integer.
    // A negative RESERVE_BYTES argument only queries SQLite's page geometry;
    // no reserve size or image bytes are changed by this file-control call.
    let result = unsafe {
        ffi::sqlite3_file_control(
            connection.handle(),
            c"main".as_ptr(),
            ffi::SQLITE_FCNTL_RESERVE_BYTES,
            (&mut reserved as *mut i32).cast(),
        )
    };
    ensure!(
        result == ffi::SQLITE_OK && (0..=255).contains(&reserved),
        "cannot inspect SQLite reserved bytes"
    );
    let usable = page_size
        .checked_sub(reserved as u64)
        .context("invalid SQLite usable page size")?;
    ensure!(usable >= 480, "invalid SQLite usable page size");
    let structural = structural_bytes(pages, page_size, usable, auto_vacuum != 0);
    let structural_reserve_bytes =
        structural_bytes(quota / page_size, page_size, usable, auto_vacuum != 0);
    let used = pages
        .checked_sub(free)
        .and_then(|n| n.checked_mul(page_size))
        .context("invalid private database allocation")?;
    // Include each reserved table's ordinary/automatic indexes. Other system
    // tables, including application Blob/outbox data, use the application
    // allowance and retain their own existing admission limits.
    let mut objects = connection.prepare_cached(
        "SELECT name FROM sqlite_schema WHERE rootpage>0 AND tbl_name COLLATE NOCASE IN (
            '__sys_raft_clients','__sys_raft_receipts','__sys_ltx_seq','__sys_ltx_lock')",
    )?;
    let mut rows = objects.query([])?;
    let mut receipt_bytes = 0_u64;
    let mut count = 0;
    while let Some(row) = rows.next()? {
        count += 1;
        ensure!(
            count <= 32,
            "receipt/control schema exceeds its object bound"
        );
        let name: String = row.get(0)?;
        // The equality constraint is important: dbstat walks only this btree,
        // not all application pages. Aggregate mode returns one bounded row.
        let bytes: u64 = connection.query_row(
            "SELECT pgsize FROM temp.__sys_raft_capacity WHERE name=?1 AND aggregate=TRUE",
            [name],
            |r| r.get(0),
        )?;
        receipt_bytes = receipt_bytes
            .checked_add(bytes)
            .context("receipt allocation overflow")?;
    }
    Ok(Usage {
        application_bytes: used
            .checked_sub(receipt_bytes)
            .and_then(|n| n.checked_sub(structural))
            .context("invalid receipt allocation")?,
        receipt_bytes,
        structural_reserve_bytes,
    })
}

/// Pointer maps and SQLite's reserved lock page are not btrees or free pages.
/// Charge their maximum quota-sized footprint separately so receipt growth at
/// a pointer-map boundary cannot consume the application's allowance. The
/// arithmetic matches SQLite's ptrmapPageno, including lock-page relocation.
fn structural_bytes(pages: u64, page_size: u64, usable: u64, auto_vacuum: bool) -> u64 {
    let lock = 0x4000_0000 / page_size + 1;
    let locked = u64::from(pages >= lock);
    let maps = if auto_vacuum && pages >= 2 {
        let stride = usable / 5 + 1;
        let count = (pages - 2) / stride + 1;
        count - u64::from(pages == lock && (lock - 2).is_multiple_of(stride))
    } else {
        0
    };
    (maps + locked) * page_size
}
