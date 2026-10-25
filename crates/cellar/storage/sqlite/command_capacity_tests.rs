// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn receipt_usage_tracks_btrees_and_freelist_without_charging_blob_payloads_to_reserve() -> Result<()>
{
    for (page_size, auto_vacuum, reserve) in [512, 4096, 65536].into_iter().flat_map(|page| {
        [0, 1, 2].into_iter().flat_map(move |mode| {
            [0, 16]
                .into_iter()
                .map(move |reserve| (page, mode, reserve))
        })
    }) {
        let connection = Connection::open_in_memory()?;
        connection.pragma_update(None, "page_size", page_size)?;
        connection.pragma_update(None, "auto_vacuum", auto_vacuum)?;
        let mut reserve: i32 = reserve;
        // SAFETY: this unused test connection and integer are live/exclusive;
        // positive RESERVE_BYTES deliberately selects the fixture page layout.
        assert_eq!(
            unsafe {
                ffi::sqlite3_file_control(
                    connection.handle(),
                    c"main".as_ptr(),
                    ffi::SQLITE_FCNTL_RESERVE_BYTES,
                    (&mut reserve as *mut i32).cast(),
                )
            },
            ffi::SQLITE_OK
        );
        connection.execute_batch("VACUUM")?;
        initialize(&connection)?;
        connection.execute_batch(
            "BEGIN;
            CREATE TABLE app(value BLOB);
            CREATE TABLE __SYS_RAFT_CLIENTS(id INTEGER, name TEXT UNIQUE);
            CREATE TABLE __sys_raft_receipts(value BLOB);
            CREATE TABLE __sys_ltx_seq(id INTEGER);
            CREATE TABLE __sys_ltx_lock(id INTEGER);
            CREATE TABLE __sys_raft_blobs(value BLOB);",
        )?;
        let initial = measure(&connection, 32 * 1024 * 1024)?;
        assert!(
            initial.receipt_bytes >= 5 * page_size as u64,
            "automatic index must count"
        );
        connection.execute("INSERT INTO app VALUES(zeroblob(1048576))", [])?;
        connection.execute("INSERT INTO __sys_raft_blobs VALUES(zeroblob(1048576))", [])?;
        let payloads = measure(&connection, 32 * 1024 * 1024)?;
        assert_eq!(payloads.receipt_bytes, initial.receipt_bytes);
        let app_trees: u64 = connection.query_row("SELECT sum(pgsize) FROM dbstat WHERE name IN ('app','__sys_raft_blobs') AND aggregate=TRUE", [], |r| r.get(0))?;
        assert!(app_trees >= 2 * 1048576);
        // Empty tables already owned their root pages; payload growth can
        // reuse that space, especially with 64 KiB pages.
        assert_eq!(
            payloads.application_bytes - initial.application_bytes,
            app_trees - 2 * page_size as u64
        );
        connection.execute(
            "INSERT INTO __sys_raft_receipts VALUES(zeroblob(131072))",
            [],
        )?;
        let receipt = measure(&connection, 32 * 1024 * 1024)?;
        let receipt_tree: u64 = connection.query_row(
            "SELECT pgsize FROM dbstat WHERE name='__sys_raft_receipts' AND aggregate=TRUE",
            [],
            |r| r.get(0),
        )?;
        assert!(receipt_tree >= 131072);
        assert_eq!(
            receipt.receipt_bytes - payloads.receipt_bytes,
            receipt_tree - page_size as u64
        );
        assert_eq!(receipt.application_bytes, payloads.application_bytes);
        let high_water: u64 = connection.query_row("PRAGMA page_count", [], |r| r.get(0))?;
        connection.execute("DELETE FROM app", [])?;
        let cleaned = measure(&connection, 32 * 1024 * 1024)?;
        assert!(cleaned.application_bytes < receipt.application_bytes);
        assert_eq!(cleaned.receipt_bytes, receipt.receipt_bytes);
        if auto_vacuum != 1 {
            assert_eq!(
                connection.query_row("PRAGMA page_count", [], |r| r.get::<_, u64>(0))?,
                high_water
            );
        }
        connection.execute("DELETE FROM __sys_raft_receipts", [])?;
        let metadata_cleanup = measure(&connection, 32 * 1024 * 1024)?;
        assert!(metadata_cleanup.receipt_bytes < cleaned.receipt_bytes);
        assert_eq!(
            metadata_cleanup.application_bytes,
            cleaned.application_bytes
        );
        let all_btrees: u64 = connection.query_row(
            "SELECT sum(pgsize) FROM dbstat WHERE aggregate=TRUE",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(
            metadata_cleanup.application_bytes + metadata_cleanup.receipt_bytes,
            all_btrees
        );
        connection.execute_batch("ROLLBACK")?;
        assert!(
            measure(&connection, 32 * 1024 * 1024).is_err(),
            "cannot measure outside a private transaction"
        );
    }
    Ok(())
}

#[test]
fn application_dbstat_table_cannot_replace_physical_accounting() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    initialize(&connection)?;
    connection.execute_batch(
        "BEGIN;
        CREATE TABLE __sys_raft_receipts(value BLOB);
        INSERT INTO __sys_raft_receipts VALUES(zeroblob(131072));
        CREATE TABLE dbstat(name TEXT, pgsize INTEGER, aggregate INTEGER);
        INSERT INTO dbstat VALUES('__sys_raft_receipts',0,1);",
    )?;
    let usage = measure(&connection, 32 * 1024 * 1024)?;
    assert!(usage.receipt_bytes >= 131072);
    connection.execute_batch("ROLLBACK")?;
    Ok(())
}

#[test]
fn pointer_map_relocation_does_not_count_sqlites_lock_page_twice() {
    let page_size = 1024;
    let usable = 770;
    let lock = 0x4000_0000 / page_size + 1;
    let before = structural_bytes(lock - 1, page_size, usable, true);
    let at = structural_bytes(lock, page_size, usable, true);
    let after = structural_bytes(lock + 1, page_size, usable, true);
    assert_eq!(at, before + page_size);
    assert_eq!(after, at + page_size);
}
