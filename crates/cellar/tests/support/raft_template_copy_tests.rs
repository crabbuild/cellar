// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn fallback_preserves_existing_destinations() -> Result<()> {
    let root = tempfile::tempdir()?;
    let source = root.path().join("source");
    let destination = root.path().join("existing");
    std::fs::write(&source, b"source bytes")?;
    std::fs::write(&destination, b"unrelated existing bytes")?;
    assert!(copy_fallback(&source, &destination).is_err());
    assert_eq!(std::fs::read(destination)?, b"unrelated existing bytes");
    Ok(())
}

#[cfg(unix)]
#[test]
fn readonly_unix_input_produces_a_private_writable_sqlite_copy() -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    let root = tempfile::tempdir()?;
    let source = root.path().join("source.sqlite");
    let destination = root.path().join("private.sqlite");
    let db = rusqlite::Connection::open(&source)?;
    db.execute_batch("CREATE TABLE counter(value);INSERT INTO counter VALUES(7)")?;
    drop(db);
    std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o400))?;
    copy_fallback(&source, &destination)?;
    assert!(
        source.metadata()?.permissions().readonly(),
        "source mode changed"
    );
    assert!(
        !destination.metadata()?.permissions().readonly(),
        "private copy inherited read-only input mode"
    );
    let db = rusqlite::Connection::open(destination)?;
    db.execute("UPDATE counter SET value=8", [])?;
    let original = rusqlite::Connection::open(source)?;
    assert_eq!(
        original.query_row("SELECT value FROM counter", [], |row| row.get::<_, u64>(0))?,
        7
    );
    Ok(())
}
