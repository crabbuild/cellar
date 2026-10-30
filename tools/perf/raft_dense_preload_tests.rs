// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn template_contains_independently_reproducible_dense_payloads() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("template.sqlite");
    let state = template(&path, 0, 4)?;
    let db = rusqlite::Connection::open(&path)?;
    let bytes: u64 = db.query_row("SELECT sum(length(bytes)) FROM dense_padding", [], |row| {
        row.get(0)
    })?;
    assert_eq!(bytes, 4 * 1024 * 1024);
    let block: Vec<u8> =
        db.query_row("SELECT bytes FROM dense_padding WHERE block=0", [], |row| {
            row.get(0)
        })?;
    // Independently generated with Python hashlib from little-endian counters.
    assert_eq!(
        hex::encode(Sha256::digest(block)),
        "bef5756dd5a77ad69f5e6deed8845e1336c287411d95cb7ef5e757e8008ffb7f"
    );
    assert!(state.length >= bytes);
    assert!(template(&path, 0, 4).is_err());
    Ok(())
}

#[test]
fn extent_install_rejects_template_changed_after_hashing() -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("template.sqlite");
    let state = template(&path, 0, 4)?;
    let good = tempfile::tempdir()?;
    let installed = install(good.path().into(), path.clone(), state, [1; 32], [2; 32])?;
    assert_eq!(installed.marker.length, state.length);
    drop(installed);
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)?;
    file.seek(SeekFrom::End(-1))?;
    let mut byte = [0];
    file.read_exact(&mut byte)?;
    byte[0] ^= 0xff;
    file.seek(SeekFrom::End(-1))?;
    file.write_all(&byte)?;
    file.sync_all()?;
    let bad = tempfile::tempdir()?;
    assert!(install(bad.path().into(), path, state, [1; 32], [2; 32]).is_err());
    Ok(())
}
