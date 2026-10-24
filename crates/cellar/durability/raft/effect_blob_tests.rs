// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn immutable_blob_refuses_corrupt_bodies_and_reference_metadata() -> Result<()> {
    let database = Connection::open_in_memory()?;
    let limits = RaftBlobLimits::default();
    let reference = put_blob(&database, limits, b"verified")?;
    ensure!(get_blob(&database, limits, reference)? == b"verified");
    ensure!(RaftBlobReference::decode(&reference.encode())? == reference);
    ensure!(RaftBlobReference::decode(&[0; 40]).is_err());
    ensure!(RaftBlobReference::decode(&reference.encode()[..39]).is_err());
    let mut wrong_length = reference;
    wrong_length.length += 1;
    ensure!(get_blob(&database, limits, wrong_length).is_err());
    database.execute(
        "UPDATE __sys_raft_blobs SET body=?1",
        [b"modified".as_slice()],
    )?;
    ensure!(get_blob(&database, limits, reference).is_err());
    ensure!(
        put_blob(&database, limits, b"verified").is_err(),
        "duplicate put hid corruption"
    );
    Ok(())
}

#[test]
fn immutable_blob_capacity_refuses_growth_but_retains_duplicate_and_empty_values() -> Result<()> {
    let database = Connection::open_in_memory()?;
    let limits = RaftBlobLimits {
        max_blob_bytes: 4,
        max_objects: 2,
        max_total_bytes: 4,
    };
    let empty = put_blob(&database, limits, b"")?;
    let full = put_blob(&database, limits, b"1234")?;
    ensure!(get_blob(&database, limits, empty)?.is_empty());
    ensure!(put_blob(&database, limits, b"1234")? == full);
    for body in [b"5".as_slice(), b"12345".as_slice()] {
        let error = put_blob(&database, limits, body).unwrap_err();
        ensure!(
            error.downcast_ref::<cellar_logic::consensus::CommandFailure>()
                == Some(&cellar_logic::consensus::CommandFailure::Overloaded)
        );
    }
    ensure!(inventory(&database, limits)? == (2, 4));
    ensure!(get_blob(&database, limits, full)? == b"1234");
    Ok(())
}
