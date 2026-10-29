// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use anyhow::{ensure, Result};

#[test]
fn older_object_layout_is_refused_without_mutating_saved_state() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("old.sqlite");
    let connection = Connection::open(&path)?;
    connection.execute_batch("CREATE TABLE __sys_local_objects(key TEXT PRIMARY KEY, etag INTEGER NOT NULL, modified_ms INTEGER NOT NULL, attributes TEXT NOT NULL, body BLOB NOT NULL);
        INSERT INTO __sys_local_objects VALUES ('saved', 1, 0, '[]', X'010203');")?;
    ensure!(LocalStore::open(&path)
        .unwrap_err()
        .to_string()
        .contains("incompatible local object schema"));
    ensure!(
        connection.query_row(
            "SELECT body FROM __sys_local_objects WHERE key = 'saved'",
            [],
            |row| row.get::<_, Vec<u8>>(0)
        )? == [1, 2, 3]
    );
    ensure!(
        connection.query_row(
            "SELECT count(*) FROM sqlite_schema WHERE name = '__sys_local_bodies'",
            [],
            |row| row.get::<_, u64>(0)
        )? == 0
    );
    Ok(())
}

#[test]
fn expired_staging_cannot_publish_or_revive_after_collection_starts() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalStore::open(directory.path().join("objects.sqlite"))?;
    let mut connection = store.connect()?;
    let id = store.stage_body(
        &mut connection,
        "unfinished",
        &vec![0x5a; READ_CHUNK_BYTES * 9],
    )?;
    connection.execute(
        "UPDATE __sys_local_bodies SET expires_ms = 0 WHERE id = ?1",
        [id],
    )?;
    store.maintain_bodies(&mut connection)?;
    ensure!(
        connection.query_row(
            "SELECT count(*) FROM __sys_local_body_chunks WHERE body_id = ?1",
            [id],
            |row| row.get::<_, u64>(0)
        )? == 5,
        "one maintenance pass exceeded its four-chunk bound"
    );
    // Simulate a wall-clock rollback after the collector witnessed expiry.
    connection.execute(
        "UPDATE __sys_local_bodies SET expires_ms = ?2 WHERE id = ?1",
        params![id, i64::MAX],
    )?;
    let transaction = connection.transaction()?;
    ensure!(LocalStore::publish_body(&transaction, id).is_err());
    transaction.rollback()?;
    store.drain_body(&mut connection, id)?;
    ensure!(store.metadata("")?.is_empty());
    Ok(())
}

#[tokio::test]
async fn large_copy_multipart_replacement_and_delete_reclaim_owned_chunks() -> Result<()> {
    use object_store::multipart::MultipartStore;
    let domain = crate::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let directory = tempfile::tempdir()?;
            let store = LocalStore::open(directory.path().join("objects.sqlite"))?;
            let payload = Bytes::from(vec![0x5a; READ_CHUNK_BYTES * 3 + 7]);
            let source = Path::from("source");
            let copy = Path::from("copy");
            store.put(&source, payload.clone().into()).await?;
            store.copy_if_not_exists(&source, &copy).await?;
            ensure!(store.get(&copy).await?.bytes().await? == payload);
            let upload = store.create_multipart(&source).await?;
            let part = MultipartStore::put_part(
                &store,
                &source,
                &upload,
                0,
                Bytes::from_static(b"replacement").into(),
            )
            .await?;
            store
                .complete_multipart(&source, &upload, vec![part])
                .await?;
            ensure!(store.get(&source).await?.bytes().await?.as_ref() == b"replacement");
            ensure!(store.get(&copy).await?.bytes().await? == payload);
            store.delete(&copy).await?;
            let connection = store.connect()?;
            ensure!(
                connection
                    .query_row("SELECT count(*) FROM __sys_local_bodies", [], |row| row
                        .get::<_, u64>(0))?
                    == 0
            );
            ensure!(
                connection.query_row(
                    "SELECT count(*) FROM __sys_local_body_chunks",
                    [],
                    |row| row.get::<_, u64>(0)
                )? == 0
            );
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result?
}

#[test]
fn abandoned_bodies_reap_after_reopen_without_touching_published_bytes() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("objects.sqlite");
    let store = LocalStore::open(&path)?;
    let payload = Bytes::from(vec![0x5a; READ_CHUNK_BYTES * 9 + 3]);
    store.put_sync("live".into(), payload.clone(), PutOptions::default())?;
    let mut connection = store.connect()?;
    let abandoned = store.stage_body(&mut connection, "unfinished", &payload)?;
    ensure!(store.read_metadata("unfinished", false).is_err());
    connection.execute(
        "UPDATE __sys_local_bodies SET expires_ms = 0 WHERE id = ?1",
        [abandoned],
    )?;
    drop(connection);
    drop(store);
    let reopened = LocalStore::open(&path)?;
    let mut connection = reopened.connect()?;
    for _ in 0..8 {
        reopened.maintain_bodies(&mut connection)?;
    }
    ensure!(
        connection.query_row(
            "SELECT count(*) FROM __sys_local_bodies WHERE id = ?1",
            [abandoned],
            |row| row.get::<_, u64>(0)
        )? == 0
    );
    ensure!(
        connection.query_row(
            "SELECT count(*) FROM __sys_local_body_chunks WHERE body_id = ?1",
            [abandoned],
            |row| row.get::<_, u64>(0)
        )? == 0
    );
    ensure!(reopened.read("live")?.body == payload);
    let next = reopened.stage_body(&mut connection, "next", &payload)?;
    ensure!(next > abandoned, "reclaimed staging identity was reused");
    let transaction = connection.transaction()?;
    ensure!(LocalStore::publish_body(&transaction, abandoned).is_err());
    transaction.rollback()?;
    Ok(())
}

#[test]
fn failed_large_cas_preserves_the_winner_and_reclaims_staging() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalStore::open(directory.path().join("objects.sqlite"))?;
    let original = store.put_sync(
        "key".into(),
        Bytes::from_static(b"old"),
        PutOptions::default(),
    )?;
    let winner = store.put_sync(
        "key".into(),
        Bytes::from_static(b"winner"),
        PutOptions::default(),
    )?;
    let refused = store.put_sync(
        "key".into(),
        Bytes::from(vec![0x5a; READ_CHUNK_BYTES * 5]),
        PutMode::Update(object_store::UpdateVersion {
            e_tag: original.e_tag,
            version: None,
        })
        .into(),
    );
    ensure!(matches!(refused, Err(Error::Precondition { .. })));
    let saved = store.read("key")?;
    ensure!(saved.body == b"winner" && Some(saved.etag.to_string()) == winner.e_tag);
    let connection = store.connect()?;
    ensure!(
        connection.query_row("SELECT count(*) FROM __sys_local_bodies", [], |row| row
            .get::<_, u64>(0))?
            == 0
    );
    ensure!(
        connection.query_row("SELECT count(*) FROM __sys_local_body_chunks", [], |row| {
            row.get::<_, u64>(0)
        })? == 0
    );
    Ok(())
}

#[test]
fn large_ranges_cross_chunk_boundaries_and_replace_without_stale_bytes() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalStore::open(directory.path().join("objects.sqlite"))?;
    let payload: Vec<u8> = (0..READ_CHUNK_BYTES * 3 + 7)
        .map(|index| (index % 251) as u8)
        .collect();
    store.put_sync(
        "key".into(),
        Bytes::from(payload.clone()),
        PutOptions::default(),
    )?;
    let metadata = store.read_metadata("key", true)?;
    ensure!(metadata.body.is_empty() && metadata.size == payload.len() as u64);
    for (offset, length) in [
        (0, 13),
        (READ_CHUNK_BYTES - 3, 17),
        (READ_CHUNK_BYTES * 2 - 1, READ_CHUNK_BYTES),
        (payload.len() - 7, 7),
    ] {
        ensure!(
            store
                .read_chunk("key", metadata.etag, metadata.size, offset as u64, length)?
                .as_ref()
                == &payload[offset..offset + length]
        );
    }
    store.put_sync(
        "key".into(),
        Bytes::from_static(b"new"),
        PutOptions::default(),
    )?;
    ensure!(matches!(
        store.read_chunk("key", metadata.etag, metadata.size, 0, 13),
        Err(Error::Precondition { .. })
    ));
    ensure!(store.read("key")?.body == b"new");
    let connection = store.connect()?;
    ensure!(
        connection.query_row("SELECT count(*) FROM __sys_local_body_chunks", [], |row| {
            row.get::<_, u64>(0)
        })? == 0
    );
    Ok(())
}
