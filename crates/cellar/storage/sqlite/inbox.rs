// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::event_stream::{DeliveryError, ErrorCode};
use sha2::{Digest, Sha256};

const SCHEMA: super::event_schema::Schema = super::event_schema::Schema {
    marker: "__sys_inbox_schema",
    ddl: include_str!("inbox.sql"),
    required: &[
        (
            "__sys_inbox",
            "processing_scope,event_id,digest,processed_ms",
        ),
        ("__sys_inbox_stats", "singleton,rows"),
    ],
};

pub(super) fn check_schema(connection: &Connection) -> anyhow::Result<bool> {
    SCHEMA.check(connection)
}

pub(super) fn restore_schema(connection: &Connection) -> anyhow::Result<()> {
    if check_schema(connection)? {
        open_schema(connection)?;
        validate_retained(connection)?;
    }
    Ok(())
}

fn open_schema(connection: &Connection) -> anyhow::Result<()> {
    SCHEMA.open(connection)
}

fn validate_retained(connection: &Connection) -> anyhow::Result<()> {
    let (stored, actual, invalid): (i64,i64,i64) = connection.query_row(
        "SELECT rows,(SELECT count(*) FROM __sys_inbox),
        (SELECT count(*) FROM __sys_inbox WHERE length(digest)!=32) FROM __sys_inbox_stats WHERE singleton=1",
        [], |row|Ok((row.get(0)?,row.get(1)?,row.get(2)?)))?;
    super::event_schema::compatible(
        stored == actual && invalid == 0,
        "inbox receipt accounting conflicts with retained history",
    )
}

fn transaction<T>(
    scope: &str,
    callback: impl FnOnce(&Connection) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    with_application_storage_write(scope, |connection| {
        anyhow::ensure!(
            !connection.is_autocommit(),
            "inbox operations require an active transaction"
        );
        with_storage_batch(connection, |connection| {
            as_framework_sql(|| {
                open_schema(connection)?;
                callback(connection)
            })
        })
    })
}

pub(crate) fn accept(
    scope: &str,
    processing_scope: &str,
    event_id: &str,
    encoded: &[u8],
    capacity: usize,
) -> anyhow::Result<bool> {
    anyhow::ensure!(
        !processing_scope.is_empty()
            && processing_scope.len() <= 128
            && !event_id.is_empty()
            && event_id.len() <= 256,
        "invalid inbox identity"
    );
    let digest = Sha256::digest(encoded);
    let now = crate::asyncrt::wall_ms();
    transaction(scope, |connection| {
        let prior = connection
            .query_row(
                "SELECT digest FROM __sys_inbox WHERE processing_scope=?1 AND event_id=?2",
                rusqlite::params![processing_scope, event_id],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;
        if let Some(prior) = prior {
            if prior.as_slice() != digest.as_slice() {
                return Err(DeliveryError::new(
                    ErrorCode::ConfigConflict,
                    "inbox event identity has different bytes",
                )
                .into());
            }
            return Ok(false);
        }
        let count = connection.query_row(
            "SELECT rows FROM __sys_inbox_stats WHERE singleton=1",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        if count >= i64::try_from(capacity)? {
            return Err(
                DeliveryError::new(ErrorCode::InboxFull, "inbox receipt capacity reached").into(),
            );
        }
        connection.execute(
            "INSERT INTO __sys_inbox VALUES(?1,?2,?3,?4)",
            rusqlite::params![processing_scope, event_id, digest.as_slice(), now],
        )?;
        Ok(true)
    })
}

pub(crate) fn prune(
    scope: &str,
    processing_scope: &str,
    before_ms: i64,
    limit: usize,
) -> anyhow::Result<usize> {
    anyhow::ensure!(
        !processing_scope.is_empty()
            && processing_scope.len() <= 128
            && before_ms >= 0
            && (1..=1000).contains(&limit),
        "invalid inbox pruning range"
    );
    transaction(scope, |connection| {
        Ok(connection.execute("DELETE FROM __sys_inbox WHERE processing_scope=?1 AND event_id IN (
            SELECT event_id FROM __sys_inbox WHERE processing_scope=?1 AND processed_ms<?2 ORDER BY processed_ms,event_id LIMIT ?3)",
            rusqlite::params![processing_scope,before_ms,i64::try_from(limit)?])?)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCOPE: &str = "Inbox:test";

    #[test]
    fn raft_inbox_receipts_share_business_rollback_and_preserve_the_command_authorizer(
    ) -> anyhow::Result<()> {
        use crate::application::event_stream::{Envelope, Record};
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("command.sqlite");
        let event = Envelope {
            id: "v1:lane:1".into(),
            lane: "lane".into(),
            sequence: 1,
            source: "Source:one".into(),
            source_incarnation: "11".repeat(16),
            family: "orders".into(),
            version: 1,
            key: "order".into(),
            created_at_ms: 0,
            payload: serde_json::json!({"value": 7}),
        };
        let record = Record {
            offset: 1,
            encoded: serde_json::to_vec(&event)?,
            event,
        };
        let inputs = CommandInputs {
            now_unix_millis: 1,
            random: [1; 32],
        };
        let mut database = command::CommandDatabase::open(&path, None, 4 * 1024 * 1024)?;
        let mut escaped = None;
        database.transaction(inputs, |transaction| {
            let storage = transaction.storage()?;
            let rolled: anyhow::Result<()> = storage.tx(|tx| {
                assert!(tx.inbox("shipping")?.accept(&record)?);
                tx.put_json("effect", &true.into())?;
                anyhow::bail!("roll back receipt and business effect")
            });
            assert!(rolled.is_err());
            assert!(storage.get_json("effect")?.is_none());
            assert!(storage.inbox("shipping")?.accept(&record)?);
            storage.put_json("effect", &true.into())?;
            assert!(!storage.inbox("shipping")?.accept(&record)?);
            let mut conflict = record.clone();
            conflict.event.payload = false.into();
            conflict.encoded = serde_json::to_vec(&conflict.event)?;
            assert!(storage.inbox("shipping")?.accept(&conflict).is_err());
            // Inbox framework SQL must never replace the private command's
            // stricter authorizer with the ordinary activation authorizer.
            assert!(storage.sql("SELECT random()", &[]).is_err());
            assert!(storage.sql("PRAGMA user_version=7", &[]).is_err());
            assert!(storage.sql("SELECT * FROM __sys_inbox", &[]).is_err());
            escaped = Some(storage.inbox("shipping")?);
            Ok(())
        })?;
        assert!(escaped.as_ref().unwrap().accept(&record).is_err());
        drop(database);
        let mut database = command::CommandDatabase::open(&path, None, 4 * 1024 * 1024)?;
        database.transaction(inputs, |transaction| {
            let storage = transaction.storage()?;
            assert!(!storage.inbox("shipping")?.accept(&record)?);
            assert_eq!(storage.get_json("effect")?, Some(true.into()));
            assert!(escaped.as_ref().unwrap().accept(&record).is_err());
            assert_eq!(storage.inbox("shipping")?.prune_before(i64::MAX, 1)?, 1);
            assert!(storage.inbox("shipping")?.accept(&record)?);
            Ok(())
        })
    }

    #[test]
    fn receipt_and_effect_rollback_and_duplicate_at_capacity_is_safe() -> anyhow::Result<()> {
        let cells = Cells::default();
        let _installed = cells.install();
        let directory = tempfile::tempdir()?;
        let path = directory
            .path()
            .join("inbox.sqlite")
            .to_string_lossy()
            .into_owned();
        open_at_epoch(SCOPE, &path, 1, true, None, false)?;
        assert!(accept(SCOPE, "shipping", "event-one", b"one", 1).is_err());
        transaction_control(SCOPE, "start", false, "cells_tx_1").unwrap();
        assert!(accept(SCOPE, "shipping", "event-one", b"one", 1)?);
        put_json(SCOPE, "effect", &true.into())?;
        transaction_control(SCOPE, "rollback", false, "cells_tx_1").unwrap();
        assert!(get_stored(SCOPE, "effect")?.is_none());
        transaction_control(SCOPE, "start", false, "cells_tx_2").unwrap();
        assert!(accept(SCOPE, "shipping", "event-one", b"one", 1)?);
        put_json(SCOPE, "effect", &true.into())?;
        assert!(!accept(SCOPE, "shipping", "event-one", b"one", 1)?);
        let full = accept(SCOPE, "shipping", "event-two", b"two", 1).unwrap_err();
        assert_eq!(
            full.downcast_ref::<DeliveryError>().unwrap().code,
            ErrorCode::InboxFull
        );
        let conflict = accept(SCOPE, "shipping", "event-one", b"changed", 1).unwrap_err();
        assert_eq!(
            conflict.downcast_ref::<DeliveryError>().unwrap().code,
            ErrorCode::ConfigConflict
        );
        transaction_control(SCOPE, "commit", false, "cells_tx_2").unwrap();
        close(SCOPE);
        open_at_epoch(SCOPE, &path, 2, false, None, false)?;
        assert_eq!(
            with(SCOPE, |c| c.query_row(
                "SELECT version FROM __sys_inbox_schema",
                [],
                |row| row.get::<_, i64>(0)
            ))
            .unwrap()?,
            1
        );
        transaction_control(SCOPE, "start", false, "cells_tx_3").unwrap();
        assert!(!accept(SCOPE, "shipping", "event-one", b"one", 1)?);
        assert_eq!(prune(SCOPE, "other-scope", i64::MAX, 1)?, 0);
        assert_eq!(prune(SCOPE, "shipping", i64::MAX, 1)?, 1);
        assert!(accept(SCOPE, "shipping", "event-two", b"two", 1)?);
        transaction_control(SCOPE, "commit", false, "cells_tx_3").unwrap();
        let count = with(SCOPE, |connection| {
            connection.query_row("SELECT rows FROM __sys_inbox_stats", [], |row| {
                row.get::<_, i64>(0)
            })
        })
        .unwrap()?;
        assert_eq!(count, 1);
        close(SCOPE);
        Ok(())
    }
}
