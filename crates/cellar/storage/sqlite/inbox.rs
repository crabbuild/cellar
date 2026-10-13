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
    optional: &[],
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
        without_sql_authorizer(connection, || {
            connection.execute_batch("SAVEPOINT cellar_inbox;")?;
            let result = (|| {
                open_schema(connection)?;
                callback(connection)
            })();
            if result.is_err() {
                connection.execute_batch("ROLLBACK TO cellar_inbox;")?;
            }
            connection.execute_batch("RELEASE cellar_inbox;")?;
            result
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
        {
            let connection = Connection::open(&path)?;
            connection.execute_batch("DROP TABLE __sys_inbox_schema;")?;
        }
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
