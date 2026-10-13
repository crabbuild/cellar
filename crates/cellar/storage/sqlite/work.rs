//! Internal durable deadlines. Clearing a reason retains its generation so a
//! delayed completion cannot consume a replacement installation.

use super::*;
use cellar_logic::work::{Installation, Reason};

pub(super) const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS __sys_work (
    scope TEXT NOT NULL,
    reason TEXT NOT NULL CHECK(reason IN (
        'outbox', 'stream-maintenance', 'resource-reconciliation'
    )),
    generation INTEGER NOT NULL CHECK(
        typeof(generation) = 'integer' AND generation > 0
    ),
    at_ms INTEGER CHECK(at_ms IS NULL OR (
        typeof(at_ms) = 'integer' AND at_ms >= 0
    )),
    PRIMARY KEY(scope, reason)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS __sys_work_due ON __sys_work(scope, at_ms, reason)
    WHERE at_ms IS NOT NULL;
";

pub(crate) fn set(scope: &str, reason: Reason, at_ms: i64) -> anyhow::Result<Option<i64>> {
    anyhow::ensure!(at_ms >= 0, "internal work deadline must be nonnegative");
    with_application_storage_write(scope, |connection| {
        connection.execute(
            "INSERT INTO __sys_work(scope,reason,generation,at_ms)
             VALUES (?1,?2,1,?3)
             ON CONFLICT(scope,reason) DO UPDATE SET
                generation=__sys_work.generation+1, at_ms=excluded.at_ms",
            rusqlite::params![scope, reason.as_str(), at_ms],
        )?;
        Ok(())
    })?;
    Ok(publish_alarm(scope))
}

pub(crate) fn clear(
    scope: &str,
    installation: Installation,
) -> anyhow::Result<(bool, Option<i64>)> {
    let generation = i64::try_from(installation.generation)?;
    let cleared = with_application_storage_write(scope, |connection| {
        Ok(connection.execute(
            "UPDATE __sys_work SET at_ms=NULL
             WHERE scope=?1 AND reason=?2 AND generation=?3 AND at_ms=?4",
            rusqlite::params![
                scope,
                installation.reason.as_str(),
                generation,
                installation.at_ms
            ],
        )? == 1)
    })?;
    Ok((cleared, cleared.then(|| publish_alarm(scope)).flatten()))
}

pub(crate) fn due(scope: &str, now_ms: i64) -> anyhow::Result<Option<Installation>> {
    with(scope, |connection| {
        anyhow::ensure!(
            connection.is_autocommit(),
            "internal work requires a committed transaction"
        );
        ensure_no_unfinished_write_cursor(connection)?;
        connection
            .query_row(
                "SELECT reason,generation,at_ms FROM __sys_work
                 WHERE scope=?1 AND at_ms<=?2 ORDER BY at_ms,reason LIMIT 1",
                rusqlite::params![scope, now_ms],
                |row| {
                    let reason: String = row.get(0)?;
                    let reason = Reason::parse(&reason).ok_or_else(|| {
                        rusqlite::Error::InvalidColumnType(
                            0,
                            "reason".into(),
                            rusqlite::types::Type::Text,
                        )
                    })?;
                    Ok(Installation {
                        reason,
                        generation: row.get(1)?,
                        at_ms: row.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

pub(crate) fn installation(scope: &str, reason: Reason) -> anyhow::Result<Option<Installation>> {
    with(scope, |connection| {
        anyhow::ensure!(
            connection.is_autocommit(),
            "internal work requires a committed transaction"
        );
        ensure_no_unfinished_write_cursor(connection)
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))?;
    current(scope, reason)
}

pub(crate) fn current(scope: &str, reason: Reason) -> anyhow::Result<Option<Installation>> {
    with(scope, |connection| {
        connection.query_row(
            "SELECT generation,at_ms FROM __sys_work WHERE scope=?1 AND reason=?2 AND at_ms IS NOT NULL",
            rusqlite::params![scope,reason.as_str()],
            |row|Ok(Installation { reason, generation:row.get(0)?, at_ms:row.get(1)? }),
        ).optional().map_err(Into::into)
    }).unwrap_or_else(||Err(anyhow::anyhow!("no db for {scope}")))
}

pub(super) fn effective(connection: &Connection, scope: &str) -> anyhow::Result<Option<i64>> {
    Ok(connection.query_row(
        "SELECT min(at_ms) FROM (
           SELECT at_ms FROM __sys_cell_alarm WHERE scope=?1
           UNION ALL
           SELECT at_ms FROM __sys_work WHERE scope=?1 AND at_ms IS NOT NULL
         )",
        [scope],
        |row| row.get(0),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCOPE: &str = "Work:source";

    fn open_cell(directory: &tempfile::TempDir, epoch: u64) -> anyhow::Result<String> {
        let path = directory
            .path()
            .join("cell.sqlite")
            .to_string_lossy()
            .into_owned();
        open_at_epoch(SCOPE, &path, epoch, true, None, false)?;
        Ok(path)
    }

    #[test]
    fn reasons_preserve_user_alarm_and_stale_completion_cannot_clear_replacement(
    ) -> anyhow::Result<()> {
        let cells = Cells::default();
        let _installed = cells.install();
        let directory = tempfile::tempdir()?;
        open_cell(&directory, 1)?;
        set_alarm(SCOPE, 300)?;
        assert_eq!(set(SCOPE, Reason::Outbox, 100)?, Some(100));
        let first = due(SCOPE, 100)?.unwrap();
        let prior = alarm_snapshot(SCOPE)?.source().unwrap().id;
        set(SCOPE, Reason::StreamMaintenance, 100)?;
        assert!(alarm_snapshot(SCOPE)?.source().unwrap().id > prior);
        assert_eq!(get_alarm(SCOPE), Some(300));
        set(SCOPE, Reason::Outbox, 200)?;
        assert!(!clear(SCOPE, first)?.0);
        let maintenance = due(SCOPE, 100)?.unwrap();
        assert_eq!(maintenance.reason, Reason::StreamMaintenance);
        assert_eq!(clear(SCOPE, maintenance)?, (true, Some(200)));
        let second = due(SCOPE, 200)?.unwrap();
        assert!(second.generation > first.generation);
        delete_alarm(SCOPE)?;
        assert_eq!(get_alarm(SCOPE), None);
        assert_eq!(alarm_snapshot(SCOPE)?.at_ms(), Some(200));
        assert_eq!(clear(SCOPE, second)?, (true, Some(-1)));
        assert!(due(SCOPE, i64::MAX)?.is_none());
        set(SCOPE, Reason::Outbox, 400)?;
        assert!(due(SCOPE, 400)?.unwrap().generation > second.generation);
        assert!(!clear(SCOPE, second)?.0);
        close(SCOPE);
        Ok(())
    }

    #[test]
    fn work_and_wake_identity_follow_root_and_nested_rollback() -> anyhow::Result<()> {
        let cells = Cells::default();
        let _installed = cells.install();
        let directory = tempfile::tempdir()?;
        open_cell(&directory, 1)?;
        set_alarm(SCOPE, 500)?;
        let prior = alarm_snapshot(SCOPE)?.source().unwrap().id;
        transaction_control(SCOPE, "start", false, "cells_tx_1").unwrap();
        assert_eq!(set(SCOPE, Reason::Outbox, 100)?, None);
        assert!(due(SCOPE, 100).is_err());
        assert!(alarm_snapshot(SCOPE).is_err());
        transaction_control(SCOPE, "start", true, "cells_tx_2").unwrap();
        set(SCOPE, Reason::StreamMaintenance, 50)?;
        transaction_control(SCOPE, "commit", true, "cells_tx_2").unwrap();
        transaction_control(SCOPE, "rollback", false, "cells_tx_1").unwrap();
        assert_eq!(alarm_snapshot(SCOPE)?.at_ms(), Some(500));
        assert_eq!(alarm_snapshot(SCOPE)?.source().unwrap().id, prior);
        assert!(due(SCOPE, 500)?.is_none());
        transaction_control(SCOPE, "start", false, "cells_tx_3").unwrap();
        set(SCOPE, Reason::Outbox, 100)?;
        transaction_control(SCOPE, "start", true, "cells_tx_4").unwrap();
        set(SCOPE, Reason::StreamMaintenance, 50)?;
        transaction_control(SCOPE, "rollback", true, "cells_tx_4").unwrap();
        assert_eq!(
            transaction_control(SCOPE, "commit", false, "cells_tx_3").unwrap(),
            Some(100)
        );
        let moves = take_alarm_moves();
        assert_eq!(moves.len(), 1);
        assert_eq!(moves[0].1.at_ms(), Some(100));
        assert_eq!(due(SCOPE, 100)?.unwrap().reason, Reason::Outbox);
        assert_eq!(get_alarm(SCOPE), Some(500));
        close(SCOPE);
        Ok(())
    }

    #[test]
    fn restore_and_reset_preserve_internal_obligations_and_generations() -> anyhow::Result<()> {
        let cells = Cells::default();
        let _installed = cells.install();
        let directory = tempfile::tempdir()?;
        let path = open_cell(&directory, 1)?;
        set(SCOPE, Reason::Outbox, 100)?;
        let original = due(SCOPE, 100)?.unwrap();
        set_alarm(SCOPE, 200)?;
        assert_eq!(persisted_alarm(&path, SCOPE, None).unwrap().0, 100);
        delete_all_with_alarm(SCOPE, true)?;
        assert!(get_alarm(SCOPE).is_none());
        assert_eq!(due(SCOPE, 100)?, Some(original));
        close(SCOPE);
        assert_eq!(persisted_alarm(&path, SCOPE, None).unwrap().0, 100);
        open_cell(&directory, 2)?;
        assert_eq!(due(SCOPE, 100)?, Some(original));
        assert_eq!(alarm_snapshot(SCOPE)?.source().unwrap().id.epoch, 2);
        clear(SCOPE, original)?;
        close(SCOPE);
        open_cell(&directory, 3)?;
        set(SCOPE, Reason::Outbox, 300)?;
        assert!(due(SCOPE, 300)?.unwrap().generation > original.generation);
        assert!(!clear(SCOPE, original)?.0);
        close(SCOPE);
        Ok(())
    }

    #[test]
    fn application_alarm_consumption_keeps_framework_work_armed() -> anyhow::Result<()> {
        let cells = Cells::default();
        let _installed = cells.install();
        let directory = tempfile::tempdir()?;
        open_cell(&directory, 1)?;
        set_alarm(SCOPE, 100)?;
        set(SCOPE, Reason::Outbox, 100)?;
        begin_alarm_handler(SCOPE, 100);
        assert!(get_alarm(SCOPE).is_none());
        finish_alarm_handler(SCOPE, true, 100);
        assert!(get_alarm(SCOPE).is_none());
        assert_eq!(alarm_snapshot(SCOPE)?.at_ms(), Some(100));
        assert_eq!(due(SCOPE, 100)?.unwrap().reason, Reason::Outbox);
        close(SCOPE);
        Ok(())
    }

    #[test]
    fn generation_exhaustion_and_invalid_deadline_leave_prior_obligation_intact(
    ) -> anyhow::Result<()> {
        let cells = Cells::default();
        let _installed = cells.install();
        let directory = tempfile::tempdir()?;
        open_cell(&directory, 1)?;
        set(SCOPE, Reason::Outbox, 100)?;
        system_sql(
            SCOPE,
            "UPDATE __sys_work SET generation=? WHERE scope=? AND reason='outbox'",
            &[i64::MAX.into(), SCOPE.into()],
        )?;
        let original = due(SCOPE, 100)?.unwrap();
        assert!(set(SCOPE, Reason::Outbox, 200).is_err());
        assert!(set(SCOPE, Reason::Outbox, -1).is_err());
        assert_eq!(due(SCOPE, 100)?, Some(original));
        assert_eq!(alarm_snapshot(SCOPE)?.at_ms(), Some(100));
        close(SCOPE);
        Ok(())
    }

    #[test]
    fn legacy_alarm_projection_is_upgraded_without_reusing_publication_identity(
    ) -> anyhow::Result<()> {
        let cells = Cells::default();
        let _installed = cells.install();
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("cell.sqlite");
        let connection = Connection::open(&path)?;
        schema(&connection)?;
        connection.execute_batch(
            "CREATE TABLE __sys_cell_wake(scope TEXT PRIMARY KEY,epoch TEXT,sequence INTEGER,at_ms INTEGER);
             INSERT INTO __sys_cell_alarm(scope,at_ms) VALUES('Work:source',200);
             INSERT INTO __sys_cell_wake VALUES('Work:source','0000000000000001',42,200);
             CREATE TRIGGER __sys_cell_wake_delete AFTER DELETE ON __sys_cell_alarm BEGIN
               UPDATE __sys_cell_wake SET sequence=sequence+1,at_ms=NULL WHERE scope=OLD.scope;
             END;"
        )?;
        drop(connection);
        open_cell(&directory, 2)?;
        assert_eq!(alarm_snapshot(SCOPE)?.source().unwrap().id.sequence, 42);
        set(SCOPE, Reason::Outbox, 100)?;
        delete_alarm(SCOPE)?;
        assert_eq!(alarm_snapshot(SCOPE)?.at_ms(), Some(100));
        assert!(alarm_snapshot(SCOPE)?.source().unwrap().id.sequence > 42);
        close(SCOPE);
        Ok(())
    }
}
