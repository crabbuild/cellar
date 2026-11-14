// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn raft_framework_work_is_atomic_generation_fenced_and_retained() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("work.sqlite");
    let inputs = CommandInputs {
        now_unix_millis: 1,
        random: [1; 32],
    };
    let mut database = command::CommandDatabase::open(&path, None, 4 * 1024 * 1024)?;
    let old = database.transaction(inputs, |transaction| {
        let storage = transaction.storage()?;
        storage
            .raft_command_authority()?
            .resources
            .durable_callbacks
            .store(true, Ordering::Release);
        assert!(!storage.clear_work(Installation {
            reason: Reason::Outbox,
            generation: 1,
            at_ms: 10
        })?);
        let rolled: anyhow::Result<()> = storage.tx(|tx| {
            tx.schedule_work(Reason::Outbox, 10)?;
            anyhow::bail!("rollback work and discovery")
        });
        assert!(rolled.is_err());
        assert!(storage.work_installation(Reason::Outbox)?.is_none());
        assert!(storage.schedule_work(Reason::SandboxCleanup, 10).is_err());
        storage.schedule_work(Reason::Outbox, 10)?;
        let old = storage.work_installation(Reason::Outbox)?.unwrap();
        storage.schedule_work(Reason::Outbox, 10)?;
        assert!(!storage.clear_work(old)?);
        let current = storage.work_installation(Reason::Outbox)?.unwrap();
        assert!(storage.clear_work(current)?);
        assert!(!storage.clear_work(current)?);
        for (reason, deadline) in [
            (Reason::Outbox, 30),
            (Reason::StreamMaintenance, 20),
            (Reason::ResourceReconciliation, 40),
        ] {
            storage.schedule_work(reason, deadline)?;
        }
        let installed = storage.work_installation(Reason::Outbox)?.unwrap();
        storage.system_sql(
            "UPDATE __sys_work SET generation=? WHERE reason='outbox'",
            &[i64::MAX.into()],
        )?;
        let before =
            raft_native_work(|db| crate::application::consensus::native_work::due(db, 40))?
                .unwrap();
        assert!(storage.schedule_work(Reason::Outbox, 1).is_err());
        assert_eq!(
            storage.work_installation(Reason::Outbox)?.unwrap().at_ms,
            installed.at_ms
        );
        assert_eq!(
            raft_native_work(|db| crate::application::consensus::native_work::due(db, 40))?
                .unwrap()
                .generation,
            before.generation
        );
        storage.system_sql(
            "UPDATE __sys_work SET generation=? WHERE reason='outbox'",
            &[i64::try_from(installed.generation)?.into()],
        )?;
        assert!(storage.sql("SELECT random()", &[]).is_err());
        assert!(storage.sql("SELECT * FROM __sys_work", &[]).is_err());
        Ok(old)
    })?;
    drop(database);
    let mut database = command::CommandDatabase::open(&path, None, 4 * 1024 * 1024)?;
    database.transaction(inputs, |transaction| {
        let storage = transaction.storage()?;
        assert!(!storage.clear_work(old)?);
        assert_eq!(
            storage.work_installation(Reason::Outbox)?.unwrap().at_ms,
            30
        );
        assert_eq!(
            raft_native_work(|db| crate::application::consensus::native_work::due(db, 40))?
                .unwrap()
                .reason,
            "stream-maintenance"
        );
        Ok(())
    })
}
