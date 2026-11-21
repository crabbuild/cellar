// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::storage::{CommandDatabase, CommandInputs};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

const INPUTS: CommandInputs = CommandInputs {
    now_unix_millis: 1000,
    random: [9; 32],
};

fn setup(database: &mut CommandDatabase) -> Result<()> {
    database.transaction(INPUTS, |tx| {
        let storage = tx.storage()?;
        storage.system_sql_schema(include_str!("schema.sql"))?;
        storage.system_sql(
            "INSERT INTO __sys_queue_messages
             (id,body,content_type,size,enqueued_at,visible_at,dlq_target,dlq_transfer_id,transfer_kind)
             VALUES ('transfer-message',X'0102','bytes',2,1000,1000,'destination','original-transfer','redrive')",
            &[],
        )?;
        Ok(())
    })
}

#[test]
#[allow(clippy::disallowed_methods)]
fn committed_transfer_claim_survives_cancel_and_reopen_then_expires() -> Result<()> {
    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    eprintln!("retained transfer-claim disk: {}", root.path().display());
    let path = root.path().join("transfer.sqlite");
    let mut database = CommandDatabase::open(&path, None, 16 * 1024 * 1024)?;
    setup(&mut database)?;
    let active = database.begin(INPUTS, 0)?;
    let original = active.turn(|tx| {
        let storage = tx.storage()?;
        let rows = pending(&storage, 0, 1000, 100)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(claim(&storage, &[&rows[0]], 1000, 4000)?.len(), 1);
        assert_eq!(next_retry(&storage, 1000)?, Some(4000));
        // A resumed competing writer cannot claim an already selected row.
        assert!(claim(&storage, &[&rows[0]], 1001, 4001)?.is_empty());
        Ok(rows)
    })?;
    // Exercise the actual SQL checkpoint/park/cancel boundary. Consensus proof
    // and native delivery remain separate RF3 integration qualifications.
    database.checkpoint(&active)?;
    database.continue_checkpoint(&active)?;
    database.park(&active)?;
    active.close_parked()?;
    drop(database);

    let mut database = CommandDatabase::open(&path, None, 16 * 1024 * 1024)?;
    database.transaction(INPUTS, |tx| {
        let storage = tx.storage()?;
        assert!(pending(&storage, 0, 3999, 100)?.is_empty());
        assert_eq!(next_retry(&storage, 3999)?, Some(4000));
        let rows = pending(&storage, 0, 4000, 100)?;
        assert_eq!(
            rows, original,
            "claim must preserve the transfer and payload"
        );
        assert_eq!(claim(&storage, &[&rows[0]], 4000, 7000)?.len(), 1);
        // A late successful response still names this exact same transfer.
        delivered(&storage, &[&original[0]])?;
        assert_eq!(next_retry(&storage, 4000)?, None);
        Ok(())
    })
}

#[test]
#[allow(clippy::disallowed_methods)]
fn rolled_back_and_stale_transfer_claims_cannot_hide_or_delete_successors() -> Result<()> {
    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    eprintln!("retained transfer-claim disk: {}", root.path().display());
    let mut database =
        CommandDatabase::open(&root.path().join("rollback.sqlite"), None, 16 * 1024 * 1024)?;
    setup(&mut database)?;
    let active = database.begin(INPUTS, 0)?;
    let original = active.turn(|tx| {
        let storage = tx.storage()?;
        let rows = pending(&storage, 0, 1000, 100)?;
        assert_eq!(claim(&storage, &[&rows[0]], 1000, 4000)?.len(), 1);
        Ok(rows)
    })?;
    database.abort(active)?;
    database.transaction(INPUTS, |tx| {
        let storage = tx.storage()?;
        assert_eq!(pending(&storage, 0, 1000, 100)?, original);
        for update in [
            "UPDATE __sys_queue_messages SET dlq_transfer_id='successor-transfer'",
            "UPDATE __sys_queue_messages SET dlq_transfer_id='original-transfer',dlq_target='other-destination'",
            "UPDATE __sys_queue_messages SET dlq_target='destination',transfer_kind='dead-letter'",
        ] {
            storage.system_sql(update, &[])?;
            assert!(claim(&storage, &[&original[0]], 1000, 4000)?.is_empty());
            delivered(&storage, &[&original[0]])?;
            assert_eq!(pending(&storage, 0, 1000, 100)?.len(), 1);
        }
        assert_eq!(next_retry(&storage, 1000)?, Some(1000));
        Ok(())
    })
}

#[test]
fn transfer_retry_index_bounds_future_claims_without_sorting_backlog() -> Result<()> {
    let connection = rusqlite::Connection::open_in_memory()?;
    connection.execute_batch(include_str!("schema.sql"))?;
    for (query, params) in [
        (PENDING_RAFT, vec![0_i64, 1000, 100]),
        (NEXT_RETRY_RAFT, vec![]),
    ] {
        let plan = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {query}"))?
            .query_map(rusqlite::params_from_iter(params), |row| {
                row.get::<_, String>(3)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        assert!(plan
            .iter()
            .any(|detail| detail.contains("__sys_queue_messages_moving_due")));
        assert!(
            !plan.iter().any(|detail| detail.contains("TEMP B-TREE")),
            "{plan:?}"
        );
        if query == PENDING_RAFT {
            assert!(
                plan.iter().any(|detail| detail.contains("SEARCH")),
                "{plan:?}"
            );
        }
    }
    Ok(())
}

#[test]
#[allow(clippy::disallowed_methods)]
fn newly_ready_transfers_cannot_displace_expired_retries_or_scan_future_backlog() -> Result<()> {
    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    eprintln!("retained transfer-fairness disk: {}", root.path().display());
    let mut database =
        CommandDatabase::open(&root.path().join("fairness.sqlite"), None, 16 * 1024 * 1024)?;
    setup(&mut database)?;
    database.transaction(INPUTS, |tx| {
        let storage = tx.storage()?;
        storage.system_sql("UPDATE __sys_queue_messages SET leased_until=900", &[])?;
        for n in 0..100 {
            storage.system_sql(
                "INSERT INTO __sys_queue_messages
                 (id,body,content_type,size,enqueued_at,visible_at,dlq_target,dlq_transfer_id,transfer_kind,leased_until)
                 VALUES (?,X'00','bytes',1,1000,1000,'destination',?,'redrive',?)",
                &[format!("ready-message-{n}").into(),format!("ready-transfer-{n}").into(),ready_at(&storage,1000)],
            )?;
        }
        Ok(())
    })?;
    let mut observed_steps = Vec::new();
    for (previous, count) in [(0, 512), (512, 4096)] {
        database.transaction(INPUTS, |tx| {
            let storage = tx.storage()?;
            for n in previous..count {
                storage.system_sql(
                    "INSERT INTO __sys_queue_messages
                     (id,body,content_type,size,enqueued_at,visible_at,dlq_target,dlq_transfer_id,transfer_kind,leased_until)
                     VALUES (?,X'00','bytes',1,1000,1000,'destination',?,'redrive',5000)",
                    &[format!("future-message-{n}").into(),format!("future-transfer-{n}").into()],
                )?;
            }
            Ok(())
        })?;
        // Warm the same owning call before measuring VM steps; elapsed wall
        // time and the compiler's SQLite plan shape are separate observations.
        database.transaction(INPUTS, |tx| pending(&tx.storage()?, 0, 1000, 100))?;
        let steps = Arc::new(AtomicUsize::new(0));
        let measuring = steps.clone();
        database.framework().progress_handler(
            1,
            Some(move || {
                measuring.fetch_add(1, Ordering::Relaxed);
                false
            }),
        )?;
        let selected = database.transaction(INPUTS, |tx| pending(&tx.storage()?, 0, 1000, 100));
        database
            .framework()
            .progress_handler(0, None::<fn() -> bool>)?;
        let selected = selected?;
        assert_eq!(
            selected.len(),
            100,
            "the original transfer batch bound changed"
        );
        assert!(
            selected
                .iter()
                .any(|row| number(row, "seq").ok() == Some(1)),
            "newly ready transfers displaced the older expired retry"
        );
        assert!(
            selected
                .iter()
                .all(|row| number(row, "seq").is_ok_and(|seq| seq <= 101)),
            "an unexpired claim entered transfer selection"
        );
        observed_steps.push(steps.load(Ordering::Relaxed));
    }
    assert!(
        observed_steps[1] <= observed_steps[0] + 256,
        "future claims forced a backlog walk: {observed_steps:?}"
    );
    eprintln!(
        "QUEUE_TRANSFER_FAIRNESS steps_512={} steps_4096={}",
        observed_steps[0], observed_steps[1]
    );
    Ok(())
}
