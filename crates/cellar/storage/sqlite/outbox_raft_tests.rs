// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::storage::{OutboxOptions, SqlValue};
use cellar_logic::{
    cell_group::{CellHistory, CellId},
    consensus::{ClusterId, IncarnationId, NamespaceId},
    work::Reason,
};

fn history(incarnation: u8) -> anyhow::Result<CellHistory> {
    Ok(CellHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: CellId::new("Order:123")?,
        incarnation: IncarnationId::new([incarnation; 16])?,
    })
}

fn binding() -> Binding {
    Binding {
        environment: "ORDERS".into(),
        resource: crate::application::event_stream::Resource {
            tenant: "tenant".into(),
            application: "shop".into(),
            id: "00000000000000000000000000000001".into(),
            name: "orders".into(),
            family: "orders".into(),
        },
        principal: "shop".into(),
        grant: "publisher".into(),
        publish: true,
        read: false,
        subscriptions: vec![],
        administer: false,
    }
}

fn event(history: &CellHistory, value: i64) -> anyhow::Result<()> {
    emit_raft(
        &binding(),
        1,
        "order".into(),
        serde_json::json!({"value":value}),
        Limits::default(),
        history,
    )
}

fn inputs() -> CommandInputs {
    CommandInputs {
        now_unix_millis: 1,
        random: [1; 32],
    }
}

#[test]
fn raft_outbox_rolls_back_state_sequences_and_discovery_without_replacing_authorizer(
) -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("source.sqlite");
    let mut database = command::CommandDatabase::open(&path, None, 4 * 1024 * 1024)?;
    let source = history(3)?;
    let mut escaped = None;
    database.transaction(inputs(), |transaction| {
        let storage = transaction.storage()?;
        let rolled: anyhow::Result<()> = storage.tx(|tx| {
            event(&source, 1)?;
            tx.put_json("business", &true.into())?;
            anyhow::bail!("roll back business, event, sequence and wake")
        });
        assert!(rolled.is_err());
        assert!(storage.get_json("business")?.is_none());
        assert_eq!(storage.outbox(&OutboxOptions::default())?.pending_rows, 0);
        assert!(work::raft_current(RAFT_STORAGE_SCOPE, Reason::Outbox)?.is_none());
        assert!(
            raft_native_work(|db| crate::application::consensus::native_work::due(
                db,
                u64::MAX >> 1
            ))?
            .is_none()
        );
        event(&source, 1)?;
        event(&source, 2)?;
        storage.put_json("business", &true.into())?;
        let page = storage.outbox(&OutboxOptions::default())?;
        assert_eq!(page.pending_rows, 2);
        assert_eq!(page.lanes.len(), 1);
        assert_eq!(page.lanes[0].revision.next_sequence, 3);
        assert_eq!(page.lanes[0].revision.source, source.cell.as_str());
        assert_eq!(
            page.lanes[0].revision.incarnation,
            raft_incarnation(&source)
        );
        assert!(work::raft_current(RAFT_STORAGE_SCOPE, Reason::Outbox)?.is_some());
        assert!(storage.sql("SELECT random()", &[]).is_err());
        assert!(storage.sql("PRAGMA user_version=7", &[]).is_err());
        assert!(storage.sql("SELECT * FROM __sys_outbox", &[]).is_err());
        escaped = Some(storage);
        Ok(())
    })?;
    assert!(escaped
        .as_ref()
        .unwrap()
        .outbox(&OutboxOptions::default())
        .is_err());
    drop(database);
    let mut database = command::CommandDatabase::open(&path, None, 4 * 1024 * 1024)?;
    database.transaction(inputs(), |transaction| {
        let storage = transaction.storage()?;
        assert!(escaped
            .as_ref()
            .unwrap()
            .outbox(&OutboxOptions::default())
            .is_err());
        assert_eq!(storage.get_json("business")?, Some(true.into()));
        let page = storage.outbox(&OutboxOptions::default())?;
        assert_eq!(page.pending_rows, 2);
        let bytes =
            storage.system_sql("SELECT envelope FROM __sys_outbox ORDER BY sequence", &[])?;
        for (index, row) in bytes.iter().enumerate() {
            let SqlValue::Blob(bytes) = &row["envelope"] else {
                anyhow::bail!("event bytes");
            };
            let envelope: Envelope = serde_json::from_slice(bytes)?;
            assert_eq!(envelope.source, source.cell.as_str());
            assert_eq!(envelope.sequence, index as i64 + 1);
            assert_eq!(envelope.payload["value"], index as i64 + 1);
        }
        Ok(())
    })
}

#[test]
fn fresh_raft_history_preserves_old_event_bytes_and_opens_a_new_producer_lane() -> anyhow::Result<()>
{
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("source.sqlite");
    let mut database = command::CommandDatabase::open(&path, None, 4 * 1024 * 1024)?;
    let old = history(3)?;
    let fresh = history(4)?;
    let prior = database.transaction(inputs(), |transaction| {
        let storage = transaction.storage()?;
        event(&old, 1)?;
        let page = storage.outbox(&OutboxOptions::default())?;
        Ok((
            page.lanes[0].revision.clone(),
            storage.system_sql("SELECT envelope FROM __sys_outbox", &[])?,
        ))
    })?;
    drop(database);
    let mut database = command::CommandDatabase::open(&path, None, 4 * 1024 * 1024)?;
    database.transaction(inputs(), |transaction| {
        let storage = transaction.storage()?;
        let limited = Limits {
            max_outbox_rows: 1,
            ..Limits::default()
        };
        let refused = emit_raft(
            &binding(),
            1,
            "order".into(),
            serde_json::json!({"value":2}),
            limited,
            &fresh,
        );
        assert!(refused.is_err());
        assert_eq!(
            storage
                .outbox(&OutboxOptions::default())?
                .incarnation
                .as_deref(),
            Some(prior.0.incarnation.as_str())
        );
        event(&fresh, 2)?;
        let page = storage.outbox(&OutboxOptions::default())?;
        assert_eq!(page.incarnation, Some(raft_incarnation(&fresh)));
        assert_eq!(page.lanes.len(), 2);
        assert_eq!(page.pending_rows, 2);
        let retained = page
            .lanes
            .iter()
            .find(|lane| lane.revision.incarnation == prior.0.incarnation)
            .unwrap();
        assert_eq!(retained.revision.lane, prior.0.lane);
        let newer = page
            .lanes
            .iter()
            .find(|lane| lane.revision.incarnation != prior.0.incarnation)
            .unwrap();
        assert_ne!(newer.revision.lane, prior.0.lane);
        assert_eq!(newer.revision.next_sequence, 2);
        assert_eq!(
            storage.system_sql(
                "SELECT envelope FROM __sys_outbox WHERE lane=?",
                &[prior.0.lane.clone().into()]
            )?,
            prior.1
        );
        assert!(storage.retry_outbox_lane(&prior.0)?);
        assert!(storage.retry_outbox_lane(&prior.0).is_err());
        let mut foreign = fresh.clone();
        foreign.cell = CellId::new("Order:other")?;
        assert!(event(&foreign, 3).is_err());
        for foreign in [
            CellHistory {
                cluster: ClusterId::new([9; 16])?,
                ..fresh.clone()
            },
            CellHistory {
                namespace: NamespaceId::new([9; 16])?,
                ..fresh.clone()
            },
        ] {
            assert_ne!(raft_incarnation(&foreign), raft_incarnation(&fresh));
            assert!(event(&foreign, 3).is_err());
        }
        assert_eq!(storage.outbox(&OutboxOptions::default())?.pending_rows, 2);
        assert!(seal_raft(&binding(), &"11".repeat(16), &history(5)?).is_err());
        Ok(())
    })
}

#[test]
fn old_raft_seal_keeps_its_lane_incarnation_after_fresh_history_writes() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut database = command::CommandDatabase::open(
        &directory.path().join("source.sqlite"),
        None,
        4 * 1024 * 1024,
    )?;
    database.transaction(inputs(), |transaction| {
        let storage = transaction.storage()?;
        event(&history(3)?, 1)?;
        storage.system_sql("DELETE FROM __sys_outbox", &[])?;
        storage.system_sql("UPDATE __sys_outbox_lanes SET settled_through=1", &[])?;
        let seal = seal_raft(&binding(), &"11".repeat(16), &history(3)?)?;
        event(&history(4)?, 2)?;
        let rows = storage.system_sql(
            "SELECT incarnation FROM __sys_outbox_lanes WHERE lane=?",
            &[seal.lane.into()],
        )?;
        assert_eq!(
            rows[0]["incarnation"],
            raft_incarnation(&history(3)?).into()
        );
        let page = storage.outbox(&OutboxOptions::default())?;
        assert!(page
            .lanes
            .iter()
            .any(|lane| lane.seal_state.as_deref() == Some("pending")
                && lane.revision.incarnation == raft_incarnation(&history(3).unwrap())));
        Ok(())
    })
}

#[test]
fn legacy_outbox_layout_refuses_without_resetting_source_or_creating_lanes() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch("CREATE TABLE __sys_outbox_schema(singleton INTEGER PRIMARY KEY,version INTEGER NOT NULL); INSERT INTO __sys_outbox_schema VALUES(1,1); CREATE TABLE __sys_event_source(singleton INTEGER PRIMARY KEY,incarnation TEXT NOT NULL,protocol INTEGER NOT NULL); INSERT INTO __sys_event_source VALUES(1,'11111111111111111111111111111111',1);")?;
    let before: i64 =
        connection.query_row("SELECT count(*) FROM sqlite_schema", [], |row| row.get(0))?;
    let error = restore_schema(&connection).unwrap_err();
    assert_eq!(
        error.downcast_ref::<DeliveryError>().unwrap().code,
        ErrorCode::UnsupportedProtocol
    );
    assert_eq!(
        connection.query_row("SELECT count(*) FROM sqlite_schema", [], |row| row
            .get::<_, i64>(0))?,
        before
    );
    assert_eq!(
        connection.query_row("SELECT incarnation FROM __sys_event_source", [], |row| {
            row.get::<_, String>(0)
        })?,
        "11".repeat(16)
    );
    Ok(())
}
