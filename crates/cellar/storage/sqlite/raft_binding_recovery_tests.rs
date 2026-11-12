// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(clippy::disallowed_methods)]
use super::*;
use cellar_logic::{
    cell_group::CellId,
    consensus::{ClusterId, NamespaceId},
};

fn fixture(phase: u8) -> Result<(Connection, CellHistory, Origin, Key)> {
    let db = Connection::open_in_memory()?;
    super::super::raft_bindings::install(&db)?;
    db.execute_batch("CREATE TABLE business(value); INSERT INTO business VALUES(37)")?;
    let scope = CellHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: CellId::new("Counter:source")?,
        incarnation: IncarnationId::new([4; 16])?,
    };
    let origin = Origin {
        group: GroupHistory {
            cluster: scope.cluster,
            namespace: scope.namespace,
            group: GroupId::new([3; 16])?,
            incarnation: IncarnationId::new([5; 16])?,
        },
        term: 7,
    };
    let key = Key {
        client: [6; 16],
        sequence: 1,
        ordinal: 1,
        fingerprint: [7; 32],
    };
    let mut child_client = [8; 16];
    child_client[..4].copy_from_slice(b"RCB1");
    let body = IntentWire {
        epoch: scope.incarnation.bytes(),
        lane: 0,
        serial: 1,
        source_cluster: scope.cluster.bytes(),
        source_namespace: scope.namespace.bytes(),
        source_group: origin.group.group.bytes(),
        source_group_incarnation: origin.group.incarnation.bytes(),
        source_term: 7,
        source_durability: 2,
        deadline_unix_ms: 1000,
        cluster: scope.cluster.bytes(),
        namespace: scope.namespace.bytes(),
        cell: "Counter:target".into(),
        schema: [9; 32],
        operation: "Add".into(),
        contract: [10; 32],
        durability: 1,
        enroll: true,
        candidate: [11; 16],
        creation: [12; 16],
        child_client,
        child_sequence: 1,
        request_digest: [13; 32],
        retry_until_ms: 60_000,
    };
    db.execute("INSERT INTO __sys_raft_binding_intents VALUES(?1,?2,?3,1,?4,?5,zeroblob(262144),?6,NULL,NULL,1000)",params![key.client.as_slice(),1_u64.to_be_bytes().as_slice(),1_u64.to_be_bytes().as_slice(),key.fingerprint.as_slice(),serde_json::to_vec(&body)?,phase])?;
    db.execute(
        "INSERT INTO __sys_raft_binding_lanes VALUES(?1,0,?2,?3)",
        params![
            body.epoch.as_slice(),
            1_u64.to_be_bytes().as_slice(),
            phase != 3
        ],
    )?;
    db.execute(
        "INSERT INTO __sys_raft_binding_sequences VALUES(?1,0,?2,?3,?4,?5,?6)",
        params![
            body.epoch.as_slice(),
            body.cluster.as_slice(),
            body.namespace.as_slice(),
            body.cell,
            body.candidate.as_slice(),
            1_u64.to_be_bytes().as_slice()
        ],
    )?;
    Ok((db, scope, origin, key))
}

fn busy(db: &Connection) -> Result<bool> {
    Ok(
        db.query_row("SELECT busy FROM __sys_raft_binding_lanes", [], |r| {
            r.get(0)
        })?,
    )
}

fn rows(db: &Connection, table: &str) -> Result<usize> {
    Ok(db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?)
}

#[test]
fn recovery_claim_and_exact_settlement_preserve_business_and_unknown_input() -> Result<()> {
    let (db, scope, origin, key) = fixture(0)?;
    assert!(due(&db, 999)?.is_none());
    let discovered = due(&db, 1000)?.unwrap();
    assert!(serde_json::to_vec(&discovered)?.len() < MAX_METADATA);
    db.execute_batch("BEGIN")?;
    assert!(claim(&db, &scope, origin, Durability::Quorum, 1000, &key).is_err());
    assert!(claim(&db, &scope, origin, Durability::S3, 999, &key).is_err());
    let pending = claim(&db, &scope, origin, Durability::S3, 1000, &key)?.unwrap();
    assert_eq!(
        db.query_row("SELECT phase FROM __sys_raft_binding_intents", [], |r| r
            .get::<_, u8>(0))?,
        4
    );
    db.execute_batch("COMMIT; BEGIN")?;
    let mut settlement = Settlement {
        key: key.clone(),
        digest: [99; 32],
        candidate_lost: false,
    };
    assert!(settle(&db, &scope, origin, Durability::S3, &settlement).is_err());
    assert!(busy(&db)?);
    settlement.digest = pending.digest;
    assert!(settle(&db, &scope, origin, Durability::Quorum, &settlement).is_err());
    settle(&db, &scope, origin, Durability::S3, &settlement)?;
    assert!(!busy(&db)?);
    assert_eq!(rows(&db, "__sys_raft_binding_intents")?, 0);
    assert_eq!(rows(&db, "__sys_raft_binding_sequences")?, 1);
    assert_eq!(
        db.query_row("SELECT value FROM business", [], |r| r.get::<_, u64>(0))?,
        37
    );
    // A replay after this lane is reused cannot release the new serial.
    db.execute(
        "UPDATE __sys_raft_binding_lanes SET sequence=?1,busy=1",
        [2_u64.to_be_bytes().as_slice()],
    )?;
    settle(&db, &scope, origin, Durability::S3, &settlement)?;
    assert!(busy(&db)?);
    db.execute_batch("COMMIT")?;
    Ok(())
}

#[test]
fn recovery_rejects_unclaimed_or_wrong_lineage_and_handles_physical_movement() -> Result<()> {
    let (db, scope, origin, key) = fixture(0)?;
    let pending = due(&db, 1000)?.unwrap();
    db.execute_batch("BEGIN")?;
    let settlement = Settlement {
        key: key.clone(),
        digest: pending.digest,
        candidate_lost: false,
    };
    assert!(settle(&db, &scope, origin, Durability::S3, &settlement).is_err());
    let mut fresh = scope.clone();
    fresh.incarnation = IncarnationId::new([20; 16])?;
    assert!(claim(&db, &fresh, origin, Durability::S3, 1000, &key).is_err());
    assert!(claim(
        &db,
        &scope,
        Origin { term: 6, ..origin },
        Durability::S3,
        1000,
        &key
    )
    .is_err());
    let moved = Origin {
        group: GroupHistory {
            group: GroupId::new([21; 16])?,
            ..origin.group
        },
        term: 1,
    };
    assert!(claim(&db, &scope, moved, Durability::S3, 1000, &key)?.is_some());
    assert!(busy(&db)?);
    db.execute_batch("ROLLBACK")?;
    Ok(())
}

#[test]
fn candidate_retirement_and_outer_failure_do_not_publish_half_settlement() -> Result<()> {
    let (db, scope, origin, key) = fixture(0)?;
    db.execute_batch("BEGIN")?;
    let pending = claim(&db, &scope, origin, Durability::S3, 1000, &key)?.unwrap();
    db.execute_batch("COMMIT; BEGIN")?;
    let settlement = Settlement {
        key: key.clone(),
        digest: pending.digest,
        candidate_lost: true,
    };
    db.execute(
        "UPDATE __sys_raft_binding_lanes SET sequence=?1",
        [2_u64.to_be_bytes().as_slice()],
    )?;
    assert!(settle(&db, &scope, origin, Durability::S3, &settlement).is_err());
    db.execute_batch("ROLLBACK")?;
    assert_eq!(rows(&db, "__sys_raft_binding_sequences")?, 1);
    assert_eq!(rows(&db, "__sys_raft_binding_intents")?, 1);
    assert!(busy(&db)?);
    db.execute_batch("BEGIN")?;
    settle(&db, &scope, origin, Durability::S3, &settlement)?;
    db.execute_batch("COMMIT")?;
    assert_eq!(rows(&db, "__sys_raft_binding_sequences")?, 0);
    assert!(!busy(&db)?);
    Ok(())
}

#[test]
fn orphaned_resolved_intent_cleanup_never_releases_a_reused_lane() -> Result<()> {
    let (db, scope, origin, key) = fixture(3)?;
    db.execute(
        "UPDATE __sys_raft_binding_lanes SET sequence=?1,busy=1",
        [2_u64.to_be_bytes().as_slice()],
    )?;
    db.execute_batch("BEGIN")?;
    assert!(claim(&db, &scope, origin, Durability::S3, 1000, &key)?.is_none());
    db.execute_batch("COMMIT")?;
    assert!(busy(&db)?);
    assert_eq!(rows(&db, "__sys_raft_binding_intents")?, 0);
    assert_eq!(rows(&db, "__sys_raft_binding_sequences")?, 1);
    Ok(())
}
