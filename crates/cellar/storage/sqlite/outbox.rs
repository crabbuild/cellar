// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Atomic source state and outgoing obligations. The dispatcher consumes only
//! committed rows; sequence allocation and work publication share the source
//! transaction and its rollback behavior.

use super::*;
use crate::application::event_stream::{Binding, DeliveryError, Envelope, ErrorCode, SealStatus};
use cellar_logic::event_stream::Limits;
use rand::RngCore;
use sha2::{Digest, Sha256};

const SCHEMA: super::event_schema::Schema = super::event_schema::Schema {
    marker: "__sys_outbox_schema",
    ddl: include_str!("outbox.sql"),
    required: &[
        ("__sys_event_source", "singleton,incarnation,protocol,source,owner"),
        ("__sys_outbox_lanes", "lane,binding,next_sequence,settled_through,status,retry_at_ms,attempts,last_error,dispatch_generation,pending_rows,pending_bytes,blocked_reason,incarnation"),
        ("__sys_outbox", "lane,sequence,envelope,size,created_at_ms"),
        ("__sys_outbox_stats", "singleton,rows,bytes"),
        ("__sys_outbox_blocked", "reason,lanes"),

        ("__sys_outbox_seals", "lane,operation,terminal,state"),
        ("__sys_outbox_batch_limits", "lane,bytes,records"),
    ],
};

pub(super) fn check_schema(connection: &Connection) -> anyhow::Result<bool> {
    let present = SCHEMA.check(connection)?;
    if present {
        super::event_schema::layout(connection, "__sys_cell_facet_names", "name")?;
        for (index, expected) in [
            ("__sys_outbox_age", "created_at_ms"),
            ("__sys_outbox_lane_age", "lane,created_at_ms"),
        ] {
            let columns = connection
                .prepare("SELECT name FROM pragma_index_info(?1) ORDER BY seqno")?
                .query_map([index], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            super::event_schema::compatible(
                columns.join(",") == expected,
                "outbox age index missing or inconsistent",
            )?;
        }
        for trigger in [
            "__sys_outbox_insert",
            "__sys_outbox_delete",
            "__sys_outbox_lane_blocked_insert",
            "__sys_outbox_lane_blocked_update",
            "__sys_outbox_lane_blocked_delete",
        ] {
            let present: bool = connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='trigger' AND name=?1)",
                [trigger],
                |row| row.get(0),
            )?;
            super::event_schema::compatible(present, "outbox accounting trigger missing")?;
        }
    }
    Ok(present)
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
    validate_policy(connection, crate::application::limits::current().events)?;
    let (rows, bytes, actual_rows, actual_bytes, invalid): (i64,i64,i64,i64,i64) = connection.query_row(
        "SELECT rows,bytes,(SELECT count(*) FROM __sys_outbox),
        (SELECT coalesce(sum(size),0) FROM __sys_outbox),
        (SELECT count(*) FROM __sys_outbox AS e WHERE size!=length(envelope) OR created_at_ms<0 OR NOT EXISTS(SELECT 1 FROM __sys_outbox_lanes WHERE lane=e.lane)) FROM __sys_outbox_stats WHERE singleton=1",
        [], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?)))?;
    super::event_schema::compatible(
        rows == actual_rows && bytes == actual_bytes && invalid == 0,
        "outbox counters or encoded sizes conflict with retained records",
    )?;
    let invalid: i64 = connection.query_row("SELECT count(*) FROM __sys_event_source WHERE singleton!=1 OR protocol!=1 OR length(incarnation)!=32 OR incarnation GLOB '*[^0-9a-f]*'", [], |row|row.get(0))?;
    super::event_schema::compatible(
        invalid == 0,
        "unsupported source incarnation or envelope protocol",
    )?;
    let (sources,lanes): (i64,i64)=connection.query_row("SELECT (SELECT count(*) FROM __sys_event_source),(SELECT count(*) FROM __sys_outbox_lanes)",[],|row|Ok((row.get(0)?,row.get(1)?)))?;
    super::event_schema::compatible(
        sources == 1 || (sources == 0 && lanes == 0),
        "producer lanes lack their original source incarnation",
    )?;
    let invalid: i64 = connection.query_row("SELECT count(*) FROM __sys_outbox_lanes AS l
        LEFT JOIN __sys_outbox_seals AS s ON s.lane=l.lane
        WHERE l.pending_rows!=(SELECT count(*) FROM __sys_outbox WHERE lane=l.lane)
        OR l.pending_bytes!=(SELECT coalesce(sum(size),0) FROM __sys_outbox WHERE lane=l.lane)
        OR (l.blocked_reason IS NOT NULL)!=(l.status='blocked' OR (l.status='sealed' AND coalesce(s.state='blocked',0)))", [], |row|row.get(0))?;
    super::event_schema::compatible(
        invalid == 0,
        "outbox lane counters or blocked state conflict with retained obligations",
    )?;
    let reasons = connection
        .prepare("SELECT reason,lanes FROM __sys_outbox_blocked ORDER BY reason")?
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    super::event_schema::compatible(
        reasons.len() == ErrorCode::BLOCKED_REASONS.len(),
        "outbox blocked reason inventory is incomplete",
    )?;
    for (reason, lanes) in reasons {
        let code: ErrorCode = serde_json::from_value(serde_json::Value::String(reason.clone()))?;
        let actual: i64 = connection.query_row(
            "SELECT count(*) FROM __sys_outbox_lanes WHERE blocked_reason=?1",
            [&reason],
            |row| row.get(0),
        )?;
        super::event_schema::compatible(
            code.is_permanent() && lanes == actual,
            "outbox blocked counters conflict with retained lanes",
        )?;
    }
    Ok(())
}

fn validate_policy(connection: &Connection, limits: Limits) -> anyhow::Result<()> {
    let (size,key,family): (i64,i64,i64) = connection.query_row(
        "SELECT coalesce(max(size),0),coalesce(max(length(CAST(json_extract(CAST(envelope AS TEXT),'$.key') AS BLOB))),0),
        coalesce(max(length(CAST(json_extract(CAST(envelope AS TEXT),'$.family') AS BLOB))),0) FROM __sys_outbox",
        [], |row|Ok((row.get(0)?,row.get(1)?,row.get(2)?)))?;
    if size > i64::try_from(limits.event_bytes())?
        || key > i64::try_from(limits.max_key_bytes)?
        || family > i64::try_from(limits.max_family_bytes)?
    {
        return Err(DeliveryError::new(ErrorCode::PayloadTooLarge,
            "retained outbox events exceed the host policy; restore compatible limits before activation").into());
    }
    Ok(())
}

fn source_owner(scope: &str, history: Option<&cellar_logic::cell_group::CellHistory>) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"cellar-event-source-owner-1");
    if let Some(history) = history {
        hash.update([1]);
        hash.update(history.cluster.bytes());
        hash.update(history.namespace.bytes());
        hash.update((history.cell.as_str().len() as u64).to_be_bytes());
        hash.update(history.cell.as_str().as_bytes());
    } else {
        hash.update([0]);
        hash.update(scope.as_bytes());
    }
    hash.finalize().into()
}

fn raft_incarnation(history: &cellar_logic::cell_group::CellHistory) -> String {
    let mut hash = Sha256::new();
    hash.update(b"cellar-raft-event-incarnation-1");
    hash.update(source_owner(history.cell.as_str(), Some(history)));
    hash.update(history.incarnation.bytes());
    hex::encode(&hash.finalize()[..16])
}

pub(crate) fn emit(
    scope: &str,
    binding: &Binding,
    version: u32,
    key: String,
    payload: serde_json::Value,
    limits: Limits,
) -> anyhow::Result<()> {
    emit_with_history(scope, binding, version, key, payload, limits, None)
}

pub(crate) fn emit_raft(
    binding: &Binding,
    version: u32,
    key: String,
    payload: serde_json::Value,
    limits: Limits,
    history: &cellar_logic::cell_group::CellHistory,
) -> anyhow::Result<()> {
    emit_with_history(
        RAFT_STORAGE_SCOPE,
        binding,
        version,
        key,
        payload,
        limits,
        Some(history),
    )
}

fn emit_with_history(
    scope: &str,
    binding: &Binding,
    version: u32,
    key: String,
    payload: serde_json::Value,
    limits: Limits,
    history: Option<&cellar_logic::cell_group::CellHistory>,
) -> anyhow::Result<()> {
    binding.validate()?;
    anyhow::ensure!(binding.publish, "event stream publication is forbidden");
    limits.validate()?;
    let now_ms = crate::asyncrt::wall_ms();
    anyhow::ensure!(now_ms >= 0, "event creation time must be nonnegative");
    let source = history.map_or(scope, |history| history.cell.as_str());
    let expected_owner = source_owner(source, history);
    let raft = command::with(|_| ()).is_some();
    anyhow::ensure!(
        raft == history.is_some(),
        "event source history differs from its execution backend"
    );
    with_application_storage_write(scope, |connection| {
        if connection.is_autocommit() {
            return Err(DeliveryError::new(
                ErrorCode::TransactionRequired,
                "event emission requires an active transaction",
            )
            .into());
        }
        with_storage_batch(connection, |connection| {
            as_framework_sql(|| {
                open_schema(connection)?;
                let incarnation = connection
                    .query_row(
                        "SELECT incarnation,source,owner FROM __sys_event_source WHERE singleton=1",
                        [],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, Vec<u8>>(2)?,
                            ))
                        },
                    )
                    .optional()?;
                let incarnation = match incarnation {
                    Some((value, stored_source, owner)) => {
                        anyhow::ensure!(
                            stored_source == source && owner.as_slice() == expected_owner,
                            "event source belongs to another logical cell"
                        );
                        if let Some(history) = history {
                            let current = raft_incarnation(history);
                            if current != value {
                                // Old lanes and envelopes retain their original
                                // identities. Fresh writes use the new history.
                                connection.execute("UPDATE __sys_event_source SET incarnation=?1 WHERE singleton=1", [&current])?;
                            }
                            current
                        } else {
                            value
                        }
                    }
                    None => {
                        let mut random =
                            history.map_or([0; 16], |history| history.incarnation.bytes());
                        if history.is_none() {
                            crate::asyncrt::rng("event-source-incarnation")
                                .try_fill_bytes(&mut random)
                                .map_err(|error| {
                                    anyhow::anyhow!(
                                        "event source identity generation failed: {error}"
                                    )
                                })?;
                        }
                        let value = history.map_or_else(|| hex::encode(random), raft_incarnation);
                        connection.execute(
                            "INSERT INTO __sys_event_source VALUES(1,?1,1,?2,?3)",
                            rusqlite::params![value, source, expected_owner.as_slice()],
                        )?;
                        value
                    }
                };
                let lane = crate::application::event_stream::lane_identity(
                    &binding.resource,
                    source,
                    &incarnation,
                )?;
                let next = connection
                    .query_row(
                        "SELECT next_sequence,status,binding FROM __sys_outbox_lanes WHERE lane=?1",
                        [&lane],
                        |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                            ))
                        },
                    )
                    .optional()?;
                let encoded_binding = serde_json::to_string(binding)?;
                let sequence = if let Some((sequence, status, prior_binding)) = next {
                    if status == "sealed" {
                        return Err(DeliveryError::new(
                            ErrorCode::ProducerSealed,
                            "outbox producer lane is sealed",
                        )
                        .into());
                    }
                    let prior: Binding = serde_json::from_str(&prior_binding)?;
                    anyhow::ensure!(
                        prior.principal == binding.principal
                            && prior.grant == binding.grant
                            && prior.resource.same_identity(&binding.resource),
                        "outbox lane publication grant conflicts with its durable identity"
                    );
                    sequence
                } else {
                    let count: i64 = connection.query_row(
                        "SELECT count(*) FROM __sys_outbox_lanes",
                        [],
                        |row| row.get(0),
                    )?;
                    if count >= i64::try_from(limits.max_source_lanes)? {
                        return Err(DeliveryError::new(
                            ErrorCode::ProducerLimit,
                            "outbox source lane limit reached",
                        )
                        .into());
                    }
                    connection.execute(
                        "INSERT INTO __sys_outbox_lanes(lane,binding,next_sequence,retry_at_ms,incarnation) VALUES(?1,?2,1,?3,?4)",
                        rusqlite::params![lane,encoded_binding,now_ms,incarnation],
                    )?;
                    1
                };
                let next_sequence = sequence
                    .checked_add(1)
                    .context("outbox sequence exhausted")?;
                let envelope = Envelope {
                    id: format!("v1:{lane}:{sequence}"),
                    lane: lane.clone(),
                    sequence,
                    source: source.to_owned(),
                    source_incarnation: incarnation,
                    family: binding.resource.family.clone(),
                    version,
                    key,
                    created_at_ms: now_ms,
                    payload,
                };
                let bytes = crate::application::event_stream::encoding::json(
                    &envelope,
                    limits.event_bytes(),
                )?;
                // Apply the same JSON recursion/shape bound as destination
                // decoding before accepting a durable delivery obligation.
                let _: Envelope = serde_json::from_slice(&bytes)?;
                let size = i64::try_from(bytes.len())?;
                let (count, stored_bytes): (i64, i64) = connection.query_row(
                    "SELECT rows,bytes FROM __sys_outbox_stats WHERE singleton=1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                if count >= i64::try_from(limits.max_outbox_rows)?
                    || !stored_bytes
                        .checked_add(size)
                        .is_some_and(|bytes| bytes <= limits.max_outbox_bytes as i64)
                {
                    return Err(DeliveryError::new(
                        ErrorCode::OutboxFull,
                        "outbox capacity reached",
                    )
                    .into());
                }
                if raft {
                    connection.execute_batch(work::SCHEMA)?;
                }
                connection.execute(
                    "INSERT INTO __sys_outbox VALUES(?1,?2,?3,?4,?5)",
                    rusqlite::params![lane, sequence, bytes, size, now_ms],
                )?;
                connection.execute(
                    "UPDATE __sys_outbox_lanes SET next_sequence=?2 WHERE lane=?1",
                    rusqlite::params![lane, next_sequence],
                )?;
                connection.execute(
                    "INSERT INTO __sys_work(scope,reason,generation,at_ms) VALUES(?1,'outbox',1,?2)
                     ON CONFLICT(scope,reason) DO UPDATE SET generation=generation+1,
                        at_ms=CASE WHEN at_ms IS NULL THEN excluded.at_ms ELSE min(at_ms,excluded.at_ms) END",
                    rusqlite::params![scope,now_ms],
                )?;
                if raft {
                    crate::application::consensus::native_work::arm_maintenance(
                        connection, "outbox", now_ms,
                    )?;
                }
                Ok(())
            })
        })
    })?;
    if !raft {
        publish_alarm(scope);
    }
    Ok(())
}

pub(crate) fn ensure_schema(scope: &str) -> anyhow::Result<()> {
    with_application_storage_write(scope, |connection| {
        with_storage_batch(connection, |connection| {
            as_framework_sql(|| open_schema(connection))
        })
    })
}

pub(crate) fn seal(scope: &str, binding: &Binding, operation: &str) -> anyhow::Result<SealStatus> {
    seal_with_history(scope, binding, operation, None)
}

pub(crate) fn seal_raft(
    binding: &Binding,
    operation: &str,
    history: &cellar_logic::cell_group::CellHistory,
) -> anyhow::Result<SealStatus> {
    seal_with_history(RAFT_STORAGE_SCOPE, binding, operation, Some(history))
}

fn seal_with_history(
    scope: &str,
    binding: &Binding,
    operation: &str,
    history: Option<&cellar_logic::cell_group::CellHistory>,
) -> anyhow::Result<SealStatus> {
    binding.validate()?;
    anyhow::ensure!(binding.publish, "event stream publication is forbidden");
    anyhow::ensure!(
        operation.len() == 32
            && operation
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "invalid seal operation identity"
    );
    let now = crate::asyncrt::wall_ms();
    let source = history.map_or(scope, |history| history.cell.as_str());
    let raft = command::with(|_| ()).is_some();
    anyhow::ensure!(
        raft == history.is_some(),
        "event source history differs from its execution backend"
    );
    let result = with_application_storage_write(scope, |connection| {
        anyhow::ensure!(
            !connection.is_autocommit(),
            "producer sealing requires an active transaction"
        );
        with_storage_batch(connection, |connection| {
            as_framework_sql(|| {
                open_schema(connection)?;
                let incarnation = connection
                    .query_row(
                        "SELECT incarnation FROM __sys_event_source WHERE singleton=1",
                        [],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?
                    .ok_or_else(|| {
                        DeliveryError::new(
                            ErrorCode::NotInitialized,
                            "source has no producer incarnation",
                        )
                    })?;
                let incarnation = history.map_or(incarnation, |history| raft_incarnation(history));
                let lane = crate::application::event_stream::lane_identity(
                    &binding.resource,
                    source,
                    &incarnation,
                )?;
                let prior = connection
                    .query_row(
                        "SELECT operation,terminal,state FROM __sys_outbox_seals WHERE lane=?1",
                        [&lane],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, String>(2)?,
                            ))
                        },
                    )
                    .optional()?;
                let (next,settled,stored,generation)=connection.query_row("SELECT next_sequence,settled_through,binding,dispatch_generation FROM __sys_outbox_lanes WHERE lane=?1",[&lane],
                    |row|Ok((row.get::<_,i64>(0)?,row.get::<_,i64>(1)?,row.get::<_,String>(2)?,row.get::<_,i64>(3)?)))
                    .optional()?.ok_or_else(||DeliveryError::new(ErrorCode::NotInitialized,"source has no producer lane for this resource"))?;
                let stored: Binding = serde_json::from_str(&stored)?;
                if stored.principal != binding.principal
                    || stored.grant != binding.grant
                    || !stored.resource.same_identity(&binding.resource)
                {
                    return Err(DeliveryError::new(
                        ErrorCode::Forbidden,
                        "producer lane belongs to a different grant",
                    )
                    .into());
                }
                if let Some((prior, terminal, state)) = prior {
                    if prior != operation {
                        return Err(DeliveryError::new(
                            ErrorCode::ConfigConflict,
                            "producer lane already has a different seal operation",
                        )
                        .into());
                    }
                    return Ok(SealStatus {
                        lane,
                        operation: prior,
                        terminal,
                        state,
                    });
                }
                let occupied = connection
                    .query_row(
                        "SELECT lane FROM __sys_outbox_seals WHERE operation=?1",
                        [operation],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?;
                if occupied.is_some() {
                    return Err(DeliveryError::new(
                        ErrorCode::ConfigConflict,
                        "seal operation identity belongs to another lane",
                    )
                    .into());
                }
                let pending = connection.query_row(
                    "SELECT count(*) FROM __sys_outbox WHERE lane=?1",
                    [&lane],
                    |row| row.get::<_, i64>(0),
                )?;
                let terminal = cellar_logic::event_stream::source_seal(next, settled, pending)
                    .map_err(|failure| {
                        DeliveryError::new(
                            if failure == cellar_logic::event_stream::Failure::PendingDelivery {
                                ErrorCode::PendingDelivery
                            } else {
                                ErrorCode::ConfigConflict
                            },
                            failure.to_string(),
                        )
                    })?;
                let generation = generation
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("outbox dispatch generation exhausted"))?;
                connection.execute("UPDATE __sys_outbox_lanes SET status='sealed',dispatch_generation=?2,retry_at_ms=?3,attempts=0,last_error=NULL,blocked_reason=NULL WHERE lane=?1",
                    rusqlite::params![lane,generation,now])?;
                connection.execute(
                    "INSERT INTO __sys_outbox_seals(lane,operation,terminal) VALUES(?1,?2,?3)",
                    rusqlite::params![lane, operation, terminal],
                )?;
                connection.execute("INSERT INTO __sys_work(scope,reason,generation,at_ms) VALUES(?1,'outbox',1,?2)
                    ON CONFLICT(scope,reason) DO UPDATE SET generation=generation+1,at_ms=CASE WHEN at_ms IS NULL THEN excluded.at_ms ELSE min(at_ms,excluded.at_ms) END",
                    rusqlite::params![scope,now])?;
                if raft {
                    crate::application::consensus::native_work::arm_maintenance(
                        connection, "outbox", now,
                    )?;
                }
                Ok(SealStatus {
                    lane,
                    operation: operation.into(),
                    terminal,
                    state: "pending".into(),
                })
            })
        })
    })?;
    if !raft {
        publish_alarm(scope);
    }
    Ok(result)
}

/// Called only after the owner has restored and opened this exact source epoch.
pub(crate) fn retained_source(scope: &str) -> anyhow::Result<bool> {
    with(scope, |connection| {
        if !check_schema(connection)? {
            return Ok(false);
        }
        let exists = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM __sys_event_source WHERE singleton=1)",
            [],
            |row| row.get(0),
        )?;
        Ok(exists)
    })
    .ok_or_else(|| anyhow::anyhow!("source recovery storage is not resident"))?
}

#[cfg(test)]
#[path = "outbox_raft_tests.rs"]
mod raft_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::event_stream::Resource;

    const SCOPE: &str = "Order:123";

    fn binding() -> Binding {
        Binding {
            environment: "ORDERS".into(),
            resource: Resource {
                tenant: "tenant".into(),
                application: "shop".into(),
                id: "00000000000000000000000000000001".into(),
                name: "orders".into(),
                family: "orders".into(),
            },
            principal: "shop".into(),
            grant: "orders-publisher".into(),
            publish: true,
            read: false,
            subscriptions: vec![],
            administer: false,
        }
    }

    fn open_cell(directory: &tempfile::TempDir, epoch: u64) -> anyhow::Result<String> {
        let path = directory
            .path()
            .join("cell.sqlite")
            .to_string_lossy()
            .into_owned();
        open_at_epoch(SCOPE, &path, epoch, true, None, false)?;
        Ok(path)
    }

    fn send(binding: &Binding, limits: Limits) -> anyhow::Result<()> {
        emit(
            SCOPE,
            binding,
            1,
            "123".into(),
            serde_json::json!({"order_id":"123"}),
            limits,
        )
    }

    fn envelopes() -> anyhow::Result<Vec<Envelope>> {
        with(SCOPE, |connection| {
            let mut statement =
                connection.prepare("SELECT envelope FROM __sys_outbox ORDER BY lane,sequence")?;
            let bytes = statement
                .query_map([], |row| row.get::<_, Vec<u8>>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            bytes
                .into_iter()
                .map(|bytes| Ok(serde_json::from_slice(&bytes)?))
                .collect()
        })
        .unwrap()
    }

    #[test]
    fn reopen_preserves_pending_envelopes_seals_and_deadlines() -> anyhow::Result<()> {
        let cells = Cells::default();
        let _installed = cells.install();
        let directory = tempfile::tempdir()?;
        open_cell(&directory, 1)?;
        transaction_control(SCOPE, "start", false, "cells_tx_1").unwrap();
        put_json(SCOPE, "status", &serde_json::json!("confirmed"))?;
        send(&binding(), Limits::default())?;
        send(&binding(), Limits::default())?;
        let mut other = binding();
        other.resource.id = "00000000000000000000000000000002".into();
        send(&other, Limits::default())?;
        let incarnation = with(SCOPE, |c| {
            c.query_row("SELECT incarnation FROM __sys_event_source", [], |row| {
                row.get::<_, String>(0)
            })
        })
        .unwrap()?;
        let lane =
            crate::application::event_stream::lane_identity(&other.resource, SCOPE, &incarnation)?;
        system_sql(
            SCOPE,
            "DELETE FROM __sys_outbox WHERE lane=?",
            &[lane.clone().into()],
        )?;
        system_sql(
            SCOPE,
            "UPDATE __sys_outbox_lanes SET settled_through=1 WHERE lane=?",
            &[lane.clone().into()],
        )?;
        seal(SCOPE, &other, "00000000000000000000000000000055")?;
        transaction_control(SCOPE, "commit", false, "cells_tx_1").unwrap();
        set_alarm(SCOPE, 200)?;
        let work = work::installation(SCOPE, cellar_logic::work::Reason::Outbox)?;
        let original = serde_json::to_vec(&envelopes()?)?;
        close(SCOPE);
        open_cell(&directory, 2)?;
        assert_eq!(serde_json::to_vec(&envelopes()?)?, original);
        assert_eq!(
            work::installation(SCOPE, cellar_logic::work::Reason::Outbox)?,
            work
        );
        assert_eq!(get_alarm(SCOPE), Some(200));
        let (version,operation,terminal,state): (i64,String,i64,String)=with(SCOPE,|c|c.query_row(
            "SELECT (SELECT version FROM __sys_outbox_schema),operation,terminal,state FROM __sys_outbox_seals WHERE lane=?1",[&lane],
            |row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)))).unwrap()?;
        assert_eq!(version, 1);
        assert_eq!(operation, "00000000000000000000000000000055");
        assert_eq!((terminal, state.as_str()), (1, "pending"));
        assert!(get_stored(SCOPE, "status")?.is_some());
        assert!(with(SCOPE, |c| validate_policy(
            c,
            Limits {
                max_event_bytes: 100,
                ..Limits::default()
            }
        ))
        .unwrap()
        .is_err());
        assert_eq!(serde_json::to_vec(&envelopes()?)?, original);
        close(SCOPE);
        Ok(())
    }

    #[test]
    fn unmarked_state_cannot_be_marked_or_repaired() -> anyhow::Result<()> {
        let cells = Cells::default();
        let _installed = cells.install();
        let directory = tempfile::tempdir()?;
        let path = open_cell(&directory, 1)?;
        transaction_control(SCOPE, "start", false, "cells_tx_1").unwrap();
        send(&binding(), Limits::default())?;
        transaction_control(SCOPE, "commit", false, "cells_tx_1").unwrap();
        let original = serde_json::to_vec(&envelopes()?)?;
        close(SCOPE);
        {
            let c = Connection::open(&path)?;
            c.execute_batch(
                "DROP TABLE __sys_outbox_schema; DROP TABLE __sys_outbox_batch_limits;
                UPDATE __sys_outbox_stats SET rows=rows+1;",
            )?;
        }
        let failure = open_cell(&directory, 2).unwrap_err();
        assert_eq!(
            failure.downcast_ref::<DeliveryError>().unwrap().code,
            ErrorCode::UnsupportedProtocol
        );
        let c = Connection::open(&path)?;
        assert_eq!(
            c.query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name='__sys_outbox_schema'",
                [],
                |row| row.get::<_, i64>(0)
            )?,
            0
        );
        let bytes: Vec<u8> =
            c.query_row("SELECT envelope FROM __sys_outbox", [], |row| row.get(0))?;
        let retained: Envelope = serde_json::from_slice(&bytes)?;
        assert_eq!(serde_json::to_vec(&vec![retained])?, original);
        assert_eq!(
            c.query_row("SELECT rows FROM __sys_outbox_stats", [], |row| row
                .get::<_, i64>(0))?,
            2
        );
        assert_eq!(
            c.query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name='__sys_outbox_batch_limits'",
                [],
                |row| row.get::<_, i64>(0)
            )?,
            0
        );
        Ok(())
    }

    #[tokio::test]
    async fn incompatible_host_limits_refuse_activation_without_changing_pending_events(
    ) -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let original_domain =
            crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let original = original_domain
            .run(async {
                let cells = Cells::default();
                let _installed = cells.install();
                open_cell(&directory, 1)?;
                transaction_control(SCOPE, "start", false, "cells_tx_1").unwrap();
                send(&binding(), Limits::default())?;
                transaction_control(SCOPE, "commit", false, "cells_tx_1").unwrap();
                let original = serde_json::to_vec(&envelopes()?)?;
                close(SCOPE);
                Ok::<_, anyhow::Error>(original)
            })
            .await??;
        original_domain.shutdown().await;
        let restricted = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let mut policy = crate::application::limits::CellLimits::default();
        policy.events.max_event_bytes = 100;
        restricted.services().configure_cell_limits(policy)?;
        restricted
            .run(async {
                let cells = Cells::default();
                let _installed = cells.install();
                let failure = open_cell(&directory, 2).unwrap_err();
                assert_eq!(
                    failure.downcast_ref::<DeliveryError>().unwrap().code,
                    ErrorCode::PayloadTooLarge
                );
                assert_eq!(
                    activation_epoch(SCOPE),
                    None,
                    "refused source was published to storage"
                );
                Ok::<_, anyhow::Error>(())
            })
            .await??;
        restricted.shutdown().await;
        let compatible = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        compatible
            .run(async {
                let cells = Cells::default();
                let _installed = cells.install();
                open_cell(&directory, 3)?;
                assert_eq!(serde_json::to_vec(&envelopes()?)?, original);
                transaction_control(SCOPE, "start", false, "cells_tx_2").unwrap();
                send(&binding(), Limits::default())?;
                transaction_control(SCOPE, "commit", false, "cells_tx_2").unwrap();
                let events = envelopes()?;
                assert_eq!(events[0].lane, events[1].lane);
                assert_eq!(events[0].source_incarnation, events[1].source_incarnation);
                assert_eq!(events[1].sequence, 2);
                close(SCOPE);
                Ok::<_, anyhow::Error>(())
            })
            .await??;
        compatible.shutdown().await;
        Ok(())
    }

    #[test]
    fn emission_and_business_state_commit_or_rollback_together() -> anyhow::Result<()> {
        let cells = Cells::default();
        let _installed = cells.install();
        let directory = tempfile::tempdir()?;
        open_cell(&directory, 1)?;
        assert!(send(&binding(), Limits::default()).is_err());
        transaction_control(SCOPE, "start", false, "cells_tx_1").unwrap();
        put_json(SCOPE, "status", &serde_json::json!("confirmed"))?;
        send(&binding(), Limits::default())?;
        assert!(alarm_snapshot(SCOPE).is_err());
        transaction_control(SCOPE, "rollback", false, "cells_tx_1").unwrap();
        assert!(get_stored(SCOPE, "status")?.is_none());
        assert!(work::due(SCOPE, i64::MAX)?.is_none());
        transaction_control(SCOPE, "start", false, "cells_tx_2").unwrap();
        put_json(SCOPE, "status", &serde_json::json!("confirmed"))?;
        send(&binding(), Limits::default())?;
        transaction_control(SCOPE, "commit", false, "cells_tx_2").unwrap();
        assert!(get_stored(SCOPE, "status")?.is_some());
        let events = envelopes()?;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, 1);
        assert_eq!(events[0].payload, serde_json::json!({"order_id":"123"}));
        assert_eq!(
            work::due(SCOPE, i64::MAX)?.unwrap().reason,
            cellar_logic::work::Reason::Outbox
        );
        close(SCOPE);
        Ok(())
    }

    #[test]
    fn rejected_first_envelope_does_not_initialize_identity_or_publish_work() -> anyhow::Result<()>
    {
        let cells = Cells::default();
        let _installed = cells.install();
        let directory = tempfile::tempdir()?;
        open_cell(&directory, 1)?;
        transaction_control(SCOPE, "start", false, "cells_tx_1").unwrap();
        put_json(SCOPE, "status", &serde_json::json!("confirmed"))?;
        let failure = send(
            &binding(),
            Limits {
                max_event_bytes: 100,
                ..Limits::default()
            },
        )
        .unwrap_err();
        assert_eq!(
            failure.downcast_ref::<DeliveryError>().unwrap().code,
            ErrorCode::PayloadTooLarge
        );
        let initialized: i64 = with(SCOPE, |connection| {
            connection.query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name='__sys_event_source'",
                [],
                |row| row.get(0),
            )
        })
        .unwrap()?;
        assert_eq!(initialized, 0);
        transaction_control(SCOPE, "commit", false, "cells_tx_1").unwrap();
        assert!(work::due(SCOPE, i64::MAX)?.is_none());
        assert!(
            matches!(get_stored(SCOPE, "status")?, Some(StoredValue::Json(value)) if value=="\"confirmed\"")
        );
        transaction_control(SCOPE, "start", false, "cells_tx_2").unwrap();
        send(&binding(), Limits::default())?;
        transaction_control(SCOPE, "commit", false, "cells_tx_2").unwrap();
        assert_eq!(envelopes()?[0].sequence, 1);
        close(SCOPE);
        Ok(())
    }

    #[test]
    fn caught_capacity_failure_consumes_no_sequence_and_leaves_no_partial_emit(
    ) -> anyhow::Result<()> {
        let cells = Cells::default();
        let _installed = cells.install();
        let directory = tempfile::tempdir()?;
        open_cell(&directory, 1)?;
        let limits = Limits {
            max_outbox_rows: 1,
            ..Limits::default()
        };
        transaction_control(SCOPE, "start", false, "cells_tx_1").unwrap();
        send(&binding(), limits)?;
        assert_eq!(
            send(&binding(), limits)
                .unwrap_err()
                .downcast_ref::<DeliveryError>()
                .unwrap()
                .code,
            ErrorCode::OutboxFull
        );
        transaction_control(SCOPE, "commit", false, "cells_tx_1").unwrap();
        assert_eq!(envelopes()?.len(), 1);
        transaction_control(SCOPE, "start", false, "cells_tx_2").unwrap();
        send(&binding(), Limits::default())?;
        transaction_control(SCOPE, "commit", false, "cells_tx_2").unwrap();
        let events = envelopes()?;
        assert_eq!(
            events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        let sizes: (i64,i64,i64,i64) = with(SCOPE,|connection|connection.query_row(
            "SELECT rows,bytes,(SELECT count(*) FROM __sys_outbox),(SELECT sum(size) FROM __sys_outbox)
             FROM __sys_outbox_stats WHERE singleton=1", [],
            |row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)))) .unwrap()?;
        assert_eq!((sizes.0, sizes.1), (sizes.2, sizes.3));
        close(SCOPE);
        Ok(())
    }

    #[test]
    fn ownership_change_preserves_lane_and_pending_event_bytes() -> anyhow::Result<()> {
        let cells = Cells::default();
        let _installed = cells.install();
        let directory = tempfile::tempdir()?;
        open_cell(&directory, 1)?;
        transaction_control(SCOPE, "start", false, "cells_tx_1").unwrap();
        send(&binding(), Limits::default())?;
        transaction_control(SCOPE, "commit", false, "cells_tx_1").unwrap();
        let original = serde_json::to_vec(&envelopes()?[0])?;
        close(SCOPE);
        open_cell(&directory, 2)?;
        assert_eq!(serde_json::to_vec(&envelopes()?[0])?, original);
        transaction_control(SCOPE, "start", false, "cells_tx_2").unwrap();
        send(&binding(), Limits::default())?;
        transaction_control(SCOPE, "commit", false, "cells_tx_2").unwrap();
        let events = envelopes()?;
        assert_eq!(events[0].lane, events[1].lane);
        assert_eq!(events[0].source_incarnation, events[1].source_incarnation);
        assert_eq!(events[1].sequence, 2);
        close(SCOPE);
        Ok(())
    }

    #[test]
    fn sealing_rolls_back_and_retains_a_recoverable_terminal_obligation() -> anyhow::Result<()> {
        let cells = Cells::default();
        let _installed = cells.install();
        let directory = tempfile::tempdir()?;
        open_cell(&directory, 1)?;
        transaction_control(SCOPE, "start", false, "cells_tx_1").unwrap();
        send(&binding(), Limits::default())?;
        let pending = seal(SCOPE, &binding(), "00000000000000000000000000000055").unwrap_err();
        assert_eq!(
            pending.downcast_ref::<DeliveryError>().unwrap().code,
            ErrorCode::PendingDelivery
        );
        send(&binding(), Limits::default())?;
        transaction_control(SCOPE, "commit", false, "cells_tx_1").unwrap();
        let lane = envelopes()?[0].lane.clone();
        // Supply the source's persisted post-acceptance facts. Native tests
        // exercise the real destination and its acceptance proof separately.
        system_sql(SCOPE, "DELETE FROM __sys_outbox", &[])?;
        system_sql(
            SCOPE,
            "UPDATE __sys_outbox_lanes SET settled_through=2",
            &[],
        )?;
        let before = work::installation(SCOPE, cellar_logic::work::Reason::Outbox)?;
        transaction_control(SCOPE, "start", false, "cells_tx_2").unwrap();
        put_json(SCOPE, "closed", &true.into())?;
        assert_eq!(
            seal(SCOPE, &binding(), "00000000000000000000000000000055")?.terminal,
            2
        );
        transaction_control(SCOPE, "rollback", false, "cells_tx_2").unwrap();
        assert!(get_stored(SCOPE, "closed")?.is_none());
        assert_eq!(
            work::installation(SCOPE, cellar_logic::work::Reason::Outbox)?,
            before
        );
        transaction_control(SCOPE, "start", false, "cells_tx_3").unwrap();
        let sealed = seal(SCOPE, &binding(), "00000000000000000000000000000055")?;
        assert_eq!(sealed.state, "pending");
        assert_eq!(sealed.lane, lane);
        assert_eq!(
            send(&binding(), Limits::default())
                .unwrap_err()
                .downcast_ref::<DeliveryError>()
                .unwrap()
                .code,
            ErrorCode::ProducerSealed
        );
        transaction_control(SCOPE, "commit", false, "cells_tx_3").unwrap();
        close(SCOPE);
        open_cell(&directory, 2)?;
        assert!(work::installation(SCOPE, cellar_logic::work::Reason::Outbox)?.is_some());
        delete_all_with_alarm(SCOPE, true)?;
        transaction_control(SCOPE, "start", false, "cells_tx_4").unwrap();
        assert_eq!(
            seal(SCOPE, &binding(), "00000000000000000000000000000055")?.terminal,
            2
        );
        let mut wrong = binding();
        wrong.grant = "wrong-grant".into();
        assert_eq!(
            seal(SCOPE, &wrong, "00000000000000000000000000000055")
                .unwrap_err()
                .downcast_ref::<DeliveryError>()
                .unwrap()
                .code,
            ErrorCode::Forbidden
        );
        assert!(seal(SCOPE, &binding(), "00000000000000000000000000000056").is_err());
        transaction_control(SCOPE, "commit", false, "cells_tx_4").unwrap();
        assert!(work::installation(SCOPE, cellar_logic::work::Reason::Outbox)?.is_some());
        close(SCOPE);
        Ok(())
    }

    #[test]
    fn nested_rollback_and_grant_conflict_cannot_change_original_obligation() -> anyhow::Result<()>
    {
        let cells = Cells::default();
        let _installed = cells.install();
        let directory = tempfile::tempdir()?;
        open_cell(&directory, 1)?;
        transaction_control(SCOPE, "start", false, "cells_tx_1").unwrap();
        send(&binding(), Limits::default())?;
        transaction_control(SCOPE, "start", true, "cells_tx_2").unwrap();
        send(&binding(), Limits::default())?;
        transaction_control(SCOPE, "rollback", true, "cells_tx_2").unwrap();
        let mut conflict = binding();
        conflict.principal = "another-principal".into();
        assert!(send(&conflict, Limits::default()).is_err());
        send(&binding(), Limits::default())?;
        transaction_control(SCOPE, "commit", false, "cells_tx_1").unwrap();
        assert_eq!(
            envelopes()?
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        close(SCOPE);
        Ok(())
    }

    #[test]
    fn resetting_user_storage_cannot_erase_pending_delivery_or_sequence_history(
    ) -> anyhow::Result<()> {
        let cells = Cells::default();
        let _installed = cells.install();
        let directory = tempfile::tempdir()?;
        open_cell(&directory, 1)?;
        transaction_control(SCOPE, "start", false, "cells_tx_1").unwrap();
        put_json(SCOPE, "status", &serde_json::json!("confirmed"))?;
        send(&binding(), Limits::default())?;
        transaction_control(SCOPE, "commit", false, "cells_tx_1").unwrap();
        assert!(delete_all_with_alarm(SCOPE, true).is_err());
        assert!(get_stored(SCOPE, "status")?.is_some());
        let first = envelopes()?[0].clone();
        system_sql(SCOPE, "DELETE FROM __sys_outbox", &[])?;
        delete_all_with_alarm(SCOPE, true)?;
        transaction_control(SCOPE, "start", false, "cells_tx_2").unwrap();
        send(&binding(), Limits::default())?;
        transaction_control(SCOPE, "commit", false, "cells_tx_2").unwrap();
        let second = envelopes()?[0].clone();
        assert_eq!(first.source_incarnation, second.source_incarnation);
        assert_eq!(first.lane, second.lane);
        assert_eq!(second.sequence, 2);
        close(SCOPE);
        Ok(())
    }

    #[test]
    fn lane_accounting_and_blocked_reasons_follow_commit_rollback_and_restore() -> anyhow::Result<()>
    {
        let cells = Cells::default();
        let _installed = cells.install();
        let directory = tempfile::tempdir()?;
        open_cell(&directory, 1)?;
        transaction_control(SCOPE, "start", false, "cells_tx_1").unwrap();
        send(&binding(), Limits::default())?;
        send(&binding(), Limits::default())?;
        let mut other = binding();
        other.resource.id = "00000000000000000000000000000002".into();
        send(&other, Limits::default())?;
        transaction_control(SCOPE, "commit", false, "cells_tx_1").unwrap();
        let lane = envelopes()?[0].lane.clone();
        let verify = || -> anyhow::Result<()> {
            with(SCOPE, |c| {
                let invalid: i64 = c.query_row("SELECT count(*) FROM __sys_outbox_lanes l WHERE pending_rows!=(SELECT count(*) FROM __sys_outbox WHERE lane=l.lane) OR pending_bytes!=(SELECT coalesce(sum(size),0) FROM __sys_outbox WHERE lane=l.lane)",[],|row|row.get(0))?;
                assert_eq!(invalid, 0);
                for (query, index) in [
                    ("EXPLAIN QUERY PLAN SELECT created_at_ms FROM __sys_outbox INDEXED BY __sys_outbox_age ORDER BY created_at_ms LIMIT 1", "__sys_outbox_age"),
                    ("EXPLAIN QUERY PLAN SELECT created_at_ms FROM __sys_outbox INDEXED BY __sys_outbox_lane_age WHERE lane='test' ORDER BY created_at_ms LIMIT 1", "__sys_outbox_lane_age"),
                ] {
                    let plan = c.prepare(query)?.query_map([],|row|row.get::<_,String>(3))?.collect::<rusqlite::Result<Vec<_>>>()?.join(" ");
                    assert!(plan.contains(index) && !plan.contains("TEMP B-TREE"), "{plan}");
                }
                Ok::<_,anyhow::Error>(())
            }).unwrap()
        };
        verify()?;
        system_sql(SCOPE,"UPDATE __sys_outbox_lanes SET status='blocked',blocked_reason='Forbidden' WHERE lane=?",&[lane.clone().into()])?;
        transaction_control(SCOPE, "start", false, "cells_tx_2").unwrap();
        system_sql(
            SCOPE,
            "DELETE FROM __sys_outbox WHERE lane=? AND sequence=1",
            &[lane.clone().into()],
        )?;
        system_sql(
            SCOPE,
            "UPDATE __sys_outbox_lanes SET blocked_reason='DestinationDeleted' WHERE lane=?",
            &[lane.clone().into()],
        )?;
        verify()?;
        let count = |reason: &str| -> anyhow::Result<i64> {
            Ok(with(SCOPE, |c| {
                c.query_row(
                    "SELECT lanes FROM __sys_outbox_blocked WHERE reason=?1",
                    [reason],
                    |row| row.get(0),
                )
            })
            .unwrap()?)
        };
        assert_eq!(count("Forbidden")?, 0);
        assert_eq!(count("DestinationDeleted")?, 1);
        transaction_control(SCOPE, "rollback", false, "cells_tx_2").unwrap();
        verify()?;
        assert_eq!(envelopes()?.len(), 3);
        assert_eq!(count("Forbidden")?, 1);
        assert_eq!(count("DestinationDeleted")?, 0);
        close(SCOPE);
        open_cell(&directory, 2)?;
        verify()?;
        assert_eq!(count("Forbidden")?, 1);
        system_sql(
            SCOPE,
            "UPDATE __sys_outbox_lanes SET status='pending',blocked_reason=NULL WHERE lane=?",
            &[lane.into()],
        )?;
        assert_eq!(count("Forbidden")?, 0);
        close(SCOPE);
        Ok(())
    }

    #[test]
    fn activation_refuses_invalid_observation_accounting_and_missing_indices() -> anyhow::Result<()>
    {
        for mutation in [
            "UPDATE __sys_outbox_lanes SET pending_rows=pending_rows+1",
            "UPDATE __sys_outbox_blocked SET lanes=1 WHERE reason='Forbidden'",
            "DROP INDEX __sys_outbox_age",
            "DROP TRIGGER __sys_outbox_lane_blocked_update",
        ] {
            let cells = Cells::default();
            let _installed = cells.install();
            let directory = tempfile::tempdir()?;
            let path = open_cell(&directory, 1)?;
            transaction_control(SCOPE, "start", false, "cells_tx_1").unwrap();
            send(&binding(), Limits::default())?;
            transaction_control(SCOPE, "commit", false, "cells_tx_1").unwrap();
            let original = serde_json::to_vec(&envelopes()?)?;
            close(SCOPE);
            Connection::open(&path)?.execute_batch(mutation)?;
            let failure = open_cell(&directory, 2).unwrap_err();
            assert_eq!(
                failure.downcast_ref::<DeliveryError>().unwrap().code,
                ErrorCode::UnsupportedProtocol,
                "{mutation}"
            );
            let c = Connection::open(&path)?;
            let bytes: Vec<u8> =
                c.query_row("SELECT envelope FROM __sys_outbox", [], |row| row.get(0))?;
            let retained: Envelope = serde_json::from_slice(&bytes)?;
            assert_eq!(serde_json::to_vec(&vec![retained])?, original);
            assert_eq!(
                c.query_row("SELECT version FROM __sys_outbox_schema", [], |row| row
                    .get::<_, i64>(0))?,
                1
            );
        }
        Ok(())
    }
}
