//! Atomic source state and outgoing obligations. The dispatcher consumes only
//! committed rows; sequence allocation and work publication share the source
//! transaction and its rollback behavior.

use super::*;
use crate::application::event_stream::{Binding, DeliveryError, Envelope, ErrorCode, SealStatus};
use cellar_logic::event_stream::Limits;
use rand::RngCore;

const SCHEMA: super::event_schema::Schema = super::event_schema::Schema {
    marker: "__sys_outbox_schema",
    ddl: include_str!("outbox.sql"),
    required: &[
        ("__sys_event_source", "singleton,incarnation,protocol"),
        ("__sys_outbox_lanes", "lane,binding,next_sequence,settled_through,status,retry_at_ms,attempts,last_error,dispatch_generation"),
        ("__sys_outbox", "lane,sequence,envelope,size,created_at_ms"),
        ("__sys_outbox_stats", "singleton,rows,bytes"),
    ],
    optional: &[
        ("__sys_outbox_seals", "lane,operation,terminal,state"),
        ("__sys_outbox_batch_limits", "lane,bytes,records"),
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
    validate_policy(connection, crate::application::limits::current().events)?;
    let (rows, bytes, actual_rows, actual_bytes, invalid): (i64,i64,i64,i64,i64) = connection.query_row(
        "SELECT rows,bytes,(SELECT count(*) FROM __sys_outbox),
        (SELECT coalesce(sum(size),0) FROM __sys_outbox),
        (SELECT count(*) FROM __sys_outbox WHERE size!=length(envelope)) FROM __sys_outbox_stats WHERE singleton=1",
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

pub(crate) fn emit(
    scope: &str,
    binding: &Binding,
    version: u32,
    key: String,
    payload: serde_json::Value,
    limits: Limits,
) -> anyhow::Result<()> {
    binding.validate()?;
    anyhow::ensure!(binding.publish, "event stream publication is forbidden");
    limits.validate()?;
    let now_ms = crate::asyncrt::wall_ms();
    anyhow::ensure!(now_ms >= 0, "event creation time must be nonnegative");
    with_application_storage_write(scope, |connection| {
        if connection.is_autocommit() {
            return Err(DeliveryError::new(
                ErrorCode::TransactionRequired,
                "event emission requires an active transaction",
            )
            .into());
        }
        without_sql_authorizer(connection, || {
            connection.execute_batch("SAVEPOINT cellar_outbox_emit;")?;
            let result = (|| -> anyhow::Result<()> {
                open_schema(connection)?;
                let incarnation = connection
                    .query_row(
                        "SELECT incarnation FROM __sys_event_source WHERE singleton=1",
                        [],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?;
                let incarnation = match incarnation {
                    Some(value) => value,
                    None => {
                        let mut random = [0; 16];
                        crate::asyncrt::rng("event-source-incarnation")
                            .try_fill_bytes(&mut random)
                            .map_err(|error| {
                                anyhow::anyhow!("event source identity generation failed: {error}")
                            })?;
                        let value = hex::encode(random);
                        connection
                            .execute("INSERT INTO __sys_event_source VALUES(1,?1,1)", [&value])?;
                        value
                    }
                };
                let lane = crate::application::event_stream::lane_identity(
                    &binding.resource,
                    scope,
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
                        "INSERT INTO __sys_outbox_lanes(lane,binding,next_sequence,retry_at_ms) VALUES(?1,?2,1,?3)",
                        rusqlite::params![lane,encoded_binding,now_ms],
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
                    source: scope.to_owned(),
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
                Ok(())
            })();
            if result.is_err() {
                connection.execute_batch("ROLLBACK TO cellar_outbox_emit;")?;
            }
            connection.execute_batch("RELEASE cellar_outbox_emit;")?;
            result
        })
    })?;
    publish_alarm(scope);
    Ok(())
}

pub(crate) fn ensure_schema(scope: &str) -> anyhow::Result<()> {
    with_application_storage_write(scope, |connection| {
        without_sql_authorizer(connection, || {
            connection.execute_batch("SAVEPOINT cellar_outbox_schema;")?;
            let result = open_schema(connection);
            if result.is_err() {
                connection.execute_batch("ROLLBACK TO cellar_outbox_schema;")?;
            }
            connection.execute_batch("RELEASE cellar_outbox_schema;")?;
            result
        })
    })
}

pub(crate) fn seal(scope: &str, binding: &Binding, operation: &str) -> anyhow::Result<SealStatus> {
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
    let result = with_application_storage_write(scope, |connection| {
        anyhow::ensure!(
            !connection.is_autocommit(),
            "producer sealing requires an active transaction"
        );
        without_sql_authorizer(connection, || {
            connection.execute_batch("SAVEPOINT cellar_outbox_seal;")?;
            let result = (|| -> anyhow::Result<SealStatus> {
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
                let lane = crate::application::event_stream::lane_identity(
                    &binding.resource,
                    scope,
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
                connection.execute("UPDATE __sys_outbox_lanes SET status='sealed',dispatch_generation=?2,retry_at_ms=?3,attempts=0,last_error=NULL WHERE lane=?1",
                    rusqlite::params![lane,generation,now])?;
                connection.execute(
                    "INSERT INTO __sys_outbox_seals(lane,operation,terminal) VALUES(?1,?2,?3)",
                    rusqlite::params![lane, operation, terminal],
                )?;
                connection.execute("INSERT INTO __sys_work(scope,reason,generation,at_ms) VALUES(?1,'outbox',1,?2)
                    ON CONFLICT(scope,reason) DO UPDATE SET generation=generation+1,at_ms=CASE WHEN at_ms IS NULL THEN excluded.at_ms ELSE min(at_ms,excluded.at_ms) END",
                    rusqlite::params![scope,now])?;
                Ok(SealStatus {
                    lane,
                    operation: operation.into(),
                    terminal,
                    state: "pending".into(),
                })
            })();
            if result.is_err() {
                connection.execute_batch("ROLLBACK TO cellar_outbox_seal;")?;
            }
            connection.execute_batch("RELEASE cellar_outbox_seal;")?;
            result
        })
    })?;
    publish_alarm(scope);
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
    fn legacy_migration_preserves_pending_envelopes_seals_and_deadlines() -> anyhow::Result<()> {
        let cells = Cells::default();
        let _installed = cells.install();
        let directory = tempfile::tempdir()?;
        let path = open_cell(&directory, 1)?;
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
        {
            let c = Connection::open(&path)?;
            c.execute_batch(
                "DROP TABLE __sys_outbox_schema; DROP TABLE __sys_outbox_batch_limits;",
            )?;
        }
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
    fn inconsistent_legacy_accounting_cannot_be_marked_or_repaired() -> anyhow::Result<()> {
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
}
