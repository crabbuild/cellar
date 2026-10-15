//! Versioned, retained source administration receipts.

use super::*;

const SCHEMA: super::event_schema::Schema = super::event_schema::Schema {
    marker: "__sys_outbox_control_schema",
    ddl: include_str!("outbox_control.sql"),
    required: &[
        (
            "__sys_outbox_operations",
            "sequence,operation,digest,operator,receipt,size,created_ms",
        ),
        ("__sys_outbox_operation_stats", "singleton,rows"),
        (
            "__sys_outbox_source_lifecycle",
            "singleton,incarnation,generation,state,operation,snapshot,work_generation",
        ),
        (
            "__sys_outbox_destroy_lanes",
            "operation,lane,resource,needs_evidence,evidence",
        ),
    ],
};

pub(super) fn check_schema(connection: &Connection) -> anyhow::Result<bool> {
    let present = SCHEMA.check(connection)?;
    if present {
        let invalid: i64 = connection.query_row("SELECT count(*) FROM __sys_outbox_source_lifecycle AS l WHERE typeof(generation)!='integer' OR typeof(work_generation)!='integer' OR generation<=0 OR state NOT IN ('checking','verified','cleaning','deleted','live') OR (state IN ('checking','verified','cleaning') AND work_generation<=0) OR length(incarnation)!=32 OR incarnation GLOB '*[^0-9a-f]*' OR length(snapshot)!=64 OR snapshot GLOB '*[^0-9a-f]*' OR NOT EXISTS(SELECT 1 FROM __sys_outbox_operations WHERE operation=l.operation)", [], |row| row.get(0))?;
        super::event_schema::compatible(
            invalid == 0,
            "source lifecycle is inconsistent with its retained receipt",
        )?;
    }
    if present {
        let active: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM __sys_outbox_source_lifecycle)",
            [],
            |row| row.get(0),
        )?;
        if active {
            let invalid:i64=connection.query_row("SELECT count(*) FROM __sys_outbox_source_lifecycle AS l JOIN __sys_outbox_operations AS o ON o.operation=l.operation WHERE incarnation!=coalesce((SELECT incarnation FROM __sys_event_source WHERE singleton=1),'') OR coalesce(json_extract(o.receipt,'$.state'),'')!=l.state OR coalesce(json_extract(o.receipt,'$.incarnation'),'')!=l.incarnation OR (l.state IN ('checking','verified','cleaning') AND NOT EXISTS(SELECT 1 FROM __sys_work WHERE reason='outbox' AND scope=json_extract(o.receipt,'$.source') AND generation=l.work_generation AND at_ms IS NOT NULL)) OR (l.state IN ('verified','cleaning','deleted') AND EXISTS(SELECT 1 FROM __sys_outbox_destroy_lanes AS d WHERE d.operation=l.operation AND d.needs_evidence=1 AND d.evidence IS NULL))",[],|row|row.get(0))?;
            super::event_schema::compatible(
                invalid == 0,
                "source lifecycle receipt, work or proof is inconsistent",
            )?;
        }
        let mut statements = connection.prepare(
            "SELECT resource,evidence FROM __sys_outbox_destroy_lanes WHERE evidence IS NOT NULL",
        )?;
        let mut proofs = statements.query([])?;
        while let Some(row) = proofs.next()? {
            let resource: String = row.get(0)?;
            let evidence: String = row.get(1)?;
            super::event_schema::compatible(
                resource.len() <= 4096 && evidence.len() <= 4096,
                "source destruction proof exceeds limit",
            )?;
            let resource: crate::application::event_stream::Resource =
                serde_json::from_str(&resource)?;
            let proof: crate::application::event_stream::RetirementProof =
                serde_json::from_str(&evidence)?;
            proof.validate(&resource)?;
        }
    }
    Ok(present)
}

pub(super) fn restore_schema(connection: &Connection) -> anyhow::Result<()> {
    if check_schema(connection)? {
        SCHEMA.open(connection)?;
        let (stored,actual,invalid):(i64,i64,i64)=connection.query_row("SELECT rows,(SELECT count(*) FROM __sys_outbox_operations),(SELECT count(*) FROM __sys_outbox_operations WHERE size!=length(CAST(receipt AS BLOB)) OR size>4096 OR length(operation)!=32 OR length(digest)!=64 OR length(CAST(operator AS BLOB))>256) FROM __sys_outbox_operation_stats WHERE singleton=1",[],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?)))?;
        super::event_schema::compatible(
            stored == actual && invalid == 0,
            "source administration receipt accounting is inconsistent",
        )?;
    }
    Ok(())
}

pub(crate) fn ensure_schema(scope: &str) -> anyhow::Result<()> {
    with_application_storage_write(scope, |connection| {
        without_sql_authorizer(connection, || SCHEMA.open(connection))
    })
}

/// Read once at activation. The serving cache is updated only after the
/// corresponding transaction commits, before the controlling turn can await.
pub(crate) fn source_admission(scope: &str) -> anyhow::Result<(u64, bool)> {
    with(scope, |c| {
        without_sql_authorizer(c, || {
            if !check_schema(c)? {
                return Ok((0, false));
            }
            c.query_row(
                "SELECT generation,state FROM __sys_outbox_source_lifecycle WHERE singleton=1",
                [],
                |row| Ok((row.get::<_, u64>(0)?, row.get::<_, String>(1)? != "live")),
            )
            .optional()
            .map(|value| value.unwrap_or((0, false)))
            .map_err(Into::into)
        })
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("source admission has no resident database")))
}

/// Reclaim a contiguous, bounded set of source obligations only after every
/// captured destination proof has committed. Caller owns the enclosing tx.
pub(crate) fn cleanup_events(scope: &str) -> anyhow::Result<(i64, i64)> {
    with_application_storage_write(scope, |c| {
        without_sql_authorizer(c, || {
            anyhow::ensure!(!c.is_autocommit(), "source cleanup requires a transaction");
            let state: String = c.query_row(
                "SELECT state FROM __sys_outbox_source_lifecycle WHERE singleton=1",
                [],
                |row| row.get(0),
            )?;
            anyhow::ensure!(state == "cleaning", "source cleanup is not admitted");
            let missing:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM __sys_outbox_destroy_lanes AS d JOIN __sys_outbox_source_lifecycle AS l ON l.operation=d.operation WHERE d.needs_evidence=1 AND d.evidence IS NULL)",[],|row|row.get(0))?;
            anyhow::ensure!(
                !missing,
                "source cleanup lacks terminal destination evidence"
            );
            let mut query = c.prepare(
                "SELECT lane,sequence,size FROM __sys_outbox ORDER BY lane,sequence LIMIT 1000",
            )?;
            let candidates = query
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            drop(query);
            let budget = i64::try_from(
                (4usize << 20).max(crate::application::limits::current().events.event_bytes()),
            )?;
            let mut rows = 0i64;
            let mut bytes = 0i64;
            for (lane, sequence, size) in candidates {
                anyhow::ensure!(
                    size >= 0 && size <= budget,
                    "source cleanup event exceeds budget"
                );
                if size > budget - bytes {
                    break;
                }
                c.execute(
                    "DELETE FROM __sys_outbox WHERE lane=?1 AND sequence=?2",
                    rusqlite::params![lane, sequence],
                )?;
                rows += 1;
                bytes += size;
            }
            Ok((rows, bytes))
        })
    })
}

pub(crate) fn cleanup_metadata(scope: &str) -> anyhow::Result<bool> {
    with_application_storage_write(scope, |c| {
        without_sql_authorizer(c, || {
            anyhow::ensure!(
                !c.is_autocommit(),
                "source metadata cleanup requires a transaction"
            );
            let admitted: bool = c.query_row(
                "SELECT EXISTS(SELECT 1 FROM __sys_outbox_source_lifecycle WHERE state='cleaning')",
                [],
                |row| row.get(0),
            )?;
            anyhow::ensure!(admitted, "source metadata cleanup is not admitted");
            let exists:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='__sys_inbox' AND type='table')",[],|row|row.get(0))?;
            if exists {
                let keys=c.prepare("SELECT processing_scope,event_id FROM __sys_inbox ORDER BY processing_scope,event_id LIMIT 1000")?
                .query_map([],|row|Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
                if !keys.is_empty() {
                    for (scope, event) in keys {
                        c.execute(
                            "DELETE FROM __sys_inbox WHERE processing_scope=?1 AND event_id=?2",
                            rusqlite::params![scope, event],
                        )?;
                    }
                    return Ok(false);
                }
            }
            let lanes = c
                .prepare("SELECT lane FROM __sys_outbox_lanes ORDER BY lane LIMIT 1000")?
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            if !lanes.is_empty() {
                for lane in lanes {
                    c.execute("DELETE FROM __sys_outbox_seals WHERE lane=?1", [&lane])?;
                    c.execute(
                        "DELETE FROM __sys_outbox_batch_limits WHERE lane=?1",
                        [&lane],
                    )?;
                    c.execute("DELETE FROM __sys_outbox_lanes WHERE lane=?1", [&lane])?;
                }
                return Ok(false);
            }
            Ok(true)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_work_and_missing_evidence_fail_before_base_schema_mutation() -> anyhow::Result<()>
    {
        let c = Connection::open_in_memory()?;
        super::super::schema(&c)?;
        let outbox = super::super::event_schema::Schema {
            marker: "__sys_outbox_schema",
            ddl: include_str!("outbox.sql"),
            required: &[],
        };
        c.execute_batch("BEGIN")?;
        outbox.open(&c)?;
        SCHEMA.open(&c)?;
        c.execute(
            "INSERT INTO __sys_event_source VALUES(1,?1,1)",
            ["0".repeat(32)],
        )?;
        c.execute_batch("CREATE TABLE business(v INTEGER); INSERT INTO business VALUES(7)")?;
        let operation = "00000000000000000000000000000001";
        let receipt=serde_json::json!({"operation":operation,"source":"Account:source","incarnation":"0".repeat(32),"state":"checking"}).to_string();
        c.execute("INSERT INTO __sys_outbox_operations(operation,digest,operator,receipt,size,created_ms) VALUES(?1,?2,'outbox',?3,?4,0)",rusqlite::params![operation,"0".repeat(64),receipt,receipt.len() as i64])?;
        c.execute(
            "INSERT INTO __sys_work VALUES('Account:source','outbox',1,0)",
            [],
        )?;
        c.execute(
            "INSERT INTO __sys_outbox_source_lifecycle VALUES(1,?1,1,'checking',?2,?3,1)",
            rusqlite::params!["0".repeat(32), operation, "0".repeat(64)],
        )?;
        c.execute_batch("COMMIT")?;
        assert!(check_schema(&c)?);
        for bad_proof in [false, true] {
            if bad_proof {
                c.execute("UPDATE __sys_work SET generation=1", [])?;
                c.execute(
                    "UPDATE __sys_outbox_source_lifecycle SET state='verified'",
                    [],
                )?;
                let encoded = receipt.replace("checking", "verified");
                c.execute(
                    "UPDATE __sys_outbox_operations SET receipt=?1,size=?2",
                    rusqlite::params![encoded, encoded.len() as i64],
                )?;
                c.execute(
                    "INSERT INTO __sys_outbox_destroy_lanes VALUES(?1,?2,'{}',1,NULL)",
                    rusqlite::params![operation, "a".repeat(64)],
                )?;
            } else {
                c.execute("UPDATE __sys_work SET generation=2", [])?;
            }
            let before: i64 = c.query_row("PRAGMA schema_version", [], |row| row.get(0))?;
            let refusal = super::super::schema(&c).unwrap_err();
            assert_eq!(
                refusal
                    .downcast_ref::<crate::application::event_stream::DeliveryError>()
                    .unwrap()
                    .code,
                crate::application::event_stream::ErrorCode::UnsupportedProtocol
            );
            assert_eq!(
                c.query_row("PRAGMA schema_version", [], |row| row.get::<_, i64>(0))?,
                before
            );
            assert_eq!(
                c.query_row("SELECT v FROM business", [], |row| row.get::<_, i64>(0))?,
                7
            );
        }
        Ok(())
    }

    #[test]
    fn source_receipts_survive_activation_and_corrupt_accounting_fails_closed() -> anyhow::Result<()>
    {
        let c = Connection::open_in_memory()?;
        c.execute_batch("BEGIN")?;
        SCHEMA.open(&c)?;
        let receipt = "{\"operation\":\"00000000000000000000000000000001\"}";
        c.execute("INSERT INTO __sys_outbox_operations(operation,digest,operator,receipt,size,created_ms) VALUES(?1,?2,'outbox',?3,?4,0)",rusqlite::params!["00000000000000000000000000000001","0".repeat(64),receipt,receipt.len() as i64])?;
        c.execute_batch("COMMIT")?;
        super::super::schema(&c)?;
        let retained: String = c.query_row(
            "SELECT receipt FROM __sys_outbox_operations WHERE sequence=1",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(retained, receipt);
        c.execute("UPDATE __sys_outbox_operation_stats SET rows=0", [])?;
        let failure = super::super::schema(&c).unwrap_err();
        assert_eq!(
            failure
                .downcast_ref::<crate::application::event_stream::DeliveryError>()
                .unwrap()
                .code,
            crate::application::event_stream::ErrorCode::UnsupportedProtocol
        );
        let retained: String = c.query_row(
            "SELECT receipt FROM __sys_outbox_operations WHERE sequence=1",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(retained, receipt);
        Ok(())
    }
}
