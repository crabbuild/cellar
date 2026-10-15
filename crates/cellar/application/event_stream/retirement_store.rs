use super::{
    authority::{Action, Authority},
    error::error,
    store, subscription_store, ErrorCode, Retirement,
};
use crate::application::storage::{SqlRow, SqlValue};
use crate::application::{Context, Result, Storage};
use base64::Engine;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub(crate) fn decode(encoded: &str) -> Result<(Retirement, String)> {
    if encoded.len() > 8192 {
        return Err(error(
            ErrorCode::PayloadTooLarge,
            "retirement request too large",
        ));
    }
    let bytes = base64::engine::general_purpose::STANDARD.decode(encoded)?;
    let request: Retirement = serde_json::from_slice(&bytes)?;
    request.validate()?;
    let digest = hex::encode(Sha256::digest(serde_json::to_vec(&request)?));
    Ok((request, digest))
}

fn request(body: &Value) -> Result<(Retirement, String)> {
    decode(
        body["request"]
            .as_str()
            .ok_or_else(|| error(ErrorCode::InvalidRequest, "retirement request missing"))?,
    )
}

fn facts(db: &Storage, force: bool, now: i64, end: i64) -> Result<()> {
    let unsealed = db.system_sql(
        "SELECT count(*) AS n FROM __sys_stream_producers WHERE sealed=0",
        &[],
    )?;
    let lagging=db.system_sql("SELECT count(*) AS n FROM __sys_stream_subscriptions WHERE state!='deleted' AND checkpoint<?",&[(end-1).into()])?;
    let leases = db.system_sql(
        "SELECT count(*) AS n FROM __sys_stream_leases WHERE expires_ms>?",
        &[now.into()],
    )?;
    cellar_logic::event_stream::retire(
        force,
        store::number(&unsealed[0], "n")?,
        store::number(&lagging[0], "n")?,
        store::number(&leases[0], "n")?,
    )
    .map_err(|failure| error(ErrorCode::PendingDelivery, failure.to_string()))
}

fn authorized(context: &Context) -> Result<(Storage, super::Definition, SqlRow, Authority)> {
    let caller = store::authority(context, Action::Administer)?;
    let db = context.storage()?;
    let (definition, row) = store::load_state(&db)?;
    store::authorize(&definition, &caller, Action::Administer, None)?;
    Ok((db, definition, row, caller))
}

pub(crate) fn check(context: &Context, body: &Value) -> Result<Value> {
    let (request, digest) = request(body)?;
    let (db, definition, row, _) = authorized(context)?;
    if store::text(&row, "lifecycle")? != "active" {
        return Err(error(ErrorCode::DestinationDeleted, "stream is closed"));
    }
    if request.revision != definition.revision {
        return Err(error(
            ErrorCode::ConfigConflict,
            "retirement revision differs from the stream",
        ));
    }
    let now = crate::asyncrt::wall_ms().max(store::number(&row, "logical_ms")?);
    facts(
        &db,
        request.force(),
        now,
        store::number(&row, "next_offset")?,
    )?;
    Ok(
        json!({"resource":definition.resource.id,"operation":request.operation,"digest":digest,"revision":definition.revision.to_string()}),
    )
}

fn response(db: &Storage, resource: &str, row: &SqlRow) -> Result<Value> {
    let (_, stream) = store::load_state(db)?;
    let end = store::number(&stream, "next_offset")?;
    let records = store::number(row, "records")?;
    let first = end
        .checked_sub(records)
        .filter(|first| *first >= 1)
        .ok_or_else(|| {
            error(
                ErrorCode::UnsupportedProtocol,
                "invalid retained retirement range",
            )
        })?;
    let affected = if records == 0 {
        None
    } else {
        Some(super::OffsetRange {
            first,
            last: end - 1,
        })
    };
    Ok(
        json!({"resource":resource,"operation":store::text(row,"operation")?,"digest":store::text(row,"digest")?,
        "operator":store::operator(row)?,"affected":affected,
        "state":if matches!(row.get("finished_ms"),Some(SqlValue::Integer(_))) {"deleted"}else{"retiring"},
        "records":store::number(row,"records")?.to_string(),"bytes":store::number(row,"bytes")?.to_string()}),
    )
}

pub(crate) fn acceptance(reply: &Value, operator: Option<&str>) -> Result<()> {
    let accepted_operator = match reply.get("operator") {
        Some(Value::Null) => None,
        Some(Value::String(value)) => Some(value.as_str()),
        _ => {
            return Err(error(
                ErrorCode::ConfigConflict,
                "retirement operator acceptance is missing or invalid",
            ))
        }
    };
    if accepted_operator != operator {
        return Err(error(
            ErrorCode::ConfigConflict,
            "retirement operator acceptance differs",
        ));
    }
    let records = subscription_store::integer(reply, "records")?;
    let bytes = subscription_store::integer(reply, "bytes")?;
    if records < 0
        || bytes < records
        || (records == 0 && bytes != 0)
        || reply["records"].as_str() != Some(records.to_string().as_str())
        || reply["bytes"].as_str() != Some(bytes.to_string().as_str())
    {
        return Err(error(
            ErrorCode::ConfigConflict,
            "invalid retirement acceptance counters",
        ));
    }
    let affected: Option<super::OffsetRange> =
        serde_json::from_value(reply.get("affected").cloned().ok_or_else(|| {
            error(
                ErrorCode::ConfigConflict,
                "retirement affected range is missing",
            )
        })?)?;
    let matches = match affected {
        None => records == 0,
        Some(range) => {
            range.first >= 1
                && range.last < i64::MAX
                && range
                    .last
                    .checked_sub(range.first)
                    .and_then(|length| length.checked_add(1))
                    == Some(records)
                && records > 0
        }
    };
    if !matches {
        return Err(error(
            ErrorCode::ConfigConflict,
            "retirement affected range conflicts with captured records",
        ));
    }
    Ok(())
}

pub(crate) fn close(context: &Context, body: &Value) -> Result<Value> {
    let (request, digest) = request(body)?;
    let (db, definition, row, caller) = authorized(context)?;
    let now = if store::text(&row, "lifecycle")? == "active" {
        store::clock(&db)?
    } else {
        // Terminal receipt replay makes no lease decision or new observation.
        store::number(&row, "logical_ms")?
    };
    db.tx(|tx| {
        let previous=tx.system_sql("SELECT * FROM __sys_stream_retirement WHERE singleton=1",&[])?;
        if let Some(previous)=previous.first() {
            if store::text(previous,"principal")?!=caller.principal || store::text(previous,"grant_id")?!=caller.grant || store::operator(previous)?!=caller.operator {return Err(error(ErrorCode::Forbidden,"retirement belongs to another caller"));}
            if store::text(previous,"operation")?!=request.operation || store::text(previous,"digest")?!=digest {return Err(error(ErrorCode::ConfigConflict,"stream already has another terminal retirement"));}
            return response(tx,&definition.resource.id,previous);
        }
        if store::text(&row,"lifecycle")?!="active" {return Err(error(ErrorCode::DestinationDeleted,"stream is closed"));}
        if request.revision!=definition.revision {return Err(error(ErrorCode::ConfigConflict,"retirement revision differs from the stream"));}
        facts(tx,request.force(),now,store::number(&row,"next_offset")?)?;
        tx.system_sql("INSERT INTO __sys_stream_retirement(singleton,operation,digest,request,principal,grant_id,operator,started_ms,records,bytes) VALUES(1,?,?,?,?,?,?,?,?,?)",
            &[request.operation.clone().into(),digest.into(),serde_json::to_string(&request)?.into(),caller.principal.into(),caller.grant.into(),caller.operator.map(SqlValue::Text).unwrap_or(SqlValue::Null),now.into(),store::number(&row,"rows")?.into(),store::number(&row,"bytes")?.into()])?;
        tx.system_sql("UPDATE __sys_stream SET lifecycle='deleted',logical_ms=? WHERE singleton=1",&[now.into()])?;
        tx.schedule_work(cellar_logic::work::Reason::StreamMaintenance,now)?;
        response(tx,&definition.resource.id,&tx.system_sql("SELECT * FROM __sys_stream_retirement WHERE singleton=1",&[])?.remove(0))
    })
}

/// Terminal closure is never rolled back to active. Purge bounded batches,
/// preserving the catalog incarnation and partition retirement receipt.
pub(crate) fn cleanup(db: &Storage, now: i64) -> Result<()> {
    db.tx(|tx| {
        let (_,row)=store::load_state(tx)?;
        let limits=crate::application::limits::current().events;
        let events=tx.system_sql("SELECT offset,size FROM __sys_stream_events ORDER BY offset LIMIT ?",&[i64::try_from(limits.sweep_rows.min(1000))?.into()])?;
        let mut bytes=0i64;let mut rows=0i64;let mut last=None;
        for event in events {
            let size=store::number(&event,"size")?;
            if rows>0 && bytes.saturating_add(size)>i64::try_from(limits.sweep_bytes.min(4<<20))? {break;}
            bytes=bytes.checked_add(size).ok_or_else(||anyhow::anyhow!("retirement byte overflow"))?;rows+=1;last=Some(store::number(&event,"offset")?);
        }
        if let Some(last)=last {
            tx.system_sql("DELETE FROM __sys_stream_events WHERE offset<=?",&[last.into()])?;
            tx.system_sql("UPDATE __sys_stream SET rows=rows-?,bytes=bytes-?,earliest_offset=? WHERE singleton=1",&[rows.into(),bytes.into(),(last+1).into()])?;
        } else {
            // Compact identity history is bounded too; sweep it rather than
            // turning a million-row terminal transaction into one large lock.
            let mut remaining_rows=limits.sweep_rows.min(1000);
            let mut remaining_bytes=i64::try_from(limits.sweep_bytes.min(4<<20))?;
            let mut deleted=0usize;
            'tables: for (table,key,size) in [
                ("__sys_stream_producers","lane","length(CAST(lane AS BLOB))+length(CAST(source AS BLOB))+length(CAST(incarnation AS BLOB))+length(CAST(principal AS BLOB))+length(CAST(grant_id AS BLOB))+32"),
                ("__sys_stream_leases","subscription","length(CAST(subscription AS BLOB))+length(CAST(token AS BLOB))+length(CAST(fetch_id AS BLOB))+48"),
                ("__sys_stream_ack_receipts","subscription","length(CAST(subscription AS BLOB))+length(CAST(receipt AS BLOB))"),
                ("__sys_stream_subscriptions","id","length(CAST(id AS BLOB))+length(CAST(name AS BLOB))+length(CAST(generation AS BLOB))+length(CAST(start AS BLOB))+length(CAST(state AS BLOB))+48"),
                ("__sys_stream_operations","sequence","size+length(CAST(operation AS BLOB))+length(CAST(digest AS BLOB))+length(CAST(principal AS BLOB))+length(CAST(grant_id AS BLOB))+32"),
            ] {
                if remaining_rows==0 {break;}
                let query=format!("SELECT {key},({size}) AS encoded_size FROM {table} ORDER BY {key} LIMIT ?");
                let candidates=tx.system_sql(&query,&[i64::try_from(remaining_rows)?.into()])?;
                for candidate in candidates {
                    let bytes=store::number(&candidate,"encoded_size")?;
                    if deleted>0 && bytes>remaining_bytes {break 'tables;}
                    let identity=candidate.get(key).ok_or_else(||anyhow::anyhow!("retirement row key missing"))?.clone();
                    tx.system_sql(&format!("DELETE FROM {table} WHERE {key}=?"),&[identity])?;
                    remaining_bytes=remaining_bytes.saturating_sub(bytes);
                    remaining_rows-=1;deleted+=1;
                }
            }

        }
        let pending=tx.system_sql("SELECT EXISTS(SELECT 1 FROM __sys_stream_events) OR EXISTS(SELECT 1 FROM __sys_stream_producers)
            OR EXISTS(SELECT 1 FROM __sys_stream_leases) OR EXISTS(SELECT 1 FROM __sys_stream_ack_receipts)
            OR EXISTS(SELECT 1 FROM __sys_stream_subscriptions) OR EXISTS(SELECT 1 FROM __sys_stream_operations) AS pending",&[])?;
        if store::number(&pending[0],"pending")?==0 {
            tx.system_sql("UPDATE __sys_stream SET rows=0,bytes=0,earliest_offset=next_offset WHERE singleton=1",&[])?;
            tx.system_sql("UPDATE __sys_stream_retirement SET finished_ms=coalesce(finished_ms,?) WHERE singleton=1",&[now.max(store::number(&row,"logical_ms")?).into()])?;
            tx.clear_current_work(cellar_logic::work::Reason::StreamMaintenance)?;
        } else {tx.schedule_work(cellar_logic::work::Reason::StreamMaintenance,now.saturating_add(1000))?;}
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retirement_acceptance_preserves_exact_ranges_and_operator_identity() -> Result<()> {
        let original = json!({"operator":"operator","records":"2","bytes":"200","affected":{"first":"9007199254740993","last":"9007199254740994"}});
        acceptance(&original, Some("operator"))?;
        assert!(acceptance(&original, Some("other")).is_err());
        for (key, value) in [
            ("records", json!("3")),
            ("bytes", json!("-1")),
            ("operator", json!(false)),
            ("affected", Value::Null),
        ] {
            let mut invalid = original.clone();
            invalid[key] = value;
            assert!(acceptance(&invalid, Some("operator")).is_err());
        }
        acceptance(
            &json!({"operator":null,"records":"0","bytes":"0","affected":null}),
            None,
        )?;
        Ok(())
    }
}
