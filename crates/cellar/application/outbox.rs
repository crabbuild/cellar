//! Bounded source dispatch through owner routing. Persisted retry deadlines
//! survive interruption; a successful destination reply permits local cleanup.

use super::event_stream::{self, Binding, Envelope};
use super::storage::{SqlRow, SqlValue};
use super::{Context, Output, Result};
use base64::Engine;
use cellar_logic::work::{Installation, Reason};
use rand::RngCore;
use serde_json::json;

fn text(row: &SqlRow, column: &str) -> Result<String> {
    match row.get(column) {
        Some(SqlValue::Text(value)) => Ok(value.clone()),
        _ => anyhow::bail!("invalid stored outbox {column}"),
    }
}

fn number(row: &SqlRow, column: &str) -> Result<i64> {
    match row.get(column) {
        Some(SqlValue::Integer(value)) => Ok(*value),
        _ => anyhow::bail!("invalid stored outbox {column}"),
    }
}

pub(crate) async fn dispatch(context: Context, installation: Installation) -> Result<Output> {
    context.check_turn()?;
    let db = context.storage()?;
    let now = crate::asyncrt::wall_ms();
    if db.work_installation(Reason::Outbox)? != Some(installation) {
        return Ok(Output::Done);
    }
    db.ensure_outbox_schema()?;
    if crate::runtime::source_destruction::pending(&db)? {
        let result = async {
            crate::runtime::source_destruction::verify(&context).await?;
            crate::runtime::source_destruction::clean(&context).await
        }
        .await;
        context.check_turn()?;
        if result.is_err() || crate::runtime::source_destruction::pending(&db)? {
            crate::runtime::source_destruction::schedule(
                &db,
                crate::asyncrt::wall_ms().saturating_add(if result.is_ok() { 100 } else { 300000 }),
            )?;
        }
        return Ok(Output::Done);
    }
    let lanes = db.system_sql(
        "SELECT lanes.lane,binding,attempts,dispatch_generation,CASE WHEN status='sealed' THEN 1 ELSE 0 END AS sealing,
         seals.operation AS seal_operation,seals.terminal AS terminal FROM __sys_outbox_lanes AS lanes
         LEFT JOIN __sys_outbox_seals AS seals ON seals.lane=lanes.lane
         WHERE retry_at_ms<=? AND ((status!='sealed' AND EXISTS(
           SELECT 1 FROM __sys_outbox AS events WHERE events.lane=lanes.lane
         )) OR (status='sealed' AND seals.state!='accepted')) ORDER BY retry_at_ms,lanes.lane LIMIT 1",
        &[now.into()],
    )?;
    let Some(selected) = lanes.first() else {
        return reschedule(&context, installation);
    };
    let lane = text(selected, "lane")?;
    let limits = crate::application::limits::current().events;
    let Some(_dispatch) = context.io.host_services().events().dispatch(
        &context.identity().unwrap().scope,
        &lane,
        limits.max_dispatch_concurrency,
    ) else {
        // Another activation turn still owns this lane, or the host is full.
        // Re-arm durable work without starting a second in-flight batch.
        db.schedule_work(Reason::Outbox, now.saturating_add(1000))?;
        return Ok(Output::Done);
    };
    let binding: Binding = serde_json::from_str(&text(selected, "binding")?)?;
    let attempts = number(selected, "attempts")?;
    let dispatch_generation = number(selected, "dispatch_generation")?
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("outbox dispatch generation exhausted"))?;
    let sealing = number(selected, "sealing")? != 0;
    let (method, request, last, seal_operation) = if sealing {
        let terminal = number(selected, "terminal")?;
        let operation = text(selected, "seal_operation")?;
        let incarnation = db
            .system_sql(
                "SELECT incarnation FROM __sys_event_source WHERE singleton=1",
                &[],
            )?
            .remove(0);
        (
            "__streamSeal",
            json!([{"protocol":1,"binding":binding,"lane":lane,"incarnation":text(&incarnation,"incarnation")?,
            "operation":operation,"terminal":terminal.to_string()}]),
            terminal,
            Some(operation),
        )
    } else {
        let (records, last) = append_batch(&db, &lane, limits)?;
        (
            "__streamAppend",
            json!([{"protocol":1,"binding":binding,"lane":lane,"records":records}]),
            last,
            None,
        )
    };
    let retry = now
        .checked_add(cellar_logic::event_stream::retry_delay_ms(
            u32::try_from(attempts).unwrap_or(u32::MAX),
            crate::asyncrt::rng("outbox-retry").next_u64(),
        ))
        .ok_or_else(|| anyhow::anyhow!("outbox retry deadline overflow"))?;
    db.tx(|tx| {
        tx.system_sql(
            "UPDATE __sys_outbox_lanes SET retry_at_ms=?,dispatch_generation=?,
              attempts=CASE WHEN attempts<9223372036854775807 THEN attempts+1 ELSE attempts END
             WHERE lane=?",
            &[
                retry.into(),
                dispatch_generation.into(),
                lane.clone().into(),
            ],
        )?;
        tx.schedule_work(Reason::Outbox, retry)
    })?;
    // Discovery must precede transmission as well as the normal data/ownership
    // proof. The saved retry deadline remains armed while the RPC is pending.
    db.sync().await?;
    let authority = event_stream::authority::Authority::delivery(&context, &binding)?;
    let services = context.io.host_services();
    let metric_timer = services.events().metrics.timer(
        crate::event_metrics::Histogram::Dispatch,
        services.domain_token(),
    );
    let result = event_stream::call_authorized(
        &context,
        authority,
        event_stream::cell_scope(&binding.resource)?,
        Some(event_stream::cell_name(&binding.resource)?),
        method,
        request,
    )
    .await;
    metric_timer.finish(result.is_ok());
    context.check_turn()?;
    let current = db.system_sql(
        "SELECT dispatch_generation FROM __sys_outbox_lanes WHERE lane=?",
        &[lane.clone().into()],
    )?;
    if current
        .first()
        .map(|row| number(row, "dispatch_generation"))
        .transpose()?
        != Some(dispatch_generation)
    {
        return Ok(Output::Done);
    }
    match result {
        Ok(response) => {
            anyhow::ensure!(
                response["protocol"].as_u64() == Some(1)
                    && response["resource"].as_str() == Some(binding.resource.id.as_str())
                    && response["lane"].as_str() == Some(lane.as_str()),
                "stream append acceptance names a different delivery identity"
            );
            if let Some(operation) = seal_operation.as_deref() {
                anyhow::ensure!(
                    response["operation"].as_str() == Some(operation)
                        && response["state"].as_str() == Some("sealed")
                        && protocol_integer(&response, "terminal")? == last,
                    "producer seal acceptance mismatch"
                );
            } else {
                anyhow::ensure!(
                    protocol_integer(&response, "accepted_through")? == last,
                    "stream acceptance does not match the complete sent prefix"
                );
            }
            db.tx(|tx| {
                if let Some(operation)=seal_operation.as_deref() {
                    tx.system_sql("UPDATE __sys_outbox_seals SET state='accepted' WHERE lane=? AND operation=? AND terminal=?",
                        &[lane.clone().into(),operation.into(),last.into()])?;
                } else {
                    capture_batch_limits(tx,&lane,&response)?;
                    tx.system_sql("DELETE FROM __sys_outbox WHERE lane=? AND sequence<=?",&[lane.clone().into(),last.into()])?;
                }
                tx.system_sql("UPDATE __sys_outbox_lanes SET settled_through=max(settled_through,?),attempts=0,last_error=NULL,blocked_reason=NULL,retry_at_ms=?,
                    status=CASE WHEN status='sealed' THEN 'sealed' ELSE 'pending' END WHERE lane=?",
                    &[last.into(),crate::asyncrt::wall_ms().into(),lane.clone().into()])?;
                Ok(())
            })?;
        }
        Err(error) => {
            if let Some(failure) = error
                .downcast_ref::<event_stream::DeliveryError>()
                .filter(|failure| failure.code == event_stream::ErrorCode::StreamFull)
            {
                capture_batch_limits(&db, &lane, &failure.data)?;
            }
            let blocked_reason = error
                .downcast_ref::<event_stream::DeliveryError>()
                .filter(|error| error.is_permanent())
                .map(|error| format!("{:?}", error.code));
            let permanent = blocked_reason.is_some();
            let error = error.to_string().chars().take(1024).collect::<String>();
            db.tx(|tx| {
                tx.system_sql("UPDATE __sys_outbox_lanes SET last_error=?,blocked_reason=?,status=CASE WHEN status='sealed' THEN 'sealed' ELSE ? END,
                    retry_at_ms=CASE WHEN ?=1 THEN ? ELSE retry_at_ms END WHERE lane=?",
                    &[error.into(),blocked_reason.map(SqlValue::Text).unwrap_or(SqlValue::Null),if permanent {"blocked"} else {"pending"}.into(),i64::from(permanent).into(),crate::asyncrt::wall_ms().saturating_add(300_000).into(),lane.clone().into()])?;
                if sealing {tx.system_sql("UPDATE __sys_outbox_seals SET state=? WHERE lane=?",
                    &[if permanent {"blocked"} else {"pending"}.into(),lane.clone().into()])?;}
                Ok(())
            })?;
        }
    }
    let current = db
        .work_installation(Reason::Outbox)?
        .ok_or_else(|| anyhow::anyhow!("outbox dispatch lost its work obligation"))?;
    reschedule(&context, current)
}

fn capture_batch_limits(db: &super::Storage, lane: &str, reply: &serde_json::Value) -> Result<()> {
    let Some(bytes) = reply["batch_bytes"]
        .as_str()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
    else {
        return Ok(());
    };
    let Some(records) = reply["batch_records"]
        .as_str()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
    else {
        return Ok(());
    };
    db.system_sql("INSERT INTO __sys_outbox_batch_limits VALUES(?,?,?) ON CONFLICT(lane) DO UPDATE SET bytes=excluded.bytes,records=excluded.records",
        &[lane.into(),bytes.into(),records.into()])?;
    Ok(())
}

fn reschedule(context: &Context, installation: Installation) -> Result<Output> {
    let db = context.storage()?;
    let deadlines = db.system_sql(
        "SELECT min(retry_at_ms) AS at_ms FROM __sys_outbox_lanes AS lanes WHERE EXISTS(
          SELECT 1 FROM __sys_outbox AS events WHERE events.lane=lanes.lane
        ) OR EXISTS(SELECT 1 FROM __sys_outbox_seals AS seals WHERE seals.lane=lanes.lane AND seals.state!='accepted')",
        &[],
    )?;
    db.tx(|tx| {
        match deadlines.first().and_then(|row| row.get("at_ms")) {
            Some(SqlValue::Integer(deadline)) => tx.schedule_work(Reason::Outbox, *deadline)?,
            Some(SqlValue::Null) => {
                tx.clear_work(installation)?;
            }
            _ => anyhow::bail!("invalid outbox dispatch deadline"),
        }
        Ok(())
    })?;
    Ok(Output::Done)
}

fn protocol_integer(reply: &serde_json::Value, key: &str) -> Result<i64> {
    let text = reply[key]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing protocol {key}"))?;
    let value = text.parse::<i64>()?;
    anyhow::ensure!(text == value.to_string(), "noncanonical protocol {key}");
    Ok(value)
}

fn append_batch(
    db: &super::Storage,
    lane: &str,
    limits: cellar_logic::event_stream::Limits,
) -> Result<(Vec<String>, i64)> {
    let caps = db.system_sql(
        "SELECT bytes,records FROM __sys_outbox_batch_limits WHERE lane=?",
        &[lane.into()],
    )?;
    let (max_bytes, max_records) = if let Some(caps) = caps.first() {
        (
            usize::try_from(number(caps, "bytes")?)?.min(limits.batch_bytes()),
            usize::try_from(number(caps, "records")?)?.min(limits.batch_records()),
        )
    } else {
        (limits.batch_bytes(), limits.batch_records())
    };
    let candidates = db.system_sql(
        "SELECT sequence,size FROM __sys_outbox WHERE lane=? ORDER BY sequence LIMIT ?",
        &[lane.into(), i64::try_from(max_records)?.into()],
    )?;
    let mut records = Vec::new();
    let mut size = 0usize;
    let mut last = None;
    for candidate in candidates {
        let sequence = number(&candidate, "sequence")?;
        let encoded_size = usize::try_from(number(&candidate, "size")?)?;
        if encoded_size > max_bytes.saturating_sub(size) && !records.is_empty() {
            break;
        }
        // If a new resource limit is below a head event, send it alone so the
        // receiver can return the permanent event-capacity error. Never skip it.
        anyhow::ensure!(
            encoded_size <= limits.batch_bytes(),
            "stored event exceeds serving batch policy"
        );
        let payload = db.system_sql(
            "SELECT envelope FROM __sys_outbox WHERE lane=? AND sequence=?",
            &[lane.into(), sequence.into()],
        )?;
        let bytes = match payload.first().and_then(|row| row.get("envelope")) {
            Some(SqlValue::Blob(bytes)) => bytes,
            _ => anyhow::bail!("invalid stored outbox envelope"),
        };
        let next_size = size
            .checked_add(bytes.len())
            .ok_or_else(|| anyhow::anyhow!("outbox batch size overflow"))?;
        anyhow::ensure!(
            encoded_size == bytes.len(),
            "outbox envelope size does not match its row"
        );
        let envelope = serde_json::from_slice::<Envelope>(bytes)?;
        anyhow::ensure!(
            envelope.lane == lane,
            "outbox envelope lane does not match its row"
        );
        anyhow::ensure!(
            envelope.sequence == number(&candidate, "sequence")?,
            "outbox envelope sequence does not match its row"
        );
        last = Some(envelope.sequence);
        records.push(base64::engine::general_purpose::STANDARD.encode(bytes));
        size = next_size;
    }
    anyhow::ensure!(!records.is_empty(), "outbox lane has no pending prefix");
    Ok((
        records,
        last.ok_or_else(|| anyhow::anyhow!("outbox lane has no pending prefix"))?,
    ))
}
