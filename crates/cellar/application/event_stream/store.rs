use super::{
    authority::{Action, Authority},
    config::{Definition, Grant, Retention, Start},
    error::{error, ErrorCode},
    Envelope,
};
use crate::application::storage::{SqlRow, SqlValue};
use crate::application::{Context, Result, Storage};
use anyhow::Context as _;
use base64::Engine;
use rand::RngCore;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub(crate) fn text(row: &SqlRow, key: &str) -> Result<String> {
    match row.get(key) {
        Some(SqlValue::Text(value)) => Ok(value.clone()),
        _ => Err(anyhow::anyhow!("invalid stored stream {key}")),
    }
}

pub(crate) fn number(row: &SqlRow, key: &str) -> Result<i64> {
    match row.get(key) {
        Some(SqlValue::Integer(value)) => Ok(*value),
        _ => Err(anyhow::anyhow!("invalid stored stream {key}")),
    }
}

pub(crate) fn operator(row: &SqlRow) -> Result<Option<String>> {
    match row.get("operator") {
        Some(SqlValue::Null) => Ok(None),
        Some(SqlValue::Text(value)) if !value.is_empty() && value.len() <= 256 => {
            Ok(Some(value.clone()))
        }
        _ => Err(error(
            ErrorCode::UnsupportedProtocol,
            "invalid stored operator provenance",
        )),
    }
}

pub(crate) fn uuid(label: &'static str) -> String {
    let mut bytes = [0; 16];
    crate::asyncrt::rng(label).fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 15) | 64;
    bytes[8] = (bytes[8] & 63) | 128;
    hex::encode(bytes)
}

pub(crate) fn validate_id(id: &str) -> Result<()> {
    if id.len() != 32
        || !id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(error(
            ErrorCode::InvalidRequest,
            "invalid operation identity",
        ));
    }
    Ok(())
}

pub(crate) fn authority(context: &Context, action: Action) -> Result<Authority> {
    let authority = context
        .event_authority
        .clone()
        .ok_or_else(|| error(ErrorCode::Forbidden, "event RPC has no host authority"))?;
    authority.validate()?;
    if authority.action != action {
        return Err(error(
            ErrorCode::Forbidden,
            "event RPC action does not match its authority",
        ));
    }
    let scope = &context
        .identity()
        .ok_or_else(|| error(ErrorCode::Forbidden, "event RPC requires a system cell"))?
        .scope;
    super::validate_target(scope, &authority)?;
    Ok(authority)
}

pub(crate) fn authorize<'a>(
    definition: &'a Definition,
    authority: &Authority,
    action: Action,
    subscription: Option<&str>,
) -> Result<&'a Grant> {
    if !definition.resource.same_identity(&authority.resource) || authority.action != action {
        return Err(error(
            ErrorCode::Forbidden,
            "event resource or operation authority mismatch",
        ));
    }
    let grant = definition
        .grants
        .iter()
        .find(|grant| grant.id == authority.grant && grant.principal == authority.principal)
        .ok_or_else(|| {
            error(
                ErrorCode::Forbidden,
                "event resource grant is absent or revoked",
            )
        })?;
    let allowed = match action {
        Action::Resolve => {
            grant.publish || grant.read || grant.administer || !grant.subscriptions.is_empty()
        }
        Action::Configure | Action::Administer => grant.administer,
        Action::Append => grant.publish,
        Action::Read => grant.read,
        Action::Consume => subscription
            .is_some_and(|name| grant.subscriptions.iter().any(|allowed| allowed == name)),
    };
    if !allowed {
        return Err(error(
            ErrorCode::Forbidden,
            "event resource grant denies this operation",
        ));
    }
    Ok(grant)
}

pub(crate) fn load(db: &Storage) -> Result<(Definition, SqlRow)> {
    let (definition, row) = load_state(db)?;
    if text(&row, "lifecycle")? == "deleted" {
        return Err(error(
            ErrorCode::DestinationDeleted,
            "stream incarnation was deleted",
        ));
    }
    Ok((definition, row))
}

pub(crate) fn resolve(context: &Context, body: &Value) -> Result<Value> {
    let caller = authority(context, Action::Resolve)?;
    let encoded = body["binding"]
        .as_str()
        .context("binding resolution request missing")?;
    if encoded.len() > (64usize << 10).div_ceil(3) * 4 {
        return Err(error(
            ErrorCode::PayloadTooLarge,
            "binding resolution request too large",
        ));
    }
    let bytes = base64::engine::general_purpose::STANDARD.decode(encoded)?;
    if bytes.len() > 64 << 10 {
        return Err(error(
            ErrorCode::PayloadTooLarge,
            "binding resolution request too large",
        ));
    }
    let binding: super::Binding = serde_json::from_slice(&bytes)?;
    binding.validate()?;
    let db = context.storage()?;
    let (definition, _) = load(&db)?;
    let grant = authorize(&definition, &caller, Action::Resolve, None)?;
    if binding.principal != caller.principal
        || binding.grant != caller.grant
        || binding.resource != definition.resource
        || (binding.publish && !grant.publish)
        || (binding.read && !grant.read)
        || (binding.administer && !grant.administer)
        || binding
            .subscriptions
            .iter()
            .any(|name| !grant.subscriptions.contains(name))
    {
        return Err(error(
            ErrorCode::Forbidden,
            "binding does not match current resource identity or grants",
        ));
    }
    for name in &binding.subscriptions {
        if db
            .system_sql(
                "SELECT id FROM __sys_stream_subscriptions WHERE name=? AND state!='deleted'",
                &[name.clone().into()],
            )?
            .is_empty()
        {
            return Err(error(
                ErrorCode::SubscriptionReset,
                "bound subscription is not live",
            ));
        }
    }
    Ok(
        json!({"resource":definition.resource,"state":"active","revision":definition.revision.to_string()}),
    )
}

pub(crate) fn load_state(db: &Storage) -> Result<(Definition, SqlRow)> {
    let present = db.system_sql(
        "SELECT name FROM sqlite_schema WHERE type='table' AND name='__sys_stream'",
        &[],
    )?;
    if present.is_empty() {
        return Err(error(
            ErrorCode::NotInitialized,
            "stream is not provisioned",
        ));
    }
    let row = db
        .system_sql("SELECT * FROM __sys_stream WHERE singleton=1", &[])?
        .into_iter()
        .next()
        .ok_or_else(|| error(ErrorCode::NotInitialized, "stream is not provisioned"))?;
    let definition: Definition = serde_json::from_str(&text(&row, "definition")?)?;
    Ok((definition, row))
}

pub(crate) fn decode_definition(encoded: &str) -> Result<(Definition, String)> {
    let maximum = super::config::MAX_DEFINITION_BYTES;
    if encoded.len() > maximum.div_ceil(3) * 4 {
        return Err(error(
            ErrorCode::PayloadTooLarge,
            "stream definition exceeds its bound",
        ));
    }
    let bytes = base64::engine::general_purpose::STANDARD.decode(encoded)?;
    if bytes.len() > maximum {
        return Err(error(
            ErrorCode::PayloadTooLarge,
            "stream definition exceeds its bound",
        ));
    }
    let definition: Definition = serde_json::from_slice(&bytes)?;
    definition.validate()?;
    let digest = hex::encode(Sha256::digest(serde_json::to_vec(&definition)?));
    Ok((definition, digest))
}

pub(crate) fn initialize(context: &Context, body: &Value) -> Result<Value> {
    let caller = authority(context, Action::Configure)?;
    let operation = body["operation"].as_str().ok_or_else(|| {
        error(
            ErrorCode::InvalidRequest,
            "configuration operation is missing",
        )
    })?;
    validate_id(operation)?;
    let encoded = body["definition"]
        .as_str()
        .ok_or_else(|| error(ErrorCode::InvalidRequest, "stream definition is missing"))?;
    let (desired, digest) = decode_definition(encoded)?;
    if !desired.resource.same_identity(&caller.resource)
        || super::cell_scope(&desired.resource)? != context.identity().unwrap().scope
    {
        return Err(error(
            ErrorCode::Forbidden,
            "configuration names a different resource",
        ));
    }
    let db = context.storage()?;
    super::schema::open(&db)?;
    db.tx(|tx| {
        let prior=tx.system_sql("SELECT * FROM __sys_stream WHERE singleton=1",&[])?.into_iter().next();
        if let Some(ref prior)=prior {
            if text(prior,"lifecycle")?=="deleted" { return Err(error(ErrorCode::DestinationDeleted,"stream incarnation was deleted")); }
            if text(prior,"config_operation")?==operation && text(prior,"config_digest")?==digest
                && text(prior,"config_principal")?==caller.principal && text(prior,"config_grant")?==caller.grant {
                return Ok(json!({"revision":number(prior,"revision")?.to_string(),"resource":desired.resource.id,"operation":operation,"digest":digest}));
            }
            let current:Definition=serde_json::from_str(&text(prior,"definition")?)?;
            authorize(&current,&caller,Action::Configure,None)?;
            if current.resource.name!=desired.resource.name || desired.revision<=current.revision || desired.max_bytes<number(prior,"bytes")?
                || desired.max_records<number(prior,"rows")? {
                return Err(error(ErrorCode::ConfigConflict,"configuration revision or live capacity conflicts"));
            }
        } else {
            caller.require_provisioning()?;
            authorize(&desired,&caller,Action::Configure,None)?;
            if desired.revision!=1 { return Err(error(ErrorCode::ConfigConflict,"new streams start at revision one")); }
        }
        let next=prior.as_ref().map(|row|number(row,"next_offset")).transpose()?.unwrap_or(1);
        let earliest=prior.as_ref().map(|row|number(row,"earliest_offset")).transpose()?.unwrap_or(1);
        for sub in &desired.subscriptions {
            let start=serde_json::to_string(&sub.start)?;
            let existing=tx.system_sql("SELECT * FROM __sys_stream_subscriptions WHERE name=? AND state!='deleted'",&[sub.name.clone().into()])?;
            if let Some(existing)=existing.first() {
                if text(existing,"start")?!=start || text(existing,"state")?=="deleted" {
                    return Err(error(ErrorCode::ConfigConflict,"subscription start or incarnation conflicts"));
                }
                if sub.required && (text(existing,"state")?=="expired" || number(existing,"checkpoint")?<earliest-1) {
                    return Err(error(ErrorCode::ConfigConflict,"required subscription history is already expired; reset explicitly"));
                }
                tx.system_sql("UPDATE __sys_stream_subscriptions SET required=? WHERE id=?",&[i64::from(sub.required).into(),text(existing,"id")?.into()])?;
            } else {
                let first=match sub.start { Start::Earliest=>earliest,Start::Latest=>next,Start::Offset(offset)=>offset };
                cellar_logic::event_stream::cursor(earliest,next,first).map_err(|_|error(ErrorCode::InvalidCursor,"subscription start is not retained"))?;
                tx.system_sql("INSERT INTO __sys_stream_subscriptions(id,name,generation,start,checkpoint,required) VALUES(?,?,?,?,?,?)",
                    &[uuid("stream-subscription-id").into(),sub.name.clone().into(),uuid("stream-subscription-generation").into(),start.into(),(first-1).into(),i64::from(sub.required).into()])?;
            }
        }
        // Dropping an existing subscription is an explicit lifecycle operation.
        let active=tx.system_sql("SELECT name FROM __sys_stream_subscriptions WHERE state!='deleted'",&[])?;
        if active.iter().any(|row|text(row,"name").is_ok_and(|name|!desired.subscriptions.iter().any(|sub|sub.name==name))) {
            return Err(error(ErrorCode::ConfigConflict,"configuration cannot silently remove a subscription"));
        }
        let limits=crate::application::limits::current().events;
        if active.len()>limits.max_subscriptions { return Err(error(ErrorCode::SubscriptionLimit,"subscription count exceeds host policy")); }
        let definition=serde_json::to_string(&desired)?;
        tx.system_sql("INSERT INTO __sys_stream(singleton,definition,revision,lifecycle,config_operation,config_digest,config_principal,config_grant)
            VALUES(1,?,?,'active',?,?,?,?) ON CONFLICT(singleton) DO UPDATE SET definition=excluded.definition,
            revision=excluded.revision,config_operation=excluded.config_operation,config_digest=excluded.config_digest,
            config_principal=excluded.config_principal,config_grant=excluded.config_grant",
            &[definition.into(),desired.revision.into(),operation.into(),digest.clone().into(),caller.principal.clone().into(),caller.grant.clone().into()])?;
        Ok(json!({"revision":desired.revision.to_string(),"resource":desired.resource.id,"operation":operation,"digest":digest}))
    })
}

pub(crate) fn sweep(
    db: &Storage,
    definition: &Definition,
    now: i64,
    incoming_rows: i64,
    incoming_bytes: i64,
) -> Result<()> {
    let row = db
        .system_sql("SELECT * FROM __sys_stream WHERE singleton=1", &[])?
        .remove(0);
    let total_rows = number(&row, "rows")?;
    let total_bytes = number(&row, "bytes")?;
    let checkpoints = if definition.retention == Retention::Required {
        db.system_sql("SELECT checkpoint FROM __sys_stream_subscriptions WHERE required=1 AND state!='deleted'",&[])?
            .iter().map(|row|number(row,"checkpoint")).collect::<Result<Vec<_>>>()?
    } else {
        vec![]
    };
    let leases = db
        .system_sql(
            "SELECT first_offset FROM __sys_stream_leases WHERE expires_ms>?",
            &[now.into()],
        )?
        .iter()
        .map(|row| number(row, "first_offset"))
        .collect::<Result<Vec<_>>>()?;
    let frontier = cellar_logic::event_stream::deletion_frontier(i64::MAX, checkpoints, leases)?;
    let limits = crate::application::limits::current().events;
    let candidates=db.system_sql("SELECT offset,size,append_ms FROM __sys_stream_events WHERE offset<=? ORDER BY offset LIMIT ?",
        &[frontier.into(),i64::try_from(limits.sweep_rows)?.into()])?;
    let cutoff = now.saturating_sub(definition.max_age_ms);
    let mut rows = 0i64;
    let mut bytes = 0i64;
    let mut last = None;
    for candidate in candidates {
        let pressure = total_rows - rows > definition.max_records.saturating_sub(incoming_rows)
            || total_bytes - bytes > definition.max_bytes.saturating_sub(incoming_bytes);
        if number(&candidate, "append_ms")? > cutoff && !pressure {
            break;
        }
        let size = number(&candidate, "size")?;
        if rows > 0 && bytes.saturating_add(size) > i64::try_from(limits.sweep_bytes)? {
            break;
        }
        rows += 1;
        bytes = bytes
            .checked_add(size)
            .ok_or_else(|| anyhow::anyhow!("stream sweep byte overflow"))?;
        last = Some(number(&candidate, "offset")?);
    }
    if let Some(last) = last {
        db.system_sql(
            "DELETE FROM __sys_stream_events WHERE offset<=?",
            &[last.into()],
        )?;
        db.system_sql(
            "UPDATE __sys_stream SET rows=rows-?,bytes=bytes-?,earliest_offset=? WHERE singleton=1",
            &[
                rows.into(),
                bytes.into(),
                last.checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("stream floor overflow"))?
                    .into(),
            ],
        )?;
        db.system_sql("UPDATE __sys_stream_subscriptions SET state='expired' WHERE state NOT IN ('deleted','expired') AND checkpoint+1<? AND required=0",
            &[(last+1).into()])?;
    }
    Ok(())
}

pub(crate) fn schedule(db: &Storage, definition: &Definition) -> Result<()> {
    let rows = db.system_sql(
        "SELECT min(append_ms) AS oldest FROM __sys_stream_events",
        &[],
    )?;
    let oldest = match rows[0].get("oldest") {
        Some(SqlValue::Integer(value)) => Some(*value),
        Some(SqlValue::Null) => None,
        _ => return Err(anyhow::anyhow!("invalid stream age")),
    };
    let leases = db.system_sql(
        "SELECT min(expires_ms) AS expires FROM __sys_stream_leases",
        &[],
    )?;
    let expiry = match leases[0].get("expires") {
        Some(SqlValue::Integer(value)) => Some(*value),
        Some(SqlValue::Null) => None,
        _ => return Err(anyhow::anyhow!("invalid lease expiry")),
    };
    let now = crate::asyncrt::wall_ms();
    let age = oldest
        .map(|value| {
            value
                .checked_add(definition.max_age_ms)
                .ok_or_else(|| anyhow::anyhow!("stream age deadline overflow"))
        })
        .transpose()?;
    if let Some(deadline) = age.into_iter().chain(expiry).min() {
        // Protected old records must not cause an immediate, unbounded wake loop.
        db.schedule_work(
            cellar_logic::work::Reason::StreamMaintenance,
            deadline.max(now.saturating_add(1000)),
        )?;
    } else {
        db.clear_current_work(cellar_logic::work::Reason::StreamMaintenance)?;
    }
    Ok(())
}

pub(crate) fn append(context: &Context, body: &Value) -> Result<Value> {
    let caller = authority(context, Action::Append)?;
    let db = context.storage()?;
    let (definition, _) = load(&db)?;
    authorize(&definition, &caller, Action::Append, None)?;
    if body["protocol"].as_u64() != Some(1) {
        return Err(error(
            ErrorCode::UnsupportedProtocol,
            "unsupported stream append protocol",
        ));
    }
    let binding: super::Binding = serde_json::from_value(body["binding"].clone())?;
    if !binding.resource.same_identity(&caller.resource)
        || binding.principal != caller.principal
        || binding.grant != caller.grant
    {
        return Err(error(
            ErrorCode::Forbidden,
            "append packet does not match host authority",
        ));
    }
    let lane = body["lane"]
        .as_str()
        .ok_or_else(|| error(ErrorCode::InvalidRequest, "append lane is missing"))?;
    let records = body["records"]
        .as_array()
        .ok_or_else(|| error(ErrorCode::InvalidRequest, "append records are missing"))?;
    let limits = crate::application::limits::current().events;
    if records.is_empty() || records.len() > limits.batch_records() {
        return Err(error(
            ErrorCode::PayloadTooLarge,
            "append record count exceeds policy",
        ));
    }
    let mut decoded = Vec::new();
    let mut bytes = 0usize;
    for record in records {
        let encoded = record.as_str().ok_or_else(|| {
            error(
                ErrorCode::InvalidRequest,
                "append record is not encoded bytes",
            )
        })?;
        if encoded.len() > limits.event_bytes().div_ceil(3) * 4 {
            return Err(error(
                ErrorCode::PayloadTooLarge,
                "append envelope exceeds policy",
            ));
        }
        let raw = base64::engine::general_purpose::STANDARD.decode(encoded)?;
        bytes = bytes
            .checked_add(raw.len())
            .ok_or_else(|| error(ErrorCode::PayloadTooLarge, "append byte overflow"))?;
        if raw.len() > limits.event_bytes() || bytes > limits.batch_bytes() {
            return Err(error(
                ErrorCode::PayloadTooLarge,
                "append batch exceeds policy",
            ));
        }
        let envelope: Envelope = serde_json::from_slice(&raw)?;
        let expected = super::lane_identity(
            &definition.resource,
            &envelope.source,
            &envelope.source_incarnation,
        )?;
        if envelope.lane != lane
            || expected != lane
            || envelope.id != format!("v1:{lane}:{}", envelope.sequence)
            || envelope.source != caller.source.as_deref().unwrap_or_default()
            || envelope.family != definition.resource.family
            || envelope.key.len() > limits.max_key_bytes
        {
            return Err(error(
                ErrorCode::InvalidRequest,
                "append envelope identity conflicts",
            ));
        }
        validate_id(&envelope.source_incarnation)?;
        decoded.push((envelope, raw));
    }
    if decoded
        .iter()
        .any(|(_, raw)| raw.len() as i64 > definition.max_bytes)
    {
        return Err(error(
            ErrorCode::PayloadTooLarge,
            "event cannot fit stream capacity",
        ));
    }
    db.tx(|tx| {
        let (_,row)=load(tx)?;
        let now=crate::asyncrt::wall_ms().max(number(&row,"logical_ms")?);
        let producers=tx.system_sql("SELECT * FROM __sys_stream_producers WHERE lane=?",&[lane.into()])?;
        let high=producers.first().map(|producer|number(producer,"accepted_through")).transpose()?.unwrap_or(0);
        let sequences=decoded.iter().map(|(event,_)|event.sequence).collect::<Vec<_>>();
        let plan=cellar_logic::event_stream::append(high,&sequences).map_err(|failure| {
            let code=if matches!(failure,cellar_logic::event_stream::Failure::SequenceGap{..}) {ErrorCode::SequenceGap}else{ErrorCode::InvalidRequest};
            error(code,failure.to_string())
        })?;
        if let Some(producer)=producers.first() {
            if text(producer,"principal")?!=caller.principal || text(producer,"source")?!=decoded[0].0.source
                || text(producer,"incarnation")?!=decoded[0].0.source_incarnation || text(producer,"grant_id")?!=caller.grant {
                return Err(error(ErrorCode::Forbidden,"producer lane belongs to another source"));
            }
            if number(producer,"sealed")?!=0 && plan.skip!=decoded.len() { return Err(error(ErrorCode::ProducerSealed,"producer lane is sealed")); }
        } else {
            let count=tx.system_sql("SELECT count(*) AS n FROM __sys_stream_producers",&[])?;
            if number(&count[0],"n")?>=i64::try_from(limits.max_producer_lanes)? { return Err(error(ErrorCode::ProducerLimit,"stream producer metadata is full")); }
        }
        for (envelope,raw) in decoded.iter().take(plan.skip) {
            let prior=tx.system_sql("SELECT envelope FROM __sys_stream_events WHERE lane=? AND sequence=?",&[lane.into(),envelope.sequence.into()])?;
            if prior.first().is_some_and(|row|row.get("envelope")!=Some(&SqlValue::Blob(raw.clone()))) {
                return Err(error(ErrorCode::ConfigConflict,"retained event identity has different bytes"));
            }
        }
        let count=i64::try_from(decoded.len()-plan.skip)?;
        let size=decoded.iter().skip(plan.skip).try_fold(0i64,|total,(_,raw)|total.checked_add(raw.len() as i64).ok_or_else(||anyhow::anyhow!("append byte overflow")))?;
        if count>0 {
            sweep(tx,&definition,now,count,size)?;
            let (_,current)=load(tx)?;
            if number(&current,"rows")?>definition.max_records.saturating_sub(count)
                || number(&current,"bytes")?>definition.max_bytes.saturating_sub(size) || count>definition.max_records || size>definition.max_bytes {
                let mut failure=super::DeliveryError::new(ErrorCode::StreamFull,"stream capacity is protected or exhausted");
                failure.data=json!({"batch_bytes":definition.max_bytes.to_string(),"batch_records":definition.max_records.to_string()});
                return Err(failure.into());
            }
            let mut offset=number(&current,"next_offset")?;
            for (envelope,raw) in decoded.iter().skip(plan.skip) {
                let next=offset.checked_add(1).ok_or_else(||error(ErrorCode::StreamFull,"stream offsets exhausted"))?;
                tx.system_sql("INSERT INTO __sys_stream_events VALUES(?,?,?,?,?,?)",
                    &[offset.into(),lane.into(),envelope.sequence.into(),SqlValue::Blob(raw.clone()),(raw.len() as i64).into(),now.into()])?;
                offset=next;
            }
            tx.system_sql("UPDATE __sys_stream SET next_offset=?,rows=rows+?,bytes=bytes+?,logical_ms=? WHERE singleton=1",
                &[offset.into(),count.into(),size.into(),now.into()])?;
        }
        tx.system_sql("INSERT INTO __sys_stream_producers(lane,source,incarnation,principal,grant_id,accepted_through) VALUES(?,?,?,?,?,?)
            ON CONFLICT(lane) DO UPDATE SET accepted_through=excluded.accepted_through",
            &[lane.into(),decoded[0].0.source.clone().into(),decoded[0].0.source_incarnation.clone().into(),caller.principal.clone().into(),caller.grant.clone().into(),plan.accepted_through.into()])?;
        schedule(tx,&definition)?;
        Ok(json!({"protocol":1,"resource":definition.resource.id,"lane":lane,"accepted_through":plan.accepted_through.to_string(),
            "batch_bytes":definition.max_bytes.min(limits.batch_bytes() as i64).to_string(),
            "batch_records":definition.max_records.min(limits.batch_records() as i64).to_string()}))
    })
}

pub(crate) fn read(context: &Context, body: &Value) -> Result<Value> {
    let caller = authority(context, Action::Read)?;
    let db = context.storage()?;
    let (definition, row) = load(&db)?;
    authorize(&definition, &caller, Action::Read, None)?;
    let earliest = number(&row, "earliest_offset")?;
    let next = number(&row, "next_offset")?;
    let requested = match body.get("next") {
        None | Some(Value::Null) => earliest,
        Some(_) => super::subscription_store::integer(body, "next")
            .map_err(|_| error(ErrorCode::InvalidCursor, "invalid cursor position"))?,
    };
    if let Err(failure) = cellar_logic::event_stream::cursor(earliest, next, requested) {
        let mut failure = super::DeliveryError::new(
            if requested < earliest {
                ErrorCode::CursorExpired
            } else {
                ErrorCode::InvalidCursor
            },
            failure.to_string(),
        );
        failure.data = json!({"earliest":earliest.to_string()});
        return Err(failure.into());
    }
    let (count, budget) = budgets(body)?;
    let records = records(&db, requested, next - 1, count, budget)?;
    let cursor = records
        .last()
        .map(|row| {
            super::subscription_store::integer(row, "offset")?
                .checked_add(1)
                .ok_or_else(|| error(ErrorCode::InvalidCursor, "cursor overflow"))
        })
        .transpose()?
        .unwrap_or(requested);
    Ok(
        json!({"resource":definition.resource.id,"next":cursor.to_string(),"earliest":earliest.to_string(),"end":next.to_string(),"records":records}),
    )
}

pub(crate) fn budgets(body: &Value) -> Result<(usize, usize)> {
    let limits = crate::application::limits::current().events;
    let count = match body.get("max_records") {
        Some(value) => value
            .as_u64()
            .ok_or_else(|| error(ErrorCode::InvalidRequest, "invalid record budget"))?,
        None => 100,
    }
    .min(limits.batch_records() as u64) as usize;
    let budget = match body.get("max_bytes") {
        Some(value) => value
            .as_u64()
            .ok_or_else(|| error(ErrorCode::InvalidRequest, "invalid byte budget"))?,
        None => limits.batch_bytes() as u64,
    }
    .min(limits.batch_bytes() as u64) as usize;
    if count == 0 || budget == 0 {
        return Err(error(
            ErrorCode::InvalidRequest,
            "batch budgets must be positive",
        ));
    }
    Ok((count, budget))
}

/// Inspect bounded metadata before copying payloads. Never materialize
/// max_records * max_event_bytes when the caller requested a small byte budget.
pub(crate) fn records(
    db: &Storage,
    first: i64,
    last: i64,
    count: usize,
    budget: usize,
) -> Result<Vec<Value>> {
    let rows = db.system_sql("SELECT offset,size FROM __sys_stream_events WHERE offset>=? AND offset<=? ORDER BY offset LIMIT ?",
        &[first.into(),last.into(),i64::try_from(count)?.into()])?;
    let mut records = Vec::new();
    let mut size = 0usize;
    let mut expected = first;
    for row in rows {
        let offset = number(&row, "offset")?;
        if offset != expected {
            return Err(error(
                ErrorCode::InvalidCursor,
                "stream interval is not contiguous",
            ));
        }
        let encoded_size = usize::try_from(number(&row, "size")?)?;
        if encoded_size > budget.saturating_sub(size) {
            if records.is_empty() {
                return Err(error(
                    ErrorCode::BatchTooSmall,
                    "first retained event exceeds batch budget",
                ));
            }
            break;
        }
        let payload = db.system_sql(
            "SELECT envelope FROM __sys_stream_events WHERE offset=?",
            &[offset.into()],
        )?;
        let Some(SqlValue::Blob(bytes)) = payload.first().and_then(|row| row.get("envelope"))
        else {
            return Err(anyhow::anyhow!("invalid stored event bytes"));
        };
        if bytes.len() != encoded_size {
            return Err(anyhow::anyhow!("stored event size conflicts"));
        }
        size += encoded_size;
        records.push(json!({"offset":offset.to_string(),"envelope":base64::engine::general_purpose::STANDARD.encode(bytes)}));
        expected = offset
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::InvalidCursor, "cursor overflow"))?;
    }
    Ok(records)
}
