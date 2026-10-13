use super::{
    authority::{Action, Authority},
    error::error,
    lifecycle::{
        AuditPage, AuditRecord, Change, ChangeAction, LeaseSummary, LifecycleReceipt, OffsetRange,
        SubscriptionIdentity, SubscriptionStatus,
    },
    store, subscription_store, Definition, ErrorCode,
};
use crate::application::storage::SqlRow;
use crate::application::{Context, Result, Storage};
use cellar_logic::subscription_lifecycle as policy;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub(crate) fn identity(definition: &Definition, sub: &SqlRow) -> Result<SubscriptionIdentity> {
    Ok(SubscriptionIdentity {
        resource: definition.resource.id.clone(),
        name: store::text(sub, "name")?,
        id: store::text(sub, "id")?,
        generation: store::text(sub, "generation")?,
    })
}

pub(crate) fn check_identity(definition: &Definition, sub: &SqlRow, body: &Value) -> Result<()> {
    let expected: SubscriptionIdentity = serde_json::from_value(body["expected"].clone())?;
    expected.validate()?;
    if expected.resource != definition.resource.id
        || expected.id != store::text(sub, "id")?
        || expected.generation != store::text(sub, "generation")?
        || expected.name != store::text(sub, "name")?
    {
        return Err(error(
            ErrorCode::SubscriptionReset,
            "subscription incarnation changed",
        ));
    }
    Ok(())
}

fn authorize(
    context: &Context,
    body: &Value,
    admin: bool,
) -> Result<(Storage, Definition, Authority, String)> {
    let action = context
        .event_authority
        .as_ref()
        .map(|caller| caller.action)
        .ok_or_else(|| error(ErrorCode::Forbidden, "missing host authority"))?;
    if (admin && action != Action::Administer)
        || !matches!(action, Action::Consume | Action::Administer)
    {
        return Err(error(
            ErrorCode::Forbidden,
            "invalid subscription authority",
        ));
    }
    let caller = store::authority(context, action)?;
    let name = body["subscription"]
        .as_str()
        .ok_or_else(|| error(ErrorCode::InvalidRequest, "subscription name missing"))?
        .to_owned();
    if name.is_empty() || name.len() > 128 {
        return Err(error(
            ErrorCode::InvalidRequest,
            "invalid subscription name",
        ));
    }
    let db = context.storage()?;
    let (definition, _) = store::load(&db)?;
    store::authorize(&definition, &caller, action, Some(&name))?;
    Ok((db, definition, caller, name))
}

fn active(db: &Storage, name: &str) -> Result<SqlRow> {
    db.system_sql(
        "SELECT * FROM __sys_stream_subscriptions WHERE name=? AND state!='deleted'",
        &[name.into()],
    )?
    .into_iter()
    .next()
    .ok_or_else(|| {
        error(
            ErrorCode::SubscriptionReset,
            "subscription is absent or deleted",
        )
    })
}

pub(crate) fn status(context: &Context, body: &Value) -> Result<Value> {
    let (db, definition, _, name) = authorize(context, body, false)?;
    let sub = active(&db, &name)?;
    let (_, row) = store::load(&db)?;
    let lease=db.system_sql("SELECT generation,first_offset,last_offset,expires_ms FROM __sys_stream_leases WHERE subscription=?",
        &[store::text(&sub,"id")?.into()])?.into_iter().next().map(|row|Ok::<_,anyhow::Error>(LeaseSummary{
            generation:store::number(&row,"generation")?,first:store::number(&row,"first_offset")?,
            last:store::number(&row,"last_offset")?,expires_ms:store::number(&row,"expires_ms")?,
        })).transpose()?;
    Ok(serde_json::to_value(SubscriptionStatus {
        identity: identity(&definition, &sub)?,
        state: store::text(&sub, "state")?,
        required: store::number(&sub, "required")? != 0,
        checkpoint: store::number(&sub, "checkpoint")?,
        earliest: store::number(&row, "earliest_offset")?,
        end: store::number(&row, "next_offset")?,
        retry_at_ms: store::number(&sub, "retry_at_ms")?,
        lease,
    })?)
}

fn state(state: &str) -> Result<policy::State> {
    match state {
        "active" => Ok(policy::State::Active),
        "paused" => Ok(policy::State::Paused),
        "expired" => Ok(policy::State::Expired),
        "deleted" => Ok(policy::State::Deleted),
        _ => Err(anyhow::anyhow!("invalid stored subscription state")),
    }
}

fn state_text(state: policy::State) -> &'static str {
    match state {
        policy::State::Active => "active",
        policy::State::Paused => "paused",
        policy::State::Expired => "expired",
        policy::State::Deleted => "deleted",
    }
}

pub(crate) fn change(context: &Context, body: &Value) -> Result<Value> {
    let (db, definition, caller, name) = authorize(context, body, true)?;
    let change: Change = serde_json::from_value(body["change"].clone())?;
    change.validate()?;
    if change.expected.resource != definition.resource.id || change.expected.name != name {
        return Err(error(
            ErrorCode::SubscriptionReset,
            "administrative request names another resource",
        ));
    }
    let digest = hex::encode(Sha256::digest(serde_json::to_vec(&change)?));
    let now = subscription_store::clock(&db)?;
    db.tx(|tx| {
        let prior=tx.system_sql("SELECT * FROM __sys_stream_operations WHERE operation=?",&[change.operation.clone().into()])?;
        if let Some(prior)=prior.first() {
            if store::text(prior,"principal")?!=caller.principal || store::text(prior,"grant_id")?!=caller.grant {
                return Err(error(ErrorCode::Forbidden,"operation belongs to another caller"));
            }
            if store::text(prior,"digest")?!=digest {return Err(error(ErrorCode::ConfigConflict,"operation identity was reused with different arguments"));}
            return Ok(serde_json::from_str(&store::text(prior,"receipt")?)?);
        }
        let sub=active(tx,&name)?;
        if identity(&definition,&sub)?!=change.expected {return Err(error(ErrorCode::SubscriptionReset,"administrative target incarnation changed"));}
        let count=tx.system_sql("SELECT count(*) AS n FROM __sys_stream_operations",&[])?;
        if store::number(&count[0],"n")?>=crate::application::limits::current().events.max_operation_receipts as i64 {
            return Err(error(ErrorCode::OperationLimit,"stream operation receipt capacity exhausted"));
        }
        let (_,row)=store::load(tx)?;
        let other=tx.system_sql("SELECT count(*) AS n FROM __sys_stream_subscriptions WHERE required=1 AND state!='deleted' AND id!=?",&[change.expected.id.clone().into()])?;
        let leased=!tx.system_sql("SELECT 1 FROM __sys_stream_leases WHERE subscription=? AND expires_ms>?",&[change.expected.id.clone().into(),now.into()])?.is_empty();
        let action=match change.action {ChangeAction::Pause=>policy::Action::Pause,ChangeAction::Resume=>policy::Action::Resume,
            ChangeAction::Reset{ref cursor}=>policy::Action::Reset{first:cursor.next},ChangeAction::Delete{force}=>policy::Action::Delete{force}};
        let plan=policy::change(policy::Facts{state:state(&store::text(&sub,"state")?)?,checkpoint:store::number(&sub,"checkpoint")?,
            earliest:store::number(&row,"earliest_offset")?,end:store::number(&row,"next_offset")?,required:store::number(&sub,"required")?!=0,
            other_required:usize::try_from(store::number(&other[0],"n")?)?,leased},action).map_err(|failure| {
                let code=match failure {cellar_logic::event_stream::Failure::CursorExpired{..}=>ErrorCode::CursorExpired,
                    cellar_logic::event_stream::Failure::LastRequiredSubscription=>ErrorCode::ConfigConflict,
                    cellar_logic::event_stream::Failure::PendingDelivery=>ErrorCode::PendingDelivery,_=>ErrorCode::InvalidCursor};
                error(code,failure.to_string())
            })?;
        if plan.state==policy::State::Deleted {
            let tombstones=tx.system_sql("SELECT count(*) AS n FROM __sys_stream_subscriptions WHERE state='deleted'",&[])?;
            if store::number(&tombstones[0],"n")?>=crate::application::limits::current().events.max_subscription_tombstones as i64 {
                return Err(error(ErrorCode::SubscriptionLimit,"subscription tombstone capacity exhausted"));
            }
        }
        let generation=if plan.invalidate {store::uuid("subscription-administration-generation")}else{change.expected.generation.clone()};
        tx.system_sql("UPDATE __sys_stream_subscriptions SET state=?,checkpoint=?,generation=?,retry_at_ms=CASE WHEN ?=1 THEN 0 ELSE retry_at_ms END WHERE id=?",
            &[state_text(plan.state).into(),plan.checkpoint.into(),generation.clone().into(),i64::from(plan.invalidate).into(),change.expected.id.clone().into()])?;
        if plan.invalidate {
            tx.system_sql("DELETE FROM __sys_stream_leases WHERE subscription=?",&[change.expected.id.clone().into()])?;
            tx.system_sql("DELETE FROM __sys_stream_ack_receipts WHERE subscription=?",&[change.expected.id.clone().into()])?;
        }
        let current=SubscriptionIdentity{generation,..change.expected.clone()};
        let receipt=LifecycleReceipt{operation:change.operation.clone(),previous:change.expected.clone(),current,action:change.action.clone(),
            state:state_text(plan.state).into(),checkpoint:plan.checkpoint,skipped:plan.skipped.map(|(first,last)|OffsetRange{first,last}),reason:change.reason.clone(),at_ms:now};
        let encoded=serde_json::to_string(&receipt)?;
        tx.system_sql("INSERT INTO __sys_stream_operations(operation,digest,principal,grant_id,receipt,size,created_ms) VALUES(?,?,?,?,?,?,?)",
            &[change.operation.into(),digest.into(),caller.principal.into(),caller.grant.into(),encoded.clone().into(),(encoded.len() as i64).into(),now.into()])?;
        store::sweep(tx,&definition,now,0,0)?;
        store::schedule(tx,&definition)?;
        Ok(serde_json::from_str(&encoded)?)
    })
}

pub(crate) fn audit(context: &Context, body: &Value) -> Result<Value> {
    let caller = store::authority(context, Action::Administer)?;
    let db = context.storage()?;
    let (definition, _) = store::load(&db)?;
    store::authorize(&definition, &caller, Action::Administer, None)?;
    let after = subscription_store::integer(body, "after")?;
    let count = body["max_records"]
        .as_u64()
        .ok_or_else(|| error(ErrorCode::InvalidRequest, "invalid audit count"))?;
    if after < 0 || count == 0 || count > 100 {
        return Err(error(ErrorCode::InvalidRequest, "invalid audit pagination"));
    }
    let rows=db.system_sql("SELECT sequence,size FROM __sys_stream_operations WHERE sequence>? ORDER BY sequence LIMIT ?",&[after.into(),(count as i64+1).into()])?;
    let mut records = Vec::new();
    let mut size = 0i64;
    let mut next = after;
    let mut more = false;
    for row in rows {
        let bytes = store::number(&row, "size")?;
        if records.len() >= count as usize || size + bytes > 512 << 10 {
            more = true;
            break;
        }
        let sequence = store::number(&row, "sequence")?;
        let payload = db
            .system_sql(
                "SELECT principal,grant_id,receipt FROM __sys_stream_operations WHERE sequence=?",
                &[sequence.into()],
            )?
            .remove(0);
        records.push(AuditRecord {
            sequence,
            principal: store::text(&payload, "principal")?,
            grant: store::text(&payload, "grant_id")?,
            receipt: serde_json::from_str(&store::text(&payload, "receipt")?)?,
        });
        size += bytes;
        next = sequence;
    }
    Ok(json!(AuditPage {
        records,
        next,
        has_more: more
    }))
}
