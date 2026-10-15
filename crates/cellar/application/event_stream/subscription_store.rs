//! Stream-owned subscription state. Every progress transition is a local
//! SQLite transaction; authorization and output proof remain host boundaries.

use super::{
    authority::Action, error::error, store, Definition, DeliveryError, ErrorCode, Receipt,
};
use crate::application::storage::SqlRow;
use crate::application::{Context, Result, Storage};
use cellar_logic::event_stream as policy;
use rand::RngCore;
use serde_json::{json, Value};

pub(crate) fn integer(body: &Value, key: &str) -> Result<i64> {
    let text = body[key]
        .as_str()
        .ok_or_else(|| error(ErrorCode::InvalidRequest, format!("missing {key}")))?;
    let value = text
        .parse::<i64>()
        .map_err(|_| error(ErrorCode::InvalidRequest, format!("invalid {key}")))?;
    if value.to_string() != text {
        return Err(error(
            ErrorCode::InvalidRequest,
            format!("noncanonical {key}"),
        ));
    }
    Ok(value)
}

fn name(body: &Value) -> Result<&str> {
    let name = body["subscription"]
        .as_str()
        .ok_or_else(|| error(ErrorCode::InvalidRequest, "subscription name is missing"))?;
    if name.is_empty() || name.len() > 128 {
        return Err(error(
            ErrorCode::InvalidRequest,
            "invalid subscription name",
        ));
    }
    Ok(name)
}

fn subscription(db: &Storage, name: &str) -> Result<SqlRow> {
    let rows = db.system_sql(
        "SELECT * FROM __sys_stream_subscriptions WHERE name=? AND state!='deleted'",
        &[name.into()],
    )?;
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| error(ErrorCode::SubscriptionReset, "subscription is absent"))?;
    if store::text(&row, "state")? == "deleted" {
        return Err(error(
            ErrorCode::SubscriptionReset,
            "subscription was deleted",
        ));
    }
    Ok(row)
}

fn authorize(context: &Context, body: &Value) -> Result<(Storage, Definition, String)> {
    let caller = store::authority(context, Action::Consume)?;
    let name = name(body)?.to_owned();
    let db = context.storage()?;
    let (definition, _) = store::load(&db)?;
    store::authorize(&definition, &caller, Action::Consume, Some(&name))?;
    Ok((db, definition, name))
}

fn receipt(definition: &Definition, sub: &SqlRow, lease: &SqlRow) -> Result<Receipt> {
    Ok(Receipt {
        resource: definition.resource.id.clone(),
        partition: 0,
        name: store::text(sub, "name")?,
        subscription: store::text(sub, "id")?,
        incarnation: store::text(sub, "generation")?,
        generation: store::number(lease, "generation")?,
        token: store::text(lease, "token")?,
        first: store::number(lease, "first_offset")?,
        last: store::number(lease, "last_offset")?,
    })
}

fn live(
    db: &Storage,
    definition: &Definition,
    sub: &SqlRow,
) -> Result<Option<(Receipt, policy::Lease)>> {
    let rows = db.system_sql(
        "SELECT * FROM __sys_stream_leases WHERE subscription=?",
        &[store::text(sub, "id")?.into()],
    )?;
    let Some(lease) = rows.first() else {
        return Ok(None);
    };
    let receipt = receipt(definition, sub, lease)?;
    let policy = policy::Lease {
        receipt: receipt.policy()?,
        issued_at_ms: store::number(lease, "issued_ms")?,
        expires_at_ms: store::number(lease, "expires_ms")?,
    };
    Ok(Some((receipt, policy)))
}

fn failure(failure: policy::Failure) -> anyhow::Error {
    let code = match failure {
        policy::Failure::SubscriptionReset => ErrorCode::SubscriptionReset,
        policy::Failure::LeaseExpired => ErrorCode::LeaseExpired,
        policy::Failure::StaleReceipt => ErrorCode::StaleReceipt,
        _ => ErrorCode::InvalidRequest,
    };
    error(code, failure.to_string())
}

fn check_scope(definition: &Definition, sub: &SqlRow, receipt: &Receipt) -> Result<()> {
    if receipt.resource != definition.resource.id
        || receipt.partition != 0
        || receipt.name != store::text(sub, "name")?
        || receipt.subscription != store::text(sub, "id")?
        || receipt.incarnation != store::text(sub, "generation")?
    {
        return Err(error(
            ErrorCode::SubscriptionReset,
            "receipt names a different subscription incarnation",
        ));
    }
    Ok(())
}

fn request_receipt(body: &Value) -> Result<Receipt> {
    Receipt::decode(
        body["receipt"]
            .as_str()
            .ok_or_else(|| error(ErrorCode::StaleReceipt, "receipt is missing"))?,
    )
}

pub(crate) struct Fetch {
    pub value: Option<Value>,
    pub retry_at_ms: Option<i64>,
}

pub(crate) fn next(context: &Context, body: &Value) -> Result<Fetch> {
    let (db, definition, name) = authorize(context, body)?;
    let fetch_id = body["fetch_id"]
        .as_str()
        .ok_or_else(|| error(ErrorCode::InvalidRequest, "fetch identity is missing"))?;
    store::validate_id(fetch_id)?;
    let (count, budget) = store::budgets(body)?;
    let now = store::clock(&db)?;
    db.tx(|tx| {
        let sub = subscription(tx,&name)?;
        super::lifecycle_store::check_identity(&definition,&sub,body)?;
        let state = store::text(&sub,"state")?;
        if state=="paused" { return Err(error(ErrorCode::SubscriptionPaused,"subscription is paused")); }
        let (_,row) = store::load(tx)?;
        let earliest = store::number(&row,"earliest_offset")?;
        let end = store::number(&row,"next_offset")?;
        let first = store::number(&sub,"checkpoint")?.checked_add(1).ok_or_else(||error(ErrorCode::InvalidCursor,"subscription cursor exhausted"))?;
        if state=="expired" || first<earliest {
            let mut failure = DeliveryError::new(ErrorCode::CursorExpired,"subscription history is no longer retained");
            failure.data = json!({"earliest":earliest.to_string()});
            return Err(failure.into());
        }
        let id = store::text(&sub,"id")?;
        if let Some((receipt,lease)) = live(tx,&definition,&sub)? {
            if lease.expires_at_ms>now {
                let saved = tx.system_sql("SELECT fetch_id FROM __sys_stream_leases WHERE subscription=?",&[id.clone().into()])?;
                if store::text(&saved[0],"fetch_id")? != fetch_id {
                    let mut failure = DeliveryError::new(ErrorCode::LeaseBusy,"subscription already has a live batch");
                    failure.data = json!({"expires_ms":lease.expires_at_ms.to_string()});
                    return Err(failure.into());
                }
                let records = store::records(tx,receipt.first,receipt.last,count,budget)?;
                if records.len() != usize::try_from(receipt.last-receipt.first+1)? {
                    return Err(error(ErrorCode::BatchTooSmall,"retry budgets cannot contain the original batch"));
                }
                return Ok(Fetch { value: Some(json!({"receipt":receipt.encode()?,"issued_ms":lease.issued_at_ms.to_string(),
                    "expires_ms":lease.expires_at_ms.to_string(),"records":records})), retry_at_ms: None });
            }
            tx.system_sql("DELETE FROM __sys_stream_leases WHERE subscription=?",&[id.clone().into()])?;
        }
        policy::cursor(earliest,end,first).map_err(failure)?;
        let retry_at = store::number(&sub,"retry_at_ms")?;
        if retry_at>now { return Ok(Fetch{value:None,retry_at_ms:Some(retry_at)}); }
        let records = store::records(tx,first,end-1,count,budget)?;
        if records.is_empty() {
            store::schedule(tx,&definition)?;
            return Ok(Fetch{value:None,retry_at_ms:None});
        }
        let last = integer(records.last().unwrap(),"offset")?;
        let generation = store::number(&sub,"next_lease_generation")?;
        let next_generation = generation.checked_add(1).ok_or_else(||error(ErrorCode::SubscriptionLimit,"lease generations exhausted"))?;
        let mut token = [0;32];
        crate::asyncrt::rng("subscription-lease").fill_bytes(&mut token);
        let limits = crate::application::limits::current().events;
        let expires = now.checked_add(limits.lease_ms).ok_or_else(||error(ErrorCode::InvalidRequest,"lease deadline overflow"))?;
        let receipt = Receipt { resource:definition.resource.id.clone(),partition:0,name,
            subscription:id.clone(),incarnation:store::text(&sub,"generation")?,generation,token:hex::encode(token),first,last };
        tx.system_sql("INSERT INTO __sys_stream_leases(subscription,generation,token,fetch_id,first_offset,last_offset,issued_ms,expires_ms) VALUES(?,?,?,?,?,?,?,?)",
            &[id.clone().into(),generation.into(),receipt.token.clone().into(),fetch_id.into(),first.into(),last.into(),now.into(),expires.into()])?;
        tx.system_sql("UPDATE __sys_stream_subscriptions SET next_lease_generation=?,retry_at_ms=0 WHERE id=?",&[next_generation.into(),id.into()])?;
        store::schedule(tx,&definition)?;
        Ok(Fetch{value:Some(json!({"receipt":receipt.encode()?,"issued_ms":now.to_string(),"expires_ms":expires.to_string(),"records":records})),retry_at_ms:None})
    })
}

pub(crate) fn ack(context: &Context, body: &Value) -> Result<Value> {
    let (db, definition, name) = authorize(context, body)?;
    let requested = request_receipt(body)?;
    let now = store::clock(&db)?;
    db.tx(|tx| {
        let sub = subscription(tx,&name)?;
        check_scope(&definition,&sub,&requested)?;
        let live = live(tx,&definition,&sub)?;
        let prior = tx.system_sql("SELECT receipt FROM __sys_stream_ack_receipts WHERE subscription=?",&[requested.subscription.clone().into()])?;
        let last_ack = prior.first().map(|row|Receipt::decode(&store::text(row,"receipt")?)).transpose()?;
        let requested_policy = requested.policy()?;
        let last_policy = last_ack.as_ref().map(Receipt::policy).transpose()?;
        let decision = policy::acknowledge(requested_policy.subscription,requested_policy.incarnation,
            store::number(&sub,"checkpoint")?,live.as_ref().map(|(_,lease)|lease),last_policy.as_ref(),&requested_policy,now).map_err(failure)?;
        let status = match decision {
            policy::Acknowledgment::AlreadyAcknowledged=>"already_acknowledged",
            policy::Acknowledgment::Obsolete=>"obsolete",
            policy::Acknowledgment::Advance{through}=>{
                tx.system_sql("UPDATE __sys_stream_subscriptions SET checkpoint=?,retry_at_ms=0 WHERE id=?",&[through.into(),requested.subscription.clone().into()])?;
                tx.system_sql("INSERT INTO __sys_stream_ack_receipts(subscription,receipt) VALUES(?,?) ON CONFLICT(subscription) DO UPDATE SET receipt=excluded.receipt",
                    &[requested.subscription.clone().into(),requested.encode()?.into()])?;
                tx.system_sql("DELETE FROM __sys_stream_leases WHERE subscription=?",&[requested.subscription.clone().into()])?;
                store::sweep(tx,&definition,now,0,0)?;
                store::schedule(tx,&definition)?;
                "acknowledged"
            }
        };
        Ok(json!({"status":status}))
    })
}

pub(crate) fn retry(context: &Context, body: &Value) -> Result<Value> {
    let (db, definition, name) = authorize(context, body)?;
    let requested = request_receipt(body)?;
    let delay = integer(body, "delay_ms")?;
    let now = store::clock(&db)?;
    let limits = crate::application::limits::current().events;
    if delay < 0 || delay > limits.max_retry_delay_ms {
        return Err(error(
            ErrorCode::InvalidRequest,
            "retry delay exceeds policy",
        ));
    }
    let retry_at = now
        .checked_add(delay)
        .ok_or_else(|| error(ErrorCode::InvalidRequest, "retry deadline overflow"))?;
    db.tx(|tx| {
        let sub = subscription(tx, &name)?;
        check_scope(&definition, &sub, &requested)?;
        let live = live(tx, &definition, &sub)?
            .ok_or_else(|| error(ErrorCode::StaleReceipt, "delivery lease is absent"))?;
        if live.0 != requested {
            return Err(error(ErrorCode::StaleReceipt, "delivery lease changed"));
        }
        if live.1.expires_at_ms <= now {
            return Err(error(ErrorCode::LeaseExpired, "delivery lease expired"));
        }
        tx.system_sql(
            "DELETE FROM __sys_stream_leases WHERE subscription=?",
            &[requested.subscription.clone().into()],
        )?;
        tx.system_sql(
            "UPDATE __sys_stream_subscriptions SET retry_at_ms=? WHERE id=?",
            &[retry_at.into(), requested.subscription.into()],
        )?;
        store::sweep(tx, &definition, now, 0, 0)?;
        store::schedule(tx, &definition)?;
        Ok(json!({"retry_at_ms":retry_at.to_string()}))
    })
}

pub(crate) fn extend(context: &Context, body: &Value) -> Result<Value> {
    let (db, definition, name) = authorize(context, body)?;
    let requested = request_receipt(body)?;
    let target = integer(body, "expires_ms")?;
    let now = store::clock(&db)?;
    db.tx(|tx| {
        let sub = subscription(tx, &name)?;
        check_scope(&definition, &sub, &requested)?;
        let live = live(tx, &definition, &sub)?
            .ok_or_else(|| error(ErrorCode::StaleReceipt, "delivery lease is absent"))?;
        let expires = policy::extend(
            &live.1,
            &requested.policy()?,
            now,
            target,
            crate::application::limits::current()
                .events
                .max_lease_lifetime_ms,
        )
        .map_err(failure)?;
        tx.system_sql(
            "UPDATE __sys_stream_leases SET expires_ms=? WHERE subscription=?",
            &[expires.into(), requested.subscription.into()],
        )?;
        store::schedule(tx, &definition)?;
        Ok(json!({"expires_ms":expires.to_string()}))
    })
}
