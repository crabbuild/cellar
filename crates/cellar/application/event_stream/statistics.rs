// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Owner-routed stream observations without scans of retained event payloads.

use super::{authority::Action, error::error, store, ErrorCode, Resource, Retention, Stream};
use crate::application::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamObservation {
    #[serde(with = "super::decimal")]
    pub observed_ms: i64,
    /// Inclusive offset through which required checkpoints/live leases allow
    /// deletion, independently of age and sweep budgets.
    #[serde(with = "super::decimal")]
    pub retention_frontier: i64,
    #[serde(with = "super::decimal")]
    pub protected_records: i64,
    #[serde(with = "super::decimal")]
    pub protected_bytes: i64,
    /// Age-expired payload bytes whose deletion is pinned by required
    /// checkpoints or unexpired leases. Capacity protection uses protected_bytes.
    #[serde(with = "super::decimal")]
    pub retention_blocked_bytes: i64,
    #[serde(with = "super::decimal")]
    pub active_leases: i64,
    #[serde(with = "super::decimal")]
    pub expired_subscriptions: i64,
    #[serde(with = "super::decimal::optional")]
    pub oldest_retained_age_ms: Option<i64>,
    /// SQLite's current page allocation, including free pages and metadata.
    /// These are not WAL/LTX/bucket sizes or filesystem allocated-block counts.
    #[serde(with = "super::decimal")]
    pub sqlite_allocated_bytes: i64,
    #[serde(with = "super::decimal")]
    pub sqlite_used_bytes: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamStats {
    pub resource: Resource,
    #[serde(with = "super::decimal")]
    pub revision: i64,
    #[serde(with = "super::decimal")]
    pub earliest: i64,
    #[serde(with = "super::decimal")]
    pub end: i64,
    #[serde(with = "super::decimal")]
    pub rows: i64,
    #[serde(with = "super::decimal")]
    pub bytes: i64,
    pub observation: StreamObservation,
}

impl StreamStats {
    pub(crate) fn validate(&self, resource: &Resource) -> Result<()> {
        self.resource.validate()?;
        let o = &self.observation;
        if !self.resource.same_identity(resource)
            || self.revision <= 0
            || self.earliest <= 0
            || self.end < self.earliest
            || self.rows != self.end - self.earliest
            || self.bytes < 0
            || (self.rows == 0) != (self.bytes == 0)
            || self.bytes < self.rows
            || o.observed_ms < 0
            || o.retention_frontier < self.earliest - 1
            || o.retention_frontier >= self.end
            || o.protected_records != self.end - 1 - o.retention_frontier
            || o.protected_bytes < 0
            || o.protected_bytes > self.bytes
            || (o.protected_records == 0) != (o.protected_bytes == 0)
            || o.retention_blocked_bytes < 0
            || o.retention_blocked_bytes > o.protected_bytes
            || !(0..=128).contains(&o.active_leases)
            || !(0..=128).contains(&o.expired_subscriptions)
            || o.oldest_retained_age_ms.is_some() != (self.rows > 0)
            || o.oldest_retained_age_ms
                .is_some_and(|age| age < 0 || age > o.observed_ms)
            || o.sqlite_allocated_bytes < 0
            || o.sqlite_used_bytes < 0
            || o.sqlite_used_bytes > o.sqlite_allocated_bytes
        {
            return Err(error(
                ErrorCode::InvalidRequest,
                "invalid stream statistics response",
            ));
        }
        Ok(())
    }
}

impl<E> Stream<E> {
    /// Read current stream statistics under administration authority.
    pub async fn stats(&self, context: &Context) -> Result<StreamStats> {
        self.check_domain()?;
        let value = super::call_bound(
            context,
            &self.binding,
            Action::Administer,
            "__streamDescribe",
            cellar_native::clone::StructuredValue::from_json(json!([{}]))?,
        )
        .await?;
        let stats: StreamStats = serde_json::from_value(value)?;
        stats.validate(self.resource())?;
        Ok(stats)
    }
}

pub(crate) fn describe(context: &Context) -> Result<Value> {
    let caller = store::authority(context, Action::Administer)?;
    let db = context.storage()?;
    let (definition, _) = store::load(&db)?;
    store::authorize(&definition, &caller, Action::Administer, None)?;
    Ok(serde_json::to_value(sample(context)?)?)
}

pub(crate) fn sample(context: &Context) -> Result<StreamStats> {
    let db = context.storage()?;
    let (definition, row) = store::load_state(&db)?;
    let now = super::lifecycle_store::observation_time(&row)?;
    let earliest = store::number(&row, "earliest_offset")?;
    let end = store::number(&row, "next_offset")?;
    let bytes = store::number(&row, "bytes")?;
    let tail = store::number(&row, "byte_position")?;
    // Current definitions admit at most 128 live subscriptions. Tombstones
    // never expand the lease query's allocation budget.
    let leases = db.system_sql(
        "SELECT first_offset,expires_ms FROM __sys_stream_leases ORDER BY subscription LIMIT 129",
        &[],
    )?;
    if leases.len() > 128 {
        return Err(error(
            ErrorCode::UnsupportedProtocol,
            "stream lease inventory exceeds the current definition bound",
        ));
    }
    let starts = leases
        .iter()
        .filter_map(|row| match store::number(row, "expires_ms") {
            Ok(expiry) if expiry > now => Some(store::number(row, "first_offset")),
            Ok(_) => None,
            Err(failure) => Some(Err(failure)),
        })
        .collect::<Result<Vec<_>>>()?;
    let checkpoints = if definition.retention == Retention::Required {
        db.system_sql("SELECT checkpoint FROM __sys_stream_subscriptions INDEXED BY __sys_stream_required_frontier WHERE required=1 AND state!='deleted' ORDER BY checkpoint LIMIT 1",&[])?
            .iter().map(|row|store::number(row,"checkpoint")).collect::<Result<Vec<_>>>()?
    } else {
        Vec::new()
    };
    let frontier = cellar_logic::event_stream::deletion_frontier(
        end - 1,
        checkpoints,
        starts.iter().copied(),
    )?
    .max(earliest - 1);
    let prefix_position = if frontier < earliest {
        None
    } else {
        Some(store::number(
            db.system_sql(
                "SELECT byte_position FROM __sys_stream_events WHERE offset=?",
                &[frontier.into()],
            )?
            .first()
            .ok_or_else(|| anyhow::anyhow!("retention frontier record missing"))?,
            "byte_position",
        )?)
    };
    let protected_bytes = match prefix_position {
        None => bytes,
        Some(position) => cellar_logic::event_stream::byte_distance(position, tail)?,
    };
    let expired=db.system_sql("SELECT offset,byte_position FROM __sys_stream_events INDEXED BY __sys_stream_events_age WHERE append_ms<=? ORDER BY append_ms DESC,offset DESC LIMIT 1",&[now.saturating_sub(definition.max_age_ms).into()])?;
    let retention_blocked_bytes = match expired.first() {
        Some(record) if store::number(record, "offset")? > frontier => {
            let base = match prefix_position {
                Some(position) => position,
                None => ((tail as u64).wrapping_sub(bytes as u64) & i64::MAX as u64) as i64,
            };
            cellar_logic::event_stream::byte_distance(
                base,
                store::number(record, "byte_position")?,
            )?
        }
        _ => 0,
    };
    let oldest = db.system_sql(
        "SELECT append_ms FROM __sys_stream_events WHERE offset=?",
        &[earliest.into()],
    )?;
    let oldest_retained_age_ms = oldest
        .first()
        .map(|row| store::number(row, "append_ms").map(|append| now.saturating_sub(append).max(0)))
        .transpose()?;
    let expired_subscriptions=db.system_sql("SELECT count(*) AS n FROM __sys_stream_subscriptions INDEXED BY __sys_stream_subscription_state WHERE state='expired'",&[])?;
    let (allocated, used) = db.sqlite_allocation()?;
    let stats = StreamStats {
        resource: definition.resource.clone(),
        revision: definition.revision,
        earliest,
        end,
        rows: store::number(&row, "rows")?,
        bytes,
        observation: StreamObservation {
            observed_ms: now,
            retention_frontier: frontier,
            protected_records: end - 1 - frontier,
            protected_bytes,
            retention_blocked_bytes,
            active_leases: starts.len() as i64,
            expired_subscriptions: store::number(&expired_subscriptions[0], "n")?,
            oldest_retained_age_ms,
            sqlite_allocated_bytes: allocated,
            sqlite_used_bytes: used,
        },
    };
    stats.validate(&definition.resource)?;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_acceptance_preserves_precision_and_rejects_foreign_or_conflicting_observations(
    ) -> Result<()> {
        let resource = Resource {
            tenant: "tenant".into(),
            application: "shop".into(),
            id: "00000000000000000000000000000001".into(),
            name: "orders".into(),
            family: "orders".into(),
        };
        let value = json!({"resource":resource,"revision":i64::MAX.to_string(),"earliest":"1","end":"2","rows":"1","bytes":"10","observation":{"observed_ms":"100","retention_frontier":"0","protected_records":"1","protected_bytes":"10","retention_blocked_bytes":"10","active_leases":"1","expired_subscriptions":"0","oldest_retained_age_ms":"10","sqlite_allocated_bytes":"65536","sqlite_used_bytes":"32768"}});
        let stats: StreamStats = serde_json::from_value(value.clone())?;
        stats.validate(&resource)?;
        assert_eq!(serde_json::to_value(stats)?, value);
        for (pointer, replacement) in [
            ("/resource/id", json!("00000000000000000000000000000002")),
            ("/rows", json!("0")),
            ("/observation/retention_frontier", json!("2")),
            ("/observation/protected_bytes", json!("11")),
            ("/observation/retention_blocked_bytes", json!("11")),
            ("/observation/active_leases", json!("129")),
            ("/observation/sqlite_used_bytes", json!("65537")),
            ("/observation/oldest_retained_age_ms", Value::Null),
        ] {
            let mut forged = value.clone();
            *forged.pointer_mut(pointer).unwrap() = replacement;
            assert!(
                serde_json::from_value::<StreamStats>(forged)?
                    .validate(&resource)
                    .is_err(),
                "{pointer}"
            );
        }
        let mut noncanonical = value;
        noncanonical["observation"]["protected_bytes"] = json!("01");
        assert!(serde_json::from_value::<StreamStats>(noncanonical).is_err());
        Ok(())
    }
}
