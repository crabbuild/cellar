// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded source diagnostics through the activation-bound storage capability.

use super::{SqlRow, SqlValue, Storage};
use crate::application::event_stream::{DeliveryError, ErrorCode, Resource};
use anyhow::{Context as _, Result};
use base64::Engine;
use serde::{Deserialize, Serialize};

const MAX_LANES: usize = 100;

#[derive(Clone, Debug)]
pub struct OutboxOptions {
    pub limit: usize,
    pub cursor: Option<String>,
}

impl Default for OutboxOptions {
    fn default() -> Self {
        Self {
            limit: MAX_LANES,
            cursor: None,
        }
    }
}

/// The observed producer revision required to retry one lane safely.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxRevision {
    pub source: String,
    pub incarnation: String,
    pub lane: String,
    #[serde(with = "crate::application::event_stream::decimal")]
    pub dispatch_generation: i64,
    #[serde(with = "crate::application::event_stream::decimal")]
    pub next_sequence: i64,
    #[serde(with = "crate::application::event_stream::decimal")]
    pub settled_through: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxBlocked {
    pub reason: ErrorCode,
    #[serde(with = "crate::application::event_stream::decimal")]
    pub lanes: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxObservation {
    #[serde(with = "crate::application::event_stream::decimal")]
    pub observed_ms: i64,
    /// None means there are no pending event rows; seal-only work is separate.
    #[serde(with = "crate::application::event_stream::decimal::optional")]
    pub oldest_pending_age_ms: Option<i64>,
    /// Fixed reason set; counts include blocked terminal seal delivery.
    pub blocked_lanes: Vec<OutboxBlocked>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxLane {
    pub revision: OutboxRevision,
    pub resource: Resource,
    pub principal: String,
    pub grant: String,
    pub status: String,
    #[serde(with = "crate::application::event_stream::decimal")]
    pub pending_rows: i64,
    #[serde(with = "crate::application::event_stream::decimal")]
    pub pending_bytes: i64,
    #[serde(with = "crate::application::event_stream::decimal")]
    pub retry_at_ms: i64,
    #[serde(with = "crate::application::event_stream::decimal")]
    pub attempts: i64,
    pub last_error: Option<String>,
    pub seal_state: Option<String>,
    pub blocked_reason: Option<ErrorCode>,
    #[serde(with = "crate::application::event_stream::decimal::optional")]
    pub oldest_pending_age_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxPage {
    pub incarnation: Option<String>,
    #[serde(with = "crate::application::event_stream::decimal")]
    pub pending_rows: i64,
    #[serde(with = "crate::application::event_stream::decimal")]
    pub pending_bytes: i64,
    pub lanes: Vec<OutboxLane>,
    pub next: Option<String>,
    pub observation: OutboxObservation,
}

fn failure(code: ErrorCode, message: &str) -> anyhow::Error {
    DeliveryError::new(code, message).into()
}

fn text(row: &SqlRow, column: &str) -> Result<String> {
    match row.get(column) {
        Some(SqlValue::Text(value)) => Ok(value.clone()),
        _ => anyhow::bail!("invalid outbox diagnostic {column}"),
    }
}

fn number(row: &SqlRow, column: &str) -> Result<i64> {
    match row.get(column) {
        Some(SqlValue::Integer(value)) => Ok(*value),
        _ => anyhow::bail!("invalid outbox diagnostic {column}"),
    }
}

fn optional_text(row: &SqlRow, column: &str) -> Result<Option<String>> {
    match row.get(column) {
        Some(SqlValue::Null) => Ok(None),
        _ => text(row, column).map(Some),
    }
}

fn valid_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn optional_number(row: &SqlRow, column: &str) -> Result<Option<i64>> {
    match row.get(column) {
        Some(SqlValue::Null) => Ok(None),
        _ => number(row, column).map(Some),
    }
}

fn age(created_ms: Option<i64>, now: i64) -> Result<Option<i64>> {
    created_ms
        .map(|created| {
            anyhow::ensure!(created >= 0, "invalid outbox creation time");
            Ok(now.saturating_sub(created).max(0))
        })
        .transpose()
}

fn after(encoded: Option<&str>, source: &str, incarnation: Option<&str>) -> Result<String> {
    if let Some(encoded) = encoded {
        let invalid = || {
            failure(
                ErrorCode::InvalidCursor,
                "outbox cursor does not match this source incarnation",
            )
        };
        if encoded.len() > 1024 {
            return Err(invalid());
        }
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded.strip_prefix("v1.").ok_or_else(invalid)?)
            .map_err(|_| invalid())?;
        let (cursor_source, cursor_incarnation, lane): (String, String, String) =
            serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if cursor_source != source
            || incarnation != Some(cursor_incarnation.as_str())
            || !valid_hex(&lane, 64)
        {
            return Err(invalid());
        }
        Ok(lane)
    } else {
        Ok(String::new())
    }
}

impl OutboxPage {
    pub(crate) fn validate(&self, source: &str, options: &OutboxOptions) -> Result<()> {
        let invalid = || {
            failure(
                ErrorCode::InvalidRequest,
                "invalid outbox observation response",
            )
        };
        let mut previous = after(
            options.cursor.as_deref(),
            source,
            self.incarnation.as_deref(),
        )?;
        let observation = &self.observation;
        let valid_age = |age: Option<i64>, rows: i64| {
            age.is_some() == (rows > 0)
                && age.is_none_or(|age| age >= 0 && age <= observation.observed_ms)
        };
        if !(1..=MAX_LANES).contains(&options.limit)
            || self.lanes.len() > options.limit
            || self.pending_rows < 0
            || self.pending_bytes < 0
            || observation.observed_ms < 0
            || !valid_age(observation.oldest_pending_age_ms, self.pending_rows)
            || observation.blocked_lanes.len() != ErrorCode::BLOCKED_REASONS.len()
            || self
                .incarnation
                .as_deref()
                .is_some_and(|id| !valid_hex(id, 32))
            || (self.incarnation.is_none()
                && (!self.lanes.is_empty()
                    || self.pending_rows != 0
                    || self.pending_bytes != 0
                    || self.next.is_some()))
        {
            return Err(invalid());
        }
        let mut total_blocked = 0i64;
        for (count, reason) in observation
            .blocked_lanes
            .iter()
            .zip(ErrorCode::BLOCKED_REASONS)
        {
            if count.reason != reason || count.lanes < 0 {
                return Err(invalid());
            }
            total_blocked = total_blocked.checked_add(count.lanes).ok_or_else(invalid)?;
        }
        if self.incarnation.is_none() && total_blocked != 0 {
            return Err(invalid());
        }
        let mut rows = 0i64;
        let mut bytes = 0i64;
        let mut seen_blocked = [0i64; 7];
        for lane in &self.lanes {
            lane.resource.validate()?;
            let revision = &lane.revision;
            if revision.source != source
                || self.incarnation.as_deref() != Some(revision.incarnation.as_str())
                || !valid_hex(&revision.lane, 64)
                || revision.lane <= previous
                || revision.lane
                    != crate::application::event_stream::lane_identity(
                        &lane.resource,
                        source,
                        &revision.incarnation,
                    )?
                || revision.dispatch_generation < 0
                || revision.next_sequence <= 0
                || revision.settled_through < 0
                || revision.settled_through >= revision.next_sequence
                || lane.pending_rows < 0
                || lane.pending_bytes < 0
                || lane.attempts < 0
                || !valid_age(lane.oldest_pending_age_ms, lane.pending_rows)
                || lane.oldest_pending_age_ms > observation.oldest_pending_age_ms
                || !matches!(lane.status.as_str(), "pending" | "blocked" | "sealed")
                || lane
                    .seal_state
                    .as_deref()
                    .is_some_and(|state| !matches!(state, "pending" | "blocked" | "accepted"))
                || (lane.status == "sealed") != lane.seal_state.is_some()
                || lane.blocked_reason.is_some()
                    != (lane.status == "blocked" || lane.seal_state.as_deref() == Some("blocked"))
                || lane.principal.is_empty()
                || lane.principal.len() > 256
                || lane.grant.is_empty()
                || lane.grant.len() > 256
            {
                return Err(invalid());
            }
            rows = rows.checked_add(lane.pending_rows).ok_or_else(invalid)?;
            bytes = bytes.checked_add(lane.pending_bytes).ok_or_else(invalid)?;
            if let Some(reason) = lane.blocked_reason {
                let index = ErrorCode::BLOCKED_REASONS
                    .iter()
                    .position(|code| *code == reason)
                    .ok_or_else(invalid)?;
                seen_blocked[index] += 1;
            }
            previous.clone_from(&revision.lane);
        }
        if (options.cursor.is_none()
            && self.next.is_none()
            && (rows != self.pending_rows
                || bytes != self.pending_bytes
                || seen_blocked
                    .iter()
                    .zip(&observation.blocked_lanes)
                    .any(|(seen, count)| *seen != count.lanes)))
            || rows > self.pending_rows
            || bytes > self.pending_bytes
            || seen_blocked
                .iter()
                .zip(&observation.blocked_lanes)
                .any(|(seen, count)| *seen > count.lanes)
        {
            return Err(invalid());
        }
        if let Some(next) = &self.next {
            if self.lanes.is_empty()
                || after(Some(next), source, self.incarnation.as_deref())? != previous
            {
                return Err(invalid());
            }
        }
        Ok(())
    }
}

impl Storage {
    pub(crate) fn outbox_incarnation(&self) -> Result<Option<String>> {
        let scope = self.scope()?;
        anyhow::ensure!(
            crate::storage::facet_opening(scope).is_none(),
            "facets cannot inspect or retry durable source lanes"
        );
        let present = self.system_sql("SELECT 1 AS present FROM sqlite_schema WHERE type='table' AND name='__sys_event_source'", &[])?;
        if present.is_empty() {
            return Ok(None);
        }
        self.system_sql(
            "SELECT incarnation FROM __sys_event_source WHERE singleton=1",
            &[],
        )?
        .first()
        .map(|row| text(row, "incarnation"))
        .transpose()
    }

    /// Inspect this source without copying event bodies or creating an outbox.
    /// Pages use a source-incarnation cursor and contain at most 100 lanes.
    /// Each page observes current state; pagination is not a frozen snapshot.
    pub fn outbox(&self, options: &OutboxOptions) -> Result<OutboxPage> {
        if options.limit == 0 || options.limit > MAX_LANES {
            return Err(failure(
                ErrorCode::InvalidRequest,
                "outbox page limit must be between 1 and 100",
            ));
        }
        let source = self.scope()?;
        let incarnation = self.outbox_incarnation()?;
        let after = after(options.cursor.as_deref(), source, incarnation.as_deref())?;
        let now = crate::asyncrt::wall_ms().max(0);
        let Some(incarnation) = incarnation else {
            return Ok(OutboxPage {
                incarnation: None,
                pending_rows: 0,
                pending_bytes: 0,
                lanes: Vec::new(),
                next: None,
                observation: OutboxObservation {
                    observed_ms: now,
                    oldest_pending_age_ms: None,
                    blocked_lanes: ErrorCode::BLOCKED_REASONS
                        .iter()
                        .map(|reason| OutboxBlocked {
                            reason: *reason,
                            lanes: 0,
                        })
                        .collect(),
                },
            });
        };
        let totals = self.system_sql(
            "SELECT rows,bytes FROM __sys_outbox_stats WHERE singleton=1",
            &[],
        )?;
        let totals = totals.first().context("outbox statistics missing")?;
        // Project only bounded identity fields. The full binding and envelope
        // are deliberately absent from the result and allocation budget.
        let rows=self.system_sql(
            "SELECT l.lane,l.next_sequence,l.settled_through,l.status,l.retry_at_ms,l.attempts,l.dispatch_generation,
            substr(l.last_error,1,1024) AS last_error,s.state AS seal_state,l.blocked_reason,
            json_extract(l.binding,'$.resource') AS resource,json_extract(l.binding,'$.principal') AS principal,
            json_extract(l.binding,'$.grant') AS grant,
            l.pending_rows,l.pending_bytes,
            (SELECT created_at_ms FROM __sys_outbox AS e INDEXED BY __sys_outbox_lane_age WHERE e.lane=l.lane ORDER BY created_at_ms LIMIT 1) AS oldest_ms
            FROM __sys_outbox_lanes AS l LEFT JOIN __sys_outbox_seals AS s ON s.lane=l.lane
            WHERE l.lane>? ORDER BY l.lane LIMIT ?",&[after.into(),i64::try_from(options.limit+1)?.into()])?;
        let lanes = rows
            .iter()
            .take(options.limit)
            .map(|row| -> Result<OutboxLane> {
                Ok(OutboxLane {
                    revision: OutboxRevision {
                        source: source.into(),
                        incarnation: incarnation.clone(),
                        lane: text(row, "lane")?,
                        dispatch_generation: number(row, "dispatch_generation")?,
                        next_sequence: number(row, "next_sequence")?,
                        settled_through: number(row, "settled_through")?,
                    },
                    resource: serde_json::from_str(&text(row, "resource")?)?,
                    principal: text(row, "principal")?,
                    grant: text(row, "grant")?,
                    status: text(row, "status")?,
                    pending_rows: number(row, "pending_rows")?,
                    pending_bytes: number(row, "pending_bytes")?,
                    retry_at_ms: number(row, "retry_at_ms")?,
                    attempts: number(row, "attempts")?,
                    last_error: optional_text(row, "last_error")?,
                    seal_state: optional_text(row, "seal_state")?,
                    blocked_reason: optional_text(row, "blocked_reason")?
                        .map(|reason| serde_json::from_value(serde_json::Value::String(reason)))
                        .transpose()?,
                    oldest_pending_age_ms: age(optional_number(row, "oldest_ms")?, now)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let next =
            if rows.len() > options.limit {
                let last = lanes.last().context("outbox continuation lacks a lane")?;
                Some(format!(
                    "v1.{}",
                    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(
                        &(source, &incarnation, &last.revision.lane)
                    )?)
                ))
            } else {
                None
            };
        let oldest = self.system_sql("SELECT created_at_ms FROM __sys_outbox INDEXED BY __sys_outbox_age ORDER BY created_at_ms LIMIT 1", &[])?;
        let reasons = self.system_sql(
            "SELECT reason,lanes FROM __sys_outbox_blocked ORDER BY reason LIMIT 8",
            &[],
        )?;
        anyhow::ensure!(
            reasons.len() == ErrorCode::BLOCKED_REASONS.len(),
            "outbox blocked reason inventory incomplete"
        );
        let blocked_lanes = ErrorCode::BLOCKED_REASONS
            .iter()
            .map(|reason| -> Result<OutboxBlocked> {
                let name = format!("{reason:?}");
                let row = reasons
                    .iter()
                    .find(|row| text(row, "reason").is_ok_and(|value| value == name))
                    .context("outbox blocked reason missing")?;
                Ok(OutboxBlocked {
                    reason: *reason,
                    lanes: number(row, "lanes")?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let page = OutboxPage {
            incarnation: Some(incarnation),
            pending_rows: number(totals, "rows")?,
            pending_bytes: number(totals, "bytes")?,
            lanes,
            next,
            observation: OutboxObservation {
                observed_ms: now,
                oldest_pending_age_ms: age(
                    oldest
                        .first()
                        .map(|row| number(row, "created_at_ms"))
                        .transpose()?,
                    now,
                )?,
                blocked_lanes,
            },
        };
        page.validate(source, options)?;
        Ok(page)
    }

    /// Re-arm one observed lane without changing its events or sequences.
    /// A stale observation fails; an already settled lane returns false.
    /// Commit proves the retry obligation, not destination delivery.
    pub fn retry_outbox_lane(&self, revision: &OutboxRevision) -> Result<bool> {
        if revision.source != self.scope()?
            || !valid_hex(&revision.incarnation, 32)
            || !valid_hex(&revision.lane, 64)
        {
            return Err(failure(
                ErrorCode::InvalidRequest,
                "outbox retry identity is invalid",
            ));
        }
        self.tx(|tx| {
            if tx.outbox_incarnation()?.as_deref() != Some(revision.incarnation.as_str()) {
                return Err(failure(ErrorCode::ConfigConflict,"outbox retry source incarnation changed"));
            }
            let rows=tx.system_sql("SELECT dispatch_generation,next_sequence,settled_through,
                EXISTS(SELECT 1 FROM __sys_outbox WHERE lane=l.lane) OR
                EXISTS(SELECT 1 FROM __sys_outbox_seals WHERE lane=l.lane AND state!='accepted') AS pending
                FROM __sys_outbox_lanes AS l WHERE lane=?",&[revision.lane.clone().into()])?;
            let row=rows.first().ok_or_else(|| failure(ErrorCode::ConfigConflict,"outbox retry lane no longer exists"))?;
            let expected=cellar_logic::event_stream::ProducerRevision {generation:revision.dispatch_generation,next_sequence:revision.next_sequence,settled_through:revision.settled_through};
            let current=cellar_logic::event_stream::ProducerRevision {generation:number(row,"dispatch_generation")?,next_sequence:number(row,"next_sequence")?,settled_through:number(row,"settled_through")?};
            let generation=match cellar_logic::event_stream::retry_producer(expected,current,number(row,"pending")?!=0) {
                Ok(Some(generation)) => generation,
                Ok(None) => return Ok(false),
                Err(cellar_logic::event_stream::RetryRefusal::Stale) => return Err(failure(ErrorCode::ConfigConflict,"outbox retry producer revision changed")),
                Err(cellar_logic::event_stream::RetryRefusal::Exhausted) => return Err(failure(ErrorCode::OperationLimit,"outbox dispatch generation exhausted")),
            };
            let now=crate::asyncrt::wall_ms();
            tx.system_sql("UPDATE __sys_outbox_lanes SET status=CASE WHEN status='sealed' THEN 'sealed' ELSE 'pending' END,retry_at_ms=?,attempts=0,blocked_reason=NULL,dispatch_generation=? WHERE lane=?",
                &[now.into(),generation.into(),revision.lane.clone().into()])?;
            tx.system_sql("UPDATE __sys_outbox_seals SET state='pending' WHERE lane=? AND state!='accepted'",&[revision.lane.clone().into()])?;
            tx.schedule_work(cellar_logic::work::Reason::Outbox,now)?;
            Ok(true)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn acceptance_checks_source_totals_reason_cardinality_and_decimal_ages() -> Result<()> {
        let source = "Order:123";
        let incarnation = "00000000000000000000000000000002";
        let resource = Resource {
            tenant: "tenant".into(),
            application: "shop".into(),
            id: "00000000000000000000000000000001".into(),
            name: "orders".into(),
            family: "orders".into(),
        };
        let lane = crate::application::event_stream::lane_identity(&resource, source, incarnation)?;
        let counts = ErrorCode::BLOCKED_REASONS
            .iter()
            .map(|reason| OutboxBlocked {
                reason: *reason,
                lanes: i64::from(*reason == ErrorCode::Forbidden),
            })
            .collect::<Vec<_>>();
        let value = json!({"incarnation":incarnation,"pending_rows":"1","pending_bytes":i64::MAX.to_string(),"lanes":[{"revision":{"source":source,"incarnation":incarnation,"lane":lane,"dispatch_generation":"1","next_sequence":"2","settled_through":"0"},"resource":resource,"principal":"shop","grant":"publisher","status":"blocked","pending_rows":"1","pending_bytes":i64::MAX.to_string(),"retry_at_ms":"100","attempts":"1","last_error":"Forbidden: publication denied","seal_state":null,"blocked_reason":"Forbidden","oldest_pending_age_ms":"10"}],"next":null,"observation":{"observed_ms":"100","oldest_pending_age_ms":"10","blocked_lanes":counts}});
        let page: OutboxPage = serde_json::from_value(value.clone())?;
        page.validate(source, &OutboxOptions::default())?;
        assert_eq!(serde_json::to_value(page)?, value);
        for (pointer, replacement) in [
            ("/lanes/0/revision/source", json!("Order:other")),
            ("/lanes/0/blocked_reason", json!("Unavailable")),
            ("/lanes/0/pending_rows", json!("0")),
            ("/observation/oldest_pending_age_ms", json!(null)),
            ("/observation/blocked_lanes/0/lanes", json!("0")),
            (
                "/observation/blocked_lanes/0/reason",
                json!("ConfigConflict"),
            ),
            ("/observation/observed_ms", json!("-1")),
        ] {
            let mut forged = value.clone();
            *forged.pointer_mut(pointer).unwrap() = replacement;
            assert!(
                serde_json::from_value::<OutboxPage>(forged)?
                    .validate(source, &OutboxOptions::default())
                    .is_err(),
                "{pointer}"
            );
        }
        let mut secret = value.clone();
        secret["lanes"][0]["envelope"] = json!("must not leak");
        assert!(serde_json::from_value::<OutboxPage>(secret).is_err());
        let mut noncanonical = value;
        noncanonical["observation"]["oldest_pending_age_ms"] = json!("01");
        assert!(serde_json::from_value::<OutboxPage>(noncanonical).is_err());
        assert_eq!(age(Some(100), 10)?, Some(0));
        assert!(age(Some(-1), 100).is_err());
        Ok(())
    }
}
