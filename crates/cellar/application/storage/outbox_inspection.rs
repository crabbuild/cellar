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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutboxPage {
    pub incarnation: Option<String>,
    #[serde(with = "crate::application::event_stream::decimal")]
    pub pending_rows: i64,
    #[serde(with = "crate::application::event_stream::decimal")]
    pub pending_bytes: i64,
    pub lanes: Vec<OutboxLane>,
    pub next: Option<String>,
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

impl Storage {
    fn outbox_incarnation(&self) -> Result<Option<String>> {
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
        let after = if let Some(encoded) = &options.cursor {
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
                || incarnation.as_deref() != Some(cursor_incarnation.as_str())
                || !valid_hex(&lane, 64)
            {
                return Err(invalid());
            }
            lane
        } else {
            String::new()
        };
        let Some(incarnation) = incarnation else {
            return Ok(OutboxPage {
                incarnation: None,
                pending_rows: 0,
                pending_bytes: 0,
                lanes: Vec::new(),
                next: None,
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
            substr(l.last_error,1,1024) AS last_error,s.state AS seal_state,
            json_extract(l.binding,'$.resource') AS resource,json_extract(l.binding,'$.principal') AS principal,
            json_extract(l.binding,'$.grant') AS grant,
            (SELECT count(*) FROM __sys_outbox AS e WHERE e.lane=l.lane) AS pending_rows,
            (SELECT coalesce(sum(size),0) FROM __sys_outbox AS e WHERE e.lane=l.lane) AS pending_bytes
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
        Ok(OutboxPage {
            incarnation: Some(incarnation),
            pending_rows: number(totals, "rows")?,
            pending_bytes: number(totals, "bytes")?,
            lanes,
            next,
        })
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
            tx.system_sql("UPDATE __sys_outbox_lanes SET status=CASE WHEN status='sealed' THEN 'sealed' ELSE 'pending' END,retry_at_ms=?,attempts=0,dispatch_generation=? WHERE lane=?",
                &[now.into(),generation.into(),revision.lane.clone().into()])?;
            tx.system_sql("UPDATE __sys_outbox_seals SET state='pending' WHERE lane=? AND state!='accepted'",&[revision.lane.clone().into()])?;
            tx.schedule_work(cellar_logic::work::Reason::Outbox,now)?;
            Ok(true)
        })
    }
}
