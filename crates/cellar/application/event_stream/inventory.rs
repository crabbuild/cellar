//! Bounded subscription inventory under the stream's current administration grant.

use super::{authority::Action, error::error, store, ErrorCode, Stream, SubscriptionStatus};
use crate::application::{Context, Result};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug)]
pub struct SubscriptionListOptions {
    /// Opaque continuation returned by the prior page; scoped to this stream UUID.
    pub cursor: Option<String>,
    pub max_records: usize,
}

impl Default for SubscriptionListOptions {
    fn default() -> Self {
        Self {
            cursor: None,
            max_records: 100,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionPage {
    pub resource: String,
    /// Includes retained deleted incarnations. Ordered by subscription UUID.
    pub records: Vec<SubscriptionStatus>,
    pub cursor: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    resource: String,
    after: String,
}

impl Cursor {
    fn validate(&self, resource: &str) -> Result<()> {
        if store::validate_id(&self.resource).is_err()
            || store::validate_id(&self.after).is_err()
            || self.resource != resource
        {
            return Err(error(
                ErrorCode::InvalidCursor,
                "invalid subscription inventory cursor",
            ));
        }
        Ok(())
    }

    fn decode(encoded: &str, resource: &str) -> Result<Self> {
        if encoded.len() > 256 {
            return Err(error(
                ErrorCode::InvalidCursor,
                "subscription cursor is too large",
            ));
        }
        let encoded = encoded
            .strip_prefix("subs1.")
            .ok_or_else(|| error(ErrorCode::InvalidCursor, "unsupported subscription cursor"))?;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| {
                error(
                    ErrorCode::InvalidCursor,
                    "invalid subscription cursor encoding",
                )
            })?;
        let cursor: Self = serde_json::from_slice(&bytes).map_err(|_| {
            error(
                ErrorCode::InvalidCursor,
                "invalid subscription cursor fields",
            )
        })?;
        cursor.validate(resource)?;
        Ok(cursor)
    }

    fn encode(&self) -> Result<String> {
        self.validate(&self.resource)?;
        Ok(format!(
            "subs1.{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(self)?)
        ))
    }
}

impl SubscriptionListOptions {
    pub(crate) fn validate(&self, resource: &str) -> Result<String> {
        if !(1..=100).contains(&self.max_records) {
            return Err(error(
                ErrorCode::InvalidRequest,
                "invalid subscription list bounds",
            ));
        }
        self.cursor
            .as_deref()
            .map(|encoded| Cursor::decode(encoded, resource).map(|cursor| cursor.after))
            .transpose()
            .map(|after| after.unwrap_or_default())
    }
}

impl SubscriptionPage {
    pub(crate) fn validate(&self, resource: &str, options: &SubscriptionListOptions) -> Result<()> {
        let mut previous = options.validate(resource)?;
        if self.resource != resource || self.records.len() > options.max_records {
            return Err(error(
                ErrorCode::InvalidRequest,
                "invalid subscription inventory response",
            ));
        }
        for record in &self.records {
            record.identity.validate()?;
            if record.identity.resource != resource
                || record.identity.id <= previous
                || !matches!(
                    record.state.as_str(),
                    "active" | "paused" | "expired" | "deleted"
                )
            {
                return Err(error(
                    ErrorCode::InvalidRequest,
                    "invalid subscription inventory record",
                ));
            }
            previous.clone_from(&record.identity.id);
        }
        if let Some(encoded) = &self.cursor {
            let cursor = Cursor::decode(encoded, resource)?;
            if self.records.is_empty() || cursor.after != previous {
                return Err(error(
                    ErrorCode::InvalidCursor,
                    "subscription continuation does not follow its page",
                ));
            }
        }
        Ok(())
    }
}

impl<E> Stream<E> {
    /// Inspect a bounded inventory using current administration authority.
    /// Pages are live observations, not a snapshot: new lower UUIDs require a
    /// fresh traversal. Deleted incarnations remain visible while retained.
    pub async fn subscriptions(
        &self,
        context: &Context,
        options: SubscriptionListOptions,
    ) -> Result<SubscriptionPage> {
        self.check_domain()?;
        options.validate(&self.resource().id)?;
        let value = super::call_bound(
            context,
            &self.binding,
            Action::Administer,
            "__streamSubscriptions",
            cellar_native::clone::StructuredValue::from_json(
                json!([{"cursor":options.cursor,"max_records":options.max_records}]),
            )?,
        )
        .await?;
        let page: SubscriptionPage = serde_json::from_value(value)?;
        page.validate(&self.resource().id, &options)?;
        Ok(page)
    }
}

pub(crate) fn list(context: &Context, body: &Value) -> Result<Value> {
    let caller = store::authority(context, Action::Administer)?;
    let db = context.storage()?;
    let (definition, stream) = store::load_state(&db)?;
    store::authorize(&definition, &caller, Action::Administer, None)?;
    let options = SubscriptionListOptions {
        cursor: match body.get("cursor") {
            None | Some(Value::Null) => None,
            Some(Value::String(encoded)) => Some(encoded.clone()),
            _ => {
                return Err(error(
                    ErrorCode::InvalidCursor,
                    "invalid subscription inventory cursor",
                ))
            }
        },
        max_records: body["max_records"]
            .as_u64()
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(|| {
                error(
                    ErrorCode::InvalidRequest,
                    "invalid subscription list bounds",
                )
            })?,
    };
    let after = options.validate(&definition.resource.id)?;
    // The primary-key range bounds the SQL page even with unrestricted host
    // limits. Include every lifecycle state rather than scanning past filtered
    // tombstones. Never expose lease tokens or event envelopes.
    let rows = db.system_sql("SELECT s.*,l.generation AS lease_generation,l.first_offset AS lease_first,l.last_offset AS lease_last,l.expires_ms AS lease_expires FROM __sys_stream_subscriptions s LEFT JOIN __sys_stream_leases l ON l.subscription=s.id WHERE s.id>? ORDER BY s.id LIMIT ?",
        &[after.into(),(options.max_records as i64+1).into()])?;
    let more = rows.len() > options.max_records;
    let mut records = Vec::new();
    for row in rows.into_iter().take(options.max_records) {
        let lease = if !matches!(
            row.get("lease_generation"),
            Some(crate::application::storage::SqlValue::Null)
        ) {
            Some(super::LeaseSummary {
                generation: store::number(&row, "lease_generation")?,
                first: store::number(&row, "lease_first")?,
                last: store::number(&row, "lease_last")?,
                expires_ms: store::number(&row, "lease_expires")?,
            })
        } else {
            None
        };
        records.push(SubscriptionStatus {
            identity: super::lifecycle_store::identity(&definition, &row)?,
            state: store::text(&row, "state")?,
            required: store::number(&row, "required")? != 0,
            checkpoint: store::number(&row, "checkpoint")?,
            earliest: store::number(&stream, "earliest_offset")?,
            end: store::number(&stream, "next_offset")?,
            retry_at_ms: store::number(&row, "retry_at_ms")?,
            lease,
        });
    }
    let cursor = if more {
        Some(
            Cursor {
                resource: definition.resource.id.clone(),
                after: records
                    .last()
                    .ok_or_else(|| {
                        error(
                            ErrorCode::InvalidRequest,
                            "empty subscription continuation page",
                        )
                    })?
                    .identity
                    .id
                    .clone(),
            }
            .encode()?,
        )
    } else {
        None
    };
    let page = SubscriptionPage {
        resource: definition.resource.id,
        records,
        cursor,
    };
    page.validate(&page.resource, &options)?;
    Ok(serde_json::to_value(page)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_cursor_rejects_foreign_resources_and_malformed_fields() -> Result<()> {
        let resource = "00000000000000000000000000000001";
        let cursor = Cursor {
            resource: resource.into(),
            after: "00000000000000000000000000000002".into(),
        };
        let encoded = cursor.encode()?;
        assert_eq!(Cursor::decode(&encoded, resource)?.after, cursor.after);
        assert!(Cursor::decode(&encoded, "00000000000000000000000000000003").is_err());
        for invalid in ["v1.%%%".to_string(), "subs1.%%%".into(), "x".repeat(257)] {
            let failure = Cursor::decode(&invalid, resource)
                .err()
                .expect("invalid cursor refused");
            assert_eq!(
                failure
                    .downcast_ref::<super::super::DeliveryError>()
                    .unwrap()
                    .code,
                ErrorCode::InvalidCursor
            );
        }
        for value in [
            json!({"resource":resource,"after":"-1"}),
            json!({"resource":resource,"after":cursor.after,"extra":true}),
        ] {
            let encoded = format!(
                "subs1.{}",
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .encode(serde_json::to_vec(&value)?)
            );
            let failure = Cursor::decode(&encoded, resource)
                .err()
                .expect("invalid fields refused");
            assert_eq!(
                failure
                    .downcast_ref::<super::super::DeliveryError>()
                    .unwrap()
                    .code,
                ErrorCode::InvalidCursor
            );
        }
        for max_records in [0, 101, usize::MAX] {
            assert!(SubscriptionListOptions {
                cursor: None,
                max_records
            }
            .validate(resource)
            .is_err());
        }
        Ok(())
    }
}
