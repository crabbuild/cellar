//! Destination-observed producer lanes, not a census of undispatched sources.

use super::{authority::Action, error::error, store, ErrorCode, Resource, Stream};
use crate::application::{Context, Result};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug)]
pub struct SourceListOptions {
    pub cursor: Option<String>,
    pub max_records: usize,
}

impl Default for SourceListOptions {
    fn default() -> Self {
        Self {
            cursor: None,
            max_records: 100,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceStatus {
    pub lane: String,
    pub source: String,
    pub incarnation: String,
    pub principal: String,
    pub grant: String,
    #[serde(with = "super::decimal")]
    pub accepted_through: i64,
    pub sealed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourcePage {
    pub resource: String,
    /// Ordered by lane digest. Includes retained sealed lanes after payload expiry.
    pub records: Vec<SourceStatus>,
    pub cursor: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    stream: String,
    after: String,
}

fn valid_lane(lane: &str) -> bool {
    lane.len() == 64
        && lane
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

impl Cursor {
    fn decode(encoded: &str, resource: &Resource) -> Result<Self> {
        let invalid = || error(ErrorCode::InvalidCursor, "invalid source inventory cursor");
        if encoded.len() > 256 {
            return Err(invalid());
        }
        let encoded = encoded.strip_prefix("sources1.").ok_or_else(invalid)?;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| invalid())?;
        let cursor: Self = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if cursor.stream != super::cell_scope(resource)? || !valid_lane(&cursor.after) {
            return Err(invalid());
        }
        Ok(cursor)
    }

    fn encode(resource: &Resource, after: String) -> Result<String> {
        let cursor = Self {
            stream: super::cell_scope(resource)?,
            after,
        };
        let encoded = format!(
            "sources1.{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(&cursor)?)
        );
        Self::decode(&encoded, resource)?;
        Ok(encoded)
    }
}

impl SourceListOptions {
    pub(crate) fn validate(&self, resource: &Resource) -> Result<String> {
        resource.validate()?;
        if !(1..=100).contains(&self.max_records) {
            return Err(error(
                ErrorCode::InvalidRequest,
                "invalid source list bounds",
            ));
        }
        self.cursor
            .as_deref()
            .map(|encoded| Cursor::decode(encoded, resource).map(|cursor| cursor.after))
            .transpose()
            .map(|after| after.unwrap_or_default())
    }
}

impl SourcePage {
    pub(crate) fn validate(&self, resource: &Resource, options: &SourceListOptions) -> Result<()> {
        let mut previous = options.validate(resource)?;
        let invalid = || {
            error(
                ErrorCode::InvalidRequest,
                "invalid source inventory response",
            )
        };
        if self.resource != resource.id || self.records.len() > options.max_records {
            return Err(invalid());
        }
        for record in &self.records {
            if !valid_lane(&record.lane)
                || record.lane <= previous
                || !cellar_logic::cell::valid_cell_scope(&record.source)
                || store::validate_id(&record.incarnation).is_err()
                || record.principal.is_empty()
                || record.principal.len() > 256
                || record.grant.is_empty()
                || record.grant.len() > 256
                || record.accepted_through < 0
                || super::lane_identity(resource, &record.source, &record.incarnation)?
                    != record.lane
            {
                return Err(invalid());
            }
            previous.clone_from(&record.lane);
        }
        if let Some(encoded) = &self.cursor {
            let cursor = Cursor::decode(encoded, resource)?;
            if self.records.is_empty() || cursor.after != previous {
                return Err(error(
                    ErrorCode::InvalidCursor,
                    "source continuation does not follow its page",
                ));
            }
        }
        Ok(())
    }
}

impl<E> Stream<E> {
    /// Inspect destination-observed source incarnations using current administration
    /// authority. Live pages are neither a snapshot nor proof that all producers
    /// are drained: a source can hold an obligation not yet seen by this stream.
    pub async fn sources(
        &self,
        context: &Context,
        options: SourceListOptions,
    ) -> Result<SourcePage> {
        self.check_domain()?;
        options.validate(self.resource())?;
        let value = super::call_bound(
            context,
            &self.binding,
            Action::Administer,
            "__streamSources",
            cellar_native::clone::StructuredValue::from_json(
                json!([{"cursor":options.cursor,"max_records":options.max_records}]),
            )?,
        )
        .await?;
        let page: SourcePage = serde_json::from_value(value)?;
        page.validate(self.resource(), &options)?;
        Ok(page)
    }
}

pub(crate) fn list(context: &Context, body: &Value) -> Result<Value> {
    let caller = store::authority(context, Action::Administer)?;
    let db = context.storage()?;
    let (definition, _) = store::load_state(&db)?;
    store::authorize(&definition, &caller, Action::Administer, None)?;
    let options = SourceListOptions {
        cursor: match body.get("cursor") {
            None | Some(Value::Null) => None,
            Some(Value::String(cursor)) => Some(cursor.clone()),
            _ => {
                return Err(error(
                    ErrorCode::InvalidCursor,
                    "invalid source inventory cursor",
                ))
            }
        },
        max_records: body["max_records"]
            .as_u64()
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(|| error(ErrorCode::InvalidRequest, "invalid source list bounds"))?,
    };
    Ok(serde_json::to_value(page(
        context,
        &definition.resource,
        &options,
    )?)?)
}

pub(crate) fn page(
    context: &Context,
    resource: &Resource,
    options: &SourceListOptions,
) -> Result<SourcePage> {
    anyhow::ensure!(
        context
            .identity()
            .is_some_and(|id| super::cell_scope(resource).is_ok_and(|scope| id.scope == scope)),
        "producer page names another owner scope"
    );
    let db = context.storage()?;
    let after = options.validate(resource)?;
    // A primary-key range reads at most 101 metadata rows. No payload joins,
    // token columns, or filtering that scans past an unbounded retained prefix.
    let rows = db.system_sql("SELECT lane,source,incarnation,principal,grant_id,accepted_through,sealed FROM __sys_stream_producers WHERE lane>? ORDER BY lane LIMIT ?",
        &[after.into(),(options.max_records as i64+1).into()])?;
    let more = rows.len() > options.max_records;
    let mut records = Vec::new();
    for row in rows.into_iter().take(options.max_records) {
        records.push(SourceStatus {
            lane: store::text(&row, "lane")?,
            source: store::text(&row, "source")?,
            incarnation: store::text(&row, "incarnation")?,
            principal: store::text(&row, "principal")?,
            grant: store::text(&row, "grant_id")?,
            accepted_through: store::number(&row, "accepted_through")?,
            sealed: store::number(&row, "sealed")? != 0,
        });
    }
    let cursor = if more {
        Some(Cursor::encode(
            resource,
            records
                .last()
                .ok_or_else(|| error(ErrorCode::InvalidRequest, "empty source continuation page"))?
                .lane
                .clone(),
        )?)
    } else {
        None
    };
    let page = SourcePage {
        resource: resource.id.clone(),
        records,
        cursor,
    };
    page.validate(resource, options)?;
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resource() -> Resource {
        Resource {
            tenant: "tenant".into(),
            application: "app".into(),
            name: "events".into(),
            family: "events".into(),
            id: "00000000000000000000000000000001".into(),
        }
    }

    fn record(resource: &Resource) -> Result<SourceStatus> {
        let source = "Order:source".to_owned();
        let incarnation = "00000000000000000000000000000002".to_owned();
        Ok(SourceStatus {
            lane: super::super::lane_identity(resource, &source, &incarnation)?,
            source,
            incarnation,
            principal: "app".into(),
            grant: "publisher".into(),
            accepted_through: i64::MAX,
            sealed: true,
        })
    }

    #[test]
    fn source_cursor_is_bound_to_tenant_application_resource_and_page_tail() -> Result<()> {
        let resource = resource();
        let record = record(&resource)?;
        let cursor = Cursor::encode(&resource, record.lane.clone())?;
        let page = SourcePage {
            resource: resource.id.clone(),
            records: vec![record],
            cursor: Some(cursor.clone()),
        };
        page.validate(&resource, &SourceListOptions::default())?;
        for changed in ["tenant", "application", "id"] {
            let mut foreign = resource.clone();
            match changed {
                "tenant" => foreign.tenant = "another".into(),
                "application" => foreign.application = "another".into(),
                _ => foreign.id = "00000000000000000000000000000003".into(),
            }
            assert!(Cursor::decode(&cursor, &foreign).is_err());
        }
        for cursor in [
            "sources1.%%%".to_string(),
            "subs1.%%%".into(),
            "x".repeat(257),
        ] {
            let error = Cursor::decode(&cursor, &resource).err().unwrap();
            assert_eq!(
                error
                    .downcast_ref::<super::super::DeliveryError>()
                    .unwrap()
                    .code,
                ErrorCode::InvalidCursor
            );
        }
        let mut wrong_tail = page;
        wrong_tail.cursor = Some(Cursor::encode(&resource, "0".repeat(64))?);
        assert!(wrong_tail
            .validate(&resource, &SourceListOptions::default())
            .is_err());
        for max_records in [0, 101, usize::MAX] {
            assert!(SourceListOptions {
                cursor: None,
                max_records
            }
            .validate(&resource)
            .is_err());
        }
        Ok(())
    }

    #[test]
    fn source_pages_preserve_large_watermarks_and_refuse_forged_identity_or_order() -> Result<()> {
        let resource = resource();
        let record = record(&resource)?;
        let page = SourcePage {
            resource: resource.id.clone(),
            records: vec![record.clone()],
            cursor: None,
        };
        let encoded = serde_json::to_value(&page)?;
        assert_eq!(
            encoded["records"][0]["accepted_through"],
            i64::MAX.to_string()
        );
        let roundtrip: SourcePage = serde_json::from_value(encoded.clone())?;
        roundtrip.validate(&resource, &SourceListOptions::default())?;
        let mut zero = roundtrip;
        zero.records[0].accepted_through = 0;
        zero.validate(&resource, &SourceListOptions::default())?;
        for field in ["source", "incarnation", "lane", "principal", "grant"] {
            let mut invalid = encoded.clone();
            invalid["records"][0][field] = json!(if matches!(field, "principal" | "grant") {
                ""
            } else {
                "bad identity"
            });
            let page: SourcePage = serde_json::from_value(invalid)?;
            assert!(page
                .validate(&resource, &SourceListOptions::default())
                .is_err());
        }
        for value in [json!(1), json!("01"), json!("9223372036854775808")] {
            let mut invalid = encoded.clone();
            invalid["records"][0]["accepted_through"] = value;
            assert!(serde_json::from_value::<SourcePage>(invalid).is_err());
        }
        let duplicate = SourcePage {
            resource: resource.id.clone(),
            records: vec![record.clone(), record],
            cursor: None,
        };
        assert!(duplicate
            .validate(&resource, &SourceListOptions::default())
            .is_err());
        Ok(())
    }
}
