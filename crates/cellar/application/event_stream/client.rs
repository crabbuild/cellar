use super::{
    authority::{Action, Authority},
    error::{error, ErrorCode},
    Context, Definition, Envelope, Stream,
};
use anyhow::Result;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cursor {
    pub resource: String,
    pub partition: u32,
    #[serde(with = "super::decimal")]
    pub next: i64,
}

impl Cursor {
    pub fn encode(&self) -> Result<String> {
        self.validate()?;
        Ok(format!(
            "v1.{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(self)?)
        ))
    }

    pub fn decode(encoded: &str) -> Result<Self> {
        if encoded.len() > 512 {
            return Err(error(ErrorCode::InvalidCursor, "cursor is too large"));
        }
        let encoded = encoded
            .strip_prefix("v1.")
            .ok_or_else(|| error(ErrorCode::InvalidCursor, "unsupported cursor format"))?;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| error(ErrorCode::InvalidCursor, "invalid cursor encoding"))?;
        let cursor: Self = serde_json::from_slice(&bytes)
            .map_err(|_| error(ErrorCode::InvalidCursor, "invalid cursor fields"))?;
        cursor.validate()?;
        Ok(cursor)
    }

    fn validate(&self) -> Result<()> {
        super::store::validate_id(&self.resource)
            .map_err(|_| error(ErrorCode::InvalidCursor, "invalid cursor resource"))?;
        if self.partition != 0 || self.next <= 0 {
            return Err(error(ErrorCode::InvalidCursor, "invalid cursor position"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ReadOptions {
    pub max_records: usize,
    pub max_bytes: usize,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            max_records: 100,
            max_bytes: 1 << 20,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Record {
    pub offset: i64,
    pub event: Envelope,
    pub(crate) encoded: Vec<u8>,
}

impl Record {
    pub fn encoded(&self) -> &[u8] {
        &self.encoded
    }
}

#[derive(Clone, Debug)]
pub struct Page {
    pub records: Vec<Record>,
    pub next: Cursor,
    pub earliest: i64,
    pub end: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Configuration {
    pub state: String,
    pub operation: String,
    pub resource: String,
    pub last_error: Option<String>,
}

impl<E> Stream<E> {
    pub async fn configure(
        &self,
        context: &Context,
        definition: &Definition,
        operation: &str,
    ) -> Result<Configuration> {
        self.check_domain()?;
        definition.validate()?;
        super::store::validate_id(operation)?;
        if !definition.resource.same_identity(self.resource()) {
            return Err(error(
                ErrorCode::Forbidden,
                "configuration resource differs from binding",
            ));
        }
        let authority = Authority::mint(context, &self.binding, Action::Configure)?;
        let encoded =
            base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(definition)?);
        let value = super::call_authorized(
            context,
            authority,
            super::catalog_scope(self.resource())?,
            Some(super::catalog_name(self.resource())?),
            "__catalogConfigure",
            json!([{"operation":operation,"definition":encoded}]),
        )
        .await?;
        Ok(serde_json::from_value(value)?)
    }

    pub async fn configuration_status(&self, context: &Context) -> Result<Configuration> {
        self.check_domain()?;
        let authority = Authority::mint(context, &self.binding, Action::Configure)?;
        let value = super::call_authorized(
            context,
            authority,
            super::catalog_scope(self.resource())?,
            Some(super::catalog_name(self.resource())?),
            "__catalogStatus",
            json!([{}]),
        )
        .await?;
        Ok(serde_json::from_value(value)?)
    }

    pub async fn read(
        &self,
        context: &Context,
        cursor: Option<&Cursor>,
        options: ReadOptions,
    ) -> Result<Page> {
        self.check_ready()?;
        if options.max_records == 0 || options.max_bytes == 0 {
            return Err(error(
                ErrorCode::InvalidRequest,
                "read budgets must be positive",
            ));
        }
        if cursor
            .is_some_and(|cursor| cursor.resource != self.resource().id || cursor.partition != 0)
        {
            return Err(error(
                ErrorCode::InvalidCursor,
                "cursor belongs to another resource",
            ));
        }
        if let Some(cursor) = cursor {
            cursor.validate()?;
        }
        let limits = crate::application::limits::current().events;
        let options = ReadOptions {
            max_records: options.max_records.min(limits.batch_records()),
            max_bytes: options.max_bytes.min(limits.batch_bytes()),
        };
        let args = cellar_native::clone::StructuredValue::from_json(json!([{
            "next":cursor.map(|cursor|cursor.next.to_string()),"max_records":options.max_records,"max_bytes":options.max_bytes
        }]))?;
        let value =
            super::call_bound(context, &self.binding, Action::Read, "__streamRead", args).await?;
        accept_page(self.resource(), cursor, options, limits, value)
    }
}

fn accept_page(
    expected: &super::Resource,
    requested: Option<&Cursor>,
    options: ReadOptions,
    limits: cellar_logic::event_stream::Limits,
    value: serde_json::Value,
) -> Result<Page> {
    let resource = value["resource"]
        .as_str()
        .ok_or_else(|| error(ErrorCode::InvalidRequest, "read response has no identity"))?;
    if resource != expected.id {
        return Err(error(
            ErrorCode::InvalidCursor,
            "read response resource mismatch",
        ));
    }
    let earliest = super::subscription_store::integer(&value, "earliest")?;
    let end = super::subscription_store::integer(&value, "end")?;
    let next = super::subscription_store::integer(&value, "next")?;
    if let Some(cursor) = requested {
        cursor.validate()?;
        if cursor.resource != expected.id {
            return Err(error(
                ErrorCode::InvalidCursor,
                "requested cursor resource mismatch",
            ));
        }
    }
    let first = requested.map_or(earliest, |cursor| cursor.next);
    if earliest <= 0
        || end < earliest
        || first < earliest
        || first > end
        || next < first
        || next > end
    {
        return Err(error(
            ErrorCode::InvalidCursor,
            "read response cursor interval conflicts",
        ));
    }
    let rows = value["records"].as_array().ok_or_else(|| {
        error(
            ErrorCode::InvalidRequest,
            "read response records are missing",
        )
    })?;
    let count = options.max_records.min(limits.batch_records());
    let budget = options.max_bytes.min(limits.batch_bytes());
    if count == 0 || budget == 0 {
        return Err(error(
            ErrorCode::InvalidRequest,
            "read budgets must be positive",
        ));
    }
    if rows.len() > count {
        return Err(error(
            ErrorCode::PayloadTooLarge,
            "read response exceeds record budget",
        ));
    }
    let mut records = Vec::new();
    let mut position = first;
    let mut total = 0usize;
    for row in rows {
        let offset = super::subscription_store::integer(row, "offset")?;
        if offset != position || offset >= end {
            return Err(error(
                ErrorCode::InvalidCursor,
                "read response skipped or repeated an offset",
            ));
        }
        let encoded = row["envelope"]
            .as_str()
            .ok_or_else(|| error(ErrorCode::InvalidRequest, "read envelope is missing"))?;
        let maximum = limits.event_bytes().min(budget.saturating_sub(total));
        if encoded.len() > maximum.div_ceil(3) * 4 {
            return Err(error(
                ErrorCode::PayloadTooLarge,
                "encoded read event exceeds receiver budget",
            ));
        }
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| error(ErrorCode::InvalidRequest, "invalid read envelope encoding"))?;
        if bytes.len() > maximum {
            return Err(error(
                ErrorCode::PayloadTooLarge,
                "read response exceeds byte budget",
            ));
        }
        total = total
            .checked_add(bytes.len())
            .ok_or_else(|| error(ErrorCode::PayloadTooLarge, "read response byte overflow"))?;
        let event: Envelope = serde_json::from_slice(&bytes)
            .map_err(|_| error(ErrorCode::InvalidRequest, "invalid read event envelope"))?;
        super::store::validate_id(&event.source_incarnation)
            .map_err(|_| error(ErrorCode::InvalidRequest, "invalid read source incarnation"))?;
        if !cellar_logic::cell::valid_cell_scope(&event.source)
            || event.sequence <= 0
            || event.version == 0
            || event.created_at_ms < 0
            || event.family != expected.family
            || event.key.len() > limits.max_key_bytes
            || event.family.len() > limits.max_family_bytes
            || event.lane
                != super::lane_identity(expected, &event.source, &event.source_incarnation)?
            || event.id != format!("v1:{}:{}", event.lane, event.sequence)
        {
            return Err(error(
                ErrorCode::InvalidRequest,
                "read event identity conflicts with stream",
            ));
        }
        position = position
            .checked_add(1)
            .ok_or_else(|| error(ErrorCode::InvalidCursor, "read cursor overflow"))?;
        records.push(Record {
            offset,
            event,
            encoded: bytes,
        });
    }
    if next != position || (records.is_empty() && first != end) {
        return Err(error(
            ErrorCode::InvalidCursor,
            "read response cursor does not match delivered history",
        ));
    }
    Ok(Page {
        records,
        next: Cursor {
            resource: expected.id.clone(),
            partition: 0,
            next,
        },
        earliest,
        end,
    })
}

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;
