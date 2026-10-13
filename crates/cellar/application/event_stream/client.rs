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
        let args = cellar_native::clone::StructuredValue::from_json(json!([{
            "next":cursor.map(|cursor|cursor.next.to_string()),"max_records":options.max_records,"max_bytes":options.max_bytes
        }]))?;
        let value =
            super::call_bound(context, &self.binding, Action::Read, "__streamRead", args).await?;
        let resource = value["resource"]
            .as_str()
            .ok_or_else(|| error(ErrorCode::InvalidRequest, "read response has no identity"))?;
        if resource != self.resource().id {
            return Err(error(
                ErrorCode::InvalidCursor,
                "read response resource mismatch",
            ));
        }
        let number = |key: &str| -> Result<i64> {
            Ok(value[key]
                .as_str()
                .ok_or_else(|| {
                    error(
                        ErrorCode::InvalidRequest,
                        "read response integer is missing",
                    )
                })?
                .parse()?)
        };
        let mut records = Vec::new();
        for row in value["records"].as_array().ok_or_else(|| {
            error(
                ErrorCode::InvalidRequest,
                "read response records are missing",
            )
        })? {
            let offset = row["offset"]
                .as_str()
                .ok_or_else(|| error(ErrorCode::InvalidRequest, "read offset is missing"))?
                .parse()?;
            let encoded = row["envelope"]
                .as_str()
                .ok_or_else(|| error(ErrorCode::InvalidRequest, "read envelope is missing"))?;
            let bytes = base64::engine::general_purpose::STANDARD.decode(encoded)?;
            records.push(Record {
                offset,
                event: serde_json::from_slice(&bytes)?,
                encoded: bytes,
            });
        }
        Ok(Page {
            records,
            next: Cursor {
                resource: self.resource().id.clone(),
                partition: 0,
                next: number("next")?,
            },
            earliest: number("earliest")?,
            end: number("end")?,
        })
    }
}
