//! Platform catalog inspection is separate from an individual resource grant.

use super::{error::error, store, ErrorCode};
use crate::application::{Context, Result};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) const METHOD: &str = "__catalogList";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogNamespace {
    pub tenant: String,
    pub application: String,
}

impl CatalogNamespace {
    pub fn validate(&self) -> Result<()> {
        for (field, value) in [("tenant", &self.tenant), ("application", &self.application)] {
            anyhow::ensure!(
                !value.is_empty() && value.len() <= 128,
                "invalid catalog {field}"
            );
        }
        Ok(())
    }

    pub(crate) fn name(&self) -> Result<String> {
        self.validate()?;
        Ok(serde_json::to_string(&(&self.tenant, &self.application))?)
    }

    pub(crate) fn scope(&self) -> Result<String> {
        let id = crate::engine_api::durable_object_id_for_name(
            crate::engine_api::EVENT_CATALOG_NAMESPACE_KEY,
            &self.name()?,
        );
        Ok(format!(
            "{}:{}",
            crate::deploy::EVENT_CATALOG_CLASS,
            crate::engine_api::durable_object_id_hex(&id)
        ))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogListOptions {
    pub cursor: Option<String>,
    pub max_records: usize,
}

impl Default for CatalogListOptions {
    fn default() -> Self {
        Self {
            cursor: None,
            max_records: 100,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CatalogState {
    Creating,
    Updating,
    Active,
    Retiring,
    Deleted,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogEntry {
    pub resource: super::Resource,
    pub state: CatalogState,
    pub operation: String,
    #[serde(with = "super::decimal::optional")]
    pub accepted_revision: Option<i64>,
    #[serde(with = "super::decimal::optional")]
    pub desired_revision: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogPage {
    pub namespace: CatalogNamespace,
    pub records: Vec<CatalogEntry>,
    pub cursor: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    catalog: String,
    after: String,
}

impl Cursor {
    fn decode(encoded: &str, namespace: &CatalogNamespace) -> Result<Self> {
        let invalid = || error(ErrorCode::InvalidCursor, "invalid catalog inventory cursor");
        if encoded.len() > 256 {
            return Err(invalid());
        }
        let encoded = encoded.strip_prefix("catalog1.").ok_or_else(invalid)?;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| invalid())?;
        let cursor: Self = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if cursor.catalog != namespace.scope()? || store::validate_id(&cursor.after).is_err() {
            return Err(invalid());
        }
        Ok(cursor)
    }

    fn encode(namespace: &CatalogNamespace, after: String) -> Result<String> {
        let encoded = format!(
            "catalog1.{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(&Self {
                catalog: namespace.scope()?,
                after
            })?)
        );
        Self::decode(&encoded, namespace)?;
        Ok(encoded)
    }
}

impl CatalogListOptions {
    pub(crate) fn validate(&self, namespace: &CatalogNamespace) -> Result<String> {
        namespace.validate()?;
        if !(1..=100).contains(&self.max_records) {
            return Err(error(
                ErrorCode::InvalidRequest,
                "invalid catalog list bounds",
            ));
        }
        self.cursor
            .as_deref()
            .map(|cursor| Cursor::decode(cursor, namespace).map(|cursor| cursor.after))
            .transpose()
            .map(|after| after.unwrap_or_default())
    }
}

impl CatalogPage {
    pub(crate) fn validate(
        &self,
        namespace: &CatalogNamespace,
        options: &CatalogListOptions,
    ) -> Result<()> {
        let mut previous = options.validate(namespace)?;
        let invalid = || {
            error(
                ErrorCode::InvalidRequest,
                "invalid catalog inventory response",
            )
        };
        if &self.namespace != namespace || self.records.len() > options.max_records {
            return Err(invalid());
        }
        for record in &self.records {
            record.resource.validate()?;
            let revisions_match_state = match record.state {
                CatalogState::Creating => {
                    record.accepted_revision.is_none() && record.desired_revision == Some(1)
                }
                CatalogState::Updating => {
                    matches!((record.accepted_revision, record.desired_revision), (Some(accepted), Some(desired)) if accepted > 0 && desired > accepted)
                }
                CatalogState::Active | CatalogState::Retiring | CatalogState::Deleted => {
                    record
                        .accepted_revision
                        .is_some_and(|revision| revision > 0)
                        && record.desired_revision.is_none()
                }
            };
            if record.resource.tenant != namespace.tenant
                || record.resource.application != namespace.application
                || record.resource.id <= previous
                || store::validate_id(&record.operation).is_err()
                || record
                    .accepted_revision
                    .is_some_and(|revision| revision < 1)
                || record.desired_revision.is_some_and(|revision| revision < 1)
                || !revisions_match_state
            {
                return Err(invalid());
            }
            previous.clone_from(&record.resource.id);
        }
        if let Some(encoded) = &self.cursor {
            if self.records.is_empty() || Cursor::decode(encoded, namespace)?.after != previous {
                return Err(error(
                    ErrorCode::InvalidCursor,
                    "catalog continuation does not follow its page",
                ));
            }
        }
        Ok(())
    }
}

fn revision(row: &crate::application::storage::SqlRow, key: &str) -> Result<Option<i64>> {
    use crate::application::storage::SqlValue;
    match row.get(key) {
        Some(SqlValue::Null) => Ok(None),
        Some(SqlValue::Integer(value)) if *value > 0 => Ok(Some(*value)),
        _ => Err(error(
            ErrorCode::Unavailable,
            "invalid stored catalog revision",
        )),
    }
}

pub(crate) fn list(context: &Context, body: &Value) -> Result<Value> {
    let caller = context.catalog_authority.as_ref().ok_or_else(|| {
        error(
            ErrorCode::Forbidden,
            "catalog inventory requires platform authority",
        )
    })?;
    let identity = context.identity().ok_or_else(|| {
        error(
            ErrorCode::Forbidden,
            "catalog inventory requires a system cell",
        )
    })?;
    caller.validate_target(&identity.scope, METHOD)?;
    let namespace = caller.namespace();
    let options: CatalogListOptions = serde_json::from_value(body.clone())?;
    let after = options.validate(namespace)?;
    let db = context.storage()?;
    // Only scalar metadata is copied. The primary-key range reads at most 101
    // rows and includes retained tombstones without grant/configuration bodies.
    let rows = db.system_sql("SELECT id,name,lifecycle,operation,tenant,application,family,accepted_revision,desired_revision
        FROM __sys_event_catalog WHERE id>? ORDER BY id LIMIT ?", &[after.into(), (options.max_records as i64 + 1).into()])?;
    let more = rows.len() > options.max_records;
    let mut records = Vec::new();
    for row in rows.into_iter().take(options.max_records) {
        let id = store::text(&row, "id")?;
        records.push(CatalogEntry {
            resource: super::Resource {
                tenant: store::text(&row, "tenant")?,
                application: store::text(&row, "application")?,
                id,
                name: store::text(&row, "name")?,
                family: store::text(&row, "family")?,
            },
            state: serde_json::from_value(Value::String(store::text(&row, "lifecycle")?))?,
            operation: store::text(&row, "operation")?,
            accepted_revision: revision(&row, "accepted_revision")?,
            desired_revision: revision(&row, "desired_revision")?,
        });
    }
    let cursor = if more {
        Some(Cursor::encode(
            namespace,
            records
                .last()
                .ok_or_else(|| error(ErrorCode::InvalidRequest, "empty catalog page"))?
                .resource
                .id
                .clone(),
        )?)
    } else {
        None
    };
    let page = CatalogPage {
        namespace: namespace.clone(),
        records,
        cursor,
    };
    page.validate(namespace, &options)?;
    Ok(serde_json::to_value(page)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn namespace() -> CatalogNamespace {
        CatalogNamespace {
            tenant: "tenant".into(),
            application: "app".into(),
        }
    }

    fn page() -> CatalogPage {
        CatalogPage {
            namespace: namespace(),
            records: vec![CatalogEntry {
                resource: super::super::Resource {
                    tenant: "tenant".into(),
                    application: "app".into(),
                    id: "00000000000000000000000000000001".into(),
                    name: "events".into(),
                    family: "events".into(),
                },
                state: CatalogState::Active,
                operation: "00000000000000000000000000000002".into(),
                accepted_revision: Some(i64::MAX),
                desired_revision: None,
            }],
            cursor: None,
        }
    }

    #[test]
    fn catalog_cursor_and_pages_bind_namespace_identity_and_tail() -> Result<()> {
        let namespace = namespace();
        let mut page = page();
        page.cursor = Some(Cursor::encode(
            &namespace,
            page.records[0].resource.id.clone(),
        )?);
        page.validate(&namespace, &CatalogListOptions::default())?;
        for foreign in [
            CatalogNamespace {
                tenant: "other".into(),
                ..namespace.clone()
            },
            CatalogNamespace {
                application: "other".into(),
                ..namespace.clone()
            },
        ] {
            assert!(Cursor::decode(page.cursor.as_ref().unwrap(), &foreign).is_err());
            assert!(page
                .validate(&foreign, &CatalogListOptions::default())
                .is_err());
        }
        page.records.push(page.records[0].clone());
        assert!(page
            .validate(&namespace, &CatalogListOptions::default())
            .is_err());
        page.records.pop();
        page.cursor = Some(Cursor::encode(&namespace, "0".repeat(32))?);
        assert!(page
            .validate(&namespace, &CatalogListOptions::default())
            .is_err());
        for max_records in [0, 101, usize::MAX] {
            assert!(CatalogListOptions {
                cursor: None,
                max_records
            }
            .validate(&namespace)
            .is_err());
        }
        for cursor in [
            "subs1.%%%".to_owned(),
            "catalog1.%%%".into(),
            "x".repeat(257),
        ] {
            assert!(Cursor::decode(&cursor, &namespace).is_err());
        }
        Ok(())
    }

    #[test]
    fn catalog_metadata_refuses_sensitive_fields_and_preserves_decimal_revisions() -> Result<()> {
        let page = page();
        let encoded = serde_json::to_value(&page)?;
        assert_eq!(
            encoded["records"][0]["accepted_revision"],
            i64::MAX.to_string()
        );
        let restored: CatalogPage = serde_json::from_value(encoded.clone())?;
        restored.validate(&namespace(), &CatalogListOptions::default())?;
        for value in [json!(1), json!("01"), json!("9223372036854775808")] {
            let mut invalid = encoded.clone();
            invalid["records"][0]["accepted_revision"] = value;
            assert!(serde_json::from_value::<CatalogPage>(invalid).is_err());
        }
        for field in ["definition", "grants", "payload", "token"] {
            let mut invalid = encoded.clone();
            invalid["records"][0][field] = json!("must not leak");
            assert!(serde_json::from_value::<CatalogPage>(invalid).is_err());
        }
        let mut invalid = page;
        invalid.records[0].accepted_revision = None;
        assert!(invalid
            .validate(&namespace(), &CatalogListOptions::default())
            .is_err());
        Ok(())
    }
}
