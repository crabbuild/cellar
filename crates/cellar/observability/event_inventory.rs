//! Bounded platform inventory metadata; no configuration bodies or receipt secrets.
use crate::application::event_stream::{
    CatalogListOptions, CatalogNamespace, CatalogPage, Resource, SourceListOptions, SourcePage,
};
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::{Deserialize, Serialize};

fn limit() -> usize {
    100
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case", deny_unknown_fields)]
pub enum InventoryRequest {
    Describe,
    Resources {
        #[serde(default = "limit")]
        limit: usize,
        #[serde(default)]
        cursor: Option<String>,
    },
    Producers {
        #[serde(default = "limit")]
        limit: usize,
        #[serde(default)]
        cursor: Option<String>,
    },
    History {
        #[serde(default = "limit")]
        limit: usize,
        #[serde(default)]
        cursor: Option<String>,
    },
}

impl InventoryRequest {
    pub fn validate(&self, scope: &str) -> Result<()> {
        ensure!(
            cellar_logic::cell::valid_cell_scope(scope) && scope.contains(':'),
            "invalid inventory scope"
        );
        let (limit, cursor) = match self {
            Self::Describe => return Ok(()),
            Self::Resources { limit, cursor }
            | Self::Producers { limit, cursor }
            | Self::History { limit, cursor } => (*limit, cursor),
        };
        ensure!(
            (1..=100).contains(&limit) && cursor.as_ref().is_none_or(|s| s.len() <= 2048),
            "invalid inventory page bounds"
        );
        let class = scope.split_once(':').unwrap().0;
        match self {
            Self::Resources { .. } => ensure!(
                class == crate::deploy::EVENT_CATALOG_CLASS,
                "resources require a catalog scope"
            ),
            Self::Producers { .. } => ensure!(
                class == crate::deploy::EVENT_STREAM_CLASS,
                "producers require a stream scope"
            ),
            Self::History { cursor, .. } => {
                ensure!(
                    !crate::application::event_stream::is_event_scope(scope),
                    "history requires a source scope"
                );
                history_position(cursor.as_deref(), scope)?;
            }
            Self::Describe => unreachable!(),
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub enum Descriptor {
    Catalog {
        scope: String,
        namespace: Option<CatalogNamespace>,
    },
    Stream {
        scope: String,
        resource: Option<Resource>,
    },
    Source {
        scope: String,
        incarnation: Option<String>,
        history: bool,
    },
}

impl Descriptor {
    pub fn validate(&self, scope: &str) -> Result<()> {
        InventoryRequest::Describe.validate(scope)?;
        let class = scope.split_once(':').unwrap().0;
        match self {
            Self::Catalog {
                scope: target,
                namespace,
            } => {
                ensure!(
                    scope == target && class == crate::deploy::EVENT_CATALOG_CLASS,
                    "catalog descriptor scope conflict"
                );
                if let Some(ns) = namespace {
                    ns.validate()?;
                    ensure!(ns.scope()? == scope, "catalog namespace identity conflict");
                }
            }
            Self::Stream {
                scope: target,
                resource,
            } => {
                ensure!(
                    scope == target && class == crate::deploy::EVENT_STREAM_CLASS,
                    "stream descriptor scope conflict"
                );
                if let Some(resource) = resource {
                    resource.validate()?;
                    ensure!(
                        crate::application::event_stream::cell_scope(resource)? == scope,
                        "stream resource identity conflict"
                    );
                }
            }
            Self::Source {
                scope: target,
                incarnation,
                ..
            } => {
                ensure!(
                    scope == target && !crate::application::event_stream::is_event_scope(scope),
                    "source descriptor scope conflict"
                );
                if let Some(id) = incarnation {
                    identity(id)?;
                }
            }
        }
        Ok(())
    }
}

fn identity(id: &str) -> Result<()> {
    ensure!(
        id.len() == 32
            && id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid inventory identity"
    );
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceHistory {
    #[serde(with = "crate::application::event_stream::decimal")]
    pub sequence: i64,
    pub operation: String,
    pub incarnation: String,
    pub retired_incarnation: Option<String>,
    /// Retry has no lifecycle state; destruction/recreation states remain explicit.
    pub state: Option<String>,
    #[serde(with = "crate::application::event_stream::decimal")]
    pub created_ms: i64,
    #[serde(with = "crate::application::event_stream::decimal::optional")]
    pub discarded_rows: Option<i64>,
    #[serde(with = "crate::application::event_stream::decimal::optional")]
    pub discarded_bytes: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryPage {
    pub scope: String,
    pub records: Vec<SourceHistory>,
    pub cursor: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryCursor {
    scope: String,
    #[serde(with = "crate::application::event_stream::decimal")]
    after: i64,
}

pub(crate) fn history_position(cursor: Option<&str>, scope: &str) -> Result<i64> {
    let Some(cursor) = cursor else { return Ok(0) };
    ensure!(cursor.len() <= 2048, "history cursor exceeds bounds");
    let encoded = cursor
        .strip_prefix("history1:")
        .ok_or_else(|| anyhow::anyhow!("unsupported history cursor"))?;
    let bytes = URL_SAFE_NO_PAD.decode(encoded)?;
    ensure!(
        URL_SAFE_NO_PAD.encode(&bytes) == encoded,
        "noncanonical history cursor"
    );
    let cursor: HistoryCursor = serde_json::from_slice(&bytes)?;
    ensure!(
        cursor.scope == scope && cursor.after > 0,
        "history cursor names another source or position"
    );
    Ok(cursor.after)
}

pub(crate) fn history_cursor(scope: &str, after: i64) -> Result<String> {
    let cursor = format!(
        "history1:{}",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&HistoryCursor {
            scope: scope.into(),
            after
        })?)
    );
    history_position(Some(&cursor), scope)?;
    Ok(cursor)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "lowercase",
    deny_unknown_fields
)]
pub enum InventoryReply {
    Descriptor(Descriptor),
    Resources(CatalogPage),
    Producers {
        resource: Resource,
        retired: bool,
        cleanup_complete: bool,
        page: SourcePage,
    },
    History(HistoryPage),
}

impl InventoryReply {
    pub fn validate(&self, scope: &str, request: &InventoryRequest) -> Result<()> {
        request.validate(scope)?;
        match (self, request) {
            (Self::Descriptor(info), InventoryRequest::Describe) => info.validate(scope),
            (Self::Resources(page), InventoryRequest::Resources { limit, cursor }) => {
                ensure!(
                    page.namespace.scope()? == scope,
                    "resource page scope conflict"
                );
                page.validate(
                    &page.namespace,
                    &CatalogListOptions {
                        max_records: *limit,
                        cursor: cursor.clone(),
                    },
                )
            }
            (
                Self::Producers {
                    resource,
                    retired,
                    cleanup_complete,
                    page,
                },
                InventoryRequest::Producers { limit, cursor },
            ) => {
                ensure!(
                    crate::application::event_stream::cell_scope(resource)? == scope,
                    "producer page scope conflict"
                );
                ensure!(
                    !*cleanup_complete
                        || (*retired && page.records.is_empty() && page.cursor.is_none()),
                    "terminal producer cleanup conflicts with records"
                );
                page.validate(
                    resource,
                    &SourceListOptions {
                        max_records: *limit,
                        cursor: cursor.clone(),
                    },
                )
            }
            (Self::History(page), InventoryRequest::History { limit, cursor }) => {
                ensure!(
                    page.scope == scope && page.records.len() <= *limit,
                    "history page scope/bound conflict"
                );
                let mut after = history_position(cursor.as_deref(), scope)?;
                for record in &page.records {
                    identity(&record.operation)?;
                    identity(&record.incarnation)?;
                    ensure!(record.sequence > after, "history ordering/time conflict");
                    if let Some(id) = &record.retired_incarnation {
                        identity(id)?;
                        ensure!(
                            *id != record.incarnation && record.state.as_deref() == Some("live"),
                            "recreation identity conflict"
                        );
                    }
                    ensure!(
                        record.state.as_ref().is_none_or(|s| matches!(
                            s.as_str(),
                            "checking" | "verified" | "cleaning" | "deleted" | "live"
                        )),
                        "unknown retained source state"
                    );
                    ensure!(
                        record.discarded_rows.is_none() == record.discarded_bytes.is_none()
                            && record.discarded_rows.is_none_or(|n| n >= 0)
                            && record
                                .discarded_bytes
                                .is_none_or(|n| n >= record.discarded_rows.unwrap_or(0)),
                        "history loss accounting conflict"
                    );
                    match record.state.as_deref() {
                        None => ensure!(
                            record.retired_incarnation.is_none() && record.discarded_rows.is_none(),
                            "retry history contains lifecycle fields"
                        ),
                        Some("live") => ensure!(
                            record.retired_incarnation.is_some() && record.discarded_rows.is_none(),
                            "recreation history lacks retired identity"
                        ),
                        Some(_) => ensure!(
                            record.retired_incarnation.is_none() && record.discarded_rows.is_some(),
                            "destruction history lacks loss accounting"
                        ),
                    }
                    after = record.sequence;
                }
                if let Some(cursor) = &page.cursor {
                    ensure!(
                        !page.records.is_empty() && history_position(Some(cursor), scope)? == after,
                        "history continuation conflict"
                    );
                }
                Ok(())
            }
            _ => anyhow::bail!("inventory reply kind conflicts with request"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_preserves_precision_and_binds_scope_tail_and_lifecycle() -> Result<()> {
        let scope = "Order:source";
        let record = SourceHistory {
            sequence: i64::MAX,
            operation: "a".repeat(32),
            incarnation: "b".repeat(32),
            retired_incarnation: Some("c".repeat(32)),
            state: Some("live".into()),
            created_ms: i64::MIN,
            discarded_rows: None,
            discarded_bytes: None,
        };
        let cursor = history_cursor(scope, record.sequence)?;
        let reply = InventoryReply::History(HistoryPage {
            scope: scope.into(),
            records: vec![record],
            cursor: Some(cursor.clone()),
        });
        let request = InventoryRequest::History {
            limit: 1,
            cursor: None,
        };
        reply.validate(scope, &request)?;
        assert!(reply.validate("Order:other", &request).is_err());
        assert!(history_position(Some(&cursor), "Order:other").is_err());
        let raw = serde_json::to_value(&reply)?;
        assert_eq!(raw["data"]["records"][0]["sequence"], i64::MAX.to_string());
        assert_eq!(
            raw["data"]["records"][0]["created_ms"],
            i64::MIN.to_string()
        );
        let mut bad = raw.clone();
        let incarnation = bad["data"]["records"][0]["incarnation"].clone();
        bad["data"]["records"][0]["retired_incarnation"] = incarnation;
        assert!(serde_json::from_value::<InventoryReply>(bad)?
            .validate(scope, &request)
            .is_err());
        let mut bad = raw;
        bad["data"]["cursor"] = serde_json::json!(history_cursor(scope, i64::MAX - 1)?);
        assert!(serde_json::from_value::<InventoryReply>(bad)?
            .validate(scope, &request)
            .is_err());
        Ok(())
    }

    #[test]
    fn descriptor_scope_and_request_kind_cannot_be_substituted() -> Result<()> {
        let ns = CatalogNamespace {
            tenant: "tenant".into(),
            application: "app".into(),
        };
        let scope = ns.scope()?;
        let info = Descriptor::Catalog {
            scope: scope.clone(),
            namespace: Some(ns),
        };
        info.validate(&scope)?;
        assert!(info.validate("__EventCatalog:foreign").is_err());
        let reply = InventoryReply::Descriptor(info);
        assert!(reply
            .validate(
                &scope,
                &InventoryRequest::Resources {
                    limit: 100,
                    cursor: None
                }
            )
            .is_err());
        assert!(InventoryRequest::History {
            limit: 100,
            cursor: None
        }
        .validate(&scope)
        .is_err());
        assert!(InventoryRequest::History {
            limit: 101,
            cursor: None
        }
        .validate("Order:source")
        .is_err());
        assert!(InventoryRequest::Producers {
            limit: 1,
            cursor: None
        }
        .validate("Order:source")
        .is_err());
        Ok(())
    }
}
