// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Platform inventory executes inside the original activation and proof gates.
use crate::application::event_stream::{
    CatalogListOptions, CatalogNamespace, DeliveryError, ErrorCode, Resource, SourceListOptions,
};
use crate::application::storage::{SqlRow, SqlValue};
use crate::application::{Context, Invocation, Output, Storage};
use crate::event_inventory::{
    history_cursor, history_position, Descriptor, HistoryPage, InventoryReply, InventoryRequest,
    SourceHistory,
};
use anyhow::{ensure, Context as _, Result};

pub(crate) const METHOD: &str = "__eventInventory";
pub(crate) const MAX_REQUEST_BYTES: usize = 4096;
pub(crate) const MAX_RPC_BYTES: usize = 32 << 10;
pub(crate) const MAX_REPLY_BYTES: usize = 256 << 10;

fn text(row: &SqlRow, key: &str) -> Result<String> {
    match row.get(key) {
        Some(SqlValue::Text(s)) => Ok(s.clone()),
        _ => anyhow::bail!("invalid inventory text column"),
    }
}

fn optional_text(row: &SqlRow, key: &str) -> Result<Option<String>> {
    match row.get(key) {
        Some(SqlValue::Null) => Ok(None),
        _ => text(row, key).map(Some),
    }
}

fn number(row: &SqlRow, key: &str) -> Result<i64> {
    match row.get(key) {
        Some(SqlValue::Integer(n)) => Ok(*n),
        _ => anyhow::bail!("invalid inventory integer column"),
    }
}

fn optional_decimal(row: &SqlRow, key: &str) -> Result<Option<i64>> {
    optional_text(row, key)?
        .map(|s| {
            let n = s.parse::<i64>()?;
            ensure!(n.to_string() == s, "noncanonical retained receipt integer");
            Ok(n)
        })
        .transpose()
}

fn present(db: &Storage, table: &str) -> Result<bool> {
    Ok(!db
        .system_sql(
            "SELECT 1 AS present FROM sqlite_schema WHERE type='table' AND name=?",
            &[table.into()],
        )?
        .is_empty())
}

fn describe(context: &Context, scope: &str) -> Result<Descriptor> {
    let db = context.storage()?;
    let descriptor = if scope.starts_with(&format!("{}:", crate::deploy::EVENT_CATALOG_CLASS)) {
        let namespace = if present(&db, "__sys_event_catalog")? {
            db.system_sql(
                "SELECT tenant,application FROM __sys_event_catalog ORDER BY id LIMIT 1",
                &[],
            )?
            .first()
            .map(|row| {
                Ok::<_, anyhow::Error>(CatalogNamespace {
                    tenant: text(row, "tenant")?,
                    application: text(row, "application")?,
                })
            })
            .transpose()?
        } else {
            None
        };
        Descriptor::Catalog {
            scope: scope.into(),
            namespace,
        }
    } else if scope.starts_with(&format!("{}:", crate::deploy::EVENT_STREAM_CLASS)) {
        let resource = if present(&db, "__sys_stream")? {
            db.system_sql("SELECT json_extract(definition,'$.resource') AS resource FROM __sys_stream WHERE singleton=1",&[])?.first()
                .map(|row| {let encoded=text(row,"resource")?;ensure!(encoded.len()<=4096,"stream identity exceeds bounds");Ok::<_,anyhow::Error>(serde_json::from_str::<Resource>(&encoded)?)}).transpose()?
        } else {
            None
        };
        Descriptor::Stream {
            scope: scope.into(),
            resource,
        }
    } else {
        Descriptor::Source {
            scope: scope.into(),
            incarnation: db.outbox_incarnation()?,
            history: present(&db, "__sys_outbox_operations")?,
        }
    };
    descriptor.validate(scope)?;
    Ok(descriptor)
}

fn history(db: &Storage, scope: &str, limit: usize, cursor: Option<&str>) -> Result<HistoryPage> {
    let after = history_position(cursor, scope)?;
    if !present(db, "__sys_outbox_operations")? {
        ensure!(after == 0, "history cursor names absent source receipts");
        return Ok(HistoryPage {
            scope: scope.into(),
            records: Vec::new(),
            cursor: None,
        });
    }
    // All operation kinds advance the range, including retries. Filtering
    // lifecycle records first would scan arbitrarily many retained retry rows.
    let rows=db.system_sql("SELECT sequence,operation,created_ms,coalesce(json_extract(receipt,'$.source'),json_extract(receipt,'$.expected.source')) AS source,coalesce(json_extract(receipt,'$.incarnation'),json_extract(receipt,'$.expected.incarnation')) AS incarnation,json_extract(receipt,'$.retired_incarnation') AS retired_incarnation,json_extract(receipt,'$.state') AS state,json_extract(receipt,'$.discarded_rows') AS discarded_rows,json_extract(receipt,'$.discarded_bytes') AS discarded_bytes FROM __sys_outbox_operations WHERE sequence>? ORDER BY sequence LIMIT ?",&[after.into(),i64::try_from(limit+1)?.into()])?;
    let more = rows.len() > limit;
    let mut records = Vec::new();
    for row in rows.into_iter().take(limit) {
        ensure!(
            text(&row, "source")? == scope,
            "retained history belongs to another source"
        );
        records.push(SourceHistory {
            sequence: number(&row, "sequence")?,
            operation: text(&row, "operation")?,
            incarnation: text(&row, "incarnation")?,
            retired_incarnation: optional_text(&row, "retired_incarnation")?,
            state: optional_text(&row, "state")?,
            created_ms: number(&row, "created_ms")?,
            discarded_rows: optional_decimal(&row, "discarded_rows")?,
            discarded_bytes: optional_decimal(&row, "discarded_bytes")?,
        });
    }
    let cursor = if more {
        Some(history_cursor(
            scope,
            records
                .last()
                .context("empty history continuation")?
                .sequence,
        )?)
    } else {
        None
    };
    Ok(HistoryPage {
        scope: scope.into(),
        records,
        cursor,
    })
}

fn run(context: &Context, scope: &str, request: &InventoryRequest) -> Result<InventoryReply> {
    request.validate(scope)?;
    let descriptor = describe(context, scope)?;
    let reply = match request {
        InventoryRequest::Describe => InventoryReply::Descriptor(descriptor),
        InventoryRequest::Resources { limit, cursor } => {
            let Descriptor::Catalog {
                namespace: Some(namespace),
                ..
            } = descriptor
            else {
                anyhow::bail!("catalog has no retained namespace identity")
            };
            InventoryReply::Resources(crate::application::event_stream::catalog_inventory::page(
                context,
                &namespace,
                &CatalogListOptions {
                    max_records: *limit,
                    cursor: cursor.clone(),
                },
            )?)
        }
        InventoryRequest::Producers { limit, cursor } => {
            let Descriptor::Stream {
                resource: Some(resource),
                ..
            } = descriptor
            else {
                anyhow::bail!("stream has no retained resource identity")
            };
            let page = crate::application::event_stream::source_inventory::page(
                context,
                &resource,
                &SourceListOptions {
                    max_records: *limit,
                    cursor: cursor.clone(),
                },
            )?;
            let db = context.storage()?;
            let state =
                db.system_sql("SELECT lifecycle FROM __sys_stream WHERE singleton=1", &[])?;
            let retired = text(
                state.first().context("stream lifecycle missing")?,
                "lifecycle",
            )? == "deleted";
            let cleanup_complete = if retired {
                let rows = db.system_sql(
                    "SELECT finished_ms IS NOT NULL AS cleaned FROM __sys_stream_retirement WHERE singleton=1",
                    &[],
                )?;
                number(
                    rows.first().context("retirement receipt missing")?,
                    "cleaned",
                )? != 0
            } else {
                false
            };
            InventoryReply::Producers {
                resource,
                retired,
                cleanup_complete,
                page,
            }
        }
        InventoryRequest::History { limit, cursor } => InventoryReply::History(history(
            &context.storage()?,
            scope,
            *limit,
            cursor.as_deref(),
        )?),
    };
    reply.validate(scope, request)?;
    Ok(reply)
}

pub(crate) async fn invoke(
    context: Context,
    invocation: Invocation,
    authority: super::rpc_authority::InventoryAuthority,
) -> Result<Output> {
    context.check_turn()?;
    let Invocation::CellRpc { method, args } = invocation else {
        anyhow::bail!("inventory requires RPC")
    };
    let scope = context
        .identity()
        .context("inventory has no owning cell")?
        .scope
        .clone();
    authority.validate_target(&scope, &method)?;
    let bytes = match &args {
        crate::engine_api::RpcData::Json(s) => s.len(),
        crate::engine_api::RpcData::Structured(b) => b.len(),
    };
    ensure!(
        bytes <= MAX_RPC_BYTES,
        "inventory RPC arguments exceed bounds"
    );
    let request: InventoryRequest =
        serde_json::from_value(crate::application::decode_rpc(&args)?.to_json()?)?;
    let value = match run(&context, &scope, &request) {
        Ok(reply) => serde_json::json!({"result":reply}),
        Err(error) => {
            let error = error
                .downcast_ref::<DeliveryError>()
                .cloned()
                .unwrap_or_else(|| {
                    DeliveryError::new(ErrorCode::Unavailable, "owner inventory unavailable")
                });
            serde_json::json!({"error":error})
        }
    };
    crate::application::event_stream::encoding::json(&value, MAX_REPLY_BYTES)?;
    Ok(Output::Rpc(crate::application::encode_rpc(
        &cellar_native::clone::StructuredValue::from_json(value)?,
    )?))
}
