// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exact-resource terminal catalog evidence for platform source destruction.

use super::{error::error, store, ErrorCode, Resource};
use crate::application::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) const METHOD: &str = "__catalogRetirementProof";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetirementProof {
    pub resource: Resource,
    pub state: String,
    pub operation: String,
    pub digest: String,
    #[serde(with = "super::decimal")]
    pub revision: i64,
}

impl RetirementProof {
    pub(crate) fn validate(&self, expected: &Resource) -> Result<()> {
        self.resource.validate()?;
        store::validate_id(&self.operation)?;
        if &self.resource != expected
            || self.state != "deleted"
            || self.revision <= 0
            || self.digest.len() != 64
            || !self
                .digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(error(
                ErrorCode::InvalidRequest,
                "invalid terminal retirement proof",
            ));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    resource: Resource,
}

pub(crate) fn inspect(context: &Context, body: &Value) -> Result<Value> {
    let caller = context.catalog_authority.as_ref().ok_or_else(|| {
        error(
            ErrorCode::Forbidden,
            "retirement evidence requires platform authority",
        )
    })?;
    let identity = context.identity().ok_or_else(|| {
        error(
            ErrorCode::Forbidden,
            "retirement evidence requires a catalog owner",
        )
    })?;
    caller.validate_target(&identity.scope, METHOD)?;
    let resource = caller.retired_resource().ok_or_else(|| {
        error(
            ErrorCode::Forbidden,
            "retirement evidence capability missing its resource",
        )
    })?;
    let request: Request = serde_json::from_value(body.clone())?;
    if &request.resource != resource {
        return Err(error(
            ErrorCode::Forbidden,
            "retirement proof request names another resource",
        ));
    }
    let db = context.storage()?;
    // Scalar primary-key lookup; no grant/configuration/payload bodies are read.
    let rows=db.system_sql("SELECT id,name,tenant,application,family,lifecycle,accepted_revision,desired_revision,operation,digest,(pending_retirement IS NOT NULL) AS pending_retirement,(pending_definition IS NOT NULL) AS pending_definition FROM __sys_event_catalog WHERE id=?",&[resource.id.clone().into()])?;
    let row = rows.first().ok_or_else(|| {
        error(
            ErrorCode::NotInitialized,
            "retirement destination has no retained catalog identity",
        )
    })?;
    let actual = Resource {
        id: store::text(row, "id")?,
        name: store::text(row, "name")?,
        tenant: store::text(row, "tenant")?,
        application: store::text(row, "application")?,
        family: store::text(row, "family")?,
    };
    if &actual != resource {
        return Err(error(
            ErrorCode::ConfigConflict,
            "retirement destination identity conflicts",
        ));
    }
    if store::text(row, "lifecycle")? != "deleted" {
        return Err(error(
            ErrorCode::PendingDelivery,
            "destination retirement is not terminal",
        ));
    }
    use crate::application::storage::SqlValue;
    if !matches!(row.get("desired_revision"), Some(SqlValue::Null))
        || store::number(row, "pending_retirement")? != 0
        || store::number(row, "pending_definition")? != 0
    {
        return Err(error(
            ErrorCode::UnsupportedProtocol,
            "terminal catalog identity retains unfinished configuration",
        ));
    }
    let operation = store::text(row, "operation")?;
    let digest = store::text(row, "digest")?;
    // Existence of a deleted row alone is insufficient: terminal closure must
    // retain its matching committed retirement receipt. Do not copy its body.
    let receipts=db.system_sql("SELECT resource,kind,digest,json_extract(result,'$.state') AS state,json_extract(result,'$.resource') AS result_resource,json_extract(result,'$.operation') AS result_operation FROM __sys_event_catalog_receipts WHERE operation=?",&[operation.clone().into()])?;
    let receipt = receipts.first().ok_or_else(|| {
        error(
            ErrorCode::UnsupportedProtocol,
            "terminal retirement receipt missing",
        )
    })?;
    if store::text(receipt, "resource")? != resource.id
        || store::text(receipt, "kind")? != "retire"
        || store::text(receipt, "digest")? != digest
        || store::text(receipt, "state")? != "deleted"
        || store::text(receipt, "result_resource")? != resource.id
        || store::text(receipt, "result_operation")? != operation
    {
        return Err(error(
            ErrorCode::UnsupportedProtocol,
            "terminal retirement receipt conflicts with catalog identity",
        ));
    }
    let proof = RetirementProof {
        resource: actual,
        state: "deleted".into(),
        operation,
        digest,
        revision: store::number(row, "accepted_revision")?,
    };
    proof.validate(resource)?;
    Ok(serde_json::to_value(proof)?)
}
