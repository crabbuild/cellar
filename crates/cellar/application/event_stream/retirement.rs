// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{
    authority::{Action, Authority},
    error::error,
    ErrorCode, Stream,
};
use crate::application::{Context, Result};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum RetirementMode {
    /// The named external procedure must quiesce every producer, including
    /// sources that have never contacted this stream. This is an attestation,
    /// not a stream-local proof of global drainage.
    Coordinated { procedure: String },
    /// Explicit acknowledgment that retained history will be destroyed and
    /// pending producer obligations may become permanently blocked.
    Force { data_loss_acknowledgment: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Retirement {
    pub operation: String,
    #[serde(with = "super::decimal")]
    pub revision: i64,
    pub reason: String,
    pub mode: RetirementMode,
}

impl Retirement {
    pub(crate) fn force(&self) -> bool {
        matches!(self.mode, RetirementMode::Force { .. })
    }

    pub(crate) fn validate(&self) -> Result<()> {
        super::store::validate_id(&self.operation)?;
        let attestation = match &self.mode {
            RetirementMode::Coordinated { procedure } => procedure,
            RetirementMode::Force {
                data_loss_acknowledgment,
            } => data_loss_acknowledgment,
        };
        if self.revision <= 0
            || self.reason.trim().is_empty()
            || self.reason.len() > 512
            || attestation.trim().is_empty()
            || attestation.len() > 512
        {
            return Err(error(ErrorCode::InvalidRequest,"retirement needs a revision, bounded reason, and explicit coordination or data-loss acknowledgment"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetirementStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affected: Option<super::OffsetRange>,
    pub resource: String,
    pub operation: String,
    pub state: String,
    pub last_error: Option<String>,
}

impl RetirementStatus {
    pub(crate) fn validate(
        &self,
        resource: &super::Resource,
        operation: &str,
        operator: Option<&str>,
    ) -> Result<()> {
        if self.resource != resource.id
            || self.operation != operation
            || !matches!(self.state.as_str(), "retiring" | "deleted")
            || self.operator.as_deref() != operator
            || self
                .operator
                .as_ref()
                .is_some_and(|s| s.is_empty() || s.len() > 256)
            || self
                .last_error
                .as_ref()
                .is_some_and(|s| s.len() > 4096 || s.chars().count() > 1024)
            || self
                .affected
                .as_ref()
                .is_some_and(|r| r.first <= 0 || r.last < r.first || r.last == i64::MAX)
            || (self.state == "deleted" && self.last_error.is_some())
        {
            return Err(error(
                ErrorCode::ConfigConflict,
                "invalid retirement acceptance",
            ));
        }
        Ok(())
    }
}

fn accept_status(
    resource: &super::Resource,
    operation: &str,
    value: serde_json::Value,
) -> Result<RetirementStatus> {
    if value["state"].as_str() == Some("deleted")
        || value.get("records").is_some()
        || value.get("bytes").is_some()
        || value.get("affected").is_some()
        || value.get("operator").is_some()
    {
        super::retirement_store::acceptance(&value, None)?;
    }
    let status: RetirementStatus = serde_json::from_value(value)
        .map_err(|_| error(ErrorCode::InvalidRequest, "invalid retirement response"))?;
    status.validate(resource, operation, None)?;
    Ok(status)
}

impl<E> Stream<E> {
    /// Persist retirement intent at the catalog. `retiring` acknowledges that
    /// obligation; `deleted` confirms logical closure and bounded cleanup.
    pub async fn retire(
        &self,
        context: &Context,
        request: &Retirement,
    ) -> Result<RetirementStatus> {
        self.check_domain()?;
        request.validate()?;
        let authority = Authority::mint(context, &self.binding, Action::Administer)?;
        let encoded =
            base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(request)?);
        let value = super::call_authorized(
            context,
            authority,
            super::catalog_scope(self.resource())?,
            Some(super::catalog_name(self.resource())?),
            "__catalogRetire",
            json!([{"request":encoded}]),
        )
        .await?;
        super::error::submitted_result(accept_status(
            self.resource(),
            request.operation.as_str(),
            value,
        ))
    }

    pub async fn retirement_status(
        &self,
        context: &Context,
        operation: &str,
    ) -> Result<RetirementStatus> {
        self.check_domain()?;
        super::store::validate_id(operation)?;
        let authority = Authority::mint(context, &self.binding, Action::Administer)?;
        let value = super::call_authorized(
            context,
            authority,
            super::catalog_scope(self.resource())?,
            Some(super::catalog_name(self.resource())?),
            "__catalogRetirementStatus",
            json!([{"operation":operation}]),
        )
        .await?;
        accept_status(self.resource(), operation, value)
    }
}

#[cfg(test)]
#[path = "retirement_acceptance_tests.rs"]
mod acceptance_tests;
