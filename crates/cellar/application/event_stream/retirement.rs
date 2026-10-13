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
        let status: RetirementStatus = serde_json::from_value(value)?;
        if status.resource != self.resource().id || status.operation != request.operation {
            return Err(error(
                ErrorCode::ConfigConflict,
                "retirement acceptance identity mismatch",
            ));
        }
        Ok(status)
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
        let status: RetirementStatus = serde_json::from_value(value)?;
        if status.resource != self.resource().id || status.operation != operation {
            return Err(error(
                ErrorCode::ConfigConflict,
                "retirement status identity mismatch",
            ));
        }
        Ok(status)
    }
}
