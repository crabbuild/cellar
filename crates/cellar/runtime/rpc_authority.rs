//! Host-minted RPC provenance. Event packets retain their existing wire shape.

use crate::application::event_stream;
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[doc(hidden)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RpcAuthority {
    Event(event_stream::Authority),
    Source(SourceAuthority),
}

impl From<event_stream::Authority> for RpcAuthority {
    fn from(authority: event_stream::Authority) -> Self {
        Self::Event(authority)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum SourceKind {
    #[serde(rename = "outbox-operator-v1")]
    Outbox,
}

#[doc(hidden)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceAuthority {
    kind: SourceKind,
    scope: String,
    operator: String,
}

impl SourceAuthority {
    pub(crate) fn mint(scope: String, operator: String) -> Result<Self> {
        let authority = Self {
            kind: SourceKind::Outbox,
            scope,
            operator,
        };
        authority.validate()?;
        Ok(authority)
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            cellar_logic::cell::valid_cell_scope(&self.scope)
                && !event_stream::is_event_scope(&self.scope),
            "invalid source operator target"
        );
        anyhow::ensure!(
            !self.operator.is_empty() && self.operator.len() <= 256,
            "invalid source operator identity"
        );
        Ok(())
    }

    pub(crate) fn operator(&self) -> &str {
        &self.operator
    }
}

impl RpcAuthority {
    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Self::Event(authority) => authority.validate(),
            Self::Source(authority) => authority.validate(),
        }
    }

    pub(crate) fn validate_target(&self, scope: &str, method: &str) -> Result<()> {
        self.validate()?;
        match self {
            Self::Event(authority) => {
                event_stream::validate_target(scope, authority)?;
                authority.validate_operator_target(scope, method)
            }
            Self::Source(authority) => {
                anyhow::ensure!(
                    authority.scope == scope && method == super::source_control::METHOD,
                    "source operator authority does not match its RPC target"
                );
                Ok(())
            }
        }
    }
}

pub(crate) fn validate_target(
    authority: Option<&RpcAuthority>,
    scope: &str,
    method: &str,
) -> Result<()> {
    match authority {
        Some(authority) => authority.validate_target(scope, method),
        None => {
            anyhow::ensure!(
                !event_stream::is_event_scope(scope) && method != super::source_control::METHOD,
                "framework RPC requires host authority"
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_authority_is_method_and_scope_bound_and_preserves_event_wire_shape() -> Result<()> {
        let source = RpcAuthority::Source(SourceAuthority::mint(
            "Order:source".into(),
            "outbox".into(),
        )?);
        assert!(source
            .validate_target("Order:source", super::super::source_control::METHOD)
            .is_ok());
        assert!(source
            .validate_target("Order:other", super::super::source_control::METHOD)
            .is_err());
        assert!(source.validate_target("Order:source", "emit").is_err());
        assert!(
            validate_target(None, "Order:source", super::super::source_control::METHOD).is_err()
        );
        assert!(validate_target(None, "Order:source", "businessMethod").is_ok());
        let binding = event_stream::Binding {
            environment: "EVENTS".into(),
            resource: event_stream::Resource {
                tenant: "tenant".into(),
                application: "app".into(),
                id: "00000000000000000000000000000001".into(),
                name: "events".into(),
                family: "events".into(),
            },
            principal: "app".into(),
            grant: "reader".into(),
            publish: false,
            read: true,
            subscriptions: Vec::new(),
            administer: false,
        };
        let authority = event_stream::Authority::resolution(&binding)?;
        let original = serde_json::to_value(&authority)?;
        assert!(original.get("operator").is_none());
        let framed = RpcAuthority::from(authority);
        assert_eq!(serde_json::to_value(&framed)?, original);
        let decoded: RpcAuthority = serde_json::from_value(original)?;
        assert!(matches!(decoded, RpcAuthority::Event(_)));
        let decoded: RpcAuthority = serde_json::from_value(serde_json::to_value(source)?)?;
        assert!(matches!(decoded, RpcAuthority::Source(_)));
        Ok(())
    }

    #[test]
    fn event_operator_authority_is_read_or_admin_and_stream_method_bound() -> Result<()> {
        use event_stream::authority::Action;
        let binding: event_stream::Binding = serde_json::from_value(serde_json::json!({
            "environment":"EVENTS","resource":{"tenant":"tenant","application":"app","id":"00000000000000000000000000000001","name":"events","family":"events"},
            "principal":"app","grant":"management","publish":true,"read":true,"administer":true
        }))?;
        assert!(
            event_stream::Authority::operator(&binding, Action::Append, "operator".into()).is_err()
        );
        let scope = event_stream::cell_scope(&binding.resource)?;
        let admin = RpcAuthority::from(event_stream::Authority::operator(
            &binding,
            Action::Administer,
            "operator".into(),
        )?);
        assert!(admin
            .validate_target(&scope, "__streamSubscriptionChange")
            .is_ok());
        assert!(admin.validate_target(&scope, "__streamAppend").is_err());
        assert!(admin
            .validate_target(
                &event_stream::catalog_scope(&binding.resource)?,
                "__catalogConfigure"
            )
            .is_err());
        assert!(admin
            .validate_target(
                &event_stream::catalog_scope(&binding.resource)?,
                "__catalogRetire"
            )
            .is_ok());
        assert!(admin.validate_target(&scope, "__catalogRetire").is_err());
        let read = RpcAuthority::from(event_stream::Authority::operator(
            &binding,
            Action::Read,
            "operator".into(),
        )?);
        assert!(read.validate_target(&scope, "__streamRead").is_ok());
        assert!(read
            .validate_target(&scope, "__streamSubscriptionChange")
            .is_err());
        assert!(validate_target(None, &scope, "__streamRead").is_err());
        Ok(())
    }
}
