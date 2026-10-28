// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Host-minted RPC provenance. Event packets retain their existing wire shape.

use crate::application::event_stream;
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[doc(hidden)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RpcAuthority {
    State(StateAuthority),
    Inventory(InventoryAuthority),
    Event(event_stream::Authority),
    Source(SourceAuthority),
    Catalog(CatalogAuthority),
    Name(event_stream::name_resolution::NameAuthority),
}

#[doc(hidden)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InventoryAuthority {
    kind: InventoryKind,
    scope: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum InventoryKind {
    #[serde(rename = "event-inventory-v1")]
    Read,
}

impl InventoryAuthority {
    pub(crate) fn mint(scope: String) -> Result<Self> {
        let authority = Self {
            kind: InventoryKind::Read,
            scope,
        };
        authority.validate()?;
        Ok(authority)
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            cellar_logic::cell::valid_cell_scope(&self.scope) && self.scope.contains(':'),
            "invalid inventory authority scope"
        );
        Ok(())
    }

    pub(crate) fn validate_target(&self, scope: &str, method: &str) -> Result<()> {
        self.validate()?;
        anyhow::ensure!(
            scope == self.scope && method == super::event_inventory::METHOD,
            "inventory authority is scope/method bound"
        );
        Ok(())
    }
}

#[doc(hidden)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateAuthority {
    kind: StateKind,
    scope: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum StateKind {
    #[serde(rename = "event-state-v1")]
    Sample,
    #[serde(rename = "filesystem-state-v1")]
    Filesystem,
}

impl StateAuthority {
    pub(crate) fn mint(scope: String) -> Result<Self> {
        Self::new(scope, StateKind::Sample)
    }

    pub(crate) fn filesystem(scope: String) -> Result<Self> {
        Self::new(scope, StateKind::Filesystem)
    }

    fn new(scope: String, kind: StateKind) -> Result<Self> {
        let authority = Self { kind, scope };
        authority.validate()?;
        Ok(authority)
    }

    pub(crate) fn is_filesystem(&self) -> bool {
        matches!(self.kind, StateKind::Filesystem)
    }

    pub(crate) fn method(&self) -> &'static str {
        match self.kind {
            StateKind::Sample => super::event_state::METHOD,
            StateKind::Filesystem => super::filesystem_state::METHOD,
        }
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            cellar_logic::cell::valid_cell_scope(&self.scope) && self.scope.contains(':'),
            "invalid state sample scope"
        );
        Ok(())
    }

    pub(crate) fn validate_target(&self, scope: &str, method: &str) -> Result<()> {
        self.validate()?;
        anyhow::ensure!(
            scope == self.scope && method == self.method(),
            "state authority is scope/method bound"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum CatalogKind {
    #[serde(rename = "catalog-operator-v1")]
    Inventory,
    #[serde(rename = "catalog-retirement-proof-v1")]
    Retired { resource: event_stream::Resource },
}

/// Platform namespace inspection; an application resource grant cannot mint it.
#[doc(hidden)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogAuthority {
    kind: CatalogKind,
    namespace: event_stream::CatalogNamespace,
    operator: String,
}

impl CatalogAuthority {
    pub(crate) fn retired(resource: event_stream::Resource, operator: String) -> Result<Self> {
        let namespace = event_stream::CatalogNamespace {
            tenant: resource.tenant.clone(),
            application: resource.application.clone(),
        };
        let authority = Self {
            kind: CatalogKind::Retired { resource },
            namespace,
            operator,
        };
        authority.validate()?;
        Ok(authority)
    }

    pub(crate) fn mint(
        namespace: event_stream::CatalogNamespace,
        operator: String,
    ) -> Result<Self> {
        let authority = Self {
            kind: CatalogKind::Inventory,
            namespace,
            operator,
        };
        authority.validate()?;
        Ok(authority)
    }

    fn validate(&self) -> Result<()> {
        self.namespace.validate()?;
        if let CatalogKind::Retired { resource } = &self.kind {
            resource.validate()?;
            anyhow::ensure!(
                resource.tenant == self.namespace.tenant
                    && resource.application == self.namespace.application,
                "retirement proof authority names another namespace"
            );
        }
        anyhow::ensure!(
            !self.operator.is_empty() && self.operator.len() <= 256,
            "invalid catalog operator identity"
        );
        Ok(())
    }

    pub(crate) fn validate_target(&self, scope: &str, method: &str) -> Result<()> {
        self.validate()?;
        anyhow::ensure!(
            scope == self.namespace.scope()?
                && method
                    == match self.kind {
                        CatalogKind::Inventory => event_stream::catalog_inventory::METHOD,
                        CatalogKind::Retired { .. } => event_stream::retirement_proof::METHOD,
                    },
            "catalog authority does not match its RPC target"
        );
        Ok(())
    }

    pub(crate) fn namespace(&self) -> &event_stream::CatalogNamespace {
        &self.namespace
    }

    pub(crate) fn retired_resource(&self) -> Option<&event_stream::Resource> {
        match &self.kind {
            CatalogKind::Retired { resource } => Some(resource),
            CatalogKind::Inventory => None,
        }
    }
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
            Self::State(authority) => authority.validate(),
            Self::Inventory(authority) => authority.validate(),
            Self::Event(authority) => authority.validate(),
            Self::Source(authority) => authority.validate(),
            Self::Catalog(authority) => authority.validate(),
            Self::Name(authority) => authority.validate(),
        }
    }

    pub(crate) fn validate_target(&self, scope: &str, method: &str) -> Result<()> {
        self.validate()?;
        match self {
            Self::State(authority) => authority.validate_target(scope, method),
            Self::Inventory(authority) => authority.validate_target(scope, method),
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
            Self::Catalog(authority) => authority.validate_target(scope, method),
            Self::Name(authority) => authority.validate_target(scope, method),
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
                !event_stream::is_event_scope(scope)
                    && method != super::source_control::METHOD
                    && method != super::event_state::METHOD
                    && method != super::filesystem_state::METHOD
                    && method != super::event_inventory::METHOD,
                "framework RPC requires host authority"
            );
            Ok(())
        }
    }
}

impl From<event_stream::name_resolution::NameAuthority> for RpcAuthority {
    fn from(authority: event_stream::name_resolution::NameAuthority) -> Self {
        Self::Name(authority)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_capability_is_separate_from_metrics_and_mutation() -> Result<()> {
        let inventory = RpcAuthority::Inventory(InventoryAuthority::mint("Order:source".into())?);
        inventory.validate_target("Order:source", super::super::event_inventory::METHOD)?;
        assert!(inventory
            .validate_target("Order:other", super::super::event_inventory::METHOD)
            .is_err());
        assert!(inventory
            .validate_target("Order:source", super::super::event_state::METHOD)
            .is_err());
        assert!(inventory
            .validate_target("Order:source", super::super::source_control::METHOD)
            .is_err());
        assert!(
            validate_target(None, "Order:source", super::super::event_inventory::METHOD).is_err()
        );
        let state = RpcAuthority::State(StateAuthority::mint("Order:source".into())?);
        assert!(state
            .validate_target("Order:source", super::super::event_inventory::METHOD)
            .is_err());
        let decoded: RpcAuthority = serde_json::from_value(serde_json::to_value(inventory)?)?;
        decoded.validate_target("Order:source", super::super::event_inventory::METHOD)?;
        Ok(())
    }

    #[test]
    fn state_sample_capability_cannot_authorize_another_scope_or_mutation() -> Result<()> {
        let authority = RpcAuthority::State(StateAuthority::mint("Order:one".into())?);
        authority.validate_target("Order:one", super::super::event_state::METHOD)?;
        assert!(authority
            .validate_target("Order:two", super::super::event_state::METHOD)
            .is_err());
        assert!(authority
            .validate_target("Order:one", super::super::source_control::METHOD)
            .is_err());
        assert!(validate_target(None, "Order:one", super::super::event_state::METHOD).is_err());
        let round_trip: RpcAuthority = serde_json::from_value(serde_json::to_value(authority)?)?;
        round_trip.validate_target("Order:one", super::super::event_state::METHOD)?;
        assert!(StateAuthority::mint("Order:one/facets/one".into()).is_err());
        Ok(())
    }

    #[test]
    fn filesystem_sampling_is_scope_bound_and_separate_from_event_sampling() -> Result<()> {
        let authority = RpcAuthority::State(StateAuthority::filesystem("Account:one".into())?);
        authority.validate_target("Account:one", super::super::filesystem_state::METHOD)?;
        for (scope, method) in [
            ("Account:two", super::super::filesystem_state::METHOD),
            ("Account:one", super::super::event_state::METHOD),
            ("Account:one", super::super::source_control::METHOD),
        ] {
            assert!(authority.validate_target(scope, method).is_err());
        }
        let event = StateAuthority::mint("Account:one".into())?;
        assert!(event
            .validate_target("Account:one", super::super::filesystem_state::METHOD)
            .is_err());
        assert!(
            validate_target(None, "Account:one", super::super::filesystem_state::METHOD).is_err()
        );
        assert!(StateAuthority::filesystem("Account:one/facets/child".into()).is_err());
        let decoded: RpcAuthority = serde_json::from_value(serde_json::to_value(authority)?)?;
        decoded.validate_target("Account:one", super::super::filesystem_state::METHOD)?;
        Ok(())
    }

    #[test]
    fn retirement_proof_capability_is_resource_scoped_and_read_only() -> Result<()> {
        let resource = event_stream::Resource {
            tenant: "tenant".into(),
            application: "app".into(),
            id: "00000000000000000000000000000001".into(),
            name: "events".into(),
            family: "events".into(),
        };
        let authority = RpcAuthority::Catalog(CatalogAuthority::retired(
            resource.clone(),
            "platform".into(),
        )?);
        let scope = event_stream::catalog_scope(&resource)?;
        authority.validate_target(&scope, event_stream::retirement_proof::METHOD)?;
        for method in [
            event_stream::catalog_inventory::METHOD,
            "__catalogConfigure",
            "__catalogRetire",
            "__streamAppend",
            "__streamRead",
        ] {
            assert!(authority.validate_target(&scope, method).is_err());
        }
        let decoded: RpcAuthority = serde_json::from_value(serde_json::to_value(&authority)?)?;
        decoded.validate_target(&scope, event_stream::retirement_proof::METHOD)?;
        let mut foreign = resource;
        foreign.application = "other".into();
        assert!(authority
            .validate_target(
                &event_stream::catalog_scope(&foreign)?,
                event_stream::retirement_proof::METHOD
            )
            .is_err());
        Ok(())
    }

    #[test]
    fn catalog_authority_is_namespace_and_method_bound_without_resource_promotion() -> Result<()> {
        let namespace = event_stream::CatalogNamespace {
            tenant: "tenant".into(),
            application: "app".into(),
        };
        let scope = namespace.scope()?;
        let catalog = RpcAuthority::Catalog(CatalogAuthority::mint(
            namespace.clone(),
            "platform".into(),
        )?);
        catalog.validate_target(&scope, event_stream::catalog_inventory::METHOD)?;
        for method in [
            "__catalogConfigure",
            "__catalogRetire",
            "__catalogStatus",
            "__streamRead",
        ] {
            assert!(catalog.validate_target(&scope, method).is_err());
        }
        let foreign = event_stream::CatalogNamespace {
            application: "other".into(),
            ..namespace
        };
        assert!(catalog
            .validate_target(&foreign.scope()?, event_stream::catalog_inventory::METHOD)
            .is_err());
        assert!(validate_target(None, &scope, event_stream::catalog_inventory::METHOD).is_err());
        let binding: event_stream::Binding = serde_json::from_value(serde_json::json!({
            "environment":"EVENTS","resource":{"tenant":"tenant","application":"app","id":"00000000000000000000000000000001","name":"events","family":"events"},
            "principal":"app","grant":"management","administer":true
        }))?;
        let resource = RpcAuthority::Event(event_stream::Authority::operator(
            &binding,
            event_stream::authority::Action::Administer,
            "resource".into(),
        )?);
        assert!(resource
            .validate_target(&scope, event_stream::catalog_inventory::METHOD)
            .is_err());
        let encoded = serde_json::to_value(catalog)?;
        let decoded: RpcAuthority = serde_json::from_value(encoded.clone())?;
        decoded.validate_target(&scope, event_stream::catalog_inventory::METHOD)?;
        let mut forged = encoded;
        forged["grant"] = serde_json::json!("application");
        assert!(serde_json::from_value::<RpcAuthority>(forged).is_err());
        Ok(())
    }

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
