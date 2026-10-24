// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Name lookup reads an existing catalog grant; it cannot provision resources.

use super::{
    authority::Authority, error::error, store, BindingConfig, CatalogNamespace, Definition,
    ErrorCode,
};
use crate::application::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub(crate) const METHOD: &str = "__catalogResolveName";
pub(crate) const MAX_REQUEST_BYTES: usize = 128 << 10;

#[doc(hidden)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NameAuthority {
    kind: String,
    resource: super::ResourceConfig,
    environment: String,
    principal: String,
    grant: String,
}

impl NameAuthority {
    pub(crate) fn new(declaration: &BindingConfig) -> Result<Self> {
        declaration.validate()?;
        let result = Self {
            kind: "event-name-resolution-v1".into(),
            resource: declaration.resource.clone(),
            environment: declaration.environment.clone(),
            principal: declaration.principal.clone(),
            grant: declaration.grant.clone(),
        };
        result.validate()?;
        Ok(result)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.resource.validate()?;
        for value in [&self.environment, &self.principal, &self.grant] {
            anyhow::ensure!(
                !value.is_empty() && value.len() <= 256,
                "invalid named binding authority"
            );
        }
        anyhow::ensure!(
            self.kind == "event-name-resolution-v1" && self.resource.id.is_none(),
            "invalid name-resolution authority"
        );
        Ok(())
    }

    pub(crate) fn namespace(&self) -> CatalogNamespace {
        CatalogNamespace {
            tenant: self.resource.tenant.clone(),
            application: self.resource.application.clone(),
        }
    }

    pub(crate) fn validate_target(&self, scope: &str, method: &str) -> Result<()> {
        self.validate()?;
        anyhow::ensure!(
            method == METHOD && scope == self.namespace().scope()?,
            "name-resolution authority does not match its target"
        );
        Ok(())
    }
}

pub(crate) fn resolve(context: &Context, body: &Value) -> Result<Value> {
    let Some(caller) = context.name_authority.as_ref() else {
        return Err(error(
            ErrorCode::Forbidden,
            "catalog name lookup requires captured declaration authority",
        ));
    };
    caller.validate_target(
        &context
            .identity()
            .ok_or_else(|| error(ErrorCode::Forbidden, "catalog identity missing"))?
            .scope,
        METHOD,
    )?;
    use base64::Engine;
    let encoded = body["declaration"]
        .as_str()
        .ok_or_else(|| error(ErrorCode::InvalidRequest, "name declaration missing"))?;
    anyhow::ensure!(
        encoded.len() <= (64usize << 10).div_ceil(3) * 4,
        "name declaration exceeds limit"
    );
    let bytes = base64::engine::general_purpose::STANDARD.decode(encoded)?;
    anyhow::ensure!(bytes.len() <= 64 << 10, "name declaration exceeds limit");
    let declaration: BindingConfig = serde_json::from_slice(&bytes)?;
    declaration.validate()?;
    if declaration.resource != caller.resource
        || declaration.environment != caller.environment
        || declaration.principal != caller.principal
        || declaration.grant != caller.grant
    {
        return Err(error(
            ErrorCode::Forbidden,
            "name declaration differs from captured host authority",
        ));
    }
    let db = context.storage()?;
    if db.system_sql("SELECT 1 AS present FROM sqlite_schema WHERE type='table' AND name='__sys_event_catalog'", &[])?.is_empty() {
        return Err(error(ErrorCode::NotInitialized, "catalog name is not initialized"));
    }
    let row = db.system_sql("SELECT lifecycle,definition FROM __sys_event_catalog WHERE name=? AND lifecycle!='deleted' LIMIT 1",
        &[declaration.resource.name.clone().into()])?.into_iter().next()
        .ok_or_else(||error(ErrorCode::NotInitialized, "catalog name is absent or deleted"))?;
    if store::text(&row, "lifecycle")? != "active" {
        return Err(error(
            ErrorCode::Unavailable,
            "catalog name is not active for binding adoption",
        ));
    }
    let definition: Definition = serde_json::from_str(&store::text(&row, "definition")?)?;
    let binding = declaration.resolve(definition.resource.clone())?;
    let authority = Authority::resolution(&binding)?;
    let grant = store::authorize(
        &definition,
        &authority,
        super::authority::Action::Resolve,
        None,
    )?;
    if (binding.publish && !grant.publish)
        || (binding.read && !grant.read)
        || (binding.administer && !grant.administer)
        || binding
            .subscriptions
            .iter()
            .any(|name| !grant.subscriptions.contains(name))
    {
        return Err(error(
            ErrorCode::Forbidden,
            "named binding exceeds current resource grants",
        ));
    }
    Ok(
        json!({"state":"active","resource":definition.resource,"revision":definition.revision.to_string()}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn declaration() -> Result<BindingConfig> {
        Ok(serde_json::from_value(
            json!({"environment":"EVENTS","resource":{"tenant":"tenant","application":"app","name":"events","family":"changed"},
            "principal":"consumer","grant":"read-events","read":true,"subscriptions":["projection"]}),
        )?)
    }

    #[test]
    fn name_authority_is_read_only_namespace_bound_and_header_bounded() -> Result<()> {
        let mut declaration = declaration()?;
        declaration.subscriptions = (0..128)
            .map(|index| format!("{index:03}{}", "x".repeat(125)))
            .collect();
        let authority = NameAuthority::new(&declaration)?;
        let framed = crate::runtime::RpcAuthority::from(authority.clone());
        let encoded = serde_json::to_vec(&framed)?;
        assert!(encoded.len().div_ceil(3) * 4 < 16384);
        assert!(!String::from_utf8(encoded.clone())?.contains(&declaration.subscriptions[0]));
        let scope = authority.namespace().scope()?;
        framed.validate_target(&scope, METHOD)?;
        for method in [
            "__catalogConfigure",
            "__catalogRetire",
            "__catalogList",
            "__streamAppend",
            "__catalogResolve",
        ] {
            assert!(framed.validate_target(&scope, method).is_err());
        }
        assert!(framed.validate_target("Order:source", METHOD).is_err());
        assert!(crate::runtime::rpc_authority::validate_target(None, &scope, METHOD).is_err());
        let decoded: crate::runtime::RpcAuthority = serde_json::from_slice(&encoded)?;
        decoded.validate_target(&scope, METHOD)?;
        let mut invalid = serde_json::to_value(authority)?;
        invalid["kind"] = json!("unrecognized-name-authority");
        assert!(serde_json::from_value::<NameAuthority>(invalid)?
            .validate()
            .is_err());
        declaration.resource.id = Some("00000000000000000000000000000001".into());
        assert!(NameAuthority::new(&declaration).is_err());
        Ok(())
    }

    #[test]
    fn activated_generation_keeps_its_original_uuid() -> Result<()> {
        let declaration = declaration()?;
        let resource = super::super::Resource {
            tenant: "tenant".into(),
            application: "app".into(),
            id: "00000000000000000000000000000001".into(),
            name: "events".into(),
            family: "changed".into(),
        };
        let first = declaration.resolve(resource.clone())?;
        let readiness = super::super::readiness::Readiness::new(false);
        assert!(readiness.binding(&declaration).is_err());
        readiness.pin(vec![first.clone()])?;
        readiness.finish(Ok(()));
        assert_eq!(readiness.binding(&declaration)?, first);
        let mut replacement = resource;
        replacement.id = "00000000000000000000000000000002".into();
        assert!(readiness
            .pin(vec![declaration.resolve(replacement)?])
            .is_err());
        assert_eq!(readiness.binding(&declaration)?, first);
        Ok(())
    }
}
