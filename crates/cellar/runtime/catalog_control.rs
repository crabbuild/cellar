// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Read-only platform namespace inventory; no resource grant is promoted here.

use crate::application::event_stream::{
    CatalogListOptions, CatalogNamespace, CatalogPage, Resource, RetirementProof,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub(crate) const MAX_REQUEST_BYTES: usize = 16 << 10;
pub(crate) const MAX_RPC_BYTES: usize = 64 << 10;
pub(crate) const MAX_REPLY_BYTES: usize = 1 << 20;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    pub namespace: CatalogNamespace,
    pub cursor: Option<String>,
    pub limit: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retired_resource: Option<Resource>,
}

impl Request {
    fn options(&self) -> CatalogListOptions {
        CatalogListOptions {
            cursor: self.cursor.clone(),
            max_records: self.limit,
        }
    }

    pub(crate) fn scope(&self) -> Result<String> {
        self.namespace.scope()
    }

    pub(crate) fn name(&self) -> Result<String> {
        self.namespace.name()
    }

    pub(crate) fn prepare(
        &self,
        operator: String,
    ) -> Result<(super::RpcAuthority, &'static str, Value)> {
        if let Some(resource) = &self.retired_resource {
            resource.validate()?;
            anyhow::ensure!(
                resource.tenant == self.namespace.tenant
                    && resource.application == self.namespace.application
                    && self.cursor.is_none()
                    && self.limit == 1,
                "invalid retirement proof target/options"
            );
            return Ok((
                super::RpcAuthority::Catalog(super::rpc_authority::CatalogAuthority::retired(
                    resource.clone(),
                    operator,
                )?),
                crate::application::event_stream::retirement_proof::METHOD,
                json!([{"resource":resource}]),
            ));
        }
        self.options().validate(&self.namespace)?;
        let authority =
            super::rpc_authority::CatalogAuthority::mint(self.namespace.clone(), operator)?;
        Ok((
            super::RpcAuthority::Catalog(authority),
            crate::application::event_stream::catalog_inventory::METHOD,
            json!([{"cursor":self.cursor,"max_records":self.limit}]),
        ))
    }

    pub(crate) fn project(&self, mut reply: Value) -> Result<Value> {
        anyhow::ensure!(
            serde_json::to_vec(&reply)?.len() <= MAX_REPLY_BYTES,
            "catalog operator reply exceeds limit"
        );
        if let Some(page) = reply.get("ok") {
            if let Some(resource) = &self.retired_resource {
                let proof: RetirementProof = serde_json::from_value(page.clone())?;
                proof.validate(resource)?;
            } else {
                let page: CatalogPage = serde_json::from_value(page.clone())?;
                page.validate(&self.namespace, &self.options())?;
            }
        }
        let object = reply
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("invalid catalog reply envelope"))?;
        anyhow::ensure!(
            object.len() == 1 && (object.contains_key("ok") || object.contains_key("error")),
            "invalid catalog reply envelope"
        );
        if let Some(value) = object.remove("ok") {
            object.insert("result".into(), value);
        }
        Ok(reply)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retirement_proof_decoder_and_projection_require_exact_terminal_identity() -> Result<()> {
        let resource: Resource = serde_json::from_value(
            json!({"tenant":"tenant","application":"app","name":"events","family":"events","id":"00000000000000000000000000000001"}),
        )?;
        let request = Request {
            namespace: CatalogNamespace {
                tenant: resource.tenant.clone(),
                application: resource.application.clone(),
            },
            cursor: None,
            limit: 1,
            retired_resource: Some(resource.clone()),
        };
        let (authority, method, args) = request.prepare("platform".into())?;
        authority.validate_target(&request.scope()?, method)?;
        let args = crate::application::encode_rpc(
            &cellar_native::clone::StructuredValue::from_json(args)?,
        )?;
        let body = crate::application::event_stream::broker::body(&args)?;
        assert_eq!(body["resource"], json!(resource));
        let proof = json!({"resource":resource,"state":"deleted","operation":"00000000000000000000000000000002","digest":"a".repeat(64),"revision":i64::MAX.to_string()});
        assert_eq!(
            request.project(json!({"ok":proof.clone()}))?["result"],
            proof
        );
        for (pointer, value) in [
            ("/state", json!("retiring")),
            ("/resource/id", json!("00000000000000000000000000000003")),
            ("/revision", json!("01")),
            ("/digest", json!("bad")),
        ] {
            let mut forged = proof.clone();
            *forged.pointer_mut(pointer).unwrap() = value;
            assert!(request.project(json!({"ok":forged})).is_err());
        }
        let mut sensitive = proof;
        sensitive["definition"] = json!({});
        assert!(request.project(json!({"ok":sensitive})).is_err());
        let mut wrong = request;
        wrong.limit = 2;
        assert!(wrong.prepare("platform".into()).is_err());
        Ok(())
    }

    #[test]
    fn catalog_control_arguments_use_the_event_rpc_decoder_contract() -> Result<()> {
        let request = Request {
            namespace: CatalogNamespace {
                tenant: "tenant".into(),
                application: "app".into(),
            },
            cursor: None,
            limit: 1,
            retired_resource: None,
        };
        let (authority, method, value) = request.prepare("platform".into())?;
        authority.validate_target(&request.scope()?, method)?;
        let args = crate::application::encode_rpc(
            &cellar_native::clone::StructuredValue::from_json(value)?,
        )?;
        let body = crate::application::event_stream::broker::body(&args)?;
        let options: CatalogListOptions = serde_json::from_value(body)?;
        assert_eq!(options.max_records, 1);
        assert!(options.cursor.is_none());
        let page = json!({"namespace":request.namespace,"records":[],"cursor":null});
        let projected = request.project(json!({"ok":page.clone()}))?;
        assert_eq!(projected, json!({"result":page}));
        for invalid in [
            json!(null),
            json!({}),
            json!({"ok":page,"error":{}}),
            json!({"result":page}),
        ] {
            assert!(request.project(invalid).is_err());
        }
        Ok(())
    }
}
