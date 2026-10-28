// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{envelope, Code, Contract, Failure, Limits, Prepared};
use crate::application::Context;
use crate::engine_api::RpcData;
use anyhow::{ensure, Result};
use std::{collections::BTreeMap, sync::Arc};

/// Immutable after application startup. Providers explicitly associate services
/// with classes; consumers may add contracts without exposing any class method.
#[derive(Clone, Default)]
pub struct Catalog {
    contracts: BTreeMap<String, Arc<Contract>>,
    classes: BTreeMap<String, BTreeMap<String, Arc<Contract>>>,
}

impl Catalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_contract(&mut self, contract: Arc<Contract>) -> Result<()> {
        if let Some(existing) = self.contracts.get(contract.digest()) {
            ensure!(
                existing.descriptor() == contract.descriptor()
                    && existing.limits() == contract.limits(),
                "conflicting RPC contract registration"
            );
        } else {
            self.contracts.insert(contract.digest().into(), contract);
        }
        Ok(())
    }

    pub fn register(&mut self, class: &str, service: &str, contract: Arc<Contract>) -> Result<()> {
        ensure!(
            !class.is_empty() && class.len() <= 256 && !class.contains(':'),
            "invalid RPC class"
        );
        ensure!(
            !crate::deploy::is_reserved_scope(&format!("{class}:rpc")),
            "framework classes cannot expose application RPC services"
        );
        let methods: Vec<_> = contract
            .methods()
            .filter(|method| {
                method
                    .name
                    .split_once('/')
                    .is_some_and(|(name, _)| name == service)
            })
            .map(|method| method.name.clone())
            .collect();
        ensure!(!methods.is_empty(), "unknown or empty RPC service");
        if let Some(existing) = self.classes.get(class) {
            ensure!(
                methods.iter().all(|method| !existing.contains_key(method)),
                "duplicate RPC service method registration"
            );
        }
        self.add_contract(contract.clone())?;
        let entries = self.classes.entry(class.into()).or_default();
        for method in methods {
            entries.insert(method, contract.clone());
        }
        Ok(())
    }

    pub fn contract(&self, digest: &str) -> Option<Arc<Contract>> {
        self.contracts.get(digest).cloned()
    }

    pub fn provider(&self, class: &str, method: &str) -> Option<Arc<Contract>> {
        self.classes.get(class)?.get(method).cloned()
    }

    pub fn classes(&self) -> impl Iterator<Item = &str> {
        self.classes.keys().map(String::as_str)
    }

    /// Validate before any business factory executes. Returned refusals carry
    /// trusted pre-handler evidence and still pass the normal host output gate.
    /// Unregistered classes retain the existing untyped/native dispatch path.
    pub fn refusal(
        &self,
        class: &str,
        method: &str,
        args: &RpcData,
    ) -> Result<Option<RpcData>, Failure> {
        self.refusal_with_proof(class, method, args, true, None)
    }

    /// Use inside an adapter after a custom application may have run a factory.
    /// Only the host's earlier catalog check can establish a proven refusal.
    pub fn refusal_after_factory(
        &self,
        class: &str,
        method: &str,
        args: &RpcData,
    ) -> Result<Option<RpcData>, Failure> {
        self.refusal_with_proof(class, method, args, false, None)
    }

    /// Adapter validation against the immutable catalog of this native turn.
    #[doc(hidden)]
    pub fn refusal_in_context(
        &self,
        context: &Context,
        class: &str,
        method: &str,
        args: &RpcData,
    ) -> Result<Option<RpcData>, Failure> {
        self.refusal_with_proof(class, method, args, false, Some(context))
    }

    /// Deliver effective codec limits without replacing the verified schemas.
    /// A composed application's catalog owns policy; generated defaults remain
    /// the fallback for contracts which it has not overridden.
    #[doc(hidden)]
    pub fn invocation_limits(&self, context: &Context) -> BTreeMap<String, Limits> {
        self.contracts
            .iter()
            .map(|(digest, fallback)| {
                let declared = context
                    .rpc_catalog
                    .as_ref()
                    .and_then(|catalog| catalog.contract(digest));
                (
                    digest.clone(),
                    declared.as_ref().unwrap_or(fallback).limits(),
                )
            })
            .collect()
    }

    fn refusal_with_proof(
        &self,
        class: &str,
        method: &str,
        args: &RpcData,
        proven: bool,
        context: Option<&Context>,
    ) -> Result<Option<RpcData>, Failure> {
        let Some(entries) = self.classes.get(class) else {
            return Ok(None);
        };
        let fallback = entries.get(method);
        let declared = context
            .and_then(|context| context.rpc_catalog.as_ref())
            .and_then(|catalog| catalog.provider(class, method));
        let contract = fallback.map(|fallback| declared.as_ref().unwrap_or(fallback));
        let limits = contract.map(|c| c.limits()).unwrap_or_default();
        let (digest, bytes) = envelope::view(args)?;
        let rejection = match contract {
            Some(contract)
                if contract.digest() != digest
                    || fallback
                        .is_some_and(|fallback| fallback.descriptor() != contract.descriptor()) =>
            {
                Some(Failure::refused(Code::ContractMismatch))
            }
            Some(contract) => {
                // Host admission needs only a borrowed preflight, not an owned
                // Prepared value that would immediately discard its body copy.
                let method = contract
                    .method(method)
                    .expect("catalog entries come from their contract");
                contract.validate_message(&method.input, bytes).err()
            }
            None => Some(Failure::refused(Code::MethodNotFound)),
        };
        rejection
            .map(|failure| {
                let failure = if proven {
                    failure
                } else {
                    Failure::new(failure.code())
                };
                Prepared::refusal(&digest, failure, limits)
            })
            .transpose()
    }

    /// Only registration-owned names become guest dispatch entries and labels.
    pub fn inventory(&self) -> serde_json::Value {
        let mut classes = serde_json::Map::new();
        for (class, entries) in &self.classes {
            let methods = entries.iter().map(|(name, contract)| {
                let method = contract.method(name).expect("catalog entries come from their contract");
                (name.clone(), serde_json::json!({"digest":contract.digest(), "input":method.input, "output":method.output, "limits":contract.limits()}))
            }).collect::<serde_json::Map<_, _>>();
            classes.insert(class.clone(), methods.into());
        }
        classes.into()
    }

    /// Expected codec shapes, once per bundle rather than repeated per method.
    /// Consumer-only adapters receive the same trusted artifact inventory.
    pub fn codec_inventory(&self) -> serde_json::Value {
        self.contracts
            .iter()
            .map(|(digest, contract)| (digest.clone(), contract.inventory()))
            .collect::<serde_json::Map<_, _>>()
            .into()
    }

    pub fn validate_bridge(&self, maximum: usize) -> Result<()> {
        ensure!(
            serde_json::to_vec(&self.codec_inventory())?.len() <= maximum,
            "RPC codec inventory exceeds language bridge value budget"
        );
        for contract in self.contracts.values() {
            ensure!(
                contract
                    .limits()
                    .max_message_bytes
                    .checked_add(16 * 1024)
                    .is_some_and(|bytes| bytes <= maximum),
                "RPC contract exceeds language bridge value budget"
            );
        }
        Ok(())
    }

    pub fn limits(&self, class: &str, method: &str) -> Option<Limits> {
        self.provider(class, method).map(|c| c.limits())
    }
}
