// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{contract::Contract, envelope, Code, Failure, Limits, Method};
use crate::application::{Cell, Context, Invocation, Output};
use crate::engine_api::RpcData;
use anyhow::{ensure, Result};
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc};

/// An owned typed handler future. Host failures remain on the outer native
/// application error channel; ordinary business rejections are message values.
pub type HandlerFuture<T> = Pin<Box<dyn Future<Output = Result<T, Failure>> + Send + 'static>>;

type Handler = Arc<dyn Fn(Context, Vec<u8>, Limits) -> HandlerFuture<Vec<u8>> + Send + Sync>;

struct Entry {
    contract: Arc<Contract>,
    handler: Handler,
}

/// A validated, invocation-local message. Adapters must prepare before lazy
/// business-object construction, then finish through the native output channel.
pub struct Prepared {
    contract: Arc<Contract>,
    method: Method,
    digest: String,
    request: Vec<u8>,
}

impl Prepared {
    pub fn new(
        contract: Arc<Contract>,
        method: &str,
        digest: String,
        request: Vec<u8>,
    ) -> Result<Self, Failure> {
        if contract.digest() != digest {
            return Err(Failure::refused(Code::ContractMismatch));
        }
        let method = contract
            .method(method)
            .cloned()
            .ok_or_else(|| Failure::refused(Code::MethodNotFound))?;
        contract.validate_message(&method.input, &request)?;
        Ok(Self {
            contract,
            method,
            digest,
            request,
        })
    }

    /// Prepare with the current host's provider policy, including when a
    /// language application is wrapped by a stricter composed application.
    #[doc(hidden)]
    pub fn from_context(
        context: &Context,
        fallback: Arc<Contract>,
        method: &str,
        args: &RpcData,
    ) -> Result<Self, Failure> {
        context
            .check_turn()
            .map_err(|_| Failure::new(Code::ContextExpired))?;
        let declared = context.identity().and_then(|identity| {
            context
                .rpc_catalog
                .as_ref()?
                .provider(&identity.class, method)
        });
        let contract = declared.unwrap_or_else(|| fallback.clone());
        if contract.descriptor() != fallback.descriptor() {
            return Err(Failure::new(Code::ContractMismatch));
        }
        let (digest, bytes) = super::decode_envelope(args, contract.limits().max_message_bytes)?;
        Self::new(contract, method, digest, bytes)
    }

    pub fn method(&self) -> &Method {
        &self.method
    }

    pub fn request(&self) -> &[u8] {
        &self.request
    }

    /// Move bytes into a guest message while retaining the output contract.
    pub fn take_request(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.request)
    }

    /// Sanitize every failure from entered business code to unknown effects.
    /// A nested call's refusal cannot prove that this handler made no writes.
    pub fn finish(self, result: Result<Vec<u8>, Failure>) -> Result<RpcData, Failure> {
        let result = result
            .map_err(|failure| Failure::new(failure.code()))
            .and_then(|bytes| {
                self.contract
                    .validate_message(&self.method.output, &bytes)
                    .map_err(|_| Failure::new(Code::InvalidResponse))?;
                Ok(bytes)
            });
        envelope::reply(&self.digest, result, self.contract.limits())
    }

    /// Encode an adapter-proven rejection before entering business code. The
    /// digest is echoed even when the target has a different contract artifact.
    pub fn refusal(
        digest: &str,
        failure: Failure,
        limits: super::Limits,
    ) -> Result<RpcData, Failure> {
        envelope::reply(digest, Err(failure), limits)
    }
}

/// Explicit method table used by generated native service registrations.
/// Only registered full method names can be dispatched; HTTP or lifecycle
/// handlers can be composed with this cell by an application's native wrapper.
#[derive(Default)]
pub struct Router {
    entries: BTreeMap<String, Entry>,
}

impl Router {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn method(
        self,
        contract: Arc<Contract>,
        method: &str,
        handler: impl Fn(Context, Vec<u8>) -> HandlerFuture<Vec<u8>> + Send + Sync + 'static,
    ) -> Result<Self> {
        self.method_with_limits(contract, method, move |context, bytes, _| {
            handler(context, bytes)
        })
    }

    /// Register a codec-aware handler. The invocation supplies the immutable
    /// application's limits so response preflight runs before serialization.
    pub fn method_with_limits(
        mut self,
        contract: Arc<Contract>,
        method: &str,
        handler: impl Fn(Context, Vec<u8>, Limits) -> HandlerFuture<Vec<u8>> + Send + Sync + 'static,
    ) -> Result<Self> {
        ensure!(
            contract.method(method).is_some(),
            "RPC method is absent from contract"
        );
        ensure!(
            !self.entries.contains_key(method),
            "duplicate RPC method registration"
        );
        self.entries.insert(
            method.to_string(),
            Entry {
                contract,
                handler: Arc::new(handler),
            },
        );
        Ok(self)
    }

    /// Reject incomplete service implementations during application startup.
    pub fn validate(&self) -> Result<()> {
        ensure!(!self.entries.is_empty(), "empty RPC registration");
        for (name, entry) in &self.entries {
            let service = name
                .split_once('/')
                .map(|(service, _)| service)
                .unwrap_or_default();
            for method in entry
                .contract
                .methods()
                .filter(|m| m.name.split_once('/').is_some_and(|(s, _)| s == service))
            {
                ensure!(
                    self.entries
                        .get(&method.name)
                        .is_some_and(|other| other.contract.digest() == entry.contract.digest()),
                    "incomplete RPC service registration"
                );
            }
        }
        Ok(())
    }

    pub async fn dispatch(
        &self,
        context: Context,
        method: &str,
        args: &RpcData,
    ) -> Result<RpcData> {
        context.check_turn()?;
        let entry = self.entries.get(method);
        let declared = context.identity().and_then(|identity| {
            context
                .rpc_catalog
                .as_deref()?
                .provider(&identity.class, method)
        });
        let contract = declared
            .as_ref()
            .or_else(|| entry.map(|entry| &entry.contract));
        let limits = contract
            .map(|contract| contract.limits())
            .unwrap_or_default();
        let (digest, bytes) = super::decode_envelope(args, limits.max_message_bytes)?;
        let Some(entry) = entry else {
            // A custom native factory may already have used its Context before
            // this router was invoked. Only host pre-factory validation can
            // prove a remote refusal had no effects.
            return Ok(Prepared::refusal(
                &digest,
                Failure::new(Code::MethodNotFound),
                limits,
            )?);
        };
        if declared
            .as_ref()
            .is_some_and(|contract| contract.descriptor() != entry.contract.descriptor())
        {
            return Ok(Prepared::refusal(
                &digest,
                Failure::new(Code::ContractMismatch),
                limits,
            )?);
        }
        let contract = declared.unwrap_or_else(|| entry.contract.clone());
        let mut prepared = match Prepared::new(contract, method, digest.clone(), bytes) {
            Ok(prepared) => prepared,
            Err(failure) => {
                return Ok(Prepared::refusal(
                    &digest,
                    Failure::new(failure.code()),
                    limits,
                )?)
            }
        };
        let request = prepared.take_request();
        // Native codec admission precedes the generated decoding closure and
        // stays with its owned request until handler/response encoding finish.
        // The factory has already run: refusal cannot promise rollback.
        let result = match super::DecodeReservation::new(
            &prepared.contract,
            &prepared.method.input,
            &request,
        ) {
            Ok(_admission) => match context.check_turn() {
                Ok(()) => (entry.handler)(context, request, limits).await,
                Err(_) => Err(Failure::new(Code::ContextExpired)),
            },
            Err(failure) => Err(failure),
        };
        Ok(prepared.finish(result)?)
    }
}

impl Cell for Router {
    fn invoke(
        self: Arc<Self>,
        context: Context,
        invocation: Invocation,
    ) -> crate::application::HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { method, args } = invocation else {
                anyhow::bail!("typed RPC cell received an unsupported event");
            };
            self.dispatch(context, &method, &args)
                .await
                .map(Output::Rpc)
        })
    }
}
