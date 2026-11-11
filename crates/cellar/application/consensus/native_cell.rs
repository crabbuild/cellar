// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ordinary cell construction and JSON RPC on the private Raft worker.

use super::{CellHistory, CommandFailure, CommandTransaction, RouteEpoch};
use crate::application::{
    Cell, CellIdentity, Context, EventState, Factory, HandlerFuture, Invocation, Output,
};
use crate::engine_api::{RpcData, WorkerConfigOptions};
use crate::worker_config::{EgressPolicy, WorkerConfig};
use anyhow::Result;
use std::sync::Arc;

pub(crate) struct NativeCellFactory {
    factory: Factory,
    config: Arc<WorkerConfig>,
    identity: Arc<()>,
}

type CellRpcHandler = dyn Fn(
        &CellHistory,
        RouteEpoch,
        &str,
        &[u8],
        &CommandTransaction<'_>,
        &mut Option<CellRpcActivation>,
    ) -> Result<HandlerFuture<Vec<u8>>>
    + Send
    + Sync;

/// Compiled dispatch keeps ordinary native configuration out of consensus
/// handle types. Its identity also fences cache reuse after factory replacement.
pub(super) struct CellRpcFactory {
    handler: Arc<CellRpcHandler>,
    identity: Arc<()>,
}

impl CellRpcFactory {
    pub(super) fn accepts(&self, activation: &CellRpcActivation) -> bool {
        activation.0.belongs_to(&self.identity)
    }

    pub(super) fn start(
        &self,
        cell: &CellHistory,
        epoch: RouteEpoch,
        operation: &str,
        input: &[u8],
        transaction: &CommandTransaction<'_>,
        activation: &mut Option<CellRpcActivation>,
    ) -> Result<HandlerFuture<Vec<u8>>> {
        (self.handler)(cell, epoch, operation, input, transaction, activation)
    }
}

// Runtime services contain SQL handles too. Keep native payloads behind this
// lifetime contract instead of recursively exposing their whole auto-trait graph.
trait LiveCell: Send + Sync {
    fn resources(&self) -> Arc<crate::application::storage::Resources>;

    fn belongs_to(&self, factory: &Arc<()>) -> bool;

    fn invoke(&self, context: Context, invocation: Invocation) -> Result<HandlerFuture<Output>>;

    fn retire(&mut self) -> Result<()>;
}

pub(crate) struct CellRpcActivation(Box<dyn LiveCell>);

struct NativeActivation {
    owner: Arc<()>,
    instance: Option<Arc<dyn Cell>>,
    resources: Arc<crate::application::storage::Resources>,
}

impl CellRpcActivation {
    pub(crate) fn resources(&self) -> Arc<crate::application::storage::Resources> {
        self.0.resources()
    }

    pub(crate) fn close(mut self) -> Result<()> {
        self.0.retire()
    }
}

impl LiveCell for NativeActivation {
    fn resources(&self) -> Arc<crate::application::storage::Resources> {
        self.resources.clone()
    }

    fn belongs_to(&self, factory: &Arc<()>) -> bool {
        Arc::ptr_eq(factory, &self.owner)
    }

    fn invoke(&self, context: Context, invocation: Invocation) -> Result<HandlerFuture<Output>> {
        let instance = self.instance.clone().ok_or(CommandFailure::Unavailable)?;
        Ok(instance.invoke(context, invocation))
    }

    fn retire(&mut self) -> Result<()> {
        self.resources
            .retired
            .store(true, std::sync::atomic::Ordering::Release);
        if let Some(instance) = self.instance.take() {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(instance)))
                .map_err(crate::native_worker::panic_error)?;
        }
        Ok(())
    }
}

impl Drop for CellRpcActivation {
    fn drop(&mut self) {
        if let Err(error) = self.0.retire() {
            self.0
                .resources()
                .failure
                .lock()
                .unwrap()
                .get_or_insert_with(|| error.to_string());
        }
    }
}

impl NativeCellFactory {
    pub(super) fn bind(class: &str, factory: Factory) -> Arc<CellRpcFactory> {
        let config = WorkerConfig::new(WorkerConfigOptions {
            script_name: class.to_owned(),
            src: String::new(),
            do_classes: Vec::new(),
            bindings: Vec::new(),
            blob_bindings: Vec::new(),
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            stream_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: String::new(),
            modules: Vec::new(),
            compat: Default::default(),
        })
        .with_egress(EgressPolicy::Deny);
        let identity = Arc::new(());
        let owner = Arc::new(Self {
            factory,
            config: Arc::new(config),
            identity: identity.clone(),
        });
        Arc::new(CellRpcFactory {
            identity,
            handler: Arc::new(
                move |cell, epoch, operation, input, transaction, activation| {
                    owner.start(cell, epoch, operation, input, transaction, activation)
                },
            ),
        })
    }

    pub(super) fn accepts(self: &Arc<Self>, activation: &CellRpcActivation) -> bool {
        activation.0.belongs_to(&self.identity)
    }

    pub(super) fn start(
        self: &Arc<Self>,
        cell: &CellHistory,
        epoch: RouteEpoch,
        operation: &str,
        input: &[u8],
        transaction: &CommandTransaction<'_>,
        activation: &mut Option<CellRpcActivation>,
    ) -> Result<HandlerFuture<Vec<u8>>> {
        let json = std::str::from_utf8(input)?;
        serde_json::from_str::<serde::de::IgnoredAny>(json)?;
        let identity = CellIdentity {
            scope: cell.cell.as_str().into(),
            class: self.config.script_name.clone(),
            name: None,
            epoch: epoch.get(),
        };
        let authority = transaction.storage()?.raft_command_authority()?;
        if let Some(cached) = activation.as_ref() {
            anyhow::ensure!(
                self.accepts(cached) && Arc::ptr_eq(&cached.resources(), &authority.resources),
                "Raft activation factory or storage changed"
            );
        } else {
            let context = self.context(identity.clone(), authority.activation());
            let instance = (self.factory)(context, &identity)?;
            *activation = Some(CellRpcActivation(Box::new(NativeActivation {
                owner: self.identity.clone(),
                instance: Some(instance),
                resources: authority.resources.clone(),
            })));
        }
        let context = self.context(identity, authority);
        let invocation = Invocation::CellRpc {
            method: operation.to_owned(),
            args: RpcData::Json(json.to_owned()),
        };
        let future = activation
            .as_ref()
            .ok_or(CommandFailure::Unavailable)?
            .0
            .invoke(context, invocation)?;
        Ok(Box::pin(async move {
            let Output::Rpc(RpcData::Json(result)) = future.await? else {
                anyhow::bail!(CommandFailure::UnsupportedCapability);
            };
            serde_json::from_str::<serde::de::IgnoredAny>(&result)?;
            Ok(result.into_bytes())
        }))
    }

    fn context(
        &self,
        identity: CellIdentity,
        storage: crate::application::storage::RaftStorageAuthority,
    ) -> Context {
        Context {
            raft_storage: Some(storage),
            container_supervision: None,
            rpc_catalog: None,
            event_authority: None,
            name_authority: None,
            catalog_authority: None,
            io: crate::host_io::IoContext::new(),
            state: Arc::new(EventState::default()),
            identity: Some(identity),
            facet: None,
            trace: None,
            vars: Arc::default(),
            sql_bindings: Arc::default(),
            kv_bindings: Arc::default(),
            queue_bindings: Arc::default(),
            workflow_bindings: Arc::default(),
            blob_bindings: Arc::default(),
            asset_binding: None,
            queue_config: None,
            generation: self.config.generation,
            worker_config: self.config.clone(),
            loader_owner: None,
            loaded_environment: None,
        }
    }
}
