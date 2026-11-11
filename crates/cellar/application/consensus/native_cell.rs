// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ordinary cell construction and JSON RPC on the private Raft worker.

use super::{CellHistory, CommandFailure, CommandTransaction, RouteEpoch};
use crate::application::{
    CellIdentity, Context, EventState, Factory, HandlerFuture, Invocation, Output,
};
use crate::engine_api::{RpcData, WorkerConfigOptions};
use crate::worker_config::{EgressPolicy, WorkerConfig};
use anyhow::Result;
use std::sync::Arc;

pub(super) type CellRpcHandler = dyn Fn(
        &CellHistory,
        RouteEpoch,
        &str,
        &[u8],
        &CommandTransaction<'_>,
    ) -> Result<HandlerFuture<Vec<u8>>>
    + Send
    + Sync;

pub(super) struct NativeCellFactory {
    factory: Factory,
    config: Arc<WorkerConfig>,
}

impl NativeCellFactory {
    pub(super) fn bind(class: &str, factory: Factory) -> Arc<CellRpcHandler> {
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
        let owner = Self {
            factory,
            config: Arc::new(config),
        };
        Arc::new(move |cell, epoch, operation, input, transaction| {
            owner.start(cell, epoch, operation, input, transaction)
        })
    }

    fn start(
        &self,
        cell: &CellHistory,
        epoch: RouteEpoch,
        operation: &str,
        input: &[u8],
        transaction: &CommandTransaction<'_>,
    ) -> Result<HandlerFuture<Vec<u8>>> {
        let json = std::str::from_utf8(input)?;
        serde_json::from_str::<serde::de::IgnoredAny>(json)?;
        let identity = CellIdentity {
            scope: cell.cell.as_str().into(),
            class: self.config.script_name.clone(),
            name: None,
            epoch: epoch.get(),
        };
        let context = self.context(
            identity.clone(),
            transaction.storage()?.raft_command_resources()?,
        );
        let instance = (self.factory)(context.clone(), &identity)?;
        let invocation = Invocation::CellRpc {
            method: operation.to_owned(),
            args: RpcData::Json(json.to_owned()),
        };
        let future = instance.invoke(context, invocation);
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
        storage: Arc<crate::application::storage::Resources>,
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
