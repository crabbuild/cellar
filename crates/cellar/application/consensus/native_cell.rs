// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ordinary cell construction and JSON RPC on the private Raft worker.

use super::{
    native_configuration::AdmittedCellConfiguration, CellHistory, CommandFailure,
    CommandTransaction, RouteEpoch,
};
use crate::application::{
    Cell, CellIdentity, Context, EventState, Factory, HandlerFuture, Invocation, Output,
};
use crate::engine_api::RpcData;
use crate::worker_config::{EgressPolicy, WorkerConfig};
use anyhow::Result;
use std::sync::{Arc, Mutex};

pub(crate) struct NativeCellFactory {
    factory: Factory,
    class: String,
    vars: Arc<std::collections::HashMap<String, String>>,
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

type ConfigureCellFactory = dyn Fn(&AdmittedCellConfiguration) -> Arc<CellRpcFactory> + Send + Sync;

/// Compiled dispatch keeps ordinary native configuration out of consensus
/// handle types. Its identity also fences cache reuse after factory replacement.
pub(super) struct CellRpcFactory {
    handler: Arc<CellRpcHandler>,
    identity: Arc<()>,
    configuration: Option<[u8; 32]>,
    configure: Arc<ConfigureCellFactory>,
}

impl CellRpcFactory {
    pub(super) fn configured(&self, config: &AdmittedCellConfiguration) -> Arc<Self> {
        (self.configure)(config)
    }

    pub(super) fn configuration_digest(&self) -> Option<[u8; 32]> {
        self.configuration
    }

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

    fn retire(&self) -> Result<()>;
}

#[derive(Clone)]
pub(crate) struct CellRpcActivation(Arc<dyn LiveCell>);

struct NativeActivation {
    owner: Arc<()>,
    instance: Mutex<Option<Arc<dyn Cell>>>,
    resources: Arc<crate::application::storage::Resources>,
}

impl CellRpcActivation {
    pub(crate) fn shared(&self) -> bool {
        Arc::strong_count(&self.0) > 1
    }

    pub(crate) fn live(&self) -> bool {
        !self
            .resources()
            .retired
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn resources(&self) -> Arc<crate::application::storage::Resources> {
        self.0.resources()
    }

    pub(crate) fn close(self) -> Result<()> {
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
        let instance = self
            .instance
            .lock()
            .map_err(|_| CommandFailure::Unavailable)?
            .clone()
            .ok_or(CommandFailure::Unavailable)?;
        Ok(instance.invoke(context, invocation))
    }

    fn retire(&self) -> Result<()> {
        self.resources
            .retired
            .store(true, std::sync::atomic::Ordering::Release);
        let instance = self
            .instance
            .lock()
            .map_err(|_| CommandFailure::Unavailable)?
            .take();
        if let Some(instance) = instance {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(instance)))
                .map_err(crate::native_worker::panic_error)?;
        }
        Ok(())
    }
}

impl Drop for NativeActivation {
    fn drop(&mut self) {
        if let Err(error) = self.retire() {
            self.resources()
                .failure
                .lock()
                .unwrap()
                .get_or_insert_with(|| error.to_string());
        }
    }
}

impl NativeCellFactory {
    pub(super) fn bind(class: &str, factory: Factory) -> Arc<CellRpcFactory> {
        Self::bind_configuration(class, factory, None)
    }

    fn bind_configuration(
        class: &str,
        factory: Factory,
        declared: Option<&AdmittedCellConfiguration>,
    ) -> Arc<CellRpcFactory> {
        let options = declared.map_or_else(
            || super::NativeCellConfiguration::default().options(class),
            AdmittedCellConfiguration::options,
        );
        let vars = Arc::new(options.vars.iter().cloned().collect());
        let config = WorkerConfig::new(options).with_egress(EgressPolicy::Deny);
        let compiled_factory = factory.clone();
        let compiled_class = class.to_owned();
        let identity = Arc::new(());
        let owner = Arc::new(Self {
            factory,
            class: class.to_owned(),
            vars,
            config: Arc::new(config),
            identity: identity.clone(),
        });
        Arc::new(CellRpcFactory {
            identity,
            configuration: declared.map(AdmittedCellConfiguration::digest),
            configure: Arc::new(move |config| {
                Self::bind_configuration(&compiled_class, compiled_factory.clone(), Some(config))
            }),
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
            class: self.class.clone(),
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
            *activation = Some(CellRpcActivation(Arc::new(NativeActivation {
                owner: self.identity.clone(),
                instance: Mutex::new(Some(instance)),
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
        storage.resources.delete_all_deletes_alarm.store(
            self.config.compat.delete_all_deletes_alarm,
            std::sync::atomic::Ordering::Release,
        );
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
            vars: self.vars.clone(),
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
