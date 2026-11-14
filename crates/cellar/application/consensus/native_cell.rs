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
    callbacks: bool,
}

pub(super) enum CellRpcMethod<'a> {
    Json(&'a str),
    Work,
    Owned { method: &'a str, json_only: bool },
}

type CellRpcHandler = dyn Fn(
        &CellHistory,
        RouteEpoch,
        CellRpcMethod<'_>,
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
    binding_classes: std::collections::BTreeSet<String>,
    sql_scopes: std::collections::BTreeSet<String>,
    kv_scopes: std::collections::BTreeSet<String>,
    queue_scopes: std::collections::BTreeSet<String>,
    queue_owners: std::collections::BTreeSet<String>,
    queue_dead_letters: std::collections::BTreeSet<String>,
    configure: Arc<ConfigureCellFactory>,
}

impl CellRpcFactory {
    pub(super) fn configured(&self, config: &AdmittedCellConfiguration) -> Arc<Self> {
        (self.configure)(config)
    }

    pub(super) fn configuration_digest(&self) -> Option<[u8; 32]> {
        self.configuration
    }

    pub(super) fn binding_classes(&self) -> &std::collections::BTreeSet<String> {
        &self.binding_classes
    }

    pub(super) fn allows_binding(&self, scope: &str) -> bool {
        let Some((class, _)) = scope.split_once(':') else {
            return false;
        };
        if class == crate::deploy::QUEUE_CLASS {
            self.queue_scopes.contains(scope)
        } else if class == crate::deploy::SQL_CLASS {
            self.sql_scopes.contains(scope)
        } else if class == crate::deploy::KV_CLASS {
            self.kv_scopes.contains(scope)
        } else {
            self.binding_classes.contains(class)
        }
    }

    pub(super) fn queue_scopes(&self) -> &std::collections::BTreeSet<String> {
        &self.queue_scopes
    }

    /// Explicit consumer/storage declarations own resources; a dead-letter
    /// reference supplies a fallback only when no explicit owner exists.
    pub(super) fn queue_ownership(&self, scope: &str) -> Option<bool> {
        if self.queue_owners.contains(scope) {
            Some(true)
        } else if self.queue_dead_letters.contains(scope) {
            Some(false)
        } else {
            None
        }
    }

    pub(super) fn accepts(&self, activation: &CellRpcActivation) -> bool {
        activation.0.belongs_to(&self.identity)
    }

    pub(super) fn start(
        &self,
        cell: &CellHistory,
        epoch: RouteEpoch,
        method: CellRpcMethod<'_>,
        input: &[u8],
        transaction: &CommandTransaction<'_>,
        activation: &mut Option<CellRpcActivation>,
    ) -> Result<HandlerFuture<Vec<u8>>> {
        (self.handler)(cell, epoch, method, input, transaction, activation)
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
    pub(super) fn bind(class: &str, factory: Factory, callbacks: bool) -> Arc<CellRpcFactory> {
        Self::bind_configuration(class, factory, None, callbacks)
    }

    fn bind_configuration(
        class: &str,
        factory: Factory,
        declared: Option<&AdmittedCellConfiguration>,
        callbacks: bool,
    ) -> Arc<CellRpcFactory> {
        let options = declared.map_or_else(
            || super::NativeCellConfiguration::default().options(class),
            AdmittedCellConfiguration::options,
        );
        let vars = Arc::new(options.vars.iter().cloned().collect());
        let mut binding_classes: std::collections::BTreeSet<_> = options
            .do_classes
            .iter()
            .cloned()
            .chain(options.bindings.iter().map(|(_, class)| class.clone()))
            .collect();
        if !options.sql_bindings.is_empty() {
            binding_classes.insert(crate::deploy::SQL_CLASS.into());
        }
        if !options.kv_bindings.is_empty() {
            binding_classes.insert(crate::deploy::KV_CLASS.into());
        }
        let kv_scopes = options
            .kv_bindings
            .iter()
            .map(|(_, identity)| crate::engine_api::kv_cell_scope(identity, 0))
            .collect();
        if !options.queue_bindings.is_empty() || !options.queue_consumers.is_empty() {
            binding_classes.insert(crate::deploy::QUEUE_CLASS.into());
        }
        let queue_scopes = options
            .queue_bindings
            .iter()
            .map(|b| b.queue.as_str())
            .chain(options.queue_consumers.iter().map(|c| c.queue.as_str()))
            .chain(
                options
                    .queue_consumers
                    .iter()
                    .filter_map(|c| c.dead_letter_queue.as_deref()),
            )
            .map(crate::engine_api::queue_cell_scope)
            .collect();
        let queue_owners = if options.queue_consumers.is_empty() {
            options
                .queue_bindings
                .iter()
                .map(|b| crate::engine_api::queue_cell_scope(&b.queue))
                .collect()
        } else {
            options
                .queue_consumers
                .iter()
                .map(|b| crate::engine_api::queue_cell_scope(&b.queue))
                .collect()
        };
        let queue_dead_letters = options
            .queue_consumers
            .iter()
            .filter_map(|b| b.dead_letter_queue.as_deref())
            .map(crate::engine_api::queue_cell_scope)
            .collect();
        let sql_scopes = options
            .sql_bindings
            .iter()
            .map(|(_, identity)| crate::engine_api::sql_cell_scope(identity))
            .collect();
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
            callbacks,
        });
        Arc::new(CellRpcFactory {
            identity,
            configuration: declared.map(AdmittedCellConfiguration::digest),
            binding_classes,
            sql_scopes,
            kv_scopes,
            queue_scopes,
            queue_owners,
            queue_dead_letters,
            configure: Arc::new(move |config| {
                Self::bind_configuration(
                    &compiled_class,
                    compiled_factory.clone(),
                    Some(config),
                    callbacks,
                )
            }),
            handler: Arc::new(move |cell, epoch, method, input, transaction, activation| {
                owner.start(cell, epoch, method, input, transaction, activation)
            }),
        })
    }

    pub(super) fn accepts(self: &Arc<Self>, activation: &CellRpcActivation) -> bool {
        activation.0.belongs_to(&self.identity)
    }

    pub(super) fn start(
        self: &Arc<Self>,
        cell: &CellHistory,
        epoch: RouteEpoch,
        method: CellRpcMethod<'_>,
        input: &[u8],
        transaction: &CommandTransaction<'_>,
        activation: &mut Option<CellRpcActivation>,
    ) -> Result<HandlerFuture<Vec<u8>>> {
        let owned_wire = matches!(method, CellRpcMethod::Owned { .. });
        let work = if matches!(method, CellRpcMethod::Work) {
            anyhow::ensure!(self.callbacks, CommandFailure::UnsupportedCapability);
            anyhow::ensure!(input.len() <= 1024, CommandFailure::Overloaded);
            let work: super::native_work::Work = serde_json::from_slice(input)?;
            anyhow::ensure!(
                work.reason == "alarm"
                    || (work.reason == "queue" && self.class == crate::deploy::QUEUE_CLASS),
                CommandFailure::UnsupportedCapability
            );
            if !super::native_work::claim(
                &*transaction.framework()?,
                &work,
                transaction.inputs().now_unix_millis,
            )? {
                return Ok(Box::pin(async { Ok(vec![0]) }));
            }
            if work.reason == "alarm" {
                transaction.begin_native_alarm(work.generation)?;
            }
            let leased_until = transaction
                .inputs()
                .now_unix_millis
                .checked_add(work.lease_ms)
                .ok_or(CommandFailure::Overloaded)?;
            Some((work, leased_until))
        } else {
            None
        };
        let (operation, args, incoming_name) = match method {
            CellRpcMethod::Owned { method, json_only } => {
                let request = super::native_rpc::decode_request(input)?;
                super::native_rpc::validate_identity(
                    &self.config.script_name,
                    &self.class,
                    cell.cell.as_str(),
                    request.name.as_deref(),
                )?;
                anyhow::ensure!(
                    !json_only || matches!(&request.args, RpcData::Json(_)),
                    CommandFailure::UnsupportedCapability
                );
                (method, request.args, request.name)
            }
            CellRpcMethod::Work => ("", RpcData::Json("null".into()), None),
            CellRpcMethod::Json(operation) => {
                let json = std::str::from_utf8(input)?;
                serde_json::from_str::<serde::de::IgnoredAny>(json)?;
                (operation, RpcData::Json(json.to_owned()), None)
            }
        };
        let name = crate::storage::raft_bindings::cell_name(
            &*transaction.framework()?,
            incoming_name.as_deref(),
        )?;
        if !self.config.bindings.is_empty()
            || !self.config.do_classes.is_empty()
            || !self.config.sql_bindings.is_empty()
            || !self.config.kv_bindings.is_empty()
            || !self.config.queue_bindings.is_empty()
            || !self.config.queue_consumers.is_empty()
        {
            crate::storage::raft_bindings::install(&*transaction.framework()?)?;
        }
        if name.is_some() {
            super::native_rpc::validate_identity(
                &self.config.script_name,
                &self.class,
                cell.cell.as_str(),
                name.as_deref(),
            )?;
        }
        let identity = CellIdentity {
            scope: cell.cell.as_str().into(),
            class: self.class.clone(),
            name,
            epoch: epoch.get(),
        };
        let authority = transaction.storage()?.raft_command_authority()?;
        if let Some(cached) = activation.as_ref() {
            anyhow::ensure!(
                self.accepts(cached) && Arc::ptr_eq(&cached.resources(), &authority.resources),
                "Raft activation factory or storage changed"
            );
        } else {
            let context = self.context(identity.clone(), authority.activation())?;
            let instance = (self.factory)(context, &identity)?;
            *activation = Some(CellRpcActivation(Arc::new(NativeActivation {
                owner: self.identity.clone(),
                instance: Mutex::new(Some(instance)),
                resources: authority.resources.clone(),
            })));
        }
        let context = self.context(identity, authority)?;
        let invocation = match &work {
            Some((work, _)) => Invocation::Alarm {
                scheduled_ms: i64::try_from(work.due)?,
            },
            None => Invocation::CellRpc {
                method: operation.to_owned(),
                args,
            },
        };
        let instance = activation
            .as_ref()
            .ok_or(CommandFailure::Unavailable)?
            .clone();
        let storage = context.storage()?;
        Ok(Box::pin(async move {
            if let Some((work, leased_until)) = work {
                // No callback entry until the exact claim survives quorum (and
                // any stronger namespace minimum). A crash retains retry work.
                storage.sync().await?;
                use futures_util::FutureExt as _;
                // Activation refusal precedes application entry. A panic in
                // invoke/poll is a known failure after the proven claim.
                let invoked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    instance.0.invoke(context, invocation)
                }));
                let result = match invoked {
                    Ok(Ok(future)) => std::panic::AssertUnwindSafe(future)
                        .catch_unwind()
                        .await
                        .map_err(crate::native_worker::panic_error)
                        .and_then(std::convert::identity),
                    Ok(Err(error)) => return Err(error),
                    Err(panic) => Err(crate::native_worker::panic_error(panic)),
                };
                let result = result.and_then(|result| {
                    anyhow::ensure!(
                        matches!(result, Output::Done),
                        CommandFailure::UnsupportedCapability
                    );
                    Ok(())
                });
                if let Err(cause) = result {
                    if work.reason == "alarm" {
                        return Err(super::native_work::Failure {
                            work,
                            leased_until,
                            at_ms: crate::asyncrt::wall_ms(),
                            cause,
                        }
                        .into());
                    }
                    return Err(cause);
                }
                crate::storage::raft_native_work(|db| {
                    super::native_work::complete(db, &work, leased_until)
                })?;
                return Ok(vec![1]);
            }
            let Output::Rpc(result) = instance.0.invoke(context, invocation)?.await? else {
                anyhow::bail!(CommandFailure::UnsupportedCapability);
            };
            if owned_wire {
                super::native_rpc::encode_response(&result)
            } else {
                let RpcData::Json(result) = result else {
                    anyhow::bail!(CommandFailure::UnsupportedCapability);
                };
                serde_json::from_str::<serde::de::IgnoredAny>(&result)?;
                Ok(result.into_bytes())
            }
        }))
    }

    fn context(
        &self,
        identity: CellIdentity,
        storage: crate::application::storage::RaftStorageAuthority,
    ) -> Result<Context> {
        storage
            .resources
            .durable_callbacks
            .store(self.callbacks, std::sync::atomic::Ordering::Release);
        storage.resources.delete_all_deletes_alarm.store(
            self.config.compat.delete_all_deletes_alarm,
            std::sync::atomic::Ordering::Release,
        );
        Ok(Context {
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
            sql_bindings: Arc::new(self.config.sql_bindings.iter().cloned().collect()),
            kv_bindings: Arc::new(self.config.kv_bindings.iter().cloned().collect()),
            queue_bindings: Arc::new(
                self.config
                    .queue_bindings
                    .iter()
                    .map(|b| {
                        (
                            b.environment.clone(),
                            crate::application::queue::Producer::new(
                                b.queue.clone(),
                                b.delivery_delay,
                                self.config.compat.queue_json_messages,
                            ),
                        )
                    })
                    .collect(),
            ),
            workflow_bindings: Arc::default(),
            blob_bindings: Arc::default(),
            asset_binding: None,
            queue_config: if self.class == crate::deploy::QUEUE_CLASS {
                Some(Arc::new(serde_json::from_value(
                    crate::queue_policy::consumer_config(&self.config)?,
                )?))
            } else {
                None
            },
            generation: self.config.generation,
            worker_config: self.config.clone(),
            loader_owner: None,
            loaded_environment: None,
        })
    }
}
