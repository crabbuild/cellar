// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A declared script/class namespace selects one physical routing capability.

use super::*;
use crate::application::cell_bindings::{Host, Route};

impl CellGroupHost {
    pub(crate) fn install_binding_host(
        &self,
        host: Arc<dyn Host>,
    ) -> Result<(), CellDispatchError> {
        self.inner.check_lifecycle()?;
        for binding in self.inner.namespaces.values() {
            binding
                .dispatcher
                .bindings
                .set(host.clone())
                .map_err(|_| CellDispatchError::Runtime(RuntimeError::AlreadyAttached))?;
        }
        Ok(())
    }

    pub(crate) fn binding_route(
        &self,
        source: &CellHistory,
        configuration: [u8; 32],
        scope: &str,
        method: &str,
    ) -> Result<Route, CellDispatchError> {
        self.inner.check_lifecycle()?;
        let key = (source.cluster, source.namespace);
        let owner = self
            .inner
            .namespaces
            .get(&key)
            .ok_or(CellDispatchError::Runtime(RuntimeError::NotFound))?;
        if owner.class.cell_configuration_digest() != Some(configuration) {
            return Err(CellDispatchError::Registration(
                RegistrationError::ContractMismatch,
            ));
        }
        let (class, _) = scope
            .split_once(':')
            .ok_or(CellDispatchError::Invalid("cell binding scope"))?;
        if !owner
            .class
            .cell_binding_classes()
            .is_some_and(|classes| classes.contains(class))
        {
            return Err(CellDispatchError::Execution(
                crate::raft::execution::ExecutionError::Refused(
                    cellar_logic::consensus::CommandFailure::UnsupportedCapability,
                ),
            ));
        }
        let (target_key, target) = select(&self.inner.namespaces, key, configuration, class)?;
        let (config, _, _) = target.dispatcher.host_binding()?;
        let limits = target
            .storage
            .lock()
            .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?
            .as_ref()
            .ok_or(CellDispatchError::Runtime(RuntimeError::Closed))?
            .options_limits();
        let contract = target.class.binding_contract(method).ok_or_else(|| {
            CellDispatchError::Registration(RegistrationError::UnknownOperation(method.into()))
        })?;
        Ok(Route {
            cluster: target_key.0,
            namespace: target_key.1,
            cell: cellar_logic::cell_group::CellId::new(scope)
                .map_err(|_| CellDispatchError::Invalid("cell binding identity"))?,
            schema: config.schema_digest,
            contract,
            durability: config.minimum_durability,
            history_hint: None,
            max_input_bytes: target.dispatcher.input_limit(),
            max_result_bytes: limits.max_result_bytes,
            max_retry_window_ms: limits.max_retry_window_ms,
            json_only: target
                .class
                .binding_json_only(method)
                .ok_or(CellDispatchError::Invalid("cell binding codec"))?,
        })
    }
}

pub(super) fn validate(
    namespaces: &BTreeMap<(ClusterId, NamespaceId), Arc<Binding>>,
) -> Result<(), CellDispatchError> {
    for (key, source) in namespaces {
        let Some(configuration) = source.class.cell_configuration_digest() else {
            continue;
        };
        for class in source.class.cell_binding_classes().into_iter().flatten() {
            select(namespaces, *key, configuration, class)?;
        }
    }
    Ok(())
}

fn select<'a>(
    namespaces: &'a BTreeMap<(ClusterId, NamespaceId), Arc<Binding>>,
    source: (ClusterId, NamespaceId),
    configuration: [u8; 32],
    class: &str,
) -> Result<((ClusterId, NamespaceId), &'a Binding), CellDispatchError> {
    let current = namespaces
        .get(&source)
        .ok_or(CellDispatchError::Runtime(RuntimeError::NotFound))?;
    if current.class.name() == class {
        return Ok((source, current));
    }
    let mut matches = namespaces.iter().filter(|(key, binding)| {
        key.0 == source.0
            && binding.class.name() == class
            && binding.class.cell_configuration_digest() == Some(configuration)
    });
    let (key, binding) = matches.next().ok_or(CellDispatchError::Invalid(
        "declared cell binding namespace is missing",
    ))?;
    if matches.next().is_some() {
        return Err(CellDispatchError::Invalid(
            "declared cell binding namespace is ambiguous",
        ));
    }
    Ok((*key, binding))
}

impl CellGroupCellHandle {
    pub(crate) fn verify_binding_recovery_schema(
        &self,
        schema: [u8; 32],
    ) -> Result<(), CellDispatchError> {
        if self.binding.class.schema_digest() != schema
            || !self.binding.class.has_binding_maintenance()
        {
            return Err(CellDispatchError::Registration(
                RegistrationError::ContractMismatch,
            ));
        }
        Ok(())
    }

    pub(crate) async fn retire_binding(
        &self,
        transport: &crate::raft::transport::RaftTransport,
        schema: [u8; 32],
        request: crate::raft::execution::binding_seal::Request,
        timeout: std::time::Duration,
    ) -> Result<(), CellDispatchError> {
        self.host.check_open()?;
        let options = self.binding.options(&self.history)?;
        self.host
            .controller
            .binding_retire_routed(
                &self.binding.dispatcher,
                transport,
                options,
                (
                    self.history.clone(),
                    schema,
                    request,
                    CellRoutingLimits {
                        timeout,
                        max_attempts: 4,
                    },
                ),
            )
            .await
    }
}
