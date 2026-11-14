// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A declared script/class namespace selects one physical routing capability.

use super::*;
use crate::application::cell_bindings::{Host, Route};

#[derive(Clone, Copy)]
enum Access {
    Binding,
    SqlOperator,
    QueueOperator,
    KvOperator,
    WorkflowOperator,
}

impl CellGroupHost {
    pub(crate) fn prepare_binding_kv_payload(
        &self,
        source: &CellHistory,
        configuration: [u8; 32],
        scope: &str,
        digest: [u8; 32],
        length: u64,
        log: &crate::raft::storage::ReplicaLog,
    ) -> Result<Option<(CellHistory, CellGroupPayloadReference)>, CellDispatchError> {
        let route = self.binding_route(
            source,
            configuration,
            scope,
            crate::application::kv::raft_wire::PREPARE,
            log,
        )?;
        self.prepare_kv_payload(&route, digest, length, log)
    }

    pub(crate) fn prepare_ingress_kv_payload(
        &self,
        scope: &str,
        digest: [u8; 32],
        length: u64,
        log: &crate::raft::storage::ReplicaLog,
    ) -> Result<Option<(CellHistory, CellGroupPayloadReference)>, CellDispatchError> {
        let route = self.kv_route(
            scope,
            crate::application::kv::raft_wire::PREPARE,
            log,
            Access::Binding,
        )?;
        self.prepare_kv_payload(&route, digest, length, log)
    }

    fn prepare_kv_payload(
        &self,
        route: &Route,
        digest: [u8; 32],
        length: u64,
        log: &crate::raft::storage::ReplicaLog,
    ) -> Result<Option<(CellHistory, CellGroupPayloadReference)>, CellDispatchError> {
        let Some(history) = &route.history_hint else {
            return Ok(None);
        };
        // This observation may select only immutable data keys. Publication
        // still follows source-proof admission; the actual write independently
        // resolves/fences its current history and verifies complete bytes.
        let store = self.kv_store_for_route(history, route)?;
        let reference = store
            .prepare_reference(history, digest, length)
            .map_err(|error| {
                CellDispatchError::Invalid(match error {
                    crate::raft::cell_groups::CellGroupPayloadError::Invalid(reason) => reason,
                    _ => "KV payload preparation",
                })
            })?;
        log.performance().increment(
            crate::raft::performance::Counter::CellKvLocalPayloadPreparations,
            1,
        );
        Ok(Some((history.clone(), reference)))
    }

    pub(crate) fn kv_binding_payload_store(
        &self,
        source: &CellHistory,
        configuration: [u8; 32],
        target: &CellHistory,
        log: &crate::raft::storage::ReplicaLog,
    ) -> Result<CellGroupPayloadStore, CellDispatchError> {
        let route = self.binding_route(
            source,
            configuration,
            target.cell.as_str(),
            crate::application::kv::raft_wire::PREPARE,
            log,
        )?;
        self.kv_store_for_route(target, &route)
    }

    pub(crate) fn kv_ingress_payload_store(
        &self,
        target: &CellHistory,
        log: &crate::raft::storage::ReplicaLog,
    ) -> Result<CellGroupPayloadStore, CellDispatchError> {
        let route = self.kv_route(
            target.cell.as_str(),
            crate::application::kv::raft_wire::PREPARE,
            log,
            Access::Binding,
        )?;
        self.kv_store_for_route(target, &route)
    }

    fn kv_store_for_route(
        &self,
        target: &CellHistory,
        route: &Route,
    ) -> Result<CellGroupPayloadStore, CellDispatchError> {
        if route.cluster != target.cluster
            || route.namespace != target.namespace
            || route.cell != target.cell
        {
            return Err(CellDispatchError::Invalid("KV payload route differs"));
        }
        let binding = self
            .inner
            .namespaces
            .get(&(target.cluster, target.namespace))
            .ok_or(CellDispatchError::Runtime(RuntimeError::NotFound))?;
        if binding.class.name() != crate::deploy::KV_CLASS {
            return Err(CellDispatchError::Invalid("KV payload class"));
        }
        let pool = self
            .inner
            .payloads
            .as_ref()
            .ok_or(CellDispatchError::Invalid(
                "KV payload owner is unavailable",
            ))?;
        binding
            .dispatcher
            .directory()
            .namespace()
            .cell_payloads(pool)
            .map_err(|_| CellDispatchError::Invalid("KV payload namespace"))
    }

    pub(crate) fn kv_payload_store(
        &self,
        source: &CellHistory,
        configuration: [u8; 32],
    ) -> Result<CellGroupPayloadStore, CellDispatchError> {
        self.inner.check_lifecycle()?;
        let binding = self
            .inner
            .namespaces
            .get(&(source.cluster, source.namespace))
            .ok_or(CellDispatchError::Runtime(RuntimeError::NotFound))?;
        if binding.class.name() != crate::deploy::KV_CLASS
            || binding.class.cell_configuration_digest() != Some(configuration)
        {
            return Err(CellDispatchError::Registration(
                RegistrationError::ContractMismatch,
            ));
        }
        let pool = self
            .inner
            .payloads
            .as_ref()
            .ok_or(CellDispatchError::Invalid(
                "KV payload owner is unavailable",
            ))?;
        binding
            .dispatcher
            .directory()
            .namespace()
            .cell_payloads(pool)
            .map_err(|_| CellDispatchError::Invalid("KV payload namespace"))
    }

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
        log: &crate::raft::storage::ReplicaLog,
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
        if !owner.class.allows_cell_binding(scope) {
            return Err(CellDispatchError::Execution(
                crate::raft::execution::ExecutionError::Refused(
                    cellar_logic::consensus::CommandFailure::UnsupportedCapability,
                ),
            ));
        }
        let (target_key, target) = if class == crate::deploy::QUEUE_CLASS {
            select_queue(&self.inner.namespaces, Some(source.cluster), scope)?
        } else {
            select(&self.inner.namespaces, key, configuration, class)?
        };
        let mut route = route(target_key, target, scope, method, Access::Binding)?;
        route.history_hint = target.dispatcher.applied_history_hint(log, &route.cell)?;
        Ok(route)
    }

    /// A stateless native caller can select only its immutable deployment's
    /// declared factory. Multiple loaded namespaces remain ambiguous.
    pub(crate) fn native_ingress_route(
        &self,
        configuration: [u8; 32],
        scope: &str,
        method: &str,
        log: &crate::raft::storage::ReplicaLog,
    ) -> Result<Route, CellDispatchError> {
        self.inner.check_lifecycle()?;
        let class = scope
            .split_once(':')
            .map(|(class, _)| class)
            .ok_or(CellDispatchError::Invalid("native ingress scope"))?;
        let mut candidates = self.inner.namespaces.iter().filter(|(_, binding)| {
            binding.class.name() == class
                && binding.class.cell_configuration_digest() == Some(configuration)
                && binding.class.has_native_ingress()
        });
        let (key, target) = candidates.next().ok_or(CellDispatchError::Invalid(
            "declared native namespace is missing",
        ))?;
        if candidates.next().is_some() {
            return Err(CellDispatchError::Invalid(
                "declared native namespace is ambiguous",
            ));
        }
        let mut selected = route(*key, target, scope, method, Access::Binding)?;
        selected.history_hint = target
            .dispatcher
            .applied_history_hint(log, &selected.cell)?;
        Ok(selected)
    }

    /// Reuse the exact binding/configuration authorization before selecting a
    /// pure resource query. No constructor or enrollment may run on this path.
    pub(crate) fn workflow_read_route(
        &self,
        source: Option<&CellHistory>,
        configuration: [u8; 32],
        scope: &str,
        method: &str,
        log: &crate::raft::storage::ReplicaLog,
    ) -> Result<Route, CellDispatchError> {
        let operation = crate::application::workflow::read_operation(method)
            .ok_or(CellDispatchError::Invalid("Workflow read operation"))?;
        let mut selected = match source {
            Some(source) => self.binding_route(source, configuration, scope, method, log)?,
            None => self.native_ingress_route(configuration, scope, method, log)?,
        };
        let target = self
            .inner
            .namespaces
            .get(&(selected.cluster, selected.namespace))
            .ok_or(CellDispatchError::Runtime(RuntimeError::NotFound))?;
        if !crate::deploy::is_workflow_class(target.class.name()) {
            return Err(CellDispatchError::Invalid("Workflow read class"));
        }
        selected.contract = target
            .class
            .contract(operation)
            .filter(|contract| contract.kind == crate::application::consensus::OperationKind::Query)
            .ok_or(CellDispatchError::Invalid("Workflow query contract"))?;
        Ok(selected)
    }

    /// Stateless SQL has no source namespace. A global database identity must
    /// select exactly one loaded SQL service; multiple clusters are ambiguous.
    pub(crate) fn sql_ingress_route(
        &self,
        scope: &str,
        method: &str,
        log: &crate::raft::storage::ReplicaLog,
    ) -> Result<Route, CellDispatchError> {
        let class = scope
            .split_once(':')
            .map(|(class, _)| class)
            .ok_or(CellDispatchError::Invalid("builtin binding scope"))?;
        if class == crate::deploy::SQL_CLASS {
            return self.sql_route(scope, method, log, Access::Binding);
        }
        if class == crate::deploy::KV_CLASS {
            return self.kv_route(scope, method, log, Access::Binding);
        }
        if class != crate::deploy::QUEUE_CLASS {
            return Err(CellDispatchError::Invalid("builtin binding class"));
        }
        self.queue_route(scope, method, log, Access::Binding)
    }

    pub(crate) fn queue_operator_route(
        &self,
        scope: &str,
        log: &crate::raft::storage::ReplicaLog,
    ) -> Result<Route, CellDispatchError> {
        self.queue_route(
            scope,
            crate::application::queue::OPERATOR,
            log,
            Access::QueueOperator,
        )
    }

    pub(crate) fn workflow_operator_route(
        &self,
        scope: &str,
        log: &crate::raft::storage::ReplicaLog,
    ) -> Result<Route, CellDispatchError> {
        self.inner.check_lifecycle()?;
        let class = scope
            .split_once(':')
            .map(|(class, _)| class)
            .filter(|class| crate::deploy::is_workflow_class(class))
            .ok_or(CellDispatchError::Invalid("Workflow operator scope"))?;
        let mut candidates = self
            .inner
            .namespaces
            .iter()
            .filter(|(_, b)| b.class.name() == class);
        let (key, target) = candidates
            .next()
            .ok_or(CellDispatchError::Invalid("Workflow namespace is missing"))?;
        if candidates.next().is_some() {
            return Err(CellDispatchError::Invalid(
                "Workflow namespace is ambiguous",
            ));
        }
        let mut selected = route(
            *key,
            target,
            scope,
            crate::application::workflow::OPERATOR,
            Access::WorkflowOperator,
        )?;
        selected.history_hint = target
            .dispatcher
            .applied_history_hint(log, &selected.cell)?;
        Ok(selected)
    }

    pub(crate) fn kv_operator_route(
        &self,
        scope: &str,
        log: &crate::raft::storage::ReplicaLog,
    ) -> Result<Route, CellDispatchError> {
        self.kv_route(
            scope,
            crate::application::kv::OPERATOR,
            log,
            Access::KvOperator,
        )
    }

    fn kv_route(
        &self,
        scope: &str,
        method: &str,
        log: &crate::raft::storage::ReplicaLog,
        access: Access,
    ) -> Result<Route, CellDispatchError> {
        self.inner.check_lifecycle()?;
        let mut services = self
            .inner
            .namespaces
            .iter()
            .filter(|(_, binding)| binding.class.name() == crate::deploy::KV_CLASS);
        let (key, target) = services.next().ok_or(CellDispatchError::Invalid(
            "declared KV namespace is missing",
        ))?;
        if services.next().is_some() {
            return Err(CellDispatchError::Invalid(
                "declared KV namespace is ambiguous",
            ));
        }
        let mut route = route(*key, target, scope, method, access)?;
        route.history_hint = target.dispatcher.applied_history_hint(log, &route.cell)?;
        Ok(route)
    }

    fn queue_route(
        &self,
        scope: &str,
        method: &str,
        log: &crate::raft::storage::ReplicaLog,
        access: Access,
    ) -> Result<Route, CellDispatchError> {
        self.inner.check_lifecycle()?;
        let (key, target) = select_queue(&self.inner.namespaces, None, scope)?;
        let mut route = route(key, target, scope, method, access)?;
        route.history_hint = target.dispatcher.applied_history_hint(log, &route.cell)?;
        Ok(route)
    }

    pub(crate) fn sql_operator_route(
        &self,
        scope: &str,
        log: &crate::raft::storage::ReplicaLog,
    ) -> Result<Route, CellDispatchError> {
        self.sql_route(
            scope,
            crate::application::sql::operator::METHOD,
            log,
            Access::SqlOperator,
        )
    }

    fn sql_route(
        &self,
        scope: &str,
        method: &str,
        log: &crate::raft::storage::ReplicaLog,
        access: Access,
    ) -> Result<Route, CellDispatchError> {
        self.inner.check_lifecycle()?;
        let mut services = self
            .inner
            .namespaces
            .iter()
            .filter(|(_, binding)| binding.class.name() == crate::deploy::SQL_CLASS);
        let (key, target) = services.next().ok_or(CellDispatchError::Invalid(
            "declared SQL namespace is missing",
        ))?;
        if services.next().is_some() {
            return Err(CellDispatchError::Invalid(
                "declared SQL namespace is ambiguous",
            ));
        }
        let mut route = route(*key, target, scope, method, access)?;
        route.history_hint = target.dispatcher.applied_history_hint(log, &route.cell)?;
        Ok(route)
    }
}

fn route(
    target_key: (ClusterId, NamespaceId),
    target: &Binding,
    scope: &str,
    method: &str,
    access: Access,
) -> Result<Route, CellDispatchError> {
    let (config, _, _) = target.dispatcher.host_binding()?;
    let limits = target
        .storage
        .lock()
        .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?
        .as_ref()
        .ok_or(CellDispatchError::Runtime(RuntimeError::Closed))?
        .options_limits();
    let contract = match access {
        Access::Binding => target.class.binding_contract(method),
        Access::SqlOperator => target.class.sql_operator_contract(),
        Access::QueueOperator => target.class.queue_operator_contract(),
        Access::KvOperator => target.class.kv_operator_contract(),
        Access::WorkflowOperator => target.class.workflow_operator_contract(),
    }
    .ok_or_else(|| {
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

pub(super) fn validate(
    namespaces: &BTreeMap<(ClusterId, NamespaceId), Arc<Binding>>,
) -> Result<(), CellDispatchError> {
    for (key, source) in namespaces {
        let Some(configuration) = source.class.cell_configuration_digest() else {
            continue;
        };
        for class in source.class.cell_binding_classes().into_iter().flatten() {
            if class == crate::deploy::QUEUE_CLASS {
                for scope in source.class.cell_queue_scopes().into_iter().flatten() {
                    select_queue(namespaces, Some(key.0), scope)?;
                }
            } else {
                select(namespaces, *key, configuration, class)?;
            }
        }
    }
    Ok(())
}

/// Namespace declarations are the durable ownership catalogue. Loading another
/// producer cannot relocate a Queue; conflicting explicit owners fail closed.
fn select_queue<'a>(
    namespaces: &'a BTreeMap<(ClusterId, NamespaceId), Arc<Binding>>,
    cluster: Option<ClusterId>,
    scope: &str,
) -> Result<((ClusterId, NamespaceId), &'a Binding), CellDispatchError> {
    let mut explicit = None;
    let mut implicit = None;
    let mut ambiguous_implicit = false;
    for (key, binding) in namespaces {
        if cluster.is_some_and(|cluster| key.0 != cluster)
            || binding.class.name() != crate::deploy::QUEUE_CLASS
        {
            continue;
        }
        match binding.class.queue_ownership(scope) {
            Some(true) => {
                if explicit.replace((*key, binding.as_ref())).is_some() {
                    return Err(CellDispatchError::Invalid(
                        "declared Queue owner is ambiguous",
                    ));
                }
            }
            Some(false) => {
                ambiguous_implicit |= implicit.replace((*key, binding.as_ref())).is_some();
            }
            None => (),
        }
    }
    explicit
        .or_else(|| (!ambiguous_implicit).then_some(implicit).flatten())
        .ok_or(CellDispatchError::Invalid(
            "declared Queue owner is missing or ambiguous",
        ))
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
            && (class == crate::deploy::SQL_CLASS
                || class == crate::deploy::KV_CLASS
                || binding.class.cell_configuration_digest() == Some(configuration))
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
