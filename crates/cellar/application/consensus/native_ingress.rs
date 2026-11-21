// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Stateless native requests retain the node's admitted group owner.

use super::CommandFailure;
use crate::application::{Context, HandlerFuture};
use crate::engine_api::RpcData;
use crate::host_rpc::CallScope;
use anyhow::Result;

pub(crate) fn event_rpc(
    context: &Context,
    scope: String,
    name: Option<String>,
    method: String,
    args: RpcData,
    authority: super::native_event_rpc::Authority,
) -> HandlerFuture<RpcData> {
    let prepared = (|| -> Result<_> {
        context.check_turn()?;
        anyhow::ensure!(
            context.identity().is_none() && context.state.raft_event,
            CommandFailure::UnsupportedCapability
        );
        authority.validate_target(&scope, name.as_deref(), &method)?;
        context.io.charge_subrequest().map_err(anyhow::Error::msg)?;
        let call_scope = ingress_scope(
            context,
            CallScope::in_context(context, context.io.rpc_deadline_ms.get().copied(), None),
        );
        let services = crate::asyncrt::services();
        let now = crate::asyncrt::mono_ms();
        let budget =
            crate::host_rpc::event_budget_ms(&method, &args, services.operation_deadline_ms()?);
        let deadline = now.checked_add(budget).ok_or(CommandFailure::Unavailable)?;
        let deadline = call_scope
            .deadline_ms
            .map_or(deadline, |parent| parent.min(deadline));
        anyhow::ensure!(
            deadline > now,
            crate::raft::completion::RuntimeError::DeadlineExceeded
        );
        let owner = services.raft_lifecycle().application_service()?;
        let mut admitted = services
            .admit_outbound_rpc(&scope, name.as_deref(), &method, &args)
            .ok_or_else(|| {
                if services.accepts_work() {
                    CommandFailure::Overloaded
                } else {
                    CommandFailure::Unavailable
                }
            })?;
        admitted.reservation = admitted
            .reservation
            .with_reply_limit(call_scope.reply_limit());
        Ok(if super::native_event_query::is_metadata(&method) {
            owner.event_read(admitted, call_scope, deadline, authority)
        } else {
            owner.event_ingress(admitted, call_scope, deadline, authority)
        })
    })();
    Box::pin(async move { prepared?.await })
}

pub(crate) fn fetch(
    context: &Context,
    scope: String,
    name: Option<String>,
    request: crate::application::Request,
) -> HandlerFuture<crate::engine_api::HttpResponse> {
    let prepared = (|| -> Result<_> {
        context.check_turn()?;
        anyhow::ensure!(
            context.identity().is_none()
                && request.entrypoint.is_none()
                && matches!(request.body, crate::engine_api::RequestBody::Bytes(_)),
            CommandFailure::UnsupportedCapability
        );
        let (class, _) = scope
            .split_once(':')
            .ok_or(CommandFailure::UnsupportedCapability)?;
        let configuration = context
            .state
            .raft_cell_classes
            .as_ref()
            .and_then(|classes| classes.get(class))
            .copied()
            .ok_or(CommandFailure::UnsupportedCapability)?;
        super::native_rpc::validate_identity(
            &context.worker_config.script_name,
            class,
            &scope,
            name.as_deref(),
        )?;
        context.io.charge_subrequest().map_err(anyhow::Error::msg)?;
        let call_scope = ingress_scope(
            context,
            CallScope::in_context(context, context.io.rpc_deadline_ms.get().copied(), None),
        );
        let services = crate::asyncrt::services();
        let now = crate::asyncrt::mono_ms();
        let deadline = now
            .checked_add(services.operation_deadline_ms()?)
            .ok_or(CommandFailure::Unavailable)?;
        let deadline = call_scope
            .deadline_ms
            .map_or(deadline, |parent| parent.min(deadline));
        anyhow::ensure!(deadline > now, super::RuntimeError::DeadlineExceeded);
        let owner = services.raft_lifecycle().application_service()?;
        let admitted = services
            .admit_outbound_fetch(&scope, name.as_deref(), &request)
            .ok_or_else(|| {
                if services.accepts_work() {
                    CommandFailure::Overloaded
                } else {
                    CommandFailure::Unavailable
                }
            })?;
        drop((scope, name, request));
        Ok(owner.native_fetch(admitted, call_scope, deadline, configuration))
    })();
    Box::pin(async move { prepared?.await })
}

pub(crate) fn rpc(
    context: &Context,
    scope: String,
    name: Option<String>,
    method: String,
    args: RpcData,
    call_scope: CallScope,
) -> HandlerFuture<RpcData> {
    submit(context, scope, name, method, args, call_scope, false)
}

pub(crate) fn resource_rpc(
    context: &Context,
    scope: String,
    name: Option<String>,
    method: String,
    args: RpcData,
    call_scope: CallScope,
) -> HandlerFuture<RpcData> {
    submit(context, scope, name, method, args, call_scope, true)
}

fn submit(
    context: &Context,
    scope: String,
    name: Option<String>,
    method: String,
    args: RpcData,
    call_scope: CallScope,
    resource: bool,
) -> HandlerFuture<RpcData> {
    let prepared = (|| -> Result<_> {
        context.check_turn()?;
        anyhow::ensure!(
            context.identity().is_none(),
            CommandFailure::UnsupportedCapability
        );
        let call_scope = ingress_scope(context, call_scope);
        let read = scope
            .split_once(':')
            .is_some_and(|(class, _)| crate::deploy::is_workflow_class(class))
            && crate::application::workflow::read_operation(&method).is_some();
        let queue_metrics = super::native_queue_metrics::is_method(&scope, &method);
        anyhow::ensure!(
            !resource || read || queue_metrics,
            CommandFailure::UnsupportedCapability
        );
        if !resource {
            context.io.charge_subrequest().map_err(anyhow::Error::msg)?;
        }
        let native = scope.split_once(':').and_then(|(class, _)| {
            context
                .state
                .raft_cell_classes
                .as_ref()?
                .get(class)
                .copied()
        });
        anyhow::ensure!(
            native.is_some()
                || context.sql_bindings.values().any(|database| {
                    crate::engine_api::sql_cell_scope(database) == scope
                        && name.as_deref() == Some(database.as_str())
                })
                || context.queue_bindings.values().any(|queue| {
                    crate::engine_api::queue_cell_scope(queue.name()) == scope
                        && name.as_deref() == Some(queue.name())
                })
                || context.kv_bindings.values().any(|identity| {
                    crate::engine_api::kv_cell_scope(identity, 0) == scope
                        && name.as_deref()
                            == Some(cellar_logic::kv::cell_name(identity, 0).as_str())
                }),
            CommandFailure::UnsupportedCapability
        );
        if native.is_some() {
            let class = scope
                .split_once(':')
                .ok_or(CommandFailure::UnsupportedCapability)?
                .0;
            super::native_rpc::validate_identity(
                &context.worker_config.script_name,
                class,
                &scope,
                name.as_deref(),
            )?;
        }
        let services = crate::asyncrt::services();
        let now = crate::asyncrt::mono_ms();
        let deadline = now
            .checked_add(services.operation_deadline_ms()?)
            .ok_or(CommandFailure::Unavailable)?;
        let deadline = call_scope
            .deadline_ms
            .map_or(deadline, |parent| parent.min(deadline));
        deadline
            .checked_sub(now)
            .filter(|remaining| *remaining != 0)
            .ok_or(crate::raft::completion::RuntimeError::DeadlineExceeded)?;
        let owner = services.raft_lifecycle().application_service()?;
        let mut admitted = services
            .admit_outbound_rpc(&scope, name.as_deref(), &method, &args)
            .ok_or_else(|| {
                if services.accepts_work() {
                    CommandFailure::Overloaded
                } else {
                    CommandFailure::Unavailable
                }
            })?;
        drop((scope, name, method, args));
        admitted.reservation = admitted
            .reservation
            .with_reply_limit(call_scope.reply_limit());
        if queue_metrics {
            return Ok(owner.queue_metrics(admitted, call_scope, deadline, native));
        }
        Ok(match native {
            Some(configuration) if read => {
                owner.workflow_read(admitted, call_scope, deadline, configuration)
            }
            Some(configuration) => {
                owner.native_ingress(admitted, call_scope, deadline, configuration)
            }
            None => owner.sql_ingress(admitted, call_scope, deadline),
        })
    })();
    Box::pin(async move { prepared?.await })
}

fn ingress_scope(context: &Context, mut scope: CallScope) -> CallScope {
    scope.ingress_cursor = Some(
        context
            .state
            .raft_ingress_cursor
            .get_or_init(Default::default)
            .clone(),
    );
    scope
}
