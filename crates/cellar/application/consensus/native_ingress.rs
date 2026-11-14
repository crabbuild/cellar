// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Stateless native and built-in RPCs retain the node's admitted group owner.

use super::CommandFailure;
use crate::application::{Context, HandlerFuture};
use crate::engine_api::RpcData;
use crate::host_rpc::CallScope;
use anyhow::Result;

pub(crate) fn rpc(
    context: &Context,
    scope: String,
    name: Option<String>,
    method: String,
    args: RpcData,
    call_scope: CallScope,
) -> HandlerFuture<RpcData> {
    submit(context, scope, name, method, args, call_scope, Purpose::Rpc)
}

pub(crate) fn resource_rpc(
    context: &Context,
    scope: String,
    name: Option<String>,
    method: String,
    args: RpcData,
    call_scope: CallScope,
) -> HandlerFuture<RpcData> {
    submit(
        context,
        scope,
        name,
        method,
        args,
        call_scope,
        Purpose::Resource,
    )
}

pub(crate) fn event_rpc(
    context: &Context,
    scope: String,
    name: Option<String>,
    method: String,
    args: RpcData,
    call_scope: CallScope,
    authority: crate::application::event_stream::Authority,
) -> HandlerFuture<RpcData> {
    submit(
        context,
        scope,
        name,
        method,
        args,
        call_scope,
        Purpose::Event(authority),
    )
}

enum Purpose {
    Rpc,
    Resource,
    Event(crate::application::event_stream::Authority),
}

fn submit(
    context: &Context,
    scope: String,
    name: Option<String>,
    method: String,
    args: RpcData,
    call_scope: CallScope,
    purpose: Purpose,
) -> HandlerFuture<RpcData> {
    let resource = matches!(&purpose, Purpose::Resource);
    let authority = match purpose {
        Purpose::Event(authority) => Some(authority),
        _ => None,
    };
    let prepared = (|| -> Result<_> {
        context.check_turn()?;
        anyhow::ensure!(
            context.identity().is_none(),
            CommandFailure::UnsupportedCapability
        );
        let read = scope
            .split_once(':')
            .is_some_and(|(class, _)| crate::deploy::is_workflow_class(class))
            && crate::application::workflow::read_operation(&method).is_some();
        anyhow::ensure!(!resource || read, CommandFailure::UnsupportedCapability);
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
            authority.as_ref().is_some_and(|authority| {
                crate::application::event_stream::validate_target(&scope, authority).is_ok()
                    && authority.validate_operator_target(&scope, &method).is_ok()
                    && context.worker_config.stream_bindings.iter().any(|binding| {
                        binding.explicit().is_ok_and(|binding| {
                            binding.principal == authority.principal
                                && binding.grant == authority.grant
                                && binding.resource.same_identity(&authority.resource)
                        })
                    })
            }) || native.is_some()
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
        if let Some(authority) = authority {
            return Ok(owner.event_ingress(admitted, call_scope, deadline, authority));
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
