// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Stateless SQL uses the node-owned group service, never Fleet routing.

use crate::application::{consensus::CommandFailure, Context, HandlerFuture};
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
    let prepared = (|| -> Result<_> {
        context.check_turn()?;
        anyhow::ensure!(
            context.identity().is_none(),
            CommandFailure::UnsupportedCapability
        );
        context.io.charge_subrequest().map_err(anyhow::Error::msg)?;
        anyhow::ensure!(
            context.sql_bindings.values().any(|database| {
                // The canonical scope is global, independent of script and alias.
                crate::engine_api::sql_cell_scope(database) == scope
                    && name.as_deref() == Some(database.as_str())
            }),
            CommandFailure::UnsupportedCapability
        );
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
        let admitted = services
            .admit_outbound_rpc(&scope, name.as_deref(), &method, &args)
            .ok_or_else(|| {
                if services.accepts_work() {
                    CommandFailure::Overloaded
                } else {
                    CommandFailure::Unavailable
                }
            })?;
        // Relinquish caller-owned spare capacity/backing before any async wait.
        drop((scope, name, method, args));
        let crate::outbound_rpc::Admitted {
            reservation,
            scope,
            name,
            method,
            args,
        } = admitted;
        let reservation = reservation.with_reply_limit(call_scope.reply_limit());
        Ok(owner.sql_ingress(scope, name, method, args, call_scope, deadline, reservation))
    })();
    Box::pin(async move { prepared?.await })
}
