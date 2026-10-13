// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld: crates/celld/js/websocket.rs.
//! Socket acceptance and metadata shared by native and source applications.

use crate::engine_api::{HttpResponseWebSocket, WsTarget};
use crate::host_gates::current_cell_event;
use crate::host_io::{EgressFrame, IoContext};
use crate::ws_registry::*;
use std::sync::Arc;

/// Prepare on the application turn, including its durability sample and owner.
/// The returned future owns the opening: dropping it cancels the connector's
/// reply receiver and removes every provisional host registration.
pub(crate) fn connect(
    context: Arc<IoContext>,
    id: u64,
    scope: String,
    url: String,
    protocols: Vec<String>,
    headers: Vec<(String, String)>,
    want_response: bool,
) -> anyhow::Result<impl std::future::Future<Output = anyhow::Result<OutboundWsOpen>> + Send> {
    let facet = dialing_facet(&context, &scope).map_err(anyhow::Error::msg)?;
    let pull = scope.is_empty().then(|| {
        let (sender, receiver) = ws_pull_channel();
        ws_pull_register(id, receiver);
        sender
    });
    ws_register_outbound(id, &scope, facet.clone());
    context.sockets.lock().unwrap().push(id);
    let pending = PendingOpen {
        context,
        id,
        completed: false,
    };
    let gate =
        crate::host_gates::egress_gate_request(&pending.context, cellar_logic::Channel::WsSelf);
    // Resolve the connector under the same services/test scope as the caller.
    let connector = crate::host_channels::outbound_ws_tx();
    Ok(async move {
        let mut pending = pending;
        crate::host_gates::await_egress_gate(gate)
            .await
            .map_err(anyhow::Error::msg)?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        connector
            .ok_or_else(|| anyhow::anyhow!("no outbound WebSocket channel"))?
            .send(OutboundWsReq {
                scope: scope.clone(),
                id,
                url,
                protocols,
                pull,
                headers,
                want_response,
                target: None,
                facet,
                reply,
            })
            .map_err(|_| anyhow::anyhow!("outbound WebSocket channel closed"))?;
        let result = receive
            .await
            .map_err(|_| anyhow::anyhow!("WebSocket connector dropped"))??;
        if result.declined.is_none() {
            // A successful cell socket belongs to the activation. Worker
            // sockets remain owned by their original event until retirement.
            if !scope.is_empty() {
                pending
                    .context
                    .sockets
                    .lock()
                    .unwrap()
                    .retain(|opened| *opened != id);
            }
            pending.completed = true;
        }
        Ok(result)
    })
}

struct PendingOpen {
    context: Arc<IoContext>,
    id: u64,
    completed: bool,
}

/// Bind only after the caller consumes its upgrade capability. Provisional
/// metadata buffers sends, while both acceptance and every frame keep the
/// caller's original gate and facet authority.
pub(crate) fn bind(
    context: Arc<IoContext>,
    id: u64,
    scope: String,
    target: HttpResponseWebSocket,
) -> anyhow::Result<impl std::future::Future<Output = anyhow::Result<OutboundWsOpen>> + Send> {
    let target = PendingTarget(Some(target));
    let local_server = match &target.0 {
        Some(HttpResponseWebSocket::Cell(target)) if target.tunnel.is_none() => Some(target.id),
        _ => None,
    };
    let facet = dialing_facet(&context, &scope).map_err(anyhow::Error::msg)?;
    let (pull, receiver) = ws_pull_channel();
    ws_pull_register(id, receiver);
    ws_register_outbound(id, &scope, facet);
    context.sockets.lock().unwrap().push(id);
    let pending = PendingOpen {
        context,
        id,
        completed: false,
    };
    let gate =
        crate::host_gates::egress_gate_request(&pending.context, cellar_logic::Channel::WsSelf);
    let connector = crate::host_channels::outbound_ws_tx();
    Ok(async move {
        let mut pending = pending;
        // Capture the entire guard so even an unpolled future owns the target.
        let mut target = target;
        crate::host_gates::await_egress_gate(gate)
            .await
            .map_err(anyhow::Error::msg)?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        let request = OutboundWsReq {
            scope: scope.clone(),
            id,
            url: String::new(),
            protocols: Vec::new(),
            pull: Some(pull),
            headers: Vec::new(),
            want_response: false,
            target: target.0.take(),
            facet: None,
            reply,
        };
        match connector {
            Some(connector) => {
                if let Err(mut error) = connector.send(request) {
                    target.0 = error.0.target.take();
                    anyhow::bail!("outbound WebSocket channel closed");
                }
            }
            None => {
                target.0 = request.target;
                anyhow::bail!("no outbound WebSocket channel");
            }
        }
        let opened = receive
            .await
            .map_err(|_| anyhow::anyhow!("WebSocket binder dropped"))??;
        if !scope.is_empty() {
            pending
                .context
                .sockets
                .lock()
                .unwrap()
                .retain(|opened| *opened != id);
        }
        pending.completed = true;
        // A pair consumed in its own cell event does not pass through the
        // normal response dispatcher. Transfer that server's cleanup as well.
        pending
            .context
            .sockets
            .lock()
            .unwrap()
            .retain(|opened| Some(*opened) != local_server);
        Ok(opened)
    })
}

struct PendingTarget(Option<HttpResponseWebSocket>);
impl Drop for PendingTarget {
    fn drop(&mut self) {
        if let Some(target) = self.0.take() {
            discard_target(target);
        }
    }
}

/// Cleanup does not disclose application data and does not await a proof.
/// Local cell targets notify the daemon; Worker and parked tunnel owners drop
/// directly, including when no connector is available during shutdown.
pub(crate) fn discard_target(target: HttpResponseWebSocket) {
    let HttpResponseWebSocket::Cell(target) = target else {
        return;
    };
    if let Some(parked) = target.tunnel {
        crate::ws_tunnel::discard(parked);
        return;
    }
    let (reply, receive) = tokio::sync::oneshot::channel();
    drop(receive);
    let request = OutboundWsReq {
        scope: String::new(),
        id: ws_next_id(),
        url: String::new(),
        protocols: Vec::new(),
        pull: None,
        headers: Vec::new(),
        want_response: false,
        facet: None,
        target: Some(HttpResponseWebSocket::Cell(target.clone())),
        reply,
    };
    if crate::host_channels::outbound_ws_tx().is_some_and(|sender| sender.send(request).is_ok()) {
        return;
    }
    ws_close(target.id, 1006, "upgrade owner was dropped");
    ws_unregister(target.id);
}
impl Drop for PendingOpen {
    fn drop(&mut self) {
        if !self.completed {
            self.context
                .sockets
                .lock()
                .unwrap()
                .retain(|opened| *opened != self.id);
            ws_pull_unregister(self.id);
            ws_unregister(self.id);
        }
    }
}

pub(crate) fn accept(id: u64, cell: &str, tags: Vec<String>) {
    let replaced = {
        let registry = ws_registry();
        let mut registry = registry.lock().unwrap();
        let replaced = registry
            .metadata
            .get(&id)
            .filter(|meta| !meta.hibernatable)
            .map(|meta| meta.scope.clone());
        registry
            .metadata
            .entry(id)
            .and_modify(|meta| {
                meta.scope = cell.into();
                meta.hibernatable = true;
                meta.tags = tags.clone();
            })
            .or_insert(WsMeta {
                scope: cell.into(),
                hibernatable: true,
                tags,
                attachment: None,
                pending: Vec::new(),
                auto_response_at: None,
                facet: None,
            });
        replaced
    };
    if let Some(scope) = replaced {
        decrement_regular_ws(&scope);
    }
    tracing::info!(ws_id = id, scope = cell, "accepted hibernatable WebSocket");
}

pub(crate) fn accept_regular(id: u64, cell: &str) {
    let inserted = {
        let registry = ws_registry();
        let mut registry = registry.lock().unwrap();
        if let std::collections::hash_map::Entry::Vacant(entry) = registry.metadata.entry(id) {
            entry.insert(WsMeta {
                scope: cell.into(),
                hibernatable: false,
                tags: Vec::new(),
                attachment: None,
                pending: Vec::new(),
                auto_response_at: None,
                facet: None,
            });
            true
        } else {
            false
        }
    };
    if inserted {
        increment_regular_ws(cell);
    }
    tracing::info!(ws_id = id, scope = cell, "accepted regular WebSocket");
}

#[derive(serde::Serialize)]
pub(crate) struct SocketRow {
    pub id: u64,
    pub tags: Vec<String>,
    pub attachment: Option<Vec<u8>>,
}

pub(crate) fn list(cell: &str, tag: Option<&str>) -> Vec<SocketRow> {
    ws_registry()
        .lock()
        .unwrap()
        .metadata
        .iter()
        .filter(|(_, meta)| {
            meta.hibernatable
                && meta.scope == cell
                && tag.is_none_or(|tag| meta.tags.iter().any(|value| value == tag))
        })
        .map(|(id, meta)| SocketRow {
            id: *id,
            tags: meta.tags.clone(),
            attachment: meta.attachment.clone(),
        })
        .collect()
}

pub(crate) fn attachment_set(id: u64, bytes: Vec<u8>) {
    if let Some(meta) = ws_registry().lock().unwrap().metadata.get_mut(&id) {
        meta.attachment = Some(bytes);
    }
}

pub(crate) fn auto_response_set(cell: &str, pair: Option<(String, String)>) {
    let pairs = ws_auto_responses();
    let mut pairs = pairs.lock().unwrap();
    if let Some(pair) = pair {
        pairs.insert(cell.into(), pair);
    } else {
        pairs.remove(cell);
    }
}

pub(crate) fn auto_response_get(cell: &str) -> Option<(String, String)> {
    ws_auto_responses().lock().unwrap().get(cell).cloned()
}

pub(crate) fn auto_response_timestamp(id: u64) -> Option<f64> {
    ws_registry()
        .lock()
        .unwrap()
        .metadata
        .get(&id)
        .and_then(|meta| meta.auto_response_at)
}

pub(crate) fn prepare_worker_handoff(context: &IoContext, id: u64) {
    let (inbound, receiver) = ws_pull_channel();
    ws_pull_register(id, receiver);
    ws_register_outbound(id, "", None);
    context.sockets.lock().unwrap().push(id);
    let replaced = ws_registry()
        .lock()
        .unwrap()
        .worker_handoffs
        .insert(id, inbound);
    drop(replaced);
}

pub(crate) fn transfer_worker_handoff(context: &IoContext, id: u64) -> Option<WorkerWebSocket> {
    let inbound = ws_registry().lock().unwrap().worker_handoffs.remove(&id)?;
    context
        .sockets
        .lock()
        .unwrap()
        .retain(|opened| *opened != id);
    Some(WorkerWebSocket { id, inbound })
}

/// Derive facet routing from the host's event frame, never from a supplied target.
pub(crate) fn dialing_facet(
    context: &IoContext,
    cell: &str,
) -> Result<Option<FacetSocket>, String> {
    if cell.is_empty() {
        return Ok(None);
    }
    let Some(EgressFrame {
        storage,
        root: Some(root),
        ..
    }) = current_cell_event(context)
    else {
        return Ok(None);
    };
    if storage != cell {
        return Err(format!(
            "websocket: the socket names {cell}, but the running Durable Object facet is {storage}"
        ));
    }
    let opening = crate::storage::facet_opening(&storage).ok_or(
        "websocket: the Durable Object facet that opened this socket is no longer running",
    )?;
    Ok(Some(FacetSocket {
        root: root.cell,
        stream: root.stream,
        epoch: root.epoch,
        opening,
    }))
}

pub(crate) fn cell_target(context: &IoContext, id: u64, scope: &str) -> Result<WsTarget, String> {
    if let Some(facet) = dialing_facet(context, scope)? {
        ws_attach_facet(id, scope, facet);
    }
    Ok(WsTarget {
        id,
        scope: scope.into(),
        tunnel: None,
    })
}
