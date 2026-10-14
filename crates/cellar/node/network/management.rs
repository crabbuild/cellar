//! Generation-bound private management. Human permission checks belong to the
//! management host; this listener accepts only authenticated fleet requests.
use super::*;
use crate::management::{NodeAction, NodeRequest};

pub(super) async fn handle(
    request: Request<Incoming>,
    app: AppHandle,
    shutdown: mpsc::UnboundedSender<ShutdownMode>,
) -> HttpReply {
    let (parts, body) = request.into_parts();
    let body = match collect_limited_body(body, MAX_PEER_CONTROL_BODY_BYTES).await {
        Ok(body) => body,
        Err(error) => return peer_response(body_read_error("management", error)),
    };
    let path = parts
        .uri
        .path_and_query()
        .map_or(parts.uri.path(), |path| path.as_str());
    if let Err(error) = app.peer_auth.verify(
        &parts.method,
        path,
        &parts.headers,
        &body,
        app.peer_auth.source(),
    ) {
        return peer_response(response(error.status(), error.message()));
    }
    if parts.method != hyper::Method::POST {
        return reply(
            StatusCode::METHOD_NOT_ALLOWED,
            serde_json::json!({"error":"POST required"}),
        );
    }
    let request: NodeRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => {
            return reply(
                StatusCode::BAD_REQUEST,
                serde_json::json!({"error":"invalid management request"}),
            )
        }
    };
    let services = crate::asyncrt::services();
    if services.actor_process_generation() != Some(request.generation.as_str()) {
        return reply(
            StatusCode::CONFLICT,
            serde_json::json!({"error":"node generation changed; refresh before continuing"}),
        );
    }
    match request.action {
        NodeAction::Stop { operation_id, mode } => {
            if app.is_draining() || !app.healthy().await {
                return reply(
                    StatusCode::CONFLICT,
                    serde_json::json!({"error":"node is not serving; inspect the stop receipt"}),
                );
            }
            let Some(operations) = services.node_operations() else {
                return reply(
                    StatusCode::CONFLICT,
                    serde_json::json!({"error":"this host has no durable stop reporter"}),
                );
            };
            match operations.begin(&operation_id, mode).await {
                Ok(receipt) => {
                    if shutdown.send(mode.into()).is_err() {
                        return reply(
                            StatusCode::SERVICE_UNAVAILABLE,
                            serde_json::json!({"error":"node driver unavailable; inspect the stop receipt"}),
                        );
                    }
                    reply(
                        StatusCode::ACCEPTED,
                        serde_json::json!({"result":{"receipt":receipt,"propagation":"drain_requested"}}),
                    )
                }
                Err(error) => reply(
                    StatusCode::CONFLICT,
                    serde_json::json!({"error":error.to_string()}),
                ),
            }
        }
        NodeAction::Cells { after, limit } => {
            if !(1..=100).contains(&limit)
                || after
                    .as_deref()
                    .is_some_and(|scope| !cellar_logic::cell::valid_cell_scope(scope))
            {
                return reply(
                    StatusCode::BAD_REQUEST,
                    serde_json::json!({"error":"invalid cell page"}),
                );
            }
            match app.owned_cells(after, limit).await {
                Ok(page) => reply(
                    StatusCode::OK,
                    serde_json::json!({"result":{"node":app.peer_auth.source(),"generation":request.generation,"authoritative":app.healthy().await,"observed_at":now_ms(),"next_cursor":page.next_cursor,"items":page.items.into_iter().map(|cell|serde_json::json!({"scope":cell.scope,"epoch":cell.epoch,"phase":cell.phase,"active_requests":cell.active_requests,"alarm_at":cell.alarm_at_ms,"alarm_firing":cell.alarm_firing})).collect::<Vec<_>>()}}),
                ),
                Err(_) => reply(
                    StatusCode::SERVICE_UNAVAILABLE,
                    serde_json::json!({"error":"node actor unavailable"}),
                ),
            }
        }
        NodeAction::State => {
            let deployment=app.runtime.as_ref().map(|runtime| {
                let generation=runtime.generation();
                serde_json::json!({"version":generation.version(),"prefix":generation.prefix(),"generation":generation.id(),"application":generation.script_name(),"cron_revision":generation.cron_revision(),"cron_expressions":generation.primary_crons})
            });
            reply(
                StatusCode::OK,
                serde_json::json!({"result":{
                    "node":app.peer_auth.source(),"generation":request.generation,
                    "authoritative":app.healthy().await,"fleet_ready":app.fleet_ready.load(std::sync::atomic::Ordering::Acquire),
                    "draining":app.is_draining(),"public_in_flight":app.public_in_flight(),
                    "deployment":deployment,"observed_at":now_ms(),
                    "capabilities":{"balancing":services.node_load().is_some(),"reload":!app.reload.is_closed(),"stop":services.node_operations().is_some()}
                }}),
            )
        }
        NodeAction::PauseBalancing | NodeAction::ResumeBalancing => {
            if app.is_draining() || !app.healthy().await {
                return reply(
                    StatusCode::CONFLICT,
                    serde_json::json!({"error":"node is not serving"}),
                );
            }
            let paused = matches!(request.action, NodeAction::PauseBalancing);
            if !crate::ownership_store::set_rebalance_paused(paused) {
                return reply(
                    StatusCode::CONFLICT,
                    serde_json::json!({"error":"node publishes no fleet lease"}),
                );
            }
            reply(
                StatusCode::OK,
                serde_json::json!({"result":{"node":app.peer_auth.source(),"generation":request.generation,"requested_paused":paused,"propagation":"awaiting_lease_publication"}}),
            )
        }
        NodeAction::Evict { scope, epoch } => {
            if !cellar_logic::cell::valid_cell_scope(&scope) {
                return reply(
                    StatusCode::BAD_REQUEST,
                    serde_json::json!({"error":"invalid cell scope"}),
                );
            }
            match app.evict_at_epoch(scope.clone(), epoch).await {
                Ok(result) => reply(
                    StatusCode::OK,
                    serde_json::json!({"result":{"node":app.peer_auth.source(),"generation":request.generation,"scope":scope,"epoch":epoch,"completed":true,"outcome":match result {cellar_logic::EvictSuccess::Evicted=>"evicted",cellar_logic::EvictSuccess::AlreadyAbsent=>"already_absent"}}}),
                ),
                Err(error) => reply(
                    StatusCode::CONFLICT,
                    serde_json::json!({"error":error.reason(),"kind":error.kind()}),
                ),
            }
        }
        NodeAction::Reload => {
            if app.is_draining() || !app.healthy().await {
                return reply(
                    StatusCode::CONFLICT,
                    serde_json::json!({"error":"node is not serving"}),
                );
            }
            let (send, receive) = tokio::sync::oneshot::channel();
            if app
                .reload
                .send(ReloadRequest {
                    force: true,
                    reply: Some(send),
                })
                .is_err()
            {
                return reply(
                    StatusCode::CONFLICT,
                    serde_json::json!({"error":"this host has no deployment watcher"}),
                );
            }
            match crate::asyncrt::timeout(std::time::Duration::from_secs(30), receive).await {
                Ok(Ok(ReloadOutcome::Adopted {
                    generation,
                    version,
                    prefix,
                })) => reply(
                    StatusCode::OK,
                    serde_json::json!({"result":{"outcome":"adopted","generation":generation,"version":version,"prefix":prefix}}),
                ),
                Ok(Ok(ReloadOutcome::Unchanged {
                    generation,
                    version,
                })) => reply(
                    StatusCode::OK,
                    serde_json::json!({"result":{"outcome":"unchanged","generation":generation,"version":version}}),
                ),
                Ok(Ok(ReloadOutcome::Failed { error, .. })) => {
                    reply(StatusCode::CONFLICT, serde_json::json!({"error":error}))
                }
                _ => reply(
                    StatusCode::GATEWAY_TIMEOUT,
                    serde_json::json!({"error":"reload outcome unknown; inspect deployment before retrying"}),
                ),
            }
        }
    }
}

fn reply(status: StatusCode, value: serde_json::Value) -> HttpReply {
    peer_response(response(status, value.to_string()))
}
