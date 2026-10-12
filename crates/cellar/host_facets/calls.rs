// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;
use crate::engine_api::RpcData;
use crate::host_channels::CallOrder;
use crate::host_turn::CellJob;
use crate::http_streams::RequestBodyGuard;

impl LoadedFacet {
    pub(crate) async fn fetch(
        &self,
        request: crate::application::Request,
        mut body_guard: RequestBodyGuard,
        cancellation: Option<Arc<crate::runtime::RequestCancellationLifetime>>,
        trace: Option<crate::telemetry::TraceContext>,
        order: Option<CallOrder>,
    ) -> Result<HttpResponse, String> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        let request_id = cancellation.as_ref().map(|lifetime| lifetime.request_id());
        let job = CellJob::Fetch {
            request_id,
            scope: self.scope.clone(),
            name: None,
            url: request.url,
            method: request.method,
            body: request.body,
            headers: request.headers,
            reply,
            order,
        };
        let driving = crate::asyncrt::spawn(crate::runtime::drive_cell_owned(
            self.slot.affiliate(),
            job,
            None,
            trace,
            cancellation,
        ));
        let replied = receive.await;
        if let Ok(Err(_)) = &replied {
            self.gate_reply().await?;
        }
        match replied {
            Ok(Ok(response)) => {
                body_guard.disarm();
                let socket = match &response.websocket {
                    Some(HttpResponseWebSocket::Cell(target)) => Some(target.id),
                    _ => None,
                };
                self.gate_response(socket).await?;
                self.attach_websocket(&response);
                Ok(response)
            }
            Ok(Err(error)) => Err(error.to_string()),
            Err(_) => match driving.await {
                Err(error) => Err(format!("facet fetch task died: {error}")),
                Ok(()) => Err("facet dropped fetch response".into()),
            },
        }
    }

    pub(crate) async fn rpc(
        &self,
        method: String,
        args: RpcData,
        cancellation: Option<Arc<crate::runtime::RequestCancellationLifetime>>,
        trace: Option<crate::telemetry::TraceContext>,
    ) -> Result<RpcData, String> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        let request_id = cancellation.as_ref().map(|lifetime| lifetime.request_id());
        let job = CellJob::Rpc {
            request_id,
            scope: self.scope.clone(),
            name: None,
            method,
            args,
            reply,
        };
        let driving = crate::asyncrt::spawn(crate::runtime::drive_cell_owned(
            self.slot.affiliate(),
            job,
            None,
            trace,
            cancellation,
        ));
        let replied = receive.await;
        if replied.is_ok() {
            self.gate_reply().await?;
        }
        match replied {
            Ok(Ok(outcome)) => {
                // Retain the source adapter's second proof for binary replies.
                if matches!(outcome.data, RpcData::Structured(_)) {
                    self.gate_reply().await?;
                }
                Ok(outcome.data)
            }
            Ok(Err(error)) => Err(error.to_string()),
            Err(_) => match driving.await {
                Err(error) => Err(format!("facet RPC task died: {error}")),
                Ok(()) => Err("facet dropped RPC result".into()),
            },
        }
    }
}

pub(crate) async fn abort_facet(
    host: FacetHost,
    scope: String,
    root: Option<(String, u64, Vec<String>)>,
) -> Result<(), String> {
    let recorded = host.is_loaded_worker();
    let slot = host.slot().await?;
    const ABORTED: &str = "the Durable Object facet was aborted";
    let below = match &root {
        Some((root, epoch, path)) => take_recorded_facets_below(root, *epoch, path),
        None => Vec::new(),
    };
    for RecordedFacet {
        slot, scope, pin, ..
    } in below
    {
        if let Err(error) = slot.turn(|worker| worker.own_cell(&scope, None)).await {
            tracing::warn!(facet = %scope, %error, "a facet did not close with its parent");
        }
        ws_close_facet(&scope, ABORTED);
        drop(pin);
    }
    let (closed, outcome) = match &root {
        Some((root, _, path)) => {
            slot.turn(|worker| worker.close_facet_tree(&scope, root, path))
                .await
        }
        None => {
            let outcome = slot
                .turn(|worker| worker.own_cell(&scope, None))
                .await
                .map(|_| ());
            (vec![scope.clone()], outcome)
        }
    };
    for scope in &closed {
        ws_close_facet(scope, ABORTED);
    }
    if let (true, Some((root, epoch, _))) = (recorded, &root) {
        forget_root_facet(root, *epoch, &slot, &scope);
    }
    outcome.map_err(|error| format!("abort facet: {error}"))
}

/// Delete the independent stream or standalone files, including descendants.
/// Deletion proceeds after the abort barrier has closed active facets.
/// Submission and standalone removal happen immediately, so a native caller
/// can serialize them with parent retirement before awaiting the host reply.
pub(crate) fn delete_facet_files(
    parent: storage::StorageIdentity,
    name: String,
) -> impl std::future::Future<Output = Result<(), String>> + Send {
    let mut names = parent.facet_path.clone();
    names.push(name.clone());
    let prepared = if let Some(sender) = FACET_TX.get() {
        let (reply, receive) = tokio::sync::oneshot::channel();
        sender
            .send(FacetReq::Delete {
                root: parent.root_scope,
                epoch: parent.epoch,
                names,
                reply,
            })
            .map(|()| Some(receive))
            .map_err(|_| "the facet channel closed".to_string())
    } else {
        delete_standalone_facet_files(&parent, &names).map(|()| None)
    };
    async move {
        let Some(receive) = prepared? else {
            return Ok(());
        };
        receive
            .await
            .map_err(|_| "the facet delete was dropped".to_string())?
            .map_err(|error| format!("delete facet storage: {error:#}"))
    }
}

fn delete_standalone_facet_files(
    parent: &storage::StorageIdentity,
    names: &[String],
) -> Result<(), String> {
    let path = standalone_facet_file(parent, names);
    let directory = path.parent().ok_or("facet database has no directory")?;
    let file = path.file_name().unwrap().to_string_lossy();
    let stem = file.strip_suffix(".sqlite").unwrap_or(&file);
    let descendants = format!("{stem}-facets-");
    let filesystem = crate::asyncrt::fs();
    let entries = match filesystem.read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(format!("list facet databases: {error}")),
    };
    for entry in entries {
        let Some(name) = entry.path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name == file
            || name == format!("{file}-wal")
            || name == format!("{file}-shm")
            || name.starts_with(&descendants)
        {
            match filesystem.remove_file(&entry.path) {
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                    return Err(format!("delete facet storage: {error}"))
                }
                _ => {}
            }
        }
    }
    Ok(())
}
