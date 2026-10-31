// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
use super::*;
use crate::raft::cell_groups::checkpoint::wire::Chunk;

pub(super) struct Source {
    pub metadata: oneshot::Receiver<Result<::raft::prelude::Snapshot, TransportError>>,
    pub chunks: mpsc::Receiver<workers::Output>,
    pub completed: oneshot::Receiver<crate::raft::cell_groups::CellGroupSeedSource>,
}

pub(super) fn source(
    state: &Arc<State>,
    endpoint: Arc<Endpoint>,
    authority: CellGroupSeedAuthority,
    pool: CellGroupCheckpointPool,
    admission: Arc<Admission>,
) -> Result<Source, TransportError> {
    let guard = state.reserve_task()?;
    let (metadata, result) = oneshot::channel();
    let (tx, rx) = mpsc::channel(1);
    let (finished, completed) = oneshot::channel();
    state
        .domain
        .blocking(move || {
            let _guard = guard;
            let _admission = admission;
            let captured = authority
                .capture(&pool)
                .and_then(|source| {
                    let snapshot = source.snapshot()?;
                    Ok((source, snapshot))
                })
                .map_err(checkpoint_error);
            let (mut source, snapshot) = match captured {
                Ok(value) => value,
                Err(error) => {
                    let _ = metadata.send(Err(error));
                    return;
                }
            };
            if metadata.send(Ok(snapshot)).is_err() {
                return;
            }
            let result = source
                .stream(|chunk| {
                    if !endpoint.active.load(Ordering::Acquire) {
                        return Err(CellGroupCheckpointError::Invalid("revoked seed endpoint"));
                    }
                    tx.blocking_send(workers::Output::Chunk(Chunk::copy(chunk)))
                        .map_err(|_| {
                            CellGroupCheckpointError::Sink(std::io::Error::new(
                                std::io::ErrorKind::BrokenPipe,
                                "seed connection closed",
                            ))
                        })
                })
                .map_err(checkpoint_error);
            let success = result.is_ok();
            if tx.blocking_send(workers::Output::End(result)).is_ok() && success {
                let _ = finished.send(source);
            }
        })
        .map_err(|_| TransportError::Closed)?
        .detach();
    Ok(Source {
        metadata: result,
        chunks: rx,
        completed,
    })
}

pub(super) fn receiver(
    state: &Arc<State>,
    applier: CellGroupApplier,
    pool: CellGroupCheckpointPool,
    admission: Arc<ReceiveAdmission>,
    grant: Arc<Grant>,
) -> Result<workers::Receive, TransportError> {
    let guard = state.reserve_task()?;
    let log = state.runtime.log().clone();
    let scratch = log.snapshot_scratch();
    let directory = log.limits().directory.clone();
    let (input, mut rx) = mpsc::channel(1);
    let (tx, result) = oneshot::channel();
    state
        .domain
        .blocking(move || {
            let _guard = guard;
            let _admission = admission;
            let outcome = (|| {
                let mut receiver = pool.receive(applier, scratch, directory)?;
                loop {
                    grant.check_log(&log)?;
                    match rx.blocking_recv() {
                        Some(workers::Input::Chunk(chunk)) => chunk.append(&mut receiver)?,
                        Some(workers::Input::End) => {
                            grant.check_log(&log)?;
                            let prepared = receiver.finish()?;
                            grant.check_log(&log)?;
                            if prepared.snapshot() != grant.snapshot() {
                                return Err(CellGroupCheckpointError::Invalid(
                                    "seed descriptor changed",
                                ));
                            }
                            return Ok(prepared);
                        }
                        None => {
                            return Err(CellGroupCheckpointError::Invalid("incomplete seed stream"))
                        }
                    }
                }
            })()
            .map_err(checkpoint_error);
            let _ = tx.send(outcome);
        })
        .map_err(|_| TransportError::Closed)?
        .detach();
    Ok(workers::Receive { input, result })
}
