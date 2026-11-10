// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Blocking owners retain task accounting and wire admission through actual I/O.
//! Channel closure aborts a partial receive; only an explicit End can seal it.
use super::*;
use crate::raft::cell_groups::{checkpoint::wire::Chunk, PreparedCellGroupCheckpoint};

pub(super) enum Input {
    Chunk(Chunk),
    End,
}

pub(super) enum Output {
    Chunk(Chunk),
    End(Result<(), TransportError>),
}

pub(super) fn source(
    state: &Arc<State>,
    endpoint: Arc<Endpoint>,
    pool: CellGroupCheckpointPool,
    snapshot: ::raft::prelude::Snapshot,
    admission: Arc<Admission>,
) -> Result<mpsc::Receiver<Output>, TransportError> {
    let guard = state.reserve_task()?;
    let (tx, rx) = mpsc::channel(1);
    state
        .domain
        .blocking(move || {
            let _guard = guard;
            let _admission = admission;
            let result = (|| {
                let mut source = endpoint.applier.capture_checkpoint(&pool, &snapshot)?;
                source.stream(|chunk| {
                    if !endpoint.active.load(Ordering::Acquire) {
                        return Err(CellGroupCheckpointError::Invalid("revoked source"));
                    }
                    tx.blocking_send(Output::Chunk(Chunk::copy(chunk)))
                        .map_err(|_| {
                            CellGroupCheckpointError::Sink(std::io::Error::new(
                                std::io::ErrorKind::BrokenPipe,
                                "snapshot connection closed",
                            ))
                        })
                })
            })()
            .map_err(checkpoint_error);
            let _ = tx.blocking_send(Output::End(result));
        })
        .map_err(|_| TransportError::Closed)?
        .detach();
    Ok(rx)
}

pub(super) struct Receive {
    pub input: mpsc::Sender<Input>,
    pub result: oneshot::Receiver<Result<PreparedCellGroupCheckpoint, TransportError>>,
}

pub(super) fn receiver(
    state: &Arc<State>,
    endpoint: Arc<Endpoint>,
    pool: CellGroupCheckpointPool,
    admission: Arc<Admission>,
) -> Result<Receive, TransportError> {
    let guard = state.reserve_task()?;
    let scratch = state.runtime.log().snapshot_scratch();
    let directory = state.runtime.log().limits().directory.clone();
    let (input, mut rx) = mpsc::channel(1);
    let (tx, result) = oneshot::channel();
    state
        .domain
        .blocking(move || {
            let _guard = guard;
            let _admission = admission;
            let outcome = (|| {
                let mut receiver = pool.receive(endpoint.applier.clone(), scratch, directory)?;
                loop {
                    if !endpoint.active.load(Ordering::Acquire) {
                        return Err(CellGroupCheckpointError::Invalid("revoked receiver"));
                    }
                    match rx.blocking_recv() {
                        Some(Input::Chunk(chunk)) => chunk.append(&mut receiver)?,
                        Some(Input::End) => return receiver.finish(),
                        None => return Err(CellGroupCheckpointError::Invalid("incomplete stream")),
                    }
                }
            })()
            .map_err(checkpoint_error);
            let _ = tx.send(outcome);
        })
        .map_err(|_| TransportError::Closed)?
        .detach();
    Ok(Receive { input, result })
}
