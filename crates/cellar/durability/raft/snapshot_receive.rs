// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Two-window assembly with one registered physical owner. The network never
//! owns a written scratch file, including between download and SQL sealing.

use super::super::{State, TransportError};
use super::snapshot_error;
use crate::asyncrt::TaskHandle;
use crate::raft::{
    budget::Permit,
    performance::{Counter, Operation, Registry, Timer},
    snapshot::{PreparedSeed, PreparedSnapshot, SeedDownload, SnapshotDownload, SnapshotError},
};
use cellar_ltx::generation::GenerationStore;
use std::{
    future::Future,
    sync::{mpsc, Arc},
};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

pub(super) trait Body: Send + 'static {
    type Sealed: Send + 'static;

    fn encoded_length(&self) -> usize;

    fn append(&mut self, offset: usize, bytes: &[u8]) -> Result<(), SnapshotError>;

    fn seal(
        self,
        images: &GenerationStore,
        reservation: Permit,
    ) -> Result<Self::Sealed, SnapshotError>;
}

impl Body for SnapshotDownload {
    type Sealed = (PreparedSnapshot, Permit);

    fn encoded_length(&self) -> usize {
        self.encoded_length()
    }

    fn append(&mut self, offset: usize, bytes: &[u8]) -> Result<(), SnapshotError> {
        self.append(offset, bytes)
    }

    fn seal(
        self,
        images: &GenerationStore,
        reservation: Permit,
    ) -> Result<Self::Sealed, SnapshotError> {
        Ok((self.finish(images)?, reservation))
    }
}

impl Body for SeedDownload {
    type Sealed = PreparedSeed;

    fn encoded_length(&self) -> usize {
        self.encoded_length()
    }

    fn append(&mut self, offset: usize, bytes: &[u8]) -> Result<(), SnapshotError> {
        self.append(offset, bytes)
    }

    fn seal(
        self,
        images: &GenerationStore,
        reservation: Permit,
    ) -> Result<Self::Sealed, SnapshotError> {
        self.finish(images, reservation)
    }
}

pub(super) struct Chunk {
    pub offset: usize,
    pub bytes: Vec<u8>,
    pub queued: Timer,
}

struct Charge {
    _slot: OwnedSemaphorePermit,
    _memory: Arc<Permit>,
}

struct Frame {
    // Drop bytes before returning the slot or refunding their memory charge.
    chunk: Chunk,
    _charge: Charge,
}

enum Request<T> {
    Append(Frame),
    End,
    Seal {
        images: GenerationStore,
        reply: mpsc::SyncSender<Result<T, SnapshotError>>,
    },
    Abort,
}

/// At most two payloads exist across the network, queue and writer. Acquire a
/// slot before allocating/reading a body; sending transfers that slot with it.
pub(super) struct Input<T> {
    requests: mpsc::Sender<Request<T>>,
    windows: Arc<Semaphore>,
    memory: Arc<Permit>,
}

pub(super) struct Slot<T> {
    requests: mpsc::Sender<Request<T>>,
    charge: Charge,
}

impl<T> Input<T> {
    pub async fn reserve(&self) -> Result<Slot<T>, TransportError> {
        let slot = self
            .windows
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| TransportError::Closed)?;
        Ok(Slot {
            requests: self.requests.clone(),
            charge: Charge {
                _slot: slot,
                _memory: self.memory.clone(),
            },
        })
    }
}

impl<T> Slot<T> {
    pub fn send(self, chunk: Chunk) -> Result<(), TransportError> {
        self.requests
            .send(Request::Append(Frame {
                chunk,
                _charge: self.charge,
            }))
            .map_err(|_| TransportError::Closed)
    }
}

struct AbortOnDrop<T> {
    requests: mpsc::Sender<Request<T>>,
    windows: Arc<Semaphore>,
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.windows.close();
        let _ = self.requests.send(Request::Abort);
    }
}

/// Metadata and a mailbox only. Dropping this handle never closes the physical
/// file on an async driver; the registered worker retains both file and charge.
pub(super) struct Received<T> {
    control: AbortOnDrop<T>,
    worker: TaskHandle<()>,
}

impl<T> Received<T> {
    pub fn seal(self, images: &GenerationStore) -> Result<T, SnapshotError> {
        let (reply, result) = mpsc::sync_channel(1);
        self.control
            .requests
            .send(Request::Seal {
                images: images.clone(),
                reply,
            })
            .map_err(|_| SnapshotError::Invalid("snapshot receive owner closed"))?;
        result
            .recv()
            .map_err(|_| SnapshotError::Invalid("snapshot receive owner closed"))?
    }

    async fn close(self) {
        drop(self.control);
        let _ = self.worker.await;
    }
}

struct Physical<D> {
    // The physical file closes before its own share of the memory refund.
    body: D,
    reservation: Arc<Permit>,
}

pub(super) async fn collect<D, F, Fut>(
    state: &Arc<State>,
    body: D,
    reservation: Permit,
    produce: F,
) -> Result<Received<D::Sealed>, TransportError>
where
    D: Body,
    F: FnOnce(Input<D::Sealed>) -> Fut,
    Fut: Future<Output = Result<(), TransportError>>,
{
    // Both reservations precede work submission, so shutdown cannot overlook
    // either the physical owner or its stop observer. Admission bounds one pair
    // per node, independent of the number of cells or image size.
    let physical_guard = state.reserve_task()?;
    let stop_guard = state.reserve_task()?;
    // Control messages must wake an idle owner even if payload slots are full.
    // Two semaphore permits bound Append messages; the non-clonable received
    // handle and stop guard bound all control senders/messages independently.
    let (requests, commands) = mpsc::channel();
    let (ready, assembled) = oneshot::channel();
    let (done, stopped) = oneshot::channel::<()>();
    let windows = Arc::new(Semaphore::new(2));
    let reservation = Arc::new(reservation);
    let input = Input {
        requests: requests.clone(),
        windows: windows.clone(),
        memory: reservation.clone(),
    };
    let abort = AbortOnDrop {
        requests: requests.clone(),
        windows: windows.clone(),
    };
    let mut stop = state.stop.subscribe();
    let performance = state.performance.clone();
    let worker = state
        .domain
        .blocking(move || {
            let _guard = physical_guard;
            assemble(
                Physical { body, reservation },
                commands,
                ready,
                &performance,
            );
            let _ = done.send(());
        })
        .map_err(|_| TransportError::Closed)?;
    let received = Received {
        control: AbortOnDrop { requests, windows },
        worker,
    };
    state.spawn_reserved(stop_guard, async move {
        // Constructed outside the future: cancellation before its first poll
        // still wakes the physical owner, including while it awaits SQL sealing.
        let _abort = abort;
        if *stop.borrow() {
            return;
        }
        crate::asyncrt::select! { _ = stop.changed() => (), _ = stopped => (), }
    })?;

    // File work proceeds on its worker while the network fills the second
    // window. End is sent only after the producer has passed all final route
    // checks and released its own buffers. Abort does not depend on a paused
    // caller polling or dropping a data-channel sender.
    let network = produce(input).await;
    let ending = if network.is_ok() {
        Request::End
    } else {
        Request::Abort
    };
    let _ = received.control.requests.send(ending);
    let physical = assembled
        .await
        .map_err(|_| TransportError::Closed)
        .and_then(|result| result.map_err(snapshot_error));
    let result = match network {
        Err(TransportError::Closed) => physical.and(Err(TransportError::Closed)),
        Err(error) => Err(error),
        Ok(()) => physical,
    };
    if let Err(error) = result {
        received.close().await;
        return Err(error);
    }
    Ok(received)
}

fn assemble<D: Body>(
    mut owner: Physical<D>,
    commands: mpsc::Receiver<Request<D::Sealed>>,
    ready: oneshot::Sender<Result<(), SnapshotError>>,
    performance: &Arc<Registry>,
) {
    let total = owner.body.encoded_length();
    let mut offset = 0usize;
    let assembled = (|| {
        while let Ok(request) = commands.recv() {
            match request {
                Request::Append(frame) => {
                    frame.chunk.queued.finish(true);
                    let timer = performance.timer(Operation::RecoveryAppend);
                    let bytes = &frame.chunk.bytes;
                    let result = if frame.chunk.offset != offset
                        || bytes.is_empty()
                        || offset
                            .checked_add(bytes.len())
                            .is_none_or(|end| end > total)
                    {
                        Err(SnapshotError::Invalid("snapshot receive order or bound"))
                    } else {
                        owner.body.append(offset, bytes)
                    };
                    timer.finish(result.is_ok());
                    result?;
                    offset += bytes.len();
                    performance.increment(Counter::RecoveryPeerAppendedBytes, bytes.len() as u64);
                }
                Request::End if offset == total => return Ok(()),
                _ => {
                    return Err(SnapshotError::Invalid(
                        "incomplete or aborted snapshot receive",
                    ))
                }
            }
        }
        Err(SnapshotError::Invalid("snapshot receive owner closed"))
    })();
    if let Err(error) = assembled {
        // Pending frame buffers retain their own share of the reservation.
        // Close them and the physical file before signalling failure.
        drop(commands);
        drop(owner);
        let _ = ready.send(Err(error));
        return;
    }
    if ready.send(Ok(())).is_err() {
        return;
    }
    if let Ok(Request::Seal { images, reply }) = commands.recv() {
        let result = match Arc::try_unwrap(owner.reservation) {
            Ok(reservation) => owner.body.seal(&images, reservation),
            Err(reservation) => {
                drop(owner.body);
                drop(reservation);
                Err(SnapshotError::Invalid(
                    "snapshot receive buffers still retained",
                ))
            }
        };
        let _ = reply.send(result);
    }
}

#[cfg(test)]
#[path = "snapshot_receive_tests.rs"]
mod tests;
