// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(clippy::disallowed_methods)]

use super::*;
use crate::raft::transport::tests::Fixture;
use anyhow::{ensure, Result};
use std::{
    fs::File,
    io::{Read, Seek, Write},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Condvar, Mutex,
    },
    time::Duration,
};
use tokio::sync::Notify;

const CHUNK: usize = 64 * 1024;

#[derive(Default)]
struct Gate {
    entered: Notify,
    released: Mutex<bool>,
    wake: Condvar,
}

impl Gate {
    fn pause(&self) {
        self.entered.notify_one();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.wake.wait(released).unwrap();
        }
    }
}

struct Release(Arc<Gate>);

impl Drop for Release {
    fn drop(&mut self) {
        *self.0.released.lock().unwrap() = true;
        self.0.wake.notify_all();
    }
}

struct FileBody {
    file: Option<File>,
    total: usize,
    write_gate: Option<Arc<Gate>>,
    close_gate: Option<Arc<Gate>>,
    written: Arc<AtomicUsize>,
    closed: Arc<AtomicBool>,
    full: bool,
}

impl Drop for FileBody {
    fn drop(&mut self) {
        if let Some(gate) = self.close_gate.take() {
            gate.pause();
        }
        drop(self.file.take());
        self.closed.store(true, Ordering::Release);
    }
}

fn io(error: std::io::Error) -> SnapshotError {
    SnapshotError::Physical(error.into())
}

impl Body for FileBody {
    type Sealed = (Vec<u8>, Permit);

    fn encoded_length(&self) -> usize {
        self.total
    }

    fn append(&mut self, offset: usize, bytes: &[u8]) -> Result<(), SnapshotError> {
        if let Some(gate) = self.write_gate.take() {
            gate.pause();
        }
        if self.full {
            return Err(SnapshotError::Physical(
                cellar_ltx::stream_snapshot::Error::ScratchFull,
            ));
        }
        let file = self.file.as_mut().unwrap();
        if file.stream_position().map_err(io)? != offset as u64 {
            return Err(SnapshotError::Invalid("test file offset"));
        }
        file.write_all(bytes).map_err(io)?;
        self.written.fetch_add(1, Ordering::Release);
        Ok(())
    }

    fn seal(
        mut self,
        _images: &GenerationStore,
        reservation: Permit,
    ) -> Result<Self::Sealed, SnapshotError> {
        let file = self.file.as_mut().unwrap();
        file.sync_all().map_err(io)?;
        file.rewind().map_err(io)?;
        let mut bytes = vec![];
        file.read_to_end(&mut bytes).map_err(io)?;
        drop(self);
        Ok((bytes, reservation))
    }
}

fn body(directory: &std::path::Path, count: usize) -> Result<FileBody> {
    Ok(FileBody {
        file: Some(
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(directory.join("receive.body"))?,
        ),
        total: count * CHUNK,
        write_gate: None,
        close_gate: None,
        written: Arc::new(AtomicUsize::new(0)),
        closed: Arc::new(AtomicBool::new(false)),
        full: false,
    })
}

fn chunk(index: usize, performance: &Arc<Registry>) -> Chunk {
    Chunk {
        offset: index * CHUNK,
        bytes: vec![index as u8 + 1; CHUNK],
        queued: performance.timer(Operation::RecoveryAppendQueue),
    }
}

async fn wait_count(count: &AtomicUsize, expected: usize) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(3), async {
        while count.load(Ordering::Acquire) != expected {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_window_overlaps_a_held_write_and_third_is_backpressured() -> Result<()> {
    let fixture = Fixture::start(Default::default(), vec![]).await?;
    let directory = tempfile::tempdir()?;
    let mut body = body(directory.path(), 3)?;
    let gate = Arc::new(Gate::default());
    let release = Release(gate.clone());
    body.write_gate = Some(gate.clone());
    let state = &fixture.transport.handle.0;
    let reservation = state
        .snapshot_receive_budget
        .acquire(super::super::memory_bound(&state.options))
        .unwrap();
    let sent = Arc::new(AtomicUsize::new(0));
    let observed = sent.clone();
    let performance = state.performance.clone();
    let transfer = collect(state, body, reservation, |input| async move {
        for index in 0..3 {
            let slot = input.reserve().await?;
            sent.fetch_add(1, Ordering::Release);
            slot.send(chunk(index, &performance))?;
        }
        Ok(())
    });
    let inspect = async move {
        let _release = release;
        tokio::time::timeout(Duration::from_secs(3), gate.entered.notified()).await?;
        wait_count(&observed, 2).await?;
        tokio::time::sleep(Duration::from_millis(30)).await;
        ensure!(
            observed.load(Ordering::Acquire) == 2,
            "unbounded third receive window"
        );
        Ok::<_, anyhow::Error>(())
    };
    let (received, checked) = futures_util::future::join(transfer, inspect).await;
    checked?;
    let received = received?;
    let images = GenerationStore::open(
        directory.path().join("images"),
        Arc::new(cellar_ltx::DirectFileSystem),
        Default::default(),
    )?;
    let (bytes, permit) = fixture
        .domain
        .blocking(move || received.seal(&images))?
        .await??;
    ensure!(bytes == [vec![1; CHUNK], vec![2; CHUNK], vec![3; CHUNK]].concat());
    drop(permit);
    ensure!(state.snapshot_receive_budget.usage() == (0, 0));
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_retains_charge_through_held_write_and_file_close() -> Result<()> {
    let fixture = Fixture::start(Default::default(), vec![]).await?;
    let directory = tempfile::tempdir()?;
    let mut body = body(directory.path(), 3)?;
    let write = Arc::new(Gate::default());
    let close = Arc::new(Gate::default());
    let write_release = Release(write.clone());
    let close_release = Release(close.clone());
    body.write_gate = Some(write.clone());
    body.close_gate = Some(close.clone());
    let state = &fixture.transport.handle.0;
    let budget = &state.snapshot_receive_budget;
    let bytes = super::super::memory_bound(&state.options);
    let performance = state.performance.clone();
    let mut transfer = Box::pin(collect(
        state,
        body,
        budget.acquire(bytes).unwrap(),
        |input| async move {
            for index in 0..3 {
                input.reserve().await?.send(chunk(index, &performance))?;
            }
            Ok(())
        },
    ));
    ensure!(futures_util::poll!(&mut transfer).is_pending());
    tokio::time::timeout(Duration::from_secs(3), write.entered.notified()).await?;
    drop(transfer);
    ensure!(budget.usage() == (bytes, 1));
    let mut closing = Box::pin(fixture.transport.close());
    ensure!(
        tokio::time::timeout(Duration::from_millis(30), &mut closing)
            .await
            .is_err()
    );
    drop(write_release);
    tokio::time::timeout(Duration::from_secs(3), close.entered.notified()).await?;
    ensure!(
        budget.usage() == (bytes, 1),
        "refunded before physical close"
    );
    ensure!(
        tokio::time::timeout(Duration::from_millis(30), &mut closing)
            .await
            .is_err()
    );
    drop(close_release);
    tokio::time::timeout(Duration::from_secs(3), &mut closing).await?;
    drop(closing);
    ensure!(budget.usage() == (0, 0));
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_stops_the_file_owner_without_polling_a_retained_network_future() -> Result<()> {
    let fixture = Fixture::start(Default::default(), vec![]).await?;
    let directory = tempfile::tempdir()?;
    let body = body(directory.path(), 1)?;
    let written = body.written.clone();
    let closed = body.closed.clone();
    let state = &fixture.transport.handle.0;
    let budget = &state.snapshot_receive_budget;
    let bytes = super::super::memory_bound(&state.options);
    let performance = state.performance.clone();
    let mut transfer = Box::pin(collect(
        state,
        body,
        budget.acquire(bytes).unwrap(),
        |input| async move {
            input.reserve().await?.send(chunk(0, &performance))?;
            let result = std::future::pending::<Result<(), TransportError>>().await;
            drop(input);
            result
        },
    ));
    ensure!(futures_util::poll!(&mut transfer).is_pending());
    wait_count(&written, 1).await?;
    tokio::time::timeout(Duration::from_secs(3), fixture.transport.close()).await?;
    ensure!(closed.load(Ordering::Acquire));
    ensure!(
        budget.usage() == (bytes, 1),
        "retained network memory was refunded early"
    );
    drop(transfer);
    ensure!(budget.usage() == (0, 0));
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_discards_an_assembled_file_while_its_handle_is_retained() -> Result<()> {
    let fixture = Fixture::start(Default::default(), vec![]).await?;
    let directory = tempfile::tempdir()?;
    let body = body(directory.path(), 1)?;
    let closed = body.closed.clone();
    let state = &fixture.transport.handle.0;
    let performance = state.performance.clone();
    let permit = state
        .snapshot_receive_budget
        .acquire(super::super::memory_bound(&state.options))
        .unwrap();
    let received = collect(state, body, permit, |input| async move {
        input.reserve().await?.send(chunk(0, &performance))?;
        Ok(())
    })
    .await?;
    ensure!(!closed.load(Ordering::Acquire));
    tokio::time::timeout(Duration::from_secs(3), fixture.transport.close()).await?;
    ensure!(closed.load(Ordering::Acquire));
    ensure!(state.snapshot_receive_budget.usage() == (0, 0));
    drop(received);
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn physical_failure_is_not_hidden_by_the_closed_network_window() -> Result<()> {
    let fixture = Fixture::start(Default::default(), vec![]).await?;
    let directory = tempfile::tempdir()?;
    let mut body = body(directory.path(), 10)?;
    body.full = true;
    let closed = body.closed.clone();
    let state = &fixture.transport.handle.0;
    let performance = state.performance.clone();
    let permit = state
        .snapshot_receive_budget
        .acquire(super::super::memory_bound(&state.options))
        .unwrap();
    let result = collect(state, body, permit, |input| async move {
        for index in 0..10 {
            input.reserve().await?.send(chunk(index, &performance))?;
        }
        Ok(())
    })
    .await;
    ensure!(matches!(result, Err(TransportError::Overloaded)));
    ensure!(closed.load(Ordering::Acquire));
    ensure!(state.snapshot_receive_budget.usage() == (0, 0));
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn network_authentication_failure_never_yields_an_assembled_handle() -> Result<()> {
    let fixture = Fixture::start(Default::default(), vec![]).await?;
    let directory = tempfile::tempdir()?;
    let body = body(directory.path(), 3)?;
    let closed = body.closed.clone();
    let state = &fixture.transport.handle.0;
    let performance = state.performance.clone();
    let permit = state
        .snapshot_receive_budget
        .acquire(super::super::memory_bound(&state.options))
        .unwrap();
    let result = collect(state, body, permit, |input| async move {
        input.reserve().await?.send(chunk(0, &performance))?;
        Err(TransportError::Authentication)
    })
    .await;
    ensure!(matches!(result, Err(TransportError::Authentication)));
    ensure!(closed.load(Ordering::Acquire));
    ensure!(state.snapshot_receive_budget.usage() == (0, 0));
    fixture.close().await
}
