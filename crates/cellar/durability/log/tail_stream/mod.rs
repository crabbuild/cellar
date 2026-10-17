//! Immutable derived tail snapshots, transferred and verified in bounded chunks.
//! A transfer is never a witness until its original identity, full digest,
//! counts and sequence coverage verify. Scratch does not supply authority.
use super::{recovery_rows, Entry, FollowerStore, TailRange, TailReq, TailWireReq};
use anyhow::{ensure, Context, Result};
use cellar_ltx::{FileSystem, HostFile};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

mod progress;

const CHUNK_BYTES: usize = 1 << 20;
const MANIFEST_BYTES: usize = 109;
const ENTRY_HEADER_BYTES: usize = 34;
const REQUEST_BYTES: usize = 4096;
const IO_IDLE: Duration = Duration::from_secs(10);

#[derive(Clone, Copy)]
struct Manifest {
    leader: [u8; 32],
    range: TailRange,
    entries: u64,
    bytes: u64,
    digest: [u8; 32],
}

impl Manifest {
    fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(MANIFEST_BYTES);
        bytes.extend_from_slice(b"CTS1");
        bytes.extend_from_slice(&self.leader);
        for value in [self.range.fragment_epoch, self.range.base, self.range.end] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes.push(u8::from(self.range.complete));
        bytes.extend_from_slice(&self.entries.to_le_bytes());
        bytes.extend_from_slice(&self.bytes.to_le_bytes());
        bytes.extend_from_slice(&self.digest);
        bytes
    }

    fn decode(bytes: &[u8], leader: &str) -> Result<Self> {
        ensure!(
            bytes.len() == MANIFEST_BYTES && &bytes[..4] == b"CTS1",
            "unsupported tail snapshot manifest"
        );
        let value = |at| u64::from_le_bytes(bytes[at..at + 8].try_into().expect("fixed manifest"));
        let manifest = Self {
            leader: bytes[4..36].try_into()?,
            range: TailRange {
                fragment_epoch: value(36),
                base: value(44),
                end: value(52),
                complete: match bytes[60] {
                    0 => false,
                    1 => true,
                    _ => anyhow::bail!("invalid tail completeness"),
                },
            },
            entries: value(61),
            bytes: value(69),
            digest: bytes[77..109].try_into()?,
        };
        ensure!(
            manifest.leader == <[u8; 32]>::from(Sha256::digest(leader.as_bytes())),
            "tail snapshot leader mismatch"
        );
        ensure!(
            manifest.range.base <= manifest.range.end,
            "tail range is inverted"
        );
        ensure!(
            manifest
                .entries
                .checked_mul(ENTRY_HEADER_BYTES as u64)
                .is_some_and(|minimum| minimum <= manifest.bytes),
            "tail snapshot count exceeds body"
        );
        ensure!(
            manifest.entries != 0 || manifest.bytes == 0,
            "empty tail has payload bytes"
        );
        if manifest.range.complete {
            ensure!(
                manifest.entries == manifest.range.end - manifest.range.base,
                "complete tail count does not cover range"
            );
        }
        Ok(manifest)
    }
}

pub(super) struct Snapshot {
    manifest: Manifest,
    file: Option<HostFile>,
    // Limit active builders/transfers per follower, including stalled clients.
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl Snapshot {
    pub fn range(&self) -> TailRange {
        self.manifest.range
    }

    fn check_length(&mut self) -> Result<()> {
        match &mut self.file {
            Some(file) => ensure!(
                file.file_len()? == self.manifest.bytes,
                "tail scratch length changed"
            ),
            None => ensure!(self.manifest.bytes == 0, "tail scratch absent"),
        }
        Ok(())
    }

    fn take(&mut self, offset: &mut u64, count: usize, digest: &mut Sha256) -> Result<Vec<u8>> {
        ensure!(
            count as u64 <= self.manifest.bytes.saturating_sub(*offset),
            "truncated tail snapshot entry"
        );
        let bytes = exact(
            self.file.as_mut().context("tail scratch absent")?,
            *offset,
            count,
        )?;
        *offset += count as u64;
        digest.update(&bytes);
        Ok(bytes)
    }

    /// Callbacks may only populate derived scratch. A late digest/read failure
    /// must abort the entire gather, never retain these rows and skip this peer.
    pub(super) fn visit(
        &mut self,
        cancelled: &AtomicBool,
        mut entry: impl FnMut(Entry) -> Result<()>,
    ) -> Result<()> {
        self.check_length()?;
        let mut offset = 0_u64;
        let mut count = 0_u64;
        let mut previous = self.manifest.range.base;
        let mut digest = Sha256::new();
        while offset < self.manifest.bytes {
            check_cancelled(cancelled)?;
            let header = self.take(&mut offset, 14, &mut digest)?;
            ensure!(
                &header[..4] == super::ENTRY_MAGIC,
                "invalid tail snapshot entry magic"
            );
            let seq = u64::from_le_bytes(header[4..12].try_into()?);
            ensure!(
                seq > previous,
                "tail snapshot sequences are not strictly ordered"
            );
            if self.manifest.range.complete {
                ensure!(
                    previous.checked_add(1) == Some(seq),
                    "complete tail snapshot has a gap"
                );
            }
            previous = seq;
            let cell_length = u16::from_le_bytes(header[12..14].try_into()?) as usize;
            let cell = self.take(&mut offset, cell_length, &mut digest)?;
            let cell = std::str::from_utf8(&cell)?.to_owned();
            let metadata = self.take(&mut offset, 20, &mut digest)?;
            let cell_epoch = u64::from_le_bytes(metadata[..8].try_into()?);
            let txid = u64::from_le_bytes(metadata[8..16].try_into()?);
            let length = u32::from_le_bytes(metadata[16..20].try_into()?) as usize;
            ensure!(
                length <= super::COMMITTED_BATCH_ENTRY_CAP - ENTRY_HEADER_BYTES - cell_length,
                "tail entry exceeds admitted writer bound"
            );
            let bytes = self.take(&mut offset, length, &mut digest)?;
            count = count.checked_add(1).context("tail count overflow")?;
            ensure!(
                count <= self.manifest.entries,
                "tail snapshot has excess entries"
            );
            entry(Entry {
                seq,
                cell,
                cell_epoch,
                txid,
                bytes,
            })?;
        }
        ensure!(
            count == self.manifest.entries,
            "tail snapshot count mismatch"
        );
        if self.manifest.range.complete {
            ensure!(
                previous == self.manifest.range.end,
                "complete tail snapshot has a missing suffix"
            );
        }
        ensure!(
            <[u8; 32]>::from(digest.finalize()) == self.manifest.digest,
            "tail snapshot digest mismatch"
        );
        self.check_length()?;
        check_cancelled(cancelled)
    }

    pub fn gather_into(
        mut self,
        mut rows: recovery_rows::Rows,
        priority: recovery_rows::Priority,
        duplicates: recovery_rows::Duplicates,
        cell: Option<&str>,
    ) -> Result<recovery_rows::Rows> {
        let cancelled = rows.cancellation();
        self.visit(&cancelled, |entry| {
            if cell.is_none_or(|cell| cell == entry.cell) {
                rows.insert(
                    (entry.cell, entry.cell_epoch, entry.txid),
                    entry.bytes,
                    priority,
                    duplicates,
                )?;
            }
            Ok(())
        })?;
        Ok(rows)
    }

    fn chunk(&mut self, offset: u64) -> Result<Vec<u8>> {
        self.check_length()?;
        ensure!(
            offset <= self.manifest.bytes,
            "tail chunk offset outside snapshot"
        );
        let size = usize::try_from((self.manifest.bytes - offset).min(CHUNK_BYTES as u64))?;
        if size == 0 {
            return Ok(Vec::new());
        }
        exact(
            self.file.as_mut().context("tail scratch absent")?,
            offset,
            size,
        )
    }
}

impl FollowerStore {
    #[cfg(test)]
    pub(super) async fn snapshot_tail(
        self: &Arc<Self>,
        req: TailReq,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Snapshot> {
        self.snapshot_tail_with_progress(req, cancelled, Arc::default())
            .await
    }

    async fn snapshot_tail_with_progress(
        self: &Arc<Self>,
        req: TailReq,
        cancelled: Arc<AtomicBool>,
        progress: Arc<AtomicU64>,
    ) -> Result<Snapshot> {
        super::validate_log_leader(&req.leader)?;
        let permit = self
            .tail_transfers
            .clone()
            .try_acquire_owned()
            .context("follower tail transfer capacity exhausted")?;
        let guard = self.guard(&req.leader).lock_owned().await;
        let store = self.clone();
        let flag = cancelled.clone();
        let filesystem: Arc<dyn FileSystem> = Arc::new(progress::ProgressFs {
            inner: store.filesystem.clone(),
            cancelled: flag.clone(),
            progress,
        });
        owned_io(cancelled, move || {
            let _guard = guard;
            let mut rows = recovery_rows::Rows::new(filesystem.clone(), store.root.clone())
                .with_cancellation(flag.clone());
            let range = store.visit_tail(filesystem.as_ref(), &req, &flag, |entry| {
                let mut encoded = Vec::new();
                let seq = entry.seq;
                super::encode_entry(&entry, &mut encoded);
                rows.insert(
                    (String::new(), 0, seq),
                    encoded,
                    recovery_rows::Priority::Preferred,
                    recovery_rows::Duplicates::First,
                )
            })?;
            let mut rows = rows.finish()?;
            let mut snapshot = Snapshot {
                manifest: Manifest {
                    leader: Sha256::digest(req.leader.as_bytes()).into(),
                    range,
                    entries: 0,
                    bytes: 0,
                    digest: [0; 32],
                },
                file: None,
                _permit: Some(permit),
            };
            let mut previous = range.base;
            let mut contiguous = true;
            let mut digest = Sha256::new();
            loop {
                check_cancelled(&flag)?;
                let batch = rows.next_batch()?;
                if batch.is_empty() {
                    break;
                }
                for ((_, _, seq), bytes) in batch {
                    if previous.checked_add(1) != Some(seq) {
                        contiguous = false;
                    }
                    previous = seq;
                    if snapshot.file.is_none() {
                        snapshot.file = Some(scratch(&*filesystem, &store.root)?);
                    }
                    snapshot
                        .file
                        .as_mut()
                        .expect("snapshot opened")
                        .write_all(&bytes)?;
                    snapshot.manifest.bytes = snapshot
                        .manifest
                        .bytes
                        .checked_add(bytes.len() as u64)
                        .context("tail bytes overflow")?;
                    snapshot.manifest.entries = snapshot
                        .manifest
                        .entries
                        .checked_add(1)
                        .context("tail count overflow")?;
                    digest.update(&bytes);
                }
            }
            snapshot.manifest.range.complete &= contiguous && previous == range.end;
            snapshot.manifest.digest = digest.finalize().into();
            // Verify the actual written bytes before exposing the manifest or
            // releasing the leader guard. Failed scratch is never a witness.
            snapshot.visit(&flag, |_| Ok(()))?;
            Ok(snapshot)
        })
        .await
    }
}

/// Serve one authenticated upgraded tail connection. The caller owns its domain
/// task and upgrade; no append, seal, adoption or GC lock spans network waiting.
pub async fn serve_tail_stream<IO>(mut io: IO, store: Arc<FollowerStore>) -> Result<()>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let cancelled = Arc::new(AtomicBool::new(false));
    let mut stop = StopOnDrop(Some(cancelled.clone()));
    let request = read_limited(&mut io, REQUEST_BYTES).await?;
    let request: TailWireReq = serde_json::from_slice(&request)?;
    let progress = Arc::new(AtomicU64::new(0));
    let preparing =
        store.snapshot_tail_with_progress(request.request, cancelled.clone(), progress.clone());
    futures_util::pin_mut!(preparing);
    let (mut input, mut output) = tokio::io::split(io);
    let disconnected = async {
        let mut byte = [0];
        input.read(&mut byte).await
    };
    futures_util::pin_mut!(disconnected);
    let mut heartbeat = crate::asyncrt::interval(Duration::from_secs(1));
    let mut observed = 0;
    let mut snapshot = loop {
        crate::asyncrt::select! {
            result = &mut preparing => break result?,
            result = &mut disconnected => {
                let count = result?;
                anyhow::bail!("tail client ended or sent excess request bytes while preparing: {count}");
            },
            _ = heartbeat.tick() => {
                let current = progress.load(Ordering::Relaxed);
                if current != observed {
                    send(&mut output, b"CTP1").await?;
                    observed = current;
                }
            },
        }
    };
    send(&mut output, &snapshot.manifest.encode()).await?;
    let mut offset = 0_u64;
    loop {
        let (returned, chunk) = owned_io(cancelled.clone(), move || {
            let chunk = snapshot.chunk(offset)?;
            Ok((snapshot, chunk))
        })
        .await?;
        snapshot = returned;
        send(&mut output, &chunk).await?;
        if chunk.is_empty() {
            break;
        }
        offset += chunk.len() as u64;
        cooperate().await;
    }
    stop.0 = None;
    Ok(())
}

/// Stage one peer independently. No rows escape before the final empty frame,
/// whole digest, counts, leader identity and original sequence range verify.
pub(super) async fn receive<IO>(
    mut io: IO,
    req: &TailReq,
    filesystem: Arc<dyn FileSystem>,
    directory: PathBuf,
    cancelled: Arc<AtomicBool>,
) -> Result<Snapshot>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    send(
        &mut io,
        &serde_json::to_vec(&TailWireReq::ranged(req.clone()))?,
    )
    .await?;
    let manifest = loop {
        let header = read_limited(&mut io, MANIFEST_BYTES).await?;
        if header == b"CTP1" {
            // Progress extends only the network idle interval. It carries no
            // range, data or witness authority, and never resets a caller's
            // operation deadline. The outer recovery wait keeps its claim.
            cooperate().await;
            continue;
        }
        break Manifest::decode(&header, &req.leader)?;
    };
    let mut snapshot = Snapshot {
        manifest,
        file: None,
        _permit: None,
    };
    let mut received = 0_u64;
    let mut digest = Sha256::new();
    loop {
        check_cancelled(&cancelled)?;
        let chunk = read_limited(&mut io, CHUNK_BYTES).await?;
        if chunk.is_empty() {
            break;
        }
        received = received
            .checked_add(chunk.len() as u64)
            .context("tail receive overflow")?;
        ensure!(
            received <= manifest.bytes,
            "tail stream exceeds snapshot length"
        );
        digest.update(&chunk);
        snapshot = owned_io(cancelled.clone(), {
            let filesystem = filesystem.clone();
            let directory = directory.clone();
            move || {
                if snapshot.file.is_none() {
                    snapshot.file = Some(scratch(&*filesystem, &directory)?);
                }
                snapshot
                    .file
                    .as_mut()
                    .expect("received snapshot opened")
                    .write_all(&chunk)?;
                Ok(snapshot)
            }
        })
        .await?;
        cooperate().await;
    }
    ensure!(
        received == manifest.bytes && <[u8; 32]>::from(digest.finalize()) == manifest.digest,
        "incomplete or corrupt tail stream"
    );
    let mut trailing = [0];
    let eof = crate::asyncrt::timeout(IO_IDLE, io.read(&mut trailing))
        .await
        .context("tail stream did not close after completion")??;
    ensure!(eof == 0, "trailing bytes after tail snapshot completion");
    let flag = cancelled.clone();
    owned_io(cancelled, move || {
        snapshot.visit(&flag, |_| Ok(()))?;
        Ok(snapshot)
    })
    .await
}

async fn read_limited<R: AsyncRead + Unpin>(io: &mut R, limit: usize) -> Result<Vec<u8>> {
    crate::asyncrt::timeout(IO_IDLE, async {
        let mut prefix = [0; 4];
        io.read_exact(&mut prefix)
            .await
            .context("tail stream ended before a complete frame")?;
        let length = u32::from_le_bytes(prefix) as usize;
        ensure!(
            length <= limit,
            "tail frame exceeds bound before allocation"
        );
        let mut bytes = vec![0; length];
        io.read_exact(&mut bytes)
            .await
            .context("tail stream ended inside a frame")?;
        Ok(bytes)
    })
    .await
    .context("tail frame idle deadline")?
}

async fn send<W: AsyncWrite + Unpin>(io: &mut W, bytes: &[u8]) -> Result<()> {
    crate::asyncrt::timeout(IO_IDLE, super::write_frame(io, bytes))
        .await
        .context("tail write idle deadline")?
}

fn scratch(filesystem: &dyn FileSystem, directory: &std::path::Path) -> Result<HostFile> {
    filesystem.create_dir_all(directory)?;
    Ok(filesystem.temporary_file(Some(directory))?)
}

fn exact(file: &mut HostFile, offset: u64, length: usize) -> Result<Vec<u8>> {
    let bytes = file.read_exact_at(offset, length)?;
    ensure!(bytes.len() == length, "short tail scratch read");
    Ok(bytes)
}

fn check_cancelled(cancelled: &AtomicBool) -> Result<()> {
    ensure!(
        !cancelled.load(Ordering::Acquire),
        "tail transfer cancelled"
    );
    Ok(())
}

pub(super) struct StopOnDrop(pub Option<Arc<AtomicBool>>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        if let Some(flag) = &self.0 {
            flag.store(true, Ordering::Release);
        }
    }
}

async fn owned_io<T: Send + 'static>(
    cancelled: Arc<AtomicBool>,
    operation: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    let mut stop = StopOnDrop(Some(cancelled.clone()));
    let result = crate::asyncrt::try_blocking(move || {
        check_cancelled(&cancelled)?;
        operation()
    })?
    .await??;
    stop.0 = None;
    Ok(result)
}

async fn cooperate() {
    let mut yielded = false;
    futures_util::future::poll_fn(|context| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            context.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await
}

#[cfg(test)]
mod tests;
