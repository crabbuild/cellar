// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded control metadata and optional owned byte attachments. Application
//! stdout is separate from this authenticated, nonblocking transport.
use super::control;
use anyhow::{ensure, Context as _, Result};
use serde_json::Value;
use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    net::TcpStream,
    sync::Arc,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const HEADER_BYTES: usize = 8;

// A turn always returns to authority retirement and native replies after this
// much socket work per direction. Capacity and maximum frame sizes are separate.
const TURN_IO_BYTES: usize = 1 << 20;

pub(super) fn frame_limit(value: usize) -> Result<usize> {
    // JSON string escaping can expand one native payload byte to six bytes.
    value
        .checked_mul(6)
        .and_then(|n| n.checked_add(4096))
        .context("Node frame limit overflow")
}

pub(super) fn validate_budget(value: usize, bridge: usize) -> Result<()> {
    // One maximum outbound value can coexist with owned control metadata and
    // its encoded frame. Input decoding needs no larger isolated allowance.
    let required = frame_limit(value)?
        .checked_mul(2)
        .and_then(|n| n.checked_add(HEADER_BYTES))
        .and_then(|n| n.checked_add(value))
        .context("Node transport byte budget overflow")?;
    ensure!(
        bridge >= required,
        "Node bridge byte budget cannot fit one maximum framed value: need {required} bytes"
    );
    Ok(())
}

pub(super) struct Frame {
    pub metadata: Value,
    pub body: Option<Vec<u8>>,
    // Decoded control/body ownership survives receive until native admission
    // takes its own reservation, or the frame is refused and dropped.
    pub _bytes: OwnedSemaphorePermit,
}

struct Queued {
    bytes: Vec<u8>,
    _bytes: OwnedSemaphorePermit,
}

struct Control {
    value: Value,
    // Fields drop in declaration order: free control strings before capacity.
    _bytes: OwnedSemaphorePermit,
}

/// Count serialization without allocating a second encoded metadata buffer.
struct MetadataSize {
    bytes: usize,
    maximum: usize,
}

impl Write for MetadataSize {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|size| *size <= self.maximum)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "Node frame exceeds limit")
            })?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) struct Transport {
    socket: TcpStream,
    input: Vec<u8>,
    input_bytes: Option<OwnedSemaphorePermit>,
    output: VecDeque<Queued>,
    // Bytes already written from the first queued frame.
    offset: usize,
    // Retained bytes across frames, including the already-written prefix of
    // the front allocation. Partial writes do not free its backing storage.
    queued: usize,
    // Per-frame metadata+body limit. Every retained buffer shares native
    // invocation/operation/reply admission's aggregate byte semaphore.
    maximum: usize,
    budget: Arc<Semaphore>,
    read_remaining: usize,
    write_remaining: usize,
    decode_remaining: usize,
    receive_yielded: bool,
}

impl Transport {
    pub fn new(socket: TcpStream, maximum: usize, budget: Arc<Semaphore>) -> Result<Self> {
        socket.set_nonblocking(true)?;
        socket.set_nodelay(true)?;
        Ok(Self {
            socket,
            input: Vec::new(),
            input_bytes: None,
            output: VecDeque::new(),
            offset: 0,
            queued: 0,
            maximum,
            budget,
            read_remaining: TURN_IO_BYTES,
            write_remaining: TURN_IO_BYTES,
            decode_remaining: TURN_IO_BYTES,
            receive_yielded: false,
        })
    }

    /// Rearm work quotas without releasing any retained frame reservations.
    /// Reads and writes have independent quotas so neither direction can starve
    /// the other. A logical receive pass may span several of these turns.
    pub fn begin_turn(&mut self) {
        self.read_remaining = TURN_IO_BYTES;
        self.write_remaining = TURN_IO_BYTES;
        self.decode_remaining = TURN_IO_BYTES;
        self.receive_yielded = false;
    }

    /// Distinguish a work-quota yield from an observed empty socket. A yield
    /// alone supplies no evidence about the worker's heartbeat/liveness.
    pub fn receive_yielded(&self) -> bool {
        self.receive_yielded
    }

    pub fn send(&mut self, value: Value) -> Result<()> {
        self.send_parts(value, None)
    }

    pub fn send_parts(&mut self, mut value: Value, body: Option<&[u8]>) -> Result<()> {
        let object = value
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("Node control must be an object"))?;
        object.insert("has_body".into(), Value::Bool(body.is_some()));
        let body = body.unwrap_or_default();
        let mut metadata = MetadataSize {
            bytes: 0,
            maximum: self
                .maximum
                .checked_sub(body.len())
                .context("Node frame exceeds limit")?,
        };
        serde_json::to_writer(&mut metadata, &value)?;
        let payload = metadata
            .bytes
            .checked_add(body.len())
            .ok_or_else(|| anyhow::anyhow!("Node frame size overflow"))?;
        ensure!(payload <= self.maximum, "Node frame exceeds limit");
        let frame_bytes = payload
            .checked_add(HEADER_BYTES)
            .ok_or_else(|| anyhow::anyhow!("Node frame size overflow"))?;
        // The owned Value and encoded frame overlap. Count its serialized
        // length conservatively for control strings, separately from the frame.
        let control = Control {
            value,
            _bytes: self.reserve(metadata.bytes)?,
        };
        let bytes = self.reserve(frame_bytes)?;
        let metadata_length = u32::try_from(metadata.bytes)?;
        let body_length = u32::try_from(body.len())?;
        let mut frame = Vec::with_capacity(frame_bytes);
        frame.extend_from_slice(&metadata_length.to_be_bytes());
        frame.extend_from_slice(&body_length.to_be_bytes());
        serde_json::to_writer(&mut frame, &control.value)?;
        frame.extend_from_slice(body);
        ensure!(
            frame.len() == frame_bytes,
            "Node frame serialization changed size"
        );
        self.queued += frame.len();
        self.output.push_back(Queued {
            bytes: frame,
            _bytes: bytes,
        });
        Ok(())
    }

    fn reserve(&self, bytes: usize) -> Result<OwnedSemaphorePermit> {
        self.budget
            .clone()
            .try_acquire_many_owned(u32::try_from(bytes)?)
            .context("Node transport capacity exhausted")
    }

    pub fn flush(&mut self) -> Result<()> {
        while let Some(frame) = self.output.front() {
            if self.write_remaining == 0 {
                break;
            }
            let end = self.offset + self.write_remaining.min(frame.bytes.len() - self.offset);
            match self.socket.write(&frame.bytes[self.offset..end]) {
                Ok(0) => anyhow::bail!("Node transport closed"),
                Ok(n) => {
                    self.offset += n;
                    self.write_remaining -= n;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => return Err(error.into()),
            }
            if self.offset == frame.bytes.len() {
                self.queued -= frame.bytes.len();
                self.output.pop_front();
                self.offset = 0;
            }
        }
        Ok(())
    }

    /// Watchdog diagnostics must not consume a buffered control or attachment.
    pub fn input_ready(&self) -> std::io::Result<bool> {
        if !self.input.is_empty() {
            return Ok(true);
        }
        match self.socket.peek(&mut [0]) {
            Ok(size) => Ok(size != 0),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub fn receive(&mut self) -> Result<Option<Frame>> {
        self.receive_yielded = false;
        // At most one bounded frame is buffered; never accumulate a peer's flood.
        loop {
            if self.decode_remaining == 0 {
                self.receive_yielded = true;
                return Ok(None);
            }
            let target = if self.input.len() < HEADER_BYTES {
                HEADER_BYTES
            } else {
                let metadata = u32::from_be_bytes(self.input[..4].try_into()?) as usize;
                let body = u32::from_be_bytes(self.input[4..HEADER_BYTES].try_into()?) as usize;
                let payload = metadata
                    .checked_add(body)
                    .ok_or_else(|| anyhow::anyhow!("Node frame size overflow"))?;
                ensure!(
                    metadata > 0 && payload <= self.maximum,
                    "Node frame exceeds limit"
                );
                payload
                    .checked_add(HEADER_BYTES)
                    .ok_or_else(|| anyhow::anyhow!("Node frame size overflow"))?
            };
            if target > HEADER_BYTES && self.input.len() == target {
                // Decode at most one frame larger than the turn quota, and do
                // it in a fresh turn. Preflight/decoding remain bounded by the
                // validated maximum frame; a large frame must still progress.
                if target > self.decode_remaining && self.decode_remaining != TURN_IO_BYTES {
                    self.receive_yielded = true;
                    return Ok(None);
                }
                self.decode_remaining = self.decode_remaining.saturating_sub(target);
                let metadata = u32::from_be_bytes(self.input[..4].try_into()?) as usize;
                let end = HEADER_BYTES + metadata;
                // Flat shape, decoded UTF-8 sizes and parser scratch are checked
                // without allocation while the encoded frame remains charged.
                let bounds = control::preflight(&self.input[HEADER_BYTES..end], target - end)?;
                let scratch = self.reserve(bounds.scratch)?;
                let bytes = self.reserve(bounds.retained)?;
                let value: Value = serde_json::from_slice(&self.input[HEADER_BYTES..end])?;
                let present = value
                    .get("has_body")
                    .and_then(Value::as_bool)
                    .ok_or_else(|| anyhow::anyhow!("Node frame requires attachment presence"))?;
                ensure!(
                    present || end == target,
                    "Node frame has an undeclared attachment"
                );
                let body = present.then(|| self.input[end..].to_vec());
                // Do not leave an uncharged high-water allocation after a frame.
                self.input = Vec::new();
                self.input_bytes = None;
                drop(scratch);
                return Ok(Some(Frame {
                    metadata: value,
                    body,
                    _bytes: bytes,
                }));
            }
            if !self.input.is_empty()
                && self
                    .input_bytes
                    .as_ref()
                    .is_some_and(|bytes| bytes.num_permits() < target)
            {
                let bytes = self.reserve(target)?;
                let mut input = Vec::with_capacity(target);
                input.extend_from_slice(&self.input);
                self.input = input;
                self.input_bytes = Some(bytes);
            }
            if self.read_remaining == 0 {
                self.receive_yielded = true;
                return Ok(None);
            }
            let mut chunk = [0; 8192];
            let capacity = chunk
                .len()
                .min(target - self.input.len())
                .min(self.read_remaining);
            match self.socket.read(&mut chunk[..capacity]) {
                Ok(0) => anyhow::bail!("Node transport disconnected"),
                Ok(n) => {
                    self.read_remaining -= n;
                    if self.input_bytes.is_none() {
                        self.input_bytes = Some(self.reserve(HEADER_BYTES)?);
                        self.input = Vec::with_capacity(HEADER_BYTES);
                    }
                    self.input.extend_from_slice(&chunk[..n]);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
                Err(error) => return Err(error.into()),
            }
        }
    }
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod tests;
