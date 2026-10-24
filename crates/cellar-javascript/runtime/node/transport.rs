//! Bounded control metadata and optional owned byte attachments. Application
//! stdout is separate from this authenticated, nonblocking transport.
use anyhow::{ensure, Result};
use serde_json::Value;
use std::{
    collections::VecDeque,
    io::{Read, Write},
    net::TcpStream,
};

const HEADER_BYTES: usize = 8;

pub(super) struct Frame {
    pub metadata: Value,
    pub body: Option<Vec<u8>>,
}

pub(super) struct Transport {
    socket: TcpStream,
    input: Vec<u8>,
    output: VecDeque<Vec<u8>>,
    // Bytes already written from the first queued frame.
    offset: usize,
    // Retained bytes across frames, including the already-written prefix of
    // the front allocation. Partial writes do not free its backing storage.
    queued: usize,
    // Per-frame metadata+body limit and aggregate outbound byte budget.
    maximum: usize,
    budget: usize,
}

impl Transport {
    pub fn new(socket: TcpStream, maximum: usize, budget: usize) -> Result<Self> {
        socket.set_nonblocking(true)?;
        socket.set_nodelay(true)?;
        Ok(Self {
            socket,
            input: Vec::new(),
            output: VecDeque::new(),
            offset: 0,
            queued: 0,
            maximum,
            budget,
        })
    }

    pub fn send(&mut self, value: Value) -> Result<()> {
        self.send_parts(value, None)
    }

    pub fn send_parts(&mut self, mut value: Value, body: Option<&[u8]>) -> Result<()> {
        let object = value
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("Node control must be an object"))?;
        object.insert("has_body".into(), Value::Bool(body.is_some()));
        let bytes = serde_json::to_vec(&value)?;
        let body = body.unwrap_or_default();
        let payload = bytes
            .len()
            .checked_add(body.len())
            .ok_or_else(|| anyhow::anyhow!("Node frame size overflow"))?;
        ensure!(payload <= self.maximum, "Node frame exceeds limit");
        let frame_bytes = payload
            .checked_add(HEADER_BYTES)
            .ok_or_else(|| anyhow::anyhow!("Node frame size overflow"))?;
        ensure!(
            frame_bytes <= self.budget.saturating_sub(self.queued),
            "Node transport capacity exhausted"
        );
        let metadata_length = u32::try_from(bytes.len())?;
        let body_length = u32::try_from(body.len())?;
        let mut frame = Vec::with_capacity(frame_bytes);
        frame.extend_from_slice(&metadata_length.to_be_bytes());
        frame.extend_from_slice(&body_length.to_be_bytes());
        frame.extend(bytes);
        frame.extend_from_slice(body);
        self.queued += frame.len();
        self.output.push_back(frame);
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        while let Some(frame) = self.output.front() {
            match self.socket.write(&frame[self.offset..]) {
                Ok(0) => anyhow::bail!("Node transport closed"),
                Ok(n) => {
                    self.offset += n;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => return Err(error.into()),
            }
            if self.offset == frame.len() {
                self.queued -= frame.len();
                self.output.pop_front();
                self.offset = 0;
            }
        }
        Ok(())
    }

    pub fn receive(&mut self) -> Result<Option<Frame>> {
        // At most one bounded frame is buffered; never accumulate a peer's flood.
        loop {
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
                let metadata = u32::from_be_bytes(self.input[..4].try_into()?) as usize;
                let end = HEADER_BYTES + metadata;
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
                self.input.clear();
                return Ok(Some(Frame {
                    metadata: value,
                    body,
                }));
            }
            let mut chunk = [0; 8192];
            let capacity = chunk.len().min(target - self.input.len());
            match self.socket.read(&mut chunk[..capacity]) {
                Ok(0) => anyhow::bail!("Node transport disconnected"),
                Ok(n) => self.input.extend_from_slice(&chunk[..n]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
                Err(error) => return Err(error.into()),
            }
        }
    }
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod tests;
