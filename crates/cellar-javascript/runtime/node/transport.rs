//! Bounded, nonblocking length-prefixed JSON transport; application stdout is separate.
use anyhow::{ensure, Result};
use serde_json::Value;
use std::{
    collections::VecDeque,
    io::{Read, Write},
    net::TcpStream,
};

pub(super) struct Transport {
    socket: TcpStream,
    input: Vec<u8>,
    output: VecDeque<Vec<u8>>,
    // Bytes already written from the first queued frame.
    offset: usize,
    // Unsent bytes across all frames, including their four-byte headers.
    queued: usize,
    // Per-frame JSON limit and aggregate outbound byte budget, respectively.
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
        let bytes = serde_json::to_vec(&value)?;
        ensure!(bytes.len() <= self.maximum, "Node frame exceeds limit");
        // The network-order header counts JSON bytes, excluding the header itself.
        let length = u32::try_from(bytes.len())?;
        ensure!(
            self.queued + bytes.len() + 4 <= self.budget,
            "Node transport capacity exhausted"
        );
        let mut frame = length.to_be_bytes().to_vec();
        frame.extend(bytes);
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
                    self.queued -= n;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => return Err(error.into()),
            }
            if self.offset == frame.len() {
                self.output.pop_front();
                self.offset = 0;
            }
        }
        Ok(())
    }

    pub fn receive(&mut self) -> Result<Option<Value>> {
        // At most one bounded frame is buffered; never accumulate a peer's flood.
        loop {
            let target = if self.input.len() < 4 {
                4
            } else {
                let n = u32::from_be_bytes(self.input[..4].try_into()?) as usize;
                ensure!(n <= self.maximum, "Node frame exceeds limit");
                n + 4
            };
            if target > 4 && self.input.len() == target {
                let value = serde_json::from_slice(&self.input[4..])?;
                self.input.clear();
                return Ok(Some(value));
            }
            ensure!(target != 4 || self.input.len() < 4, "Node empty frame");
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
mod tests {
    use super::*;
    use std::net::TcpListener;

    #[test]
    fn fragmented_frames_and_announced_limits() -> Result<()> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let mut peer = TcpStream::connect(listener.local_addr()?)?;
        let mut transport = Transport::new(listener.accept()?.0, 128, 256)?;
        let data = serde_json::to_vec(&serde_json::json!({"type":"heartbeat"}))?;
        let header = (data.len() as u32).to_be_bytes();
        peer.write_all(&header[..2])?;
        assert!(transport.receive()?.is_none());
        peer.write_all(&header[2..])?;
        peer.write_all(&data[..3])?;
        assert!(transport.receive()?.is_none());
        peer.write_all(&data[3..])?;
        let until = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            if let Some(value) = transport.receive()? {
                assert_eq!(value["type"], "heartbeat");
                break;
            }
            assert!(std::time::Instant::now() < until);
            std::thread::yield_now();
        }
        peer.write_all(&129_u32.to_be_bytes())?;
        loop {
            match transport.receive() {
                Err(_) => break,
                Ok(None) => {}
                Ok(Some(_)) => panic!(),
            }
            assert!(std::time::Instant::now() < until);
            std::thread::yield_now();
        }
        assert!(transport
            .send(serde_json::json!({"huge":"x".repeat(129)}))
            .is_err());
        Ok(())
    }
}
