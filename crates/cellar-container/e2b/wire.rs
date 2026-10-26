// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Incremental Connect JSON envelopes; never retain an unbounded response stream.
use anyhow::{ensure, Result};
use serde_json::Value;

pub(super) struct Decoder {
    bytes: Vec<u8>,
    maximum: usize,
    pub ended: bool,
}

impl Decoder {
    pub fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
            ended: false,
        }
    }

    pub fn push(&mut self, mut input: &[u8]) -> Result<Vec<Value>> {
        let mut values = Vec::new();
        while !input.is_empty() {
            ensure!(!self.ended, "data after Connect terminal envelope");
            let target = if self.bytes.len() < 5 {
                5
            } else {
                5 + u32::from_be_bytes(self.bytes[1..5].try_into().unwrap()) as usize
            };
            let count = (target - self.bytes.len()).min(input.len());
            self.bytes.extend_from_slice(&input[..count]);
            input = &input[count..];
            if self.bytes.len() < 5 {
                continue;
            }
            let length = u32::from_be_bytes(self.bytes[1..5].try_into().unwrap()) as usize;
            ensure!(length <= self.maximum, "Connect frame exceeds byte budget");
            ensure!(
                matches!(self.bytes[0], 0 | 2),
                "unsupported Connect flags/compression"
            );
            if self.bytes.len() != length + 5 {
                continue;
            }
            let value: Value = serde_json::from_slice(&self.bytes[5..])?;
            if self.bytes[0] == 2 {
                ensure!(value.is_object(), "invalid Connect terminal envelope");
                ensure!(
                    value.get("error").is_none(),
                    "E2B process stream returned a terminal error"
                );
                self.ended = true;
            } else {
                anyhow::ensure!(
                    values.len() < 1024,
                    "too many Connect envelopes in one transport chunk"
                );
                values.push(value);
            }
            self.bytes.clear();
        }
        Ok(values)
    }

    pub fn finish(&self) -> Result<()> {
        ensure!(
            self.ended && self.bytes.is_empty(),
            "truncated Connect response"
        );
        Ok(())
    }
}

pub(super) fn frame(value: &Value) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value)?;
    let mut out = Vec::with_capacity(5 + bytes.len());
    out.push(0);
    out.extend_from_slice(&u32::try_from(bytes.len())?.to_be_bytes());
    out.extend(bytes);
    Ok(out)
}

#[cfg(test)]
#[path = "wire_tests.rs"]
mod tests;
