// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded incremental NDJSON; transport records and helper records use the
//! same framing but retain separate limits and validation.
use anyhow::{ensure, Result};
use serde_json::Value;

pub(super) struct Decoder {
    bytes: Vec<u8>,
    maximum: usize,
}

impl Decoder {
    pub fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
        }
    }

    pub fn push(&mut self, mut input: &[u8]) -> Result<Vec<Value>> {
        let mut values = Vec::new();
        while !input.is_empty() {
            let length = input
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(input.len(), |index| index + 1);
            ensure!(
                length <= self.maximum.saturating_sub(self.bytes.len()),
                "Vercel JSON line exceeds byte limit"
            );
            self.bytes.extend_from_slice(&input[..length]);
            input = &input[length..];
            if self.bytes.last() == Some(&b'\n') {
                ensure!(
                    values.len() < 1024,
                    "too many Vercel records in one transport chunk"
                );
                let value: Value = serde_json::from_slice(&self.bytes)?;
                ensure!(value.is_object(), "Vercel JSON record must be an object");
                values.push(value);
                self.bytes.clear();
            }
        }
        Ok(values)
    }

    pub fn finish(&self) -> Result<()> {
        ensure!(self.bytes.is_empty(), "truncated Vercel JSON stream");
        Ok(())
    }
}

#[cfg(test)]
#[path = "wire_tests.rs"]
mod tests;
