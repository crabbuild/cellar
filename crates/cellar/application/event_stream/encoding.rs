// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bound framework allocations while serializing, before any durable mutation.

use super::error::{error, ErrorCode};
use crate::application::Result;
use serde::Serialize;
use std::io::{self, Write};

struct Budget {
    bytes: Vec<u8>,
    maximum: usize,
    exceeded: bool,
}

impl Write for Budget {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let Some(length) = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|length| *length <= self.maximum)
        else {
            self.exceeded = true;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "event encoding exceeds its byte budget",
            ));
        };
        if length > self.bytes.capacity() {
            let capacity = length
                .max(self.bytes.capacity().saturating_mul(2))
                .max(4096)
                .min(self.maximum);
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(io::Error::other)?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn json<T: Serialize>(value: &T, maximum: usize) -> Result<Vec<u8>> {
    let mut budget = Budget {
        bytes: Vec::new(),
        maximum,
        exceeded: false,
    };
    let result = serde_json::to_writer(&mut budget, value);
    if budget.exceeded {
        return Err(error(
            ErrorCode::PayloadTooLarge,
            "event encoding exceeds its byte budget",
        ));
    }
    result.map_err(|failure| {
        error(
            if failure.is_io() {
                ErrorCode::Unavailable
            } else {
                ErrorCode::InvalidRequest
            },
            failure.to_string(),
        )
    })?;
    Ok(budget.bytes)
}

pub(crate) fn payload<T: Serialize>(value: &T, maximum: usize) -> Result<serde_json::Value> {
    let bytes = json(value, maximum)?;
    // Value's parser converts oversized integer literals to f64, whereas
    // its serializer rejected them. Preserve that rejection before parsing
    // the bounded serialization; do not silently round an application's u128.
    // This only checks numeric range. serde_json still validates JSON syntax
    // and nesting, including raw fragments a custom serializer may supply.
    let mut index = 0;
    let mut quoted = false;
    let mut escaped = false;
    while index < bytes.len() {
        let byte = bytes[index];
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
            index += 1;
        } else if byte == b'"' {
            quoted = true;
            index += 1;
        } else if byte == b'-' || byte.is_ascii_digit() {
            let first = index;
            while index < bytes.len()
                && matches!(bytes[index], b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
            {
                index += 1;
            }
            let token = &bytes[first..index];
            if !token.iter().any(|byte| matches!(byte, b'.' | b'e' | b'E')) {
                let token = std::str::from_utf8(token)?;
                let valid = if token.starts_with('-') {
                    token.parse::<i64>().is_ok()
                } else {
                    token.parse::<u64>().is_ok()
                };
                if !valid {
                    return Err(error(
                        ErrorCode::InvalidRequest,
                        "integer payloads must fit i64 or u64; encode larger integers as strings",
                    ));
                }
            }
        } else {
            index += 1;
        }
    }
    serde_json::from_slice(&bytes)
        .map_err(|failure| error(ErrorCode::InvalidRequest, failure.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_encoding_preserves_integer_ranges_and_does_not_read_numbers_in_strings() -> Result<()>
    {
        let original = serde_json::json!({"minimum":i64::MIN,"maximum":u64::MAX,"float":1.0e20,
            "text":"\\\"340282366920938463463374607431768211455", "nested":[{"integer":i64::MAX}]});
        let decoded = payload(&original, 1024)?;
        assert_eq!(decoded, original);
        assert_eq!(decoded["minimum"].as_i64(), Some(i64::MIN));
        assert_eq!(decoded["maximum"].as_u64(), Some(u64::MAX));
        for integer in [u64::MAX as u128 + 1, u128::MAX] {
            let failure = payload(&[integer], 1024).unwrap_err();
            assert_eq!(
                failure
                    .downcast_ref::<super::super::DeliveryError>()
                    .unwrap()
                    .code,
                ErrorCode::InvalidRequest
            );
        }
        let failure = payload(&[i64::MIN as i128 - 1], 1024).unwrap_err();
        assert_eq!(
            failure
                .downcast_ref::<super::super::DeliveryError>()
                .unwrap()
                .code,
            ErrorCode::InvalidRequest
        );
        Ok(())
    }
}
