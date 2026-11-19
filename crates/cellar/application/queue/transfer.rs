// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A transfer receipt binds the complete immutable payload, even after its row is gone.

use super::wire;
use anyhow::{ensure, Result};
use cellar_native::clone::{StructuredValue, Value};
use sha2::{Digest, Sha256};

pub(super) struct Transfer {
    pub id: String,
    pub message: String,
    pub source: Option<String>,
    pub content_type: String,
    pub body: Vec<u8>,
    pub fingerprint: [u8; 32],
}

impl Transfer {
    pub fn decode(graph: &StructuredValue, value: &Value, maximum: usize) -> Result<Self> {
        let field = |name| wire::field(graph, value, name);
        let id = wire::string(field("transferId"))?;
        ensure!(
            matches!(field("deadLetterSource"), Value::Null | Value::String(_)),
            "QUEUE_ERROR: a Queue transfer has an invalid dead-letter source"
        );
        let source = wire::optional_string(field("deadLetterSource"))?;
        let entry = field("entry");
        let field = |name| wire::field(graph, entry, name);
        let message = wire::string(field("id"))?;
        let content_type = wire::string(field("contentType"))?;
        let body = wire::bytes(graph, field("body"))?;
        ensure!(
            body.len() <= maximum,
            "QUEUE_ERROR: transferred message body exceeds the host limit"
        );
        ensure!(
            matches!(field("size"), Value::Number(size) if *size == body.len() as f64),
            "QUEUE_ERROR: transferred message size does not match its body"
        );
        let mut hash = Sha256::new();
        hash.update(b"cellar-queue-transfer-payload-1\0");
        Self::field(&mut hash, id.as_bytes());
        Self::field(&mut hash, message.as_bytes());
        hash.update([u8::from(source.is_some())]);
        if let Some(source) = &source {
            Self::field(&mut hash, source.as_bytes());
        }
        Self::field(&mut hash, content_type.as_bytes());
        // Hash decoded bytes: ArrayBuffer and array wire representations must
        // identify the same transfer. No second full-sized encoding is needed.
        Self::field(&mut hash, &body);
        Ok(Self {
            id,
            message,
            source,
            content_type,
            body,
            fingerprint: hash.finalize().into(),
        })
    }

    fn field(hash: &mut Sha256, bytes: &[u8]) {
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
    }
}
