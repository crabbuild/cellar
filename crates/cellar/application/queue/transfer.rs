// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A transfer receipt binds the complete immutable payload, even after its row is gone.

use super::wire;
use anyhow::{ensure, Result};
use cellar_native::clone::{StructuredValue, Value};
use sha2::{Digest, Sha256};

/// Expected request refusals are distinct from storage and durability failures.
#[derive(Debug)]
pub(super) enum Refusal {
    Invalid(String),
    Conflict,
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) => f.write_str(message),
            Self::Conflict => f.write_str(
                "QUEUE_ERROR: a DLQ transfer collided with another message payload or identity",
            ),
        }
    }
}

impl std::error::Error for Refusal {}

impl Refusal {
    pub fn invalid(error: anyhow::Error) -> Self {
        Self::Invalid(error.to_string())
    }
}

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
        let id = wire::string(field("transferId")).map_err(Refusal::invalid)?;
        ensure!(
            matches!(field("deadLetterSource"), Value::Null | Value::String(_)),
            Refusal::Invalid(
                "QUEUE_ERROR: a Queue transfer has an invalid dead-letter source".into()
            )
        );
        let source = wire::optional_string(field("deadLetterSource")).map_err(Refusal::invalid)?;
        let entry = field("entry");
        let field = |name| wire::field(graph, entry, name);
        let message = wire::string(field("id")).map_err(Refusal::invalid)?;
        let content_type = wire::string(field("contentType")).map_err(Refusal::invalid)?;
        let body = wire::bytes(graph, field("body")).map_err(Refusal::invalid)?;
        ensure!(
            body.len() <= maximum,
            "QUEUE_ERROR: transferred message body exceeds the host limit"
        );
        ensure!(
            matches!(field("size"), Value::Number(size) if *size == body.len() as f64),
            Refusal::Invalid(
                "QUEUE_ERROR: transferred message size does not match its body".into()
            )
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

#[cfg(test)]
mod tests {
    use super::*;

    fn input(size: f64) -> StructuredValue {
        StructuredValue::from_json(serde_json::json!({
            "transferId":"transfer", "deadLetterSource":null,
            "entry":{"id":"message","body":[1,2],"contentType":"bytes","size":size}
        }))
        .unwrap()
    }

    #[test]
    fn host_capacity_is_not_a_durable_payload_refusal() {
        let graph = input(2.0);
        let error = Transfer::decode(&graph, &graph.root, 1)
            .err()
            .expect("host ceiling");
        assert!(
            !error.is::<Refusal>(),
            "a local capacity failure became a permanent reply"
        );
    }

    #[test]
    fn malformed_payload_is_a_durable_refusal() {
        let graph = input(2.5);
        let error = Transfer::decode(&graph, &graph.root, 2)
            .err()
            .expect("fractional size");
        assert!(matches!(
            error.downcast_ref::<Refusal>(),
            Some(Refusal::Invalid(_))
        ));
    }
}
