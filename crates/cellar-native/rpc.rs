// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The existing RPC response envelope, including remote error values. Live
//! capability envelopes require a host resolver and remain explicit refusals.

use crate::clone::{self, Object, StructuredValue, Value};
use anyhow::{anyhow, bail, ensure, Result};

#[derive(Debug)]
pub struct RemoteError {
    /// The complete [error, own-properties] envelope, including its causes.
    pub value: StructuredValue,
    pub message: String,
}

impl std::fmt::Display for RemoteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for RemoteError {}

pub fn decode_response(bytes: &[u8]) -> Result<StructuredValue> {
    match bytes.first() {
        Some(0xff) => clone::decode(bytes),
        Some(0) => {
            let value = clone::decode(&bytes[1..])?;
            let Value::Reference(root) = value.root else {
                bail!("invalid RPC error envelope")
            };
            let Some(Object::DenseArray { elements, .. }) = value.objects.get(root as usize) else {
                bail!("invalid RPC error envelope");
            };
            ensure!(elements.len() == 2, "invalid RPC error envelope");
            let message = match &elements[0] {
                Value::Reference(id) => match value.objects.get(*id as usize) {
                    Some(Object::Error {
                        message: Some(message),
                        ..
                    }) => String::from_utf16_lossy(message),
                    _ => "remote RPC threw a structured value".into(),
                },
                Value::String(message) => String::from_utf16_lossy(message),
                _ => "remote RPC threw a value".into(),
            };
            Err(RemoteError { value, message }.into())
        }
        Some(1 | 2) => bail!("RPC result requires native capability materialization"),
        _ => Err(anyhow!("invalid RPC response envelope")),
    }
}

pub fn encode_error(message: &str, name: &str, cause: Option<&str>) -> Result<Vec<u8>> {
    let mut objects = vec![
        Object::DenseArray {
            elements: vec![Value::Reference(1), Value::Reference(2)],
            properties: Vec::new(),
        },
        Object::Error {
            prototype: None,
            message: Some(message.encode_utf16().collect()),
            stack: None,
            cause: cause.map(|_| Value::Reference(3)),
        },
        Object::Object(vec![("name".into(), name.into())]),
    ];
    if let Some(cause) = cause {
        objects.push(Object::Error {
            prototype: None,
            message: Some(cause.encode_utf16().collect()),
            stack: None,
            cause: None,
        });
    }
    let graph = StructuredValue {
        root: Value::Reference(0),
        objects,
    };
    let mut bytes = vec![0];
    bytes.extend(clone::encode(&graph)?);
    Ok(bytes)
}
