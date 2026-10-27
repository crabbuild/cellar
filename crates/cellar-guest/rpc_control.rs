// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Decode the private named-RPC control object before native SDK admission.
use anyhow::{ensure, Result};
use serde::{
    de::{self, MapAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::{Map, Value};
use std::{
    fmt,
    io::{self, Write},
};

const MAX_BYTES: usize = 16 * 1024;
const LAYOUT_BYTES: usize = 1024;

/// Minted only when the adapter cannot reserve control decode storage, before
/// any native SDK call exists. It never crosses an entered RPC handler as proof.
#[derive(Debug)]
pub(crate) struct Capacity;

impl fmt::Display for Capacity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RPC control decode capacity exhausted")
    }
}

impl std::error::Error for Capacity {}

/// One wire-sized decoded string allowance, up to two for parser scratch vector
/// growth, and fixed flat-object layout. Encoded input is charged separately.
pub(crate) fn allocation(payload: &str) -> Result<usize> {
    ensure!(
        payload.len() <= MAX_BYTES,
        "RPC control metadata exceeds limit"
    );
    Ok(payload.len() * 3 + LAYOUT_BYTES)
}

#[derive(Deserialize)]
#[serde(field_identifier)]
enum Field {
    #[serde(rename = "binding")]
    Binding,
    #[serde(rename = "name")]
    Name,
    #[serde(rename = "method")]
    Method,
    #[serde(rename = "contract_digest")]
    Digest,
    #[serde(rename = "timeout_ms")]
    Timeout,
    #[serde(other)]
    Unknown,
}

impl Field {
    fn name(&self) -> Option<&'static str> {
        match self {
            Self::Binding => Some("binding"),
            Self::Name => Some("name"),
            Self::Method => Some("method"),
            Self::Digest => Some("contract_digest"),
            Self::Timeout => Some("timeout_ms"),
            Self::Unknown => None,
        }
    }
}

struct Control(Value);

impl<'de> Deserialize<'de> for Control {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct Fields;

        impl<'de> Visitor<'de> for Fields {
            type Value = Control;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a flat RPC control object")
            }

            fn visit_map<A: MapAccess<'de>>(
                self,
                mut input: A,
            ) -> std::result::Result<Control, A::Error> {
                let mut fields = Map::new();
                while let Some(field) = input.next_key::<Field>()? {
                    // Refuse before reading an unknown value. Do not materialize
                    // nested graphs or echo an attacker-controlled field name.
                    let name = field
                        .name()
                        .ok_or_else(|| de::Error::custom("unknown field in RPC control"))?;
                    if fields.contains_key(name) {
                        return Err(de::Error::custom("duplicate RPC control field"));
                    }
                    let value = match field {
                        Field::Timeout => input
                            .next_value::<Option<u32>>()?
                            .map(Value::from)
                            .unwrap_or(Value::Null),
                        _ => Value::String(input.next_value::<String>()?),
                    };
                    fields.insert(name.into(), value);
                }
                if ["binding", "name", "method", "contract_digest"]
                    .iter()
                    .any(|name| !fields.contains_key(*name))
                {
                    return Err(de::Error::custom("missing RPC control field"));
                }
                Ok(Control(Value::Object(fields)))
            }
        }
        deserializer.deserialize_map(Fields)
    }
}

pub(crate) fn decode(payload: &str) -> Result<Value> {
    allocation(payload)?;
    let Control(value) = serde_json::from_str(payload)?;
    validate(&value)?;
    Ok(value)
}

/// Also guard direct native callers, whose metadata is already an owned Value.
pub(crate) fn validate(value: &Value) -> Result<()> {
    let fields = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("RPC control must be an object"))?;
    ensure!(
        fields.keys().all(|key| matches!(
            key.as_str(),
            "binding" | "name" | "method" | "contract_digest" | "timeout_ms"
        )),
        "unknown field in RPC control"
    );
    let field = |name| crate::values::field(value, name);
    ensure!(!field("binding")?.is_empty(), "empty RPC binding");
    ensure!(field("method")?.len() <= 256, "RPC method exceeds limit");
    ensure!(
        field("contract_digest")?.len() == 64,
        "invalid RPC contract digest"
    );
    ensure!(
        field("name")?.len() <= MAX_BYTES && field("binding")?.len() <= MAX_BYTES,
        "RPC control metadata exceeds limit"
    );
    if let Some(timeout) = value.get("timeout_ms").filter(|value| !value.is_null()) {
        ensure!(
            timeout
                .as_u64()
                .and_then(|number| u32::try_from(number).ok())
                .is_some(),
            "invalid RPC timeout"
        );
    }

    struct Size(usize);

    impl Write for Size {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .filter(|size| *size <= MAX_BYTES)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "RPC control metadata exceeds limit",
                    )
                })?;
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    // Count escaped wire bytes without allocating a serialized metadata copy.
    serde_json::to_writer(Size(0), value)?;
    Ok(())
}

#[cfg(test)]
#[path = "rpc_control_tests.rs"]
mod tests;
