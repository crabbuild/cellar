use anyhow::{anyhow, bail, Result};
use cellar_native::clone::{Object, StructuredValue, Value, ViewKind};

pub(super) fn array(graph: &StructuredValue, value: &Value) -> Result<Vec<Value>> {
    match value {
        Value::Reference(id) => match graph.objects.get(*id as usize) {
            Some(Object::DenseArray { elements, .. }) => Ok(elements.clone()),
            Some(Object::SparseArray { length, properties }) => {
                anyhow::ensure!(
                    *length as usize <= cellar_native::clone::Limits::default().values,
                    "KV_ERROR: array exceeds value limit"
                );
                let mut elements = vec![Value::Undefined; *length as usize];
                for (key, value) in properties {
                    let index = match key {
                        Value::Number(index)
                            if index.is_finite() && *index >= 0.0 && index.fract() == 0.0 =>
                        {
                            Some(*index as usize)
                        }
                        Value::String(index) => String::from_utf16(index)
                            .ok()
                            .and_then(|index| index.parse::<usize>().ok()),
                        _ => None,
                    };
                    if let Some(element) = index.and_then(|index| elements.get_mut(index)) {
                        *element = value.clone();
                    }
                }
                Ok(elements)
            }
            _ => bail!("KV_ERROR: expected an array"),
        },
        _ => bail!("KV_ERROR: expected an array"),
    }
}

pub(super) fn field<'a>(graph: &'a StructuredValue, value: &Value, name: &str) -> &'a Value {
    if let Value::Reference(id) = value {
        if let Some(Object::Object(properties)) = graph.objects.get(*id as usize) {
            if let Some((_, value)) = properties.iter().rev().find(|(key, _)| {
                matches!(key, Value::String(key) if key.iter().copied().eq(name.encode_utf16()))
            }) {
                return value;
            }
        }
    }
    &Value::Undefined
}

pub(super) fn string(value: &Value) -> Result<String> {
    match value {
        Value::String(value) => String::from_utf16(value)
            .map_err(|_| anyhow!("KV_ERROR: text contains a lone surrogate")),
        _ => bail!("KV_ERROR: expected text"),
    }
}

pub(super) fn number(value: &Value) -> Result<i64> {
    match value {
        Value::Number(value)
            if value.is_finite() && *value >= i64::MIN as f64 && *value < 9223372036854775808.0 =>
        {
            Ok(*value as i64)
        }
        _ => bail!("KV_ERROR: expected a finite timestamp or count"),
    }
}

pub(super) fn optional_string(value: &Value) -> Result<Option<String>> {
    match value {
        Value::Null | Value::Undefined => Ok(None),
        _ => string(value).map(Some),
    }
}

pub(super) fn optional_number(value: &Value) -> Result<Option<i64>> {
    match value {
        Value::Null | Value::Undefined => Ok(None),
        _ => number(value).map(Some),
    }
}

pub(super) fn bytes(graph: &StructuredValue, value: &Value) -> Result<Vec<u8>> {
    match value {
        Value::Reference(id) => match graph.objects.get(*id as usize) {
            Some(Object::ArrayBuffer { bytes, .. }) => Ok(bytes.clone()),
            Some(Object::View {
                buffer,
                offset,
                length,
                kind: ViewKind::Uint8 | ViewKind::Uint8Clamped,
                ..
            }) => {
                let Some(Object::ArrayBuffer { bytes, .. }) = graph.objects.get(*buffer as usize)
                else {
                    bail!("KV_ERROR: byte view has no buffer")
                };
                let start = usize::try_from(*offset)?;
                let end = start
                    .checked_add(usize::try_from(*length)?)
                    .ok_or_else(|| anyhow!("KV_ERROR: byte view is outside its buffer"))?;
                Ok(bytes
                    .get(start..end)
                    .ok_or_else(|| anyhow!("KV_ERROR: byte view is outside its buffer"))?
                    .to_vec())
            }
            Some(Object::DenseArray { elements, .. }) => elements
                .iter()
                .map(|value| {
                    // JSON control callers use arrays; JS Uint8Array uses ToUint8.
                    let value = number(value)?;
                    Ok(value as u8)
                })
                .collect(),
            _ => bail!("KV_ERROR: expected bytes"),
        },
        _ => bail!("KV_ERROR: expected bytes"),
    }
}

pub(super) fn set_bytes(
    graph: &mut StructuredValue,
    parent: &Value,
    name: &str,
    bytes: Vec<u8>,
) -> Result<()> {
    set_binary(graph, parent, name, bytes, true)
}

pub(super) fn set_buffer(
    graph: &mut StructuredValue,
    parent: &Value,
    name: &str,
    bytes: Vec<u8>,
) -> Result<()> {
    set_binary(graph, parent, name, bytes, false)
}

fn set_binary(
    graph: &mut StructuredValue,
    parent: &Value,
    name: &str,
    bytes: Vec<u8>,
    typed_view: bool,
) -> Result<()> {
    let length = bytes.len() as u64;
    let buffer = u32::try_from(graph.objects.len())?;
    graph.objects.push(Object::ArrayBuffer {
        bytes,
        max_length: None,
        immutable: false,
    });
    let id = if typed_view {
        let view = u32::try_from(graph.objects.len())?;
        graph.objects.push(Object::View {
            buffer,
            kind: ViewKind::Uint8,
            offset: 0,
            length,
            flags: 0,
        });
        view
    } else {
        buffer
    };
    let Value::Reference(parent) = parent else {
        bail!("KV_ERROR: expected an object")
    };
    let Some(Object::Object(properties)) = graph.objects.get_mut(*parent as usize) else {
        bail!("KV_ERROR: expected an object")
    };
    if let Some((_, value)) = properties.iter_mut().find(
        |(key, _)| matches!(key, Value::String(key) if key.iter().copied().eq(name.encode_utf16())),
    ) {
        *value = Value::Reference(id);
    } else {
        properties.push((Value::from(name), Value::Reference(id)));
    }
    Ok(())
}

/// JSON.stringify(Uint8Array) uses numbered object properties. Stream that
/// representation without allocating one JSON object per byte.
pub(super) fn json_get(graph: &StructuredValue) -> Result<String> {
    use serde::ser::{Error, SerializeMap, SerializeSeq};

    struct Indices(Vec<u8>);

    impl serde::Serialize for Indices {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            let mut map = serializer.serialize_map(Some(self.0.len()))?;
            for (index, byte) in self.0.iter().enumerate() {
                map.serialize_entry(&index.to_string(), byte)?;
            }
            map.end()
        }
    }

    struct Row<'a>(&'a StructuredValue, &'a Value);

    impl serde::Serialize for Row<'_> {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            let mut map = serializer.serialize_map(None)?;
            for name in ["key", "found", "value", "tag", "metadata", "expiration"] {
                let value = field(self.0, self.1, name);
                match value {
                    Value::Undefined => {}
                    Value::Null => map.serialize_entry(name, &())?,
                    Value::String(_) => {
                        map.serialize_entry(name, &string(value).map_err(S::Error::custom)?)?
                    }
                    Value::Boolean(value) => map.serialize_entry(name, value)?,
                    Value::Number(_) => {
                        map.serialize_entry(name, &number(value).map_err(S::Error::custom)?)?
                    }
                    _ if name == "value" => {
                        let inline = matches!(value, Value::Reference(id) if matches!(self.0.objects.get(*id as usize), Some(Object::ArrayBuffer{..})));
                        map.serialize_entry(
                            name,
                            &Indices(if inline {
                                Vec::new()
                            } else {
                                bytes(self.0, value).map_err(S::Error::custom)?
                            }),
                        )?;
                    }
                    _ => return Err(S::Error::custom("invalid KV JSON reply")),
                }
            }
            map.end()
        }
    }

    struct Rows<'a>(&'a StructuredValue, Vec<Value>);

    impl serde::Serialize for Rows<'_> {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            let mut seq = serializer.serialize_seq(Some(self.1.len()))?;
            for row in &self.1 {
                seq.serialize_element(&Row(self.0, row))?;
            }
            seq.end()
        }
    }
    Ok(serde_json::to_string(&Rows(
        graph,
        array(graph, &graph.root)?,
    ))?)
}
