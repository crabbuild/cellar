use super::*;
use serde_json::{Map, Number, Value as Json};

struct Projection<'a> {
    graph: &'a StructuredValue,
    active: Vec<bool>,
    bytes: usize,
    values: usize,
}

pub(super) fn project(graph: &StructuredValue) -> Result<Json> {
    let limits = Limits::default();
    ensure!(
        graph.objects.len() <= limits.objects,
        "JSON projection exceeds object limit"
    );
    Projection {
        graph,
        active: vec![false; graph.objects.len()],
        bytes: limits.bytes,
        values: limits.values,
    }
    .value(&graph.root, 0)
}

impl Projection<'_> {
    fn charge(&mut self, bytes: usize) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .ok_or_else(|| anyhow!("JSON projection exceeds byte limit"))?;
        Ok(())
    }

    fn string(&mut self, value: &[u16]) -> Result<String> {
        ensure!(
            value.len() <= self.bytes,
            "JSON projection exceeds byte limit"
        );
        let string = String::from_utf16(value)
            .map_err(|_| anyhow!("lone surrogate cannot be represented in JSON text"))?;
        self.charge(serde_json::to_string(&string)?.len())?;
        Ok(string)
    }

    fn key(&mut self, value: &Value) -> Result<String> {
        match value {
            Value::String(value) => self.string(value),
            Value::Number(value) if value.is_finite() => {
                let key = value.to_string();
                self.charge(key.len() + 2)?;
                Ok(key)
            }
            _ => bail!("invalid JSON object property key"),
        }
    }

    fn value(&mut self, value: &Value, depth: usize) -> Result<Json> {
        ensure!(
            depth <= Limits::default().depth && self.values > 0,
            "JSON projection exceeds value or depth limit"
        );
        self.values -= 1;
        self.charge(1)?; // One delimiter per expanded value.
        Ok(match value {
            Value::Null | Value::Hole => {
                self.charge(4)?;
                Json::Null
            }
            Value::Boolean(value) => {
                self.charge(5)?;
                Json::Bool(*value)
            }
            Value::Number(value) => {
                let number = if value.is_finite() && value.fract() == 0.0 {
                    if (-9223372036854775808.0..9223372036854775808.0).contains(value) {
                        Some(Number::from(*value as i64))
                    } else if (0.0..18446744073709551616.0).contains(value) {
                        Some(Number::from(*value as u64))
                    } else {
                        Number::from_f64(*value)
                    }
                } else {
                    Number::from_f64(*value)
                };
                self.charge(number.as_ref().map_or(4, |n| n.to_string().len()))?;
                number.map_or(Json::Null, Json::Number)
            }
            Value::String(value) => Json::String(self.string(value)?),
            Value::Reference(id) => {
                let id = *id as usize;
                ensure!(id < self.active.len(), "invalid JSON object reference");
                ensure!(
                    !self.active[id],
                    "cyclic structured value cannot be projected to JSON"
                );
                self.active[id] = true;
                self.charge(2)?;
                let result = match &self.graph.objects[id] {
                    Object::Object(properties) => {
                        let mut output = Map::new();
                        for (key, value) in properties {
                            let key = self.key(key)?;
                            if matches!(value, Value::Undefined) {
                                output.remove(&key);
                            } else {
                                output.insert(key, self.value(value, depth + 1)?);
                            }
                        }
                        Json::Object(output)
                    }
                    Object::DenseArray { elements, .. } => Json::Array(
                        elements
                            .iter()
                            .map(|value| self.element(value, depth + 1))
                            .collect::<Result<_>>()?,
                    ),
                    Object::SparseArray { length, properties } => {
                        let length = *length as usize;
                        ensure!(
                            length <= self.values && length <= self.bytes / 5,
                            "JSON array expansion exceeds resource limit"
                        );
                        let mut elements = vec![&Value::Hole; length];
                        for (key, value) in properties {
                            let key = self.key(key)?;
                            if let Ok(index) = key.parse::<usize>() {
                                if index < length && index.to_string() == key {
                                    elements[index] = value;
                                }
                            }
                        }
                        Json::Array(
                            elements
                                .into_iter()
                                .map(|value| self.element(value, depth + 1))
                                .collect::<Result<_>>()?,
                        )
                    }
                    _ => bail!("structured object kind has no lossless plain JSON projection"),
                };
                self.active[id] = false;
                result
            }
            Value::Undefined => bail!("undefined has no root JSON value"),
            Value::BigInt { .. } => bail!("BigInt cannot be projected to JSON"),
        })
    }

    fn element(&mut self, value: &Value, depth: usize) -> Result<Json> {
        self.value(
            if matches!(value, Value::Undefined) {
                &Value::Null
            } else {
                value
            },
            depth,
        )
    }
}
