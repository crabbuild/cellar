//! Compact, bounded success values; larger/deeper replies retain JSON transport.
use pyo3::{
    prelude::*,
    types::{PyDict, PyList, PyString},
    IntoPyObjectExt,
};
use serde_json::Value;

pub(crate) enum ReplyValue {
    Json(String),
    Compact { bytes: Vec<u8>, encoded_len: usize },
}

impl ReplyValue {
    pub(crate) fn new(encoded: String, compact: Option<Vec<u8>>) -> Self {
        match compact {
            Some(bytes) => Self::Compact {
                bytes,
                encoded_len: encoded.len(),
            },
            None => Self::Json(encoded),
        }
    }

    pub(crate) fn encoded_len(&self) -> usize {
        match self {
            Self::Json(value) => value.len(),
            Self::Compact { encoded_len, .. } => *encoded_len,
        }
    }

    pub(crate) fn to_python(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, usize)> {
        match self {
            Self::Json(value) => Ok((PyString::new(py, value).into_any().unbind(), value.len())),
            Self::Compact { bytes, .. } => {
                let mut reader = Reader { bytes, copied: 0 };
                let value = reader.value(py, 0)?;
                if !reader.bytes.is_empty() {
                    return Err(invalid());
                }
                let envelope = PyDict::new(py);
                envelope.set_item("ok", true)?;
                envelope.set_item("value", value)?;
                Ok((envelope.into_any().unbind(), reader.copied + 7))
            }
        }
    }
}

// Fixed-width numbers avoid numeric parsing on the interpreter thread. Bounded
// shape selection avoids slow per-element conversions for large SQL results.
// The native queue owns a byte buffer, never an expanded JSON value tree.
pub(crate) fn prepare(value: &Value, encoded_len: usize) -> Option<Vec<u8>> {
    let size = size(value, 0, &mut 64)?;
    if size > encoded_len {
        return None;
    }
    let mut bytes = Vec::with_capacity(size);
    encode(value, &mut bytes)?;
    debug_assert_eq!(bytes.len(), size);
    Some(bytes)
}

fn length_size(mut value: usize) -> usize {
    let mut size = 1;
    while value >= 128 {
        value >>= 7;
        size += 1;
    }
    size
}

fn size(value: &Value, depth: usize, remaining: &mut usize) -> Option<usize> {
    if depth > 8 || *remaining == 0 {
        return None;
    }
    *remaining -= 1;
    match value {
        Value::Null | Value::Bool(_) => Some(1),
        Value::Number(_) => Some(9),
        Value::String(value) => 1usize
            .checked_add(length_size(value.len()))?
            .checked_add(value.len()),
        Value::Array(values) => values
            .iter()
            .try_fold(1 + length_size(values.len()), |total, v| {
                total.checked_add(size(v, depth + 1, remaining)?)
            }),
        Value::Object(values) => {
            values
                .iter()
                .try_fold(1 + length_size(values.len()), |total, (k, v)| {
                    if *remaining == 0 {
                        return None;
                    }
                    *remaining -= 1;
                    total
                        .checked_add(length_size(k.len()))?
                        .checked_add(k.len())?
                        .checked_add(size(v, depth + 1, remaining)?)
                })
        }
    }
}

fn length(mut value: usize, out: &mut Vec<u8>) {
    while value >= 128 {
        out.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    out.push(value as u8);
}

fn string(value: &str, out: &mut Vec<u8>) {
    length(value.len(), out);
    out.extend_from_slice(value.as_bytes());
}

fn encode(value: &Value, out: &mut Vec<u8>) -> Option<()> {
    match value {
        Value::Null => out.push(0),
        Value::Bool(value) => out.push(if *value { 2 } else { 1 }),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                out.push(3);
                out.extend_from_slice(&value.to_le_bytes());
            } else if let Some(value) = value.as_u64() {
                out.push(4);
                out.extend_from_slice(&value.to_le_bytes());
            } else {
                let value = value.as_f64()?;
                out.push(5);
                out.extend_from_slice(&value.to_le_bytes());
            }
        }
        Value::String(value) => {
            out.push(6);
            string(value, out);
        }
        Value::Array(values) => {
            out.push(7);
            length(values.len(), out);
            for value in values {
                encode(value, out)?;
            }
        }
        Value::Object(values) => {
            out.push(8);
            length(values.len(), out);
            for (key, value) in values {
                string(key, out);
                encode(value, out)?;
            }
        }
    }
    Some(())
}

fn invalid() -> PyErr {
    pyo3::exceptions::PyValueError::new_err("Invalid compact SDK reply")
}

struct Reader<'a> {
    bytes: &'a [u8],
    copied: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, count: usize) -> PyResult<&'a [u8]> {
        let value = self.bytes.get(..count).ok_or_else(invalid)?;
        self.bytes = &self.bytes[count..];
        Ok(value)
    }

    fn length(&mut self) -> PyResult<usize> {
        let mut value = 0u64;
        for shift in (0..70).step_by(7) {
            let byte = self.take(1)?[0];
            if shift == 63 && byte > 1 {
                return Err(invalid());
            }
            value |= u64::from(byte & 127) << shift;
            if byte < 128 {
                return usize::try_from(value).map_err(|_| invalid());
            }
        }
        Err(invalid())
    }

    fn string(&mut self) -> PyResult<&'a str> {
        let length = self.length()?;
        let text = std::str::from_utf8(self.take(length)?).map_err(|_| invalid())?;
        self.copied += length;
        Ok(text)
    }

    fn value(&mut self, py: Python<'_>, depth: usize) -> PyResult<Py<PyAny>> {
        if depth > 8 {
            return Err(invalid());
        }
        match self.take(1)?[0] {
            0 => Ok(py.None()),
            1 => false.into_py_any(py),
            2 => true.into_py_any(py),
            3 => {
                i64::from_le_bytes(self.take(8)?.try_into().map_err(|_| invalid())?).into_py_any(py)
            }
            4 => {
                u64::from_le_bytes(self.take(8)?.try_into().map_err(|_| invalid())?).into_py_any(py)
            }
            5 => {
                let number = f64::from_le_bytes(self.take(8)?.try_into().map_err(|_| invalid())?);
                if !number.is_finite() {
                    return Err(invalid());
                }
                number.into_py_any(py)
            }
            6 => self.string()?.into_py_any(py),
            7 => {
                let count = self.length()?;
                if count > self.bytes.len() {
                    return Err(invalid());
                }
                let values = (0..count)
                    .map(|_| self.value(py, depth + 1))
                    .collect::<PyResult<Vec<_>>>()?;
                Ok(PyList::new(py, values)?.into_any().unbind())
            }
            8 => {
                let count = self.length()?;
                if count > self.bytes.len() / 2 {
                    return Err(invalid());
                }
                let result = PyDict::new(py);
                for _ in 0..count {
                    let key = self.string()?;
                    result.set_item(key, self.value(py, depth + 1)?)?;
                }
                Ok(result.into_any().unbind())
            }
            _ => Err(invalid()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn compact_selection_keeps_encoded_budget_and_bounded_shape() {
        for value in [
            Value::Null,
            json!(true),
            json!({"present":true,"value":u64::MAX}),
            json!([i64::MIN, -0.0, "é🦀\\\"\n"]),
            json!("x".repeat(65536)),
        ] {
            let encoded = serde_json::to_string(&json!({"ok":true,"value":value})).unwrap();
            let bytes = prepare(&value, encoded.len()).unwrap();
            assert!(bytes.len() <= encoded.len());
            assert!(bytes.capacity() <= encoded.len());
            assert!(prepare(&value, bytes.len() - 1).is_none());
        }
        assert!(prepare(&json!(vec![1; 1000]), usize::MAX).is_none());
        let mut deep = Value::Null;
        for _ in 0..9 {
            deep = json!([deep]);
        }
        assert!(prepare(&deep, usize::MAX).is_none());
    }

    #[test]
    fn lengths_and_utf8_are_checked_before_allocation() {
        for expected in [0, 1, 127, 128, 16383, 16384, usize::MAX] {
            let mut bytes = Vec::new();
            length(expected, &mut bytes);
            assert_eq!(bytes.len(), length_size(expected));
            let mut reader = Reader {
                bytes: &bytes,
                copied: 0,
            };
            assert_eq!(reader.length().unwrap(), expected);
            assert!(reader.bytes.is_empty());
        }
        for bytes in [&[128][..], &[255; 10][..], &[1, 255][..], &[5, 65][..]] {
            let mut reader = Reader { bytes, copied: 0 };
            assert!(reader.string().is_err());
            assert_eq!(reader.copied, 0);
        }
    }
}
