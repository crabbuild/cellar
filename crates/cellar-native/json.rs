// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! JSON data operations on owned UTF-16 graphs. These retain JSON.parse's
//! binary64 numbers and lone surrogates, and JSON.stringify's data semantics.
//! Application callbacks (replacers, revivers, getters and custom toJSON)
//! belong to the caller; no executable capability is embedded in this graph.

use crate::clone::{Limits, Object, Properties, StructuredValue, Value, ViewKind};
use anyhow::{anyhow, bail, ensure, Result};
use std::collections::{BTreeMap, HashMap};

pub fn parse(text: &[u16]) -> Result<StructuredValue> {
    parse_with_limits(text, Limits::default())
}

pub fn parse_utf8(text: &str) -> Result<StructuredValue> {
    parse(&text.encode_utf16().collect::<Vec<_>>())
}

pub fn parse_with_limits(text: &[u16], limits: Limits) -> Result<StructuredValue> {
    ensure!(
        text.len() <= limits.bytes / 2,
        "JSON input exceeds byte limit"
    );
    let mut parser = Parser {
        text,
        at: 0,
        limits,
        remaining: limits.values,
        objects: Vec::new(),
    };
    let root = parser.value(0)?;
    parser.space();
    ensure!(parser.at == text.len(), "unexpected text after JSON value");
    Ok(StructuredValue {
        root,
        objects: parser.objects,
    })
}

struct Parser<'a> {
    text: &'a [u16],
    at: usize,
    limits: Limits,
    remaining: usize,
    objects: Vec<Object>,
}

impl Parser<'_> {
    fn space(&mut self) {
        while self
            .text
            .get(self.at)
            .is_some_and(|unit| matches!(unit, 9 | 10 | 13 | 32))
        {
            self.at += 1;
        }
    }

    fn take(&mut self, unit: u16) -> bool {
        if self.text.get(self.at) == Some(&unit) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, unit: u16) -> Result<()> {
        ensure!(
            self.take(unit),
            "unexpected JSON token at code unit {}",
            self.at
        );
        Ok(())
    }

    fn literal(&mut self, text: &str) -> Result<()> {
        for unit in text.bytes() {
            self.expect(u16::from(unit))?;
        }
        Ok(())
    }

    fn string(&mut self) -> Result<Vec<u16>> {
        self.expect(34)?;
        let mut output = Vec::new();
        loop {
            let unit = *self
                .text
                .get(self.at)
                .ok_or_else(|| anyhow!("unterminated JSON string"))?;
            self.at += 1;
            match unit {
                34 => return Ok(output),
                92 => {
                    let escaped = *self
                        .text
                        .get(self.at)
                        .ok_or_else(|| anyhow!("unterminated JSON escape"))?;
                    self.at += 1;
                    output.push(match escaped {
                        34 | 47 | 92 => escaped,
                        98 => 8,
                        102 => 12,
                        110 => 10,
                        114 => 13,
                        116 => 9,
                        117 => {
                            let mut value = 0;
                            for _ in 0..4 {
                                let unit = *self
                                    .text
                                    .get(self.at)
                                    .ok_or_else(|| anyhow!("short JSON Unicode escape"))?;
                                self.at += 1;
                                let digit = match unit {
                                    48..=57 => unit - 48,
                                    65..=70 => unit - 65 + 10,
                                    97..=102 => unit - 97 + 10,
                                    _ => bail!("invalid JSON Unicode escape"),
                                };
                                value = (value << 4) | digit;
                            }
                            value
                        }
                        _ => bail!("invalid JSON string escape"),
                    });
                }
                0..=31 => bail!("unescaped JSON control character"),
                _ => output.push(unit),
            }
        }
    }

    fn digit(&mut self) -> bool {
        if self
            .text
            .get(self.at)
            .is_some_and(|unit| matches!(unit, 48..=57))
        {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn number(&mut self) -> Result<Value> {
        let start = self.at;
        self.take(45);
        if !self.take(48) {
            ensure!(
                self.text
                    .get(self.at)
                    .is_some_and(|unit| matches!(unit, 49..=57)),
                "invalid JSON number"
            );
            while self.digit() {}
        }
        if self.take(46) {
            ensure!(self.digit(), "JSON fraction requires a digit");
            while self.digit() {}
        }
        if self.take(101) || self.take(69) {
            if !self.take(43) {
                self.take(45);
            }
            ensure!(self.digit(), "JSON exponent requires a digit");
            while self.digit() {}
        }
        let text = String::from_utf16(&self.text[start..self.at])?;
        Ok(Value::Number(text.parse::<f64>()?))
    }

    fn value(&mut self, depth: usize) -> Result<Value> {
        ensure!(
            depth <= self.limits.depth && self.remaining > 0,
            "JSON exceeds value or depth limit"
        );
        self.remaining -= 1;
        self.space();
        Ok(match self.text.get(self.at).copied() {
            Some(34) => Value::String(self.string()?),
            Some(110) => {
                self.literal("null")?;
                Value::Null
            }
            Some(116) => {
                self.literal("true")?;
                Value::Boolean(true)
            }
            Some(102) => {
                self.literal("false")?;
                Value::Boolean(false)
            }
            Some(45 | 48..=57) => self.number()?,
            Some(91 | 123) => {
                let array = self.take(91);
                if !array {
                    self.expect(123)?;
                }
                ensure!(
                    self.objects.len() < self.limits.objects,
                    "JSON exceeds object limit"
                );
                let id = u32::try_from(self.objects.len())?;
                self.objects.push(Object::Object(Vec::new()));
                self.space();
                let close = if array { 93 } else { 125 };
                let mut elements = Vec::new();
                let mut properties = Vec::new();
                let mut keys = HashMap::new();
                if !self.take(close) {
                    loop {
                        if array {
                            elements.push(self.value(depth + 1)?);
                        } else {
                            ensure!(self.remaining > 0, "JSON exceeds value limit");
                            self.remaining -= 1;
                            self.space();
                            let key = self.string()?;
                            self.space();
                            self.expect(58)?;
                            let value = self.value(depth + 1)?;
                            if let Some(index) = keys.get(&key).copied() {
                                properties[index] = (Value::String(key), value);
                            } else {
                                keys.insert(key.clone(), properties.len());
                                properties.push((Value::String(key), value));
                            }
                        }
                        self.space();
                        if self.take(close) {
                            break;
                        }
                        self.expect(44)?;
                    }
                }
                self.objects[id as usize] = if array {
                    Object::DenseArray {
                        elements,
                        properties: Vec::new(),
                    }
                } else {
                    properties.sort_by_key(|(key, _)| match key {
                        Value::String(key) => index(key).unwrap_or(u32::MAX),
                        _ => u32::MAX,
                    });
                    Object::Object(properties)
                };
                Value::Reference(id)
            }
            _ => bail!("unexpected JSON value at code unit {}", self.at),
        })
    }
}

/// None is JSON.stringify(undefined). The returned text is UTF-8: unpaired
/// surrogates are escaped, so the original UTF-16 survives parsing.
pub fn stringify(graph: &StructuredValue) -> Result<Option<String>> {
    stringify_with_limits(graph, Limits::default())
}

pub fn stringify_with_limits(graph: &StructuredValue, limits: Limits) -> Result<Option<String>> {
    if matches!(graph.root, Value::Undefined) {
        return Ok(None);
    }
    ensure!(
        graph.objects.len() <= limits.objects,
        "JSON exceeds object limit"
    );
    let mut writer = Writer {
        graph,
        limits,
        remaining: limits.values,
        active: vec![false; graph.objects.len()],
        output: String::new(),
    };
    writer.value(&graph.root, 0)?;
    Ok(Some(writer.output))
}

struct Writer<'a> {
    graph: &'a StructuredValue,
    limits: Limits,
    remaining: usize,
    active: Vec<bool>,
    output: String,
}

fn index(key: &[u16]) -> Option<u32> {
    if key.is_empty() || (key[0] == 48 && key.len() != 1) {
        return None;
    }
    let mut value = 0u32;
    for unit in key {
        if !(48..=57).contains(unit) {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(u32::from(unit - 48))?;
    }
    (value != u32::MAX).then_some(value)
}

fn key(value: &Value, limit: usize) -> Result<Vec<u16>> {
    match value {
        Value::String(value) => {
            ensure!(value.len() <= limit / 2, "JSON key exceeds byte limit");
            Ok(value.clone())
        }
        Value::Number(value) => Ok(ryu_js::Buffer::new()
            .format(*value)
            .encode_utf16()
            .collect()),
        _ => bail!("invalid JSON property key"),
    }
}

fn date(milliseconds: f64) -> Option<String> {
    if !milliseconds.is_finite() || milliseconds.abs() > 8_640_000_000_000_000.0 {
        return None;
    }
    let ms = milliseconds.trunc() as i64;
    let z = ms.div_euclid(86_400_000) + 719468;
    let era = z.div_euclid(146097);
    let day = z - era * 146097;
    let y = (day - day / 1460 + day / 36524 - day / 146096) / 365;
    let doy = day - (365 * y + y / 4 - y / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + era * 400 + i64::from(month <= 2);
    let year = if (0..=9999).contains(&year) {
        format!("{year:04}")
    } else {
        format!(
            "{}{abs:06}",
            if year < 0 { "-" } else { "+" },
            abs = year.abs()
        )
    };
    let time = ms.rem_euclid(86_400_000);
    Some(format!(
        "{year}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        time / 3_600_000,
        time / 60_000 % 60,
        time / 1000 % 60,
        time % 1000
    ))
}

impl Writer<'_> {
    fn push(&mut self, text: &str) -> Result<()> {
        ensure!(
            text.len() <= self.limits.bytes.saturating_sub(self.output.len()),
            "JSON exceeds byte limit"
        );
        self.output.push_str(text);
        Ok(())
    }

    fn string(&mut self, units: &[u16]) -> Result<()> {
        self.push("\"")?;
        for scalar in char::decode_utf16(units.iter().copied()) {
            match scalar {
                Err(error) => self.push(&format!("\\u{:04x}", error.unpaired_surrogate()))?,
                Ok('"') => self.push("\\\"")?,
                Ok('\\') => self.push("\\\\")?,
                Ok('\u{8}') => self.push("\\b")?,
                Ok('\u{c}') => self.push("\\f")?,
                Ok('\n') => self.push("\\n")?,
                Ok('\r') => self.push("\\r")?,
                Ok('\t') => self.push("\\t")?,
                Ok(value) if value < ' ' => self.push(&format!("\\u{:04x}", u32::from(value)))?,
                Ok(value) => self.push(value.encode_utf8(&mut [0; 4]))?,
            }
        }
        self.push("\"")
    }

    fn array(&mut self, length: usize, get: impl Fn(usize) -> Value, depth: usize) -> Result<()> {
        ensure!(
            length <= self.remaining
                && length <= self.limits.bytes.saturating_sub(self.output.len()) / 2,
            "JSON array expansion exceeds limit"
        );
        self.push("[")?;
        for position in 0..length {
            if position > 0 {
                self.push(",")?;
            }
            let value = get(position);
            self.value(
                if matches!(value, Value::Undefined | Value::Hole) {
                    &Value::Null
                } else {
                    &value
                },
                depth + 1,
            )?;
        }
        self.push("]")
    }

    fn object(&mut self, properties: &Properties, depth: usize) -> Result<()> {
        ensure!(
            properties.len() <= self.remaining,
            "JSON properties exceed value limit"
        );
        // Replacing a property keeps its original insertion position. Array
        // indices precede other keys, sorted by numeric index.
        let mut ordered = Vec::<(Vec<u16>, &Value)>::new();
        let mut seen = HashMap::new();
        let mut key_bytes = self.limits.bytes;
        for (property, value) in properties {
            let key = key(property, key_bytes)?;
            key_bytes = key_bytes
                .checked_sub(key.len().saturating_mul(2))
                .ok_or_else(|| anyhow!("JSON keys exceed byte limit"))?;
            if let Some(position) = seen.get(&key).copied() {
                ordered[position] = (key, value);
            } else {
                seen.insert(key.clone(), ordered.len());
                ordered.push((key, value));
            }
        }
        ordered.sort_by_key(|(key, _)| index(key).unwrap_or(u32::MAX));
        self.push("{")?;
        let mut comma = false;
        for (key, value) in ordered {
            self.remaining = self
                .remaining
                .checked_sub(1)
                .ok_or_else(|| anyhow!("JSON exceeds value limit"))?;
            if matches!(value, Value::Undefined) {
                continue;
            }
            if comma {
                self.push(",")?;
            }
            comma = true;
            self.string(&key)?;
            self.push(":")?;
            self.value(value, depth + 1)?;
        }
        self.push("}")
    }

    fn view(
        &mut self,
        buffer: u32,
        kind: ViewKind,
        offset: u64,
        length: u64,
        depth: usize,
    ) -> Result<()> {
        let Some(Object::ArrayBuffer { bytes, .. }) = self.graph.objects.get(buffer as usize)
        else {
            bail!("invalid JSON typed-view buffer")
        };
        let width = match kind {
            ViewKind::Int8 | ViewKind::Uint8 | ViewKind::Uint8Clamped | ViewKind::DataView => 1,
            ViewKind::Int16 | ViewKind::Uint16 | ViewKind::Float16 => 2,
            ViewKind::Int32 | ViewKind::Uint32 | ViewKind::Float32 => 4,
            _ => 8,
        };
        let start = usize::try_from(offset)?;
        let length = usize::try_from(length)?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| anyhow!("JSON typed-view range overflow"))?;
        ensure!(
            end <= bytes.len() && start % width == 0 && length % width == 0,
            "invalid JSON typed-view range"
        );
        if kind == ViewKind::DataView {
            return self.push("{}");
        }
        let count = length / width;
        ensure!(
            count <= self.remaining / 2,
            "JSON typed view exceeds value limit"
        );
        ensure!(
            count == 0 || !matches!(kind, ViewKind::BigInt64 | ViewKind::BigUint64),
            "BigInt cannot be serialized to JSON"
        );
        self.push("{")?;
        for (position, bytes) in bytes[start..end].chunks_exact(width).enumerate() {
            let number = match kind {
                ViewKind::Int8 => i8::from_ne_bytes(bytes.try_into()?) as f64,
                ViewKind::Uint8 | ViewKind::Uint8Clamped => bytes[0] as f64,
                ViewKind::Int16 => i16::from_ne_bytes(bytes.try_into()?) as f64,
                ViewKind::Uint16 => u16::from_ne_bytes(bytes.try_into()?) as f64,
                ViewKind::Int32 => i32::from_ne_bytes(bytes.try_into()?) as f64,
                ViewKind::Uint32 => u32::from_ne_bytes(bytes.try_into()?) as f64,
                ViewKind::Float16 => {
                    let bits = u16::from_ne_bytes(bytes.try_into()?);
                    let exponent = (bits >> 10) & 31;
                    let mantissa = bits & 1023;
                    let sign = if bits & 0x8000 == 0 { 1.0 } else { -1.0 };
                    match exponent {
                        0 => sign * (mantissa as f64) * 2f64.powi(-24),
                        31 => {
                            if mantissa == 0 {
                                sign * f64::INFINITY
                            } else {
                                f64::NAN
                            }
                        }
                        _ => sign * (1024 + mantissa) as f64 * 2f64.powi(i32::from(exponent) - 25),
                    }
                }
                ViewKind::Float32 => f32::from_ne_bytes(bytes.try_into()?) as f64,
                ViewKind::Float64 => f64::from_ne_bytes(bytes.try_into()?),
                _ => unreachable!("empty BigInt views do not iterate"),
            };
            if position > 0 {
                self.push(",")?;
            }
            self.remaining -= 1;
            self.string(&position.to_string().encode_utf16().collect::<Vec<_>>())?;
            self.push(":")?;
            self.value(&Value::Number(number), depth + 1)?;
        }
        self.push("}")
    }

    fn value(&mut self, value: &Value, depth: usize) -> Result<()> {
        ensure!(
            depth <= self.limits.depth && self.remaining > 0,
            "JSON exceeds value or depth limit"
        );
        self.remaining -= 1;
        match value {
            Value::Null => self.push("null"),
            Value::Boolean(value) => self.push(if *value { "true" } else { "false" }),
            Value::Number(value) => {
                let mut buffer = ryu_js::Buffer::new();
                self.push(if value.is_finite() {
                    buffer.format(*value)
                } else {
                    "null"
                })
            }
            Value::String(value) => self.string(value),
            Value::BigInt { .. } => bail!("BigInt cannot be serialized to JSON"),
            Value::Undefined | Value::Hole => bail!("invalid root JSON value"),
            Value::Reference(id) => {
                let id = *id as usize;
                ensure!(id < self.active.len(), "invalid JSON object reference");
                ensure!(
                    !self.active[id],
                    "cyclic value cannot be serialized to JSON"
                );
                self.active[id] = true;
                let result = match &self.graph.objects[id] {
                    Object::Object(properties) => self.object(properties, depth),
                    Object::DenseArray {
                        elements,
                        properties,
                    } => {
                        ensure!(
                            properties.len() <= self.remaining,
                            "JSON array properties exceed value limit"
                        );
                        let mut overrides = BTreeMap::new();
                        for (property, value) in properties {
                            if let Some(index) = index(&key(property, self.limits.bytes)?) {
                                overrides.insert(index as usize, value);
                            }
                        }
                        self.array(
                            elements.len(),
                            |position| {
                                overrides
                                    .get(&position)
                                    .copied()
                                    .unwrap_or(&elements[position])
                                    .clone()
                            },
                            depth,
                        )
                    }
                    Object::SparseArray { length, properties } => {
                        ensure!(
                            properties.len() <= self.remaining,
                            "JSON array properties exceed value limit"
                        );
                        let mut elements = BTreeMap::new();
                        for (property, value) in properties {
                            if let Some(index) = index(&key(property, self.limits.bytes)?) {
                                elements.insert(index as usize, value);
                            }
                        }
                        self.array(
                            *length as usize,
                            |position| {
                                elements
                                    .get(&position)
                                    .copied()
                                    .unwrap_or(&Value::Hole)
                                    .clone()
                            },
                            depth,
                        )
                    }
                    Object::Boxed(value) => {
                        ensure!(
                            matches!(
                                value,
                                Value::Boolean(_)
                                    | Value::Number(_)
                                    | Value::String(_)
                                    | Value::BigInt { .. }
                            ),
                            "invalid boxed JSON primitive"
                        );
                        self.value(value, depth + 1)
                    }
                    Object::Date(value) => match date(*value) {
                        Some(value) => self.string(&value.encode_utf16().collect::<Vec<_>>()),
                        None => self.push("null"),
                    },
                    Object::View {
                        buffer,
                        kind,
                        offset,
                        length,
                        ..
                    } => self.view(*buffer, *kind, *offset, *length, depth),
                    Object::Map(_)
                    | Object::Set(_)
                    | Object::RegExp { .. }
                    | Object::ArrayBuffer { .. }
                    | Object::Error { .. } => self.push("{}"),
                };
                self.active[id] = false;
                result
            }
        }
    }
}
