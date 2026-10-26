// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Owned structured values and a compatibility reader for persisted clones.
//!
//! Object references retain cycles and shared identity; UTF-16 retains lone
//! surrogates. The wire tags follow the versioned format in V8's
//! src/objects/value-serializer.cc (versions 10 through 16). No engine is used.
//! Transfer IDs and delegate host objects require external capabilities and
//! are rejected rather than guessed. Doubles and UTF-16 use little endian,
//! matching cellar's supported x86-64 and ARM64 deployments.

use anyhow::{anyhow, bail, ensure, Result};
mod bytes_envelope;
pub use bytes_envelope::decode_tagged_bytes;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Undefined,
    Null,
    Boolean(bool),
    Number(f64),
    BigInt {
        negative: bool,
        magnitude: Vec<u8>,
    },
    String(Vec<u16>),
    Reference(u32),
    /// Valid only as an absent element of a dense array.
    Hole,
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.encode_utf16().collect())
    }
}

pub type Properties = Vec<(Value, Value)>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ViewKind {
    Int8,
    Uint8,
    Uint8Clamped,
    Int16,
    Uint16,
    Int32,
    Uint32,
    Float16,
    Float32,
    Float64,
    BigInt64,
    BigUint64,
    DataView,
}

impl ViewKind {
    fn tag(self) -> u8 {
        match self {
            Self::Int8 => b'b',
            Self::Uint8 => b'B',
            Self::Uint8Clamped => b'C',
            Self::Int16 => b'w',
            Self::Uint16 => b'W',
            Self::Int32 => b'd',
            Self::Uint32 => b'D',
            Self::Float16 => b'h',
            Self::Float32 => b'f',
            Self::Float64 => b'F',
            Self::BigInt64 => b'q',
            Self::BigUint64 => b'Q',
            Self::DataView => b'?',
        }
    }

    fn decode(tag: u8) -> Result<Self> {
        Ok(match tag {
            b'b' => Self::Int8,
            b'B' => Self::Uint8,
            b'C' => Self::Uint8Clamped,
            b'w' => Self::Int16,
            b'W' => Self::Uint16,
            b'd' => Self::Int32,
            b'D' => Self::Uint32,
            b'h' => Self::Float16,
            b'f' => Self::Float32,
            b'F' => Self::Float64,
            b'q' => Self::BigInt64,
            b'Q' => Self::BigUint64,
            b'?' => Self::DataView,
            _ => bail!("unknown structured-clone view kind {tag:#x}"),
        })
    }

    fn element_size(self) -> usize {
        match self {
            Self::Int8 | Self::Uint8 | Self::Uint8Clamped | Self::DataView => 1,
            Self::Int16 | Self::Uint16 | Self::Float16 => 2,
            Self::Int32 | Self::Uint32 | Self::Float32 => 4,
            Self::Float64 | Self::BigInt64 | Self::BigUint64 => 8,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Object {
    Object(Properties),
    DenseArray {
        elements: Vec<Value>,
        properties: Properties,
    },
    SparseArray {
        length: u32,
        properties: Properties,
    },
    Date(f64),
    Boxed(Value),
    RegExp {
        source: Vec<u16>,
        flags: u32,
    },
    Map(Properties),
    Set(Vec<Value>),
    ArrayBuffer {
        bytes: Vec<u8>,
        max_length: Option<u64>,
        immutable: bool,
    },
    View {
        buffer: u32,
        kind: ViewKind,
        offset: u64,
        length: u64,
        flags: u32,
    },
    Error {
        prototype: Option<u8>,
        message: Option<Vec<u16>>,
        stack: Option<Vec<u16>>,
        cause: Option<Value>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct StructuredValue {
    pub root: Value,
    pub objects: Vec<Object>,
}

impl StructuredValue {
    /// Project a plain structured RPC value into JSON, with bounded expansion
    /// of aliases. Undefined object fields are omitted; absent/undefined array
    /// elements and non-finite numbers become null. Cycles, lone surrogates,
    /// BigInt and non-JSON object kinds are refused instead of losing their
    /// identity or representation. The persisted graph remains unchanged.
    pub fn to_json(&self) -> Result<serde_json::Value> {
        json::project(self)
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self> {
        fn convert(
            value: serde_json::Value,
            objects: &mut Vec<Object>,
            depth: usize,
            remaining: &mut usize,
        ) -> Result<Value> {
            ensure!(
                depth <= Limits::default().depth && *remaining > 0,
                "JSON clone exceeds value or depth limit"
            );
            *remaining -= 1;
            Ok(match value {
                serde_json::Value::Null => Value::Null,
                serde_json::Value::Bool(value) => Value::Boolean(value),
                serde_json::Value::Number(value) => Value::Number(
                    value
                        .as_f64()
                        .ok_or_else(|| anyhow!("JSON number is outside clone range"))?,
                ),
                serde_json::Value::String(value) => Value::String(value.encode_utf16().collect()),
                value => {
                    ensure!(
                        objects.len() < Limits::default().objects,
                        "JSON clone exceeds object limit"
                    );
                    let id = u32::try_from(objects.len())?;
                    objects.push(Object::Object(Vec::new()));
                    objects[id as usize] = match value {
                        serde_json::Value::Array(values) => Object::DenseArray {
                            elements: values
                                .into_iter()
                                .map(|value| convert(value, objects, depth + 1, remaining))
                                .collect::<Result<_>>()?,
                            properties: Vec::new(),
                        },
                        serde_json::Value::Object(values) => Object::Object(
                            values
                                .into_iter()
                                .map(|(key, value)| {
                                    Ok((
                                        key.as_str().into(),
                                        convert(value, objects, depth + 1, remaining)?,
                                    ))
                                })
                                .collect::<Result<_>>()?,
                        ),
                        _ => unreachable!("compound JSON values remain"),
                    };
                    Value::Reference(id)
                }
            })
        }
        let mut objects = Vec::new();
        let root = convert(value, &mut objects, 0, &mut Limits::default().values)?;
        Ok(Self { root, objects })
    }
}

mod graph;
mod json;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub bytes: usize,
    pub objects: usize,
    pub values: usize,
    pub depth: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            bytes: 64 * 1024 * 1024,
            objects: 1_000_000,
            values: 2_000_000,
            depth: 256,
        }
    }
}

pub fn decode(bytes: &[u8]) -> Result<StructuredValue> {
    decode_with_limits(bytes, Limits::default())
}

pub fn decode_with_limits(bytes: &[u8], limits: Limits) -> Result<StructuredValue> {
    ensure!(
        bytes.len() <= limits.bytes,
        "structured clone exceeds byte limit"
    );
    let mut reader = Reader {
        bytes,
        at: 0,
        version: 0,
        broken_v13: false,
        objects: Vec::new(),
        values: 0,
        limits,
    };
    ensure!(
        reader.byte()? == 0xff,
        "structured clone has no version header"
    );
    reader.version = reader.u32()?;
    ensure!(
        (10..=16).contains(&reader.version),
        "unsupported structured-clone version {}",
        reader.version
    );
    let start = reader.at;
    match reader.document() {
        Ok(value) => Ok(value),
        Err(_) if reader.version == 13 => {
            // V8's version-13 producer briefly wrote the future view flags.
            reader.at = start;
            reader.objects.clear();
            reader.values = 0;
            reader.broken_v13 = true;
            reader.document()
        }
        Err(error) => Err(error),
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
    version: u32,
    broken_v13: bool,
    objects: Vec<Option<Object>>,
    values: usize,
    limits: Limits,
}

impl<'a> Reader<'a> {
    fn document(&mut self) -> Result<StructuredValue> {
        let root = self.value(0)?;
        self.padding();
        ensure!(
            self.at == self.bytes.len(),
            "trailing structured-clone bytes"
        );
        let objects = std::mem::take(&mut self.objects)
            .into_iter()
            .map(|object| object.ok_or_else(|| anyhow!("unfinished clone object")))
            .collect::<Result<_>>()?;
        Ok(StructuredValue { root, objects })
    }

    fn padding(&mut self) {
        while self.bytes.get(self.at) == Some(&0) {
            self.at += 1;
        }
    }

    fn peek(&mut self) -> Option<u8> {
        self.padding();
        self.bytes.get(self.at).copied()
    }

    fn byte(&mut self) -> Result<u8> {
        let byte = *self
            .bytes
            .get(self.at)
            .ok_or_else(|| anyhow!("truncated structured clone at {}", self.at))?;
        self.at += 1;
        Ok(byte)
    }

    fn raw(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(length)
            .ok_or_else(|| anyhow!("clone length overflow"))?;
        let bytes = self
            .bytes
            .get(self.at..end)
            .ok_or_else(|| anyhow!("truncated clone payload"))?;
        self.at = end;
        Ok(bytes)
    }

    fn varint(&mut self, bits: u32) -> Result<u64> {
        let mut result = 0;
        for shift in (0..bits).step_by(7) {
            let byte = self.byte()?;
            let value = u64::from(byte & 0x7f);
            ensure!(
                value <= (u64::MAX >> (64 - bits + shift)),
                "clone varint overflow"
            );
            result |= value << shift;
            if byte & 0x80 == 0 {
                return Ok(result);
            }
        }
        bail!("clone varint overflow")
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(self.varint(32)? as u32)
    }

    fn size(&mut self) -> Result<u64> {
        self.varint(if self.version >= 16 { 64 } else { 32 })
    }

    fn double(&mut self) -> Result<f64> {
        Ok(f64::from_le_bytes(self.raw(8)?.try_into().unwrap()))
    }

    fn bigint(&mut self) -> Result<Value> {
        let bits = self.u32()?;
        ensure!(bits != 1, "invalid negative-zero clone bigint");
        Ok(Value::BigInt {
            negative: bits & 1 != 0,
            magnitude: self.raw((bits >> 1) as usize)?.to_vec(),
        })
    }

    fn string(&mut self, depth: usize) -> Result<Vec<u16>> {
        if self.version < 12 {
            return self.utf8();
        }
        match self.value(depth)? {
            Value::String(value) => Ok(value),
            _ => bail!("clone requires a string"),
        }
    }

    fn utf8(&mut self) -> Result<Vec<u16>> {
        let len = self.u32()? as usize;
        let bytes = self.raw(len)?;
        Ok(std::str::from_utf8(bytes)?.encode_utf16().collect())
    }

    fn reserve(&mut self) -> Result<u32> {
        ensure!(
            self.objects.len() < self.limits.objects,
            "clone object limit exceeded"
        );
        let id = u32::try_from(self.objects.len())?;
        self.objects.push(None);
        Ok(id)
    }

    fn properties(&mut self, end: u8, depth: usize) -> Result<Properties> {
        let mut properties = Vec::new();
        while self.peek() != Some(end) {
            let key = self.value(depth)?;
            ensure!(
                matches!(&key, Value::String(_) | Value::Number(_)),
                "invalid clone property key"
            );
            let value = self.value(depth)?;
            properties.push((key, value));
        }
        self.byte()?;
        ensure!(
            self.u32()? as usize == properties.len(),
            "clone property count mismatch"
        );
        Ok(properties)
    }

    fn value(&mut self, depth: usize) -> Result<Value> {
        ensure!(depth <= self.limits.depth, "clone nesting limit exceeded");
        self.values += 1;
        ensure!(
            self.values <= self.limits.values,
            "clone value limit exceeded"
        );
        self.padding();
        let tag = self.byte()?;
        let value = match tag {
            b'?' => {
                self.u32()?;
                self.value(depth + 1)?
            }
            b'_' => Value::Undefined,
            b'0' => Value::Null,
            b'T' => Value::Boolean(true),
            b'F' => Value::Boolean(false),
            b'I' => {
                let n = self.u32()?;
                Value::Number(((n >> 1) as i32 ^ -((n & 1) as i32)) as f64)
            }
            b'U' => Value::Number(self.u32()? as f64),
            b'N' => Value::Number(self.double()?),
            b'Z' => self.bigint()?,
            b'S' => Value::String(self.utf8()?),
            b'"' => {
                let len = self.u32()? as usize;
                Value::String(self.raw(len)?.iter().map(|b| u16::from(*b)).collect())
            }
            b'c' => {
                let len = self.u32()? as usize;
                ensure!(len.is_multiple_of(2), "odd clone UTF-16 byte length");
                Value::String(
                    self.raw(len)?
                        .as_chunks::<2>()
                        .0
                        .iter()
                        .map(|bytes| u16::from_le_bytes(*bytes))
                        .collect(),
                )
            }
            b'^' => {
                let id = self.u32()?;
                ensure!(
                    (id as usize) < self.objects.len(),
                    "invalid clone object reference"
                );
                Value::Reference(id)
            }
            b'o' | b'a' | b'A' | b'D' | b'y' | b'x' | b'n' | b'z' | b's' | b'R' | b';' | b'\''
            | b'B' | b'C' | b'~' | b'r' => {
                let id = self.reserve()?;
                let object = self.object(tag, depth + 1)?;
                self.objects[id as usize] = Some(object);
                Value::Reference(id)
            }
            b't' | b'u' | b'p' | b'w' | b'm' | b'\\' => {
                bail!("clone requires an external transfer or host capability ({tag:#x})")
            }
            _ => bail!("unknown structured-clone tag {tag:#x}"),
        };
        if let Value::Reference(buffer) = value {
            if matches!(
                self.objects.get(buffer as usize),
                Some(Some(Object::ArrayBuffer { .. }))
            ) && self.peek() == Some(b'V')
            {
                self.byte()?;
                return self.view(buffer);
            }
        }
        Ok(value)
    }

    fn object(&mut self, tag: u8, depth: usize) -> Result<Object> {
        ensure!(depth <= self.limits.depth, "clone nesting limit exceeded");
        Ok(match tag {
            b'o' => Object::Object(self.properties(b'{', depth)?),
            b'a' => {
                let length = self.u32()?;
                let properties = self.properties(b'@', depth)?;
                ensure!(self.u32()? == length, "clone array length mismatch");
                Object::SparseArray { length, properties }
            }
            b'A' => {
                let length = self.u32()? as usize;
                ensure!(
                    length <= self.bytes.len() - self.at
                        && length <= self.limits.values.saturating_sub(self.values),
                    "clone dense array exceeds value limit"
                );
                let mut elements = Vec::with_capacity(length);
                for _ in 0..length {
                    if self.peek() == Some(b'-') {
                        self.byte()?;
                        self.values += 1;
                        elements.push(Value::Hole);
                    } else {
                        let value = self.value(depth)?;
                        elements.push(if self.version < 11 && matches!(value, Value::Undefined) {
                            Value::Hole
                        } else {
                            value
                        });
                    }
                }
                let properties = self.properties(b'$', depth)?;
                ensure!(
                    self.u32()? as usize == length,
                    "clone array length mismatch"
                );
                Object::DenseArray {
                    elements,
                    properties,
                }
            }
            b'D' => Object::Date(self.double()?),
            b'y' | b'x' => Object::Boxed(Value::Boolean(tag == b'y')),
            b'n' => Object::Boxed(Value::Number(self.double()?)),
            b'z' => Object::Boxed(self.bigint()?),
            b's' => Object::Boxed(Value::String(self.string(depth)?)),
            b'R' => Object::RegExp {
                source: self.string(depth)?,
                flags: self.u32()?,
            },
            b';' => {
                let mut entries = Vec::new();
                while self.peek() != Some(b':') {
                    let key = self.value(depth)?;
                    let value = self.value(depth)?;
                    entries.push((key, value));
                }
                self.byte()?;
                ensure!(
                    self.u32()? as usize == entries.len() * 2,
                    "clone map count mismatch"
                );
                Object::Map(entries)
            }
            b'\'' => {
                let mut entries = Vec::new();
                while self.peek() != Some(b',') {
                    entries.push(self.value(depth)?);
                }
                self.byte()?;
                ensure!(
                    self.u32()? as usize == entries.len(),
                    "clone set count mismatch"
                );
                Object::Set(entries)
            }
            b'B' | b'C' | b'~' => {
                let length = self.size()?;
                let max_length = if tag == b'~' {
                    let max = self.size()?;
                    ensure!(max >= length, "invalid resizable clone buffer");
                    Some(max)
                } else {
                    None
                };
                Object::ArrayBuffer {
                    bytes: self.raw(usize::try_from(length)?)?.to_vec(),
                    max_length,
                    immutable: tag == b'C',
                }
            }
            b'r' => {
                let mut prototype = None;
                let mut message = None;
                let mut stack = None;
                let mut cause = None;
                loop {
                    match self.varint(8)? as u8 {
                        tag @ (b'E' | b'R' | b'F' | b'S' | b'T' | b'U') if prototype.is_none() => {
                            prototype = Some(tag)
                        }
                        b'm' if message.is_none() => message = Some(self.string(depth)?),
                        b's' if stack.is_none() => stack = Some(self.string(depth)?),
                        b'c' if cause.is_none() => cause = Some(self.value(depth)?),
                        b'.' => break,
                        _ => bail!("invalid clone error field"),
                    }
                }
                Object::Error {
                    prototype,
                    message,
                    stack,
                    cause,
                }
            }
            _ => unreachable!("object tags checked before reserving"),
        })
    }

    fn view(&mut self, buffer: u32) -> Result<Value> {
        let kind = ViewKind::decode(self.varint(8)? as u8)?;
        let offset = self.size()?;
        let length = self.size()?;
        let flags = if self.version >= 14 || self.broken_v13 {
            self.u32()?
        } else {
            0
        };
        let Some(Some(Object::ArrayBuffer {
            bytes, max_length, ..
        })) = self.objects.get(buffer as usize)
        else {
            bail!("view has no buffer");
        };
        ensure!(
            offset <= bytes.len() as u64 && length <= bytes.len() as u64 - offset,
            "clone view exceeds buffer"
        );
        let size = kind.element_size() as u64;
        ensure!(
            offset.is_multiple_of(size) && length.is_multiple_of(size),
            "misaligned clone view"
        );
        ensure!(
            flags & !3 == 0
                && (if max_length.is_some() {
                    flags & 2 != 0
                } else {
                    flags == 0
                }),
            "invalid clone view flags"
        );
        let id = self.reserve()?;
        self.objects[id as usize] = Some(Object::View {
            buffer,
            kind,
            offset,
            length,
            flags,
        });
        Ok(Value::Reference(id))
    }
}

/// Encode the persisted version cellar already writes, including explicit
/// references for cycles. Transfer-only capabilities need their host protocol.
pub fn encode(value: &StructuredValue) -> Result<Vec<u8>> {
    encode_with_limits(value, Limits::default())
}

pub fn encode_with_limits(value: &StructuredValue, limits: Limits) -> Result<Vec<u8>> {
    ensure!(
        value.objects.len() <= limits.objects,
        "clone object limit exceeded"
    );
    let mut writer = Writer {
        graph: value,
        bytes: vec![0xff, 15],
        ids: vec![None; value.objects.len()],
        next_id: 0,
        values: 0,
        limits,
    };
    writer.value(&value.root, 0)?;
    // Validate the constructed document through the same bounded reader.
    decode_with_limits(&writer.bytes, limits)?;
    Ok(writer.bytes)
}

struct Writer<'a> {
    graph: &'a StructuredValue,
    bytes: Vec<u8>,
    ids: Vec<Option<u32>>,
    next_id: u32,
    values: usize,
    limits: Limits,
}

impl Writer<'_> {
    fn raw(&mut self, bytes: &[u8]) -> Result<()> {
        ensure!(
            bytes.len() <= self.limits.bytes.saturating_sub(self.bytes.len()),
            "clone byte limit exceeded"
        );
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn byte(&mut self, byte: u8) -> Result<()> {
        self.raw(&[byte])
    }

    fn varint(&mut self, mut value: u32) -> Result<()> {
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            self.byte(byte | if value == 0 { 0 } else { 0x80 })?;
            if value == 0 {
                return Ok(());
            }
        }
    }

    fn size(&mut self, value: usize) -> Result<()> {
        self.varint(u32::try_from(value)?)
    }

    fn double(&mut self, value: f64) -> Result<()> {
        self.raw(&value.to_le_bytes())
    }

    fn string(&mut self, string: &[u16]) -> Result<()> {
        if string.iter().all(|unit| *unit <= 255) {
            self.byte(b'"')?;
            self.size(string.len())?;
            for unit in string {
                self.byte(*unit as u8)?;
            }
        } else {
            let len = u32::try_from(string.len())?
                .checked_mul(2)
                .ok_or_else(|| anyhow!("clone string length overflow"))?;
            let count = if len == 0 {
                1
            } else {
                (32 - len.leading_zeros()).div_ceil(7) as usize
            };
            if !(self.bytes.len() + 1 + count).is_multiple_of(2) {
                self.byte(0)?;
            }
            self.byte(b'c')?;
            self.varint(len)?;
            for unit in string {
                self.raw(&unit.to_le_bytes())?;
            }
        }
        Ok(())
    }

    fn bigint(&mut self, negative: bool, magnitude: &[u8]) -> Result<()> {
        ensure!(
            !negative || !magnitude.is_empty(),
            "invalid negative-zero clone bigint"
        );
        let len = u32::try_from(magnitude.len())?;
        ensure!(len <= u32::MAX >> 1, "clone bigint is too large");
        self.varint((len << 1) | u32::from(negative))?;
        self.raw(magnitude)
    }

    fn value(&mut self, value: &Value, depth: usize) -> Result<()> {
        ensure!(depth <= self.limits.depth, "clone nesting limit exceeded");
        self.values += 1;
        ensure!(
            self.values <= self.limits.values,
            "clone value limit exceeded"
        );
        match value {
            Value::Undefined => self.byte(b'_'),
            Value::Null => self.byte(b'0'),
            Value::Boolean(value) => self.byte(if *value { b'T' } else { b'F' }),
            Value::Number(value) => {
                self.byte(b'N')?;
                self.double(*value)
            }
            Value::BigInt {
                negative,
                magnitude,
            } => {
                self.byte(b'Z')?;
                self.bigint(*negative, magnitude)
            }
            Value::String(value) => self.string(value),
            Value::Hole => bail!("clone hole is outside a dense array"),
            Value::Reference(id) => self.reference(*id, depth + 1),
        }
    }

    fn assign(&mut self, id: u32) {
        self.ids[id as usize] = Some(self.next_id);
        self.next_id += 1;
    }

    fn reference(&mut self, id: u32, depth: usize) -> Result<()> {
        let object = self
            .graph
            .objects
            .get(id as usize)
            .ok_or_else(|| anyhow!("invalid clone object reference"))?;
        if let Some(wire_id) = self.ids[id as usize] {
            self.byte(b'^')?;
            return self.varint(wire_id);
        }
        ensure!(depth <= self.limits.depth, "clone nesting limit exceeded");
        if let Object::View {
            buffer,
            kind,
            offset,
            length,
            flags,
        } = object
        {
            ensure!(
                matches!(
                    self.graph.objects.get(*buffer as usize),
                    Some(Object::ArrayBuffer { .. })
                ),
                "view has no buffer"
            );
            self.value(&Value::Reference(*buffer), depth)?;
            self.assign(id);
            self.byte(b'V')?;
            self.varint(u32::from(kind.tag()))?;
            self.varint(u32::try_from(*offset)?)?;
            self.varint(u32::try_from(*length)?)?;
            return self.varint(*flags);
        }
        self.assign(id);
        self.object(object, depth)
    }

    fn properties(&mut self, properties: &Properties, end: u8, depth: usize) -> Result<()> {
        for (key, value) in properties {
            self.value(key, depth)?;
            self.value(value, depth)?;
        }
        self.byte(end)?;
        self.size(properties.len())
    }

    fn object(&mut self, object: &Object, depth: usize) -> Result<()> {
        match object {
            Object::Object(properties) => {
                self.byte(b'o')?;
                self.properties(properties, b'{', depth)
            }
            Object::SparseArray { length, properties } => {
                self.byte(b'a')?;
                self.varint(*length)?;
                self.properties(properties, b'@', depth)?;
                self.varint(*length)
            }
            Object::DenseArray {
                elements,
                properties,
            } => {
                self.byte(b'A')?;
                self.size(elements.len())?;
                for element in elements {
                    if matches!(element, Value::Hole) {
                        self.values += 1;
                        ensure!(
                            self.values <= self.limits.values,
                            "clone value limit exceeded"
                        );
                        self.byte(b'-')?;
                    } else {
                        self.value(element, depth)?;
                    }
                }
                self.properties(properties, b'$', depth)?;
                self.size(elements.len())
            }
            Object::Date(value) => {
                self.byte(b'D')?;
                self.double(*value)
            }
            Object::Boxed(value) => match value {
                Value::Boolean(value) => self.byte(if *value { b'y' } else { b'x' }),
                Value::Number(value) => {
                    self.byte(b'n')?;
                    self.double(*value)
                }
                Value::BigInt {
                    negative,
                    magnitude,
                } => {
                    self.byte(b'z')?;
                    self.bigint(*negative, magnitude)
                }
                Value::String(value) => {
                    self.byte(b's')?;
                    self.string(value)
                }
                _ => bail!("invalid boxed clone value"),
            },
            Object::RegExp { source, flags } => {
                self.byte(b'R')?;
                self.string(source)?;
                self.varint(*flags)
            }
            Object::Map(entries) => {
                self.byte(b';')?;
                for (key, value) in entries {
                    self.value(key, depth)?;
                    self.value(value, depth)?;
                }
                self.byte(b':')?;
                self.size(
                    entries
                        .len()
                        .checked_mul(2)
                        .ok_or_else(|| anyhow!("clone map length overflow"))?,
                )
            }
            Object::Set(entries) => {
                self.byte(b'\'')?;
                for value in entries {
                    self.value(value, depth)?;
                }
                self.byte(b',')?;
                self.size(entries.len())
            }
            Object::ArrayBuffer {
                bytes,
                max_length,
                immutable,
            } => {
                ensure!(
                    !(*immutable && max_length.is_some()),
                    "immutable buffer cannot be resizable"
                );
                self.byte(if max_length.is_some() {
                    b'~'
                } else if *immutable {
                    b'C'
                } else {
                    b'B'
                })?;
                self.size(bytes.len())?;
                if let Some(max) = max_length {
                    self.varint(u32::try_from(*max)?)?;
                }
                self.raw(bytes)
            }
            Object::Error {
                prototype,
                message,
                stack,
                cause,
            } => {
                self.byte(b'r')?;
                if let Some(prototype) = prototype {
                    ensure!(
                        matches!(prototype, b'E' | b'R' | b'F' | b'S' | b'T' | b'U'),
                        "invalid clone error prototype"
                    );
                    self.byte(*prototype)?;
                }
                if let Some(message) = message {
                    self.byte(b'm')?;
                    self.string(message)?;
                }
                if let Some(stack) = stack {
                    self.byte(b's')?;
                    self.string(stack)?;
                }
                if let Some(cause) = cause {
                    self.byte(b'c')?;
                    self.value(cause, depth)?;
                }
                self.byte(b'.')
            }
            Object::View { .. } => unreachable!("views write their buffers before receiving an id"),
        }
    }
}
