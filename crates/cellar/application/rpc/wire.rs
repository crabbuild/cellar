// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded Protobuf wire and decoded-layout preflight. It observes duplicate
//! occurrences before an ordinary codec can merge them into a larger graph.
use super::contract::{Contract, Field, Limits};
use anyhow::{bail, ensure, Result};
use prost_types::field_descriptor_proto::Type;
use std::{collections::BTreeMap, sync::OnceLock};

type Messages = BTreeMap<String, BTreeMap<u32, Field>>;
type MapDefaults = BTreeMap<String, String>;

#[derive(Debug)]
pub(super) struct ResourceLimit(&'static str);

impl std::fmt::Display for ResourceLimit {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.write_str(self.0)
    }
}

impl std::error::Error for ResourceLimit {}

struct NativeBytes<'a> {
    layouts: &'a BTreeMap<String, usize>,
    bytes: usize,
    maximum: usize,
    stack_bytes: usize,
    maximum_stack_bytes: usize,
}

struct Budget<'a> {
    elements: usize,
    slots: usize,
    native: Option<NativeBytes<'a>>,
}

impl Budget<'_> {
    fn slots(&mut self, count: usize) -> Result<()> {
        self.slots = self
            .slots
            .checked_sub(count)
            .ok_or(ResourceLimit("RPC decoded layout exceeds slot limit"))?;
        Ok(())
    }

    fn native_slots(&mut self, count: usize) -> Result<()> {
        if let Some(native) = &mut self.native {
            let bytes = count
                .checked_mul(super::native_decode::LAYOUT_BYTES)
                .and_then(|bytes| bytes.checked_mul(super::native_decode::CAPACITY_FACTOR))
                .ok_or(ResourceLimit("RPC native decoder layout exceeds capacity"))?;
            native.bytes = native
                .bytes
                .checked_add(bytes)
                .filter(|bytes| *bytes <= native.maximum)
                .ok_or(ResourceLimit(
                    "RPC native decoder exceeds its byte allowance",
                ))?;
        }
        Ok(())
    }

    fn native_message(&mut self, name: &str) -> Result<()> {
        if let Some(native) = &self.native {
            let count = *native
                .layouts
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("unknown native RPC layout"))?;
            self.native_slots(count)?;
        }
        Ok(())
    }

    fn native_enter(&mut self, name: &str) -> Result<Option<usize>> {
        self.native_message(name)?;
        let Some(native) = &mut self.native else {
            return Ok(None);
        };
        let previous = native.stack_bytes;
        let frame = native
            .layouts
            .get(name)
            .and_then(|slots| slots.checked_mul(super::native_decode::LAYOUT_BYTES))
            .and_then(|bytes| bytes.checked_mul(super::native_decode::CAPACITY_FACTOR))
            .and_then(|bytes| bytes.checked_add(super::native_decode::FRAME_BYTES))
            .ok_or(ResourceLimit("RPC native decoder stack exceeds capacity"))?;
        native.stack_bytes = previous
            .checked_add(frame)
            .filter(|bytes| *bytes <= native.maximum_stack_bytes)
            .ok_or(ResourceLimit(
                "RPC native decoder exceeds its stack allowance",
            ))?;
        Ok(Some(previous))
    }

    fn native_leave(&mut self, previous: Option<usize>) {
        if let (Some(native), Some(previous)) = (&mut self.native, previous) {
            native.stack_bytes = previous;
        }
    }
}

pub(super) fn validate(contract: &Contract, message: &str, bytes: &[u8]) -> Result<()> {
    if bytes.len() > contract.limits().max_message_bytes {
        return Err(ResourceLimit("RPC message exceeds byte limit").into());
    }
    let mut budget = Budget {
        elements: contract.limits().max_elements,
        slots: contract.limits().max_decoded_slots,
        native: None,
    };
    walk(
        &contract.messages,
        Some(&contract.map_defaults),
        contract.limits(),
        message,
        bytes,
        0,
        &mut budget,
    )
}

pub(super) fn native_decode_bytes(
    contract: &Contract,
    message: &str,
    bytes: &[u8],
    maximum: usize,
    maximum_stack_bytes: usize,
) -> Result<usize> {
    if bytes.len() > contract.limits().max_message_bytes {
        return Err(ResourceLimit("RPC message exceeds byte limit").into());
    }
    let fixed = bytes
        .len()
        .checked_mul(super::native_decode::CAPACITY_FACTOR)
        .and_then(|bytes| bytes.checked_add(super::native_decode::FIXED_BYTES))
        .filter(|bytes| *bytes <= maximum)
        .ok_or(ResourceLimit(
            "RPC native decoder exceeds its byte allowance",
        ))?;
    let mut budget = Budget {
        elements: contract.limits().max_elements,
        slots: contract.limits().max_decoded_slots,
        native: Some(NativeBytes {
            layouts: &contract.native_layouts,
            bytes: fixed,
            maximum,
            stack_bytes: 0,
            maximum_stack_bytes,
        }),
    };
    walk(
        &contract.messages,
        Some(&contract.map_defaults),
        contract.limits(),
        message,
        bytes,
        0,
        &mut budget,
    )?;
    Ok(budget
        .native
        .expect("native decoder accounting is installed")
        .bytes)
}

pub(super) fn validate_reply(bytes: &[u8], limits: Limits) -> Result<()> {
    static MESSAGES: OnceLock<Messages> = OnceLock::new();
    let messages = MESSAGES.get_or_init(|| {
        let field = |kind, type_name: &str| Field {
            kind,
            type_name: type_name.into(),
            repeated: false,
        };
        BTreeMap::from([
            (
                "Reply".into(),
                BTreeMap::from([
                    (1, field(Type::Bytes, "")),
                    (2, field(Type::Message, "Failure")),
                ]),
            ),
            (
                "Failure".into(),
                BTreeMap::from([
                    (1, field(Type::Int32, "")),
                    (2, field(Type::Int32, "")),
                    (3, field(Type::String, "")),
                ]),
            ),
        ])
    });
    ensure!(
        bytes.len() <= limits.reply_limit(),
        "RPC reply exceeds byte limit"
    );
    // Framework control metadata has its own fixed allowance. A tiny business
    // limit must still carry a trustworthy refusal or an empty response.
    let control = Limits {
        max_elements: 16,
        max_depth: 2,
        max_decoded_slots: 32,
        ..limits
    };
    let mut budget = Budget {
        elements: 16,
        slots: 32,
        native: None,
    };
    walk(messages, None, control, "Reply", bytes, 0, &mut budget)
}

/// Borrow the sole failure arm without decoding a business-response buffer.
/// Full control validation still precedes the caller's failure codec. Reuse the
/// wire parser so non-minimal varints cannot bypass outcome normalization.
pub(super) fn failure_payload(bytes: &[u8]) -> Option<&[u8]> {
    payload(bytes, 0x12)
}

/// Borrow the sole response body after complete framework control validation.
pub(super) fn response_payload(bytes: &[u8]) -> Option<&[u8]> {
    payload(bytes, 0x0a)
}

fn payload(mut bytes: &[u8], tag: u64) -> Option<&[u8]> {
    if varint(&mut bytes).ok()? != tag {
        return None;
    }
    let payload = delimited(&mut bytes).ok()?;
    bytes.is_empty().then_some(payload)
}

fn charge(remaining: &mut Budget<'_>) -> Result<()> {
    remaining.elements = remaining
        .elements
        .checked_sub(1)
        .ok_or(ResourceLimit("RPC message exceeds element limit"))?;
    remaining.slots(1)?;
    remaining.native_slots(1)
}

fn walk(
    messages: &Messages,
    map_defaults: Option<&MapDefaults>,
    limits: Limits,
    message: &str,
    mut bytes: &[u8],
    depth: usize,
    remaining: &mut Budget<'_>,
) -> Result<()> {
    if depth > limits.max_depth {
        return Err(ResourceLimit("RPC message exceeds depth limit").into());
    }
    let fields = messages
        .get(message)
        .ok_or_else(|| anyhow::anyhow!("unknown RPC message"))?;
    remaining.slots(fields.len() + 1)?;
    let previous_stack = remaining.native_enter(message)?;
    let default = map_defaults.and_then(|maps| maps.get(message));
    let mut map_value_present = false;
    let mut control_seen = 0_u8;
    while !bytes.is_empty() {
        charge(remaining)?;
        let tag = varint(&mut bytes)?;
        let number = tag >> 3;
        ensure!(
            number > 0 && number <= 536_870_911,
            "invalid Protobuf field number"
        );
        let wire = (tag & 7) as u8;
        if map_defaults.is_none() {
            ensure!(
                fields.contains_key(&(number as u32)),
                "unknown RPC control field"
            );
            let bit = 1_u8 << number;
            ensure!(
                control_seen & bit == 0 && (message != "Reply" || control_seen == 0),
                "duplicate RPC control field or reply result"
            );
            control_seen |= bit;
        }
        if number == 2 && default.is_some() {
            map_value_present = true;
        }
        match fields.get(&(number as u32)) {
            Some(field) if wire == 2 && field.repeated && packable(field.kind) => {
                let mut packed = delimited(&mut bytes)?;
                while !packed.is_empty() {
                    charge(remaining)?;
                    scalar(field, scalar_wire(field.kind), &mut packed)?;
                }
            }
            Some(field) => {
                ensure!(
                    wire == scalar_wire(field.kind),
                    "invalid Protobuf field wire type"
                );
                match field.kind {
                    Type::Message => walk(
                        messages,
                        map_defaults,
                        limits,
                        &field.type_name,
                        delimited(&mut bytes)?,
                        depth + 1,
                        remaining,
                    )?,
                    Type::String => {
                        std::str::from_utf8(delimited(&mut bytes)?)?;
                    }
                    Type::Bytes => {
                        delimited(&mut bytes)?;
                    }
                    _ => scalar(field, wire, &mut bytes)?,
                }
            }
            None => skip(wire, &mut bytes)?,
        }
    }
    // A missing message-valued map value still constructs a default object in
    // language decoders. Charge its flat layout without pretending it was a
    // nested wire occurrence or recursively constructing any default messages.
    if let Some(default) = default.filter(|_| !map_value_present) {
        let fields = messages
            .get(default)
            .ok_or_else(|| anyhow::anyhow!("unknown RPC map default"))?;
        remaining.slots(fields.len() + 1)?;
        remaining.native_message(default)?;
    }
    remaining.native_leave(previous_stack);
    Ok(())
}

fn scalar(field: &Field, wire: u8, bytes: &mut &[u8]) -> Result<()> {
    match wire {
        0 => {
            let value = varint(bytes)?;
            match field.kind {
                Type::Uint32 | Type::Sint32 => {
                    ensure!(value <= u32::MAX as u64, "Protobuf integer exceeds 32 bits")
                }
                Type::Int32 | Type::Enum => ensure!(
                    value <= u32::MAX as u64 || value >= i32::MIN as i64 as u64,
                    "Protobuf signed integer exceeds 32 bits"
                ),
                _ => {}
            }
        }
        1 => {
            take(bytes, 8)?;
        }
        5 => {
            take(bytes, 4)?;
        }
        _ => bail!("invalid Protobuf scalar wire type"),
    }
    Ok(())
}

fn scalar_wire(kind: Type) -> u8 {
    match kind {
        Type::Double | Type::Fixed64 | Type::Sfixed64 => 1,
        Type::String | Type::Bytes | Type::Message => 2,
        Type::Float | Type::Fixed32 | Type::Sfixed32 => 5,
        _ => 0,
    }
}

fn packable(kind: Type) -> bool {
    !matches!(
        kind,
        Type::String | Type::Bytes | Type::Message | Type::Group
    )
}

fn skip(wire: u8, bytes: &mut &[u8]) -> Result<()> {
    match wire {
        0 => {
            varint(bytes)?;
        }
        1 => {
            take(bytes, 8)?;
        }
        2 => {
            delimited(bytes)?;
        }
        5 => {
            take(bytes, 4)?;
        }
        _ => bail!("Protobuf groups/invalid wire types are unsupported"),
    }
    Ok(())
}

fn delimited<'a>(bytes: &mut &'a [u8]) -> Result<&'a [u8]> {
    let n = usize::try_from(varint(bytes)?)?;
    take(bytes, n)
}

fn take<'a>(bytes: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    ensure!(n <= bytes.len(), "truncated Protobuf field");
    let (value, rest) = bytes.split_at(n);
    *bytes = rest;
    Ok(value)
}

fn varint(bytes: &mut &[u8]) -> Result<u64> {
    let mut result = 0;
    for shift in (0..70).step_by(7) {
        let byte = *take(bytes, 1)?
            .first()
            .ok_or_else(|| anyhow::anyhow!("truncated varint"))?;
        ensure!(shift != 63 || byte <= 1, "Protobuf varint exceeds 64 bits");
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
    }
    bail!("unterminated Protobuf varint")
}
