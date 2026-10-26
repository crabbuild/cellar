// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Allocation and schema-version admission before descriptor decoding.

use anyhow::{bail, ensure, Context as _, Result};
use prost::encoding::{decode_key, decode_varint, WireType};

use super::MAX_MESSAGE_DEPTH;

// Declarations and their option/metadata records have a separate aggregate
// bound. Keep room for the existing 100,000-symbol inventory budget to remain
// independently enforceable, even when declarations have option records.
const MAX_NODES: usize = 200_000;
const MAX_WIRE_FIELDS: usize = 1_000_000;

/// The descriptor model understood by the pinned prost-types dependency. A
/// new attribute must be supported explicitly, not silently discarded while
/// computing the immutable contract artifact. Business-message unknown-field
/// handling is independent of this descriptor admission policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Node {
    Set,
    File,
    Message,
    Field,
    Oneof,
    Enum,
    EnumValue,
    Service,
    Method,
    Range,
    SourceInfo,
    Location,
    FileOptions,
    MessageOptions,
    FieldOptions,
    OneofOptions,
    EnumOptions,
    EnumValueOptions,
    ServiceOptions,
    MethodOptions,
}

impl Node {
    fn fields(self) -> &'static [u32] {
        match self {
            Self::Set => &[1],
            Self::File => &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            Self::Message => &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            Self::Field => &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 17],
            Self::Oneof | Self::Range => &[1, 2],
            Self::Enum => &[1, 2, 3, 4, 5],
            Self::EnumValue | Self::Service => &[1, 2, 3],
            Self::Method => &[1, 2, 3, 4, 5, 6],
            Self::SourceInfo => &[1],
            Self::Location => &[1, 2, 3, 4, 6],
            Self::FileOptions => &[
                1, 8, 9, 10, 11, 16, 17, 18, 20, 23, 27, 31, 36, 37, 39, 40, 41, 42, 44, 45,
            ],
            Self::MessageOptions => &[1, 2, 3, 7],
            Self::FieldOptions => &[1, 2, 3, 5, 6, 10],
            Self::OneofOptions => &[],
            Self::EnumOptions => &[2, 3],
            Self::EnumValueOptions => &[1],
            Self::ServiceOptions => &[33],
            Self::MethodOptions => &[33, 34],
        }
    }

    fn is_options(self) -> bool {
        matches!(
            self,
            Self::FileOptions
                | Self::MessageOptions
                | Self::FieldOptions
                | Self::OneofOptions
                | Self::EnumOptions
                | Self::EnumValueOptions
                | Self::ServiceOptions
                | Self::MethodOptions
        )
    }

    fn child(self, tag: u32) -> Option<Self> {
        Some(match (self, tag) {
            (Self::Set, 1) => Self::File,
            (Self::File, 4) | (Self::Message, 3) => Self::Message,
            (Self::File, 5) | (Self::Message, 4) => Self::Enum,
            (Self::File, 6) => Self::Service,
            (Self::File, 8) => Self::FileOptions,
            (Self::File, 9) => Self::SourceInfo,
            (Self::Message, 2) => Self::Field,
            (Self::Message, 7) => Self::MessageOptions,
            (Self::Message, 8) => Self::Oneof,
            (Self::Message, 9) | (Self::Enum, 4) => Self::Range,
            (Self::Field, 8) => Self::FieldOptions,
            (Self::Oneof, 2) => Self::OneofOptions,
            (Self::Enum, 2) => Self::EnumValue,
            (Self::Enum, 3) => Self::EnumOptions,
            (Self::EnumValue, 3) => Self::EnumValueOptions,
            (Self::Service, 2) => Self::Method,
            (Self::Service, 3) => Self::ServiceOptions,
            (Self::Method, 4) => Self::MethodOptions,
            (Self::SourceInfo, 1) => Self::Location,
            _ => return None,
        })
    }
}

#[derive(Default)]
struct Budget {
    nodes: usize,
    wire_fields: usize,
    files: usize,
    types: usize,
    fields: usize,
}

impl Budget {
    fn node(&mut self, kind: Node) -> Result<()> {
        self.nodes += 1;
        ensure!(
            self.nodes <= MAX_NODES,
            "RPC descriptor node limit exceeded"
        );
        match kind {
            Node::File => {
                self.files += 1;
                ensure!(self.files <= 256, "invalid RPC descriptor file count");
            }
            Node::Message | Node::Enum => {
                self.types += 1;
                ensure!(self.types <= 4096, "too many RPC message and enum types");
            }
            Node::Field => {
                self.fields += 1;
                ensure!(self.fields <= 65_536, "too many RPC fields");
            }
            _ => {}
        }
        Ok(())
    }

    fn field(&mut self) -> Result<()> {
        self.wire_fields += 1;
        ensure!(
            self.wire_fields <= MAX_WIRE_FIELDS,
            "RPC descriptor wire-field limit exceeded"
        );
        Ok(())
    }
}

struct Frame<'a> {
    kind: Node,
    remaining: &'a [u8],
    message_depth: usize,
}

/// Walk borrowed slices with an explicitly bounded stack before prost allocates
/// any descriptor collections. Count encoded occurrences before singular-message
/// merging, including source metadata that normalization will discard later.
pub(super) fn preflight(bytes: &[u8]) -> Result<()> {
    let mut budget = Budget::default();
    budget.node(Node::Set)?;
    let mut stack = vec![Frame {
        kind: Node::Set,
        remaining: bytes,
        message_depth: 0,
    }];
    while let Some(mut frame) = stack.pop() {
        while !frame.remaining.is_empty() {
            let (tag, wire) =
                decode_key(&mut frame.remaining).context("invalid RPC descriptor key")?;
            budget.field()?;
            ensure!(
                !(frame.kind.is_options() && tag == 999),
                "unresolved RPC descriptor options"
            );
            ensure!(
                frame.kind.fields().contains(&tag),
                "unsupported RPC descriptor field {tag} in {:?}",
                frame.kind
            );
            ensure!(
                !matches!((frame.kind, tag), (Node::File, 7) | (Node::Message, 5 | 6)),
                "RPC extensions are unsupported"
            );
            let body = take(&mut frame.remaining, wire)?;
            if let Some(kind) = frame.kind.child(tag) {
                let body = body.context("invalid RPC descriptor message wire type")?;
                budget.node(kind)?;
                let message_depth = if kind == Node::Message {
                    if frame.kind == Node::Message {
                        frame.message_depth + 1
                    } else {
                        0
                    }
                } else {
                    frame.message_depth
                };
                ensure!(
                    message_depth <= MAX_MESSAGE_DEPTH,
                    "RPC descriptor nesting exceeds limit"
                );
                stack.push(frame);
                stack.push(Frame {
                    kind,
                    remaining: body,
                    message_depth,
                });
                break;
            }
        }
    }
    Ok(())
}

fn take<'a>(bytes: &mut &'a [u8], wire: WireType) -> Result<Option<&'a [u8]>> {
    let (length, message) = match wire {
        WireType::Varint => {
            decode_varint(bytes).context("invalid RPC descriptor varint")?;
            return Ok(None);
        }
        WireType::ThirtyTwoBit => (4, false),
        WireType::SixtyFourBit => (8, false),
        WireType::LengthDelimited => (
            usize::try_from(decode_varint(bytes).context("invalid RPC descriptor length")?)?,
            true,
        ),
        WireType::StartGroup | WireType::EndGroup => bail!("RPC descriptor groups are unsupported"),
    };
    ensure!(length <= bytes.len(), "truncated RPC descriptor field");
    let (body, remaining) = bytes.split_at(length);
    *bytes = remaining;
    Ok(message.then_some(body))
}

#[cfg(test)]
#[path = "admission_tests.rs"]
mod tests;
