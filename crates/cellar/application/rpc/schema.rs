// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Descriptor semantics that must agree before any language generator runs.
use anyhow::{ensure, Context, Result};
use prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, EnumDescriptorProto, FieldDescriptorProto,
};
use std::collections::{BTreeMap, BTreeSet};

use super::contract::valid_identifier;

pub(super) fn messages(messages: &BTreeMap<String, &DescriptorProto>) -> Result<()> {
    let mut map_uses = BTreeSet::new();
    for (name, message) in messages {
        ensure!(
            !message
                .options
                .as_ref()
                .is_some_and(|options| options.message_set_wire_format()),
            "RPC MessageSet wire format is unsupported in proto3"
        );
        oneofs(message).with_context(|| format!("invalid RPC oneofs in {name}"))?;
        reserved_fields(message).with_context(|| format!("invalid RPC reservations in {name}"))?;
        for field in &message.field {
            let Some(target) = field
                .type_name
                .as_deref()
                .and_then(|name| name.strip_prefix('.'))
            else {
                continue;
            };
            let Some(entry) = messages.get(target).filter(|entry| is_map(entry)) else {
                continue;
            };
            map_field(name, field, target, entry)
                .with_context(|| format!("invalid RPC map in {name}"))?;
            ensure!(map_uses.insert(target), "RPC map entry has multiple owners");
        }
    }
    for (name, message) in messages {
        if is_map(message) {
            ensure!(map_uses.contains(name.as_str()), "orphaned RPC map entry");
        }
    }
    Ok(())
}

pub(super) fn is_map(message: &DescriptorProto) -> bool {
    message
        .options
        .as_ref()
        .is_some_and(|options| options.map_entry())
}

fn oneofs(message: &DescriptorProto) -> Result<()> {
    let mut members = vec![Vec::new(); message.oneof_decl.len()];
    for field in &message.field {
        if let Some(index) = field.oneof_index {
            let index = usize::try_from(index).context("negative RPC oneof index")?;
            let fields = members.get_mut(index).context("unknown RPC oneof index")?;
            ensure!(
                field.label == Some(Label::Optional as i32),
                "RPC oneof fields must be singular"
            );
            fields.push(field);
        } else {
            ensure!(
                !field.proto3_optional(),
                "optional RPC field has no synthetic oneof"
            );
        }
    }
    let mut synthetic_seen = false;
    for fields in members {
        ensure!(!fields.is_empty(), "empty RPC oneof");
        if fields.iter().any(|field| field.proto3_optional()) {
            ensure!(
                fields.len() == 1,
                "synthetic RPC oneof must have exactly one optional field"
            );
            synthetic_seen = true;
        } else {
            ensure!(
                !synthetic_seen,
                "synthetic RPC oneofs must follow real oneofs"
            );
        }
    }
    Ok(())
}

fn reserved_fields(message: &DescriptorProto) -> Result<()> {
    let mut names = BTreeSet::new();
    for name in &message.reserved_name {
        ensure!(
            valid_identifier(name, false) && names.insert(name.as_str()),
            "invalid or duplicate reserved RPC field name"
        );
    }
    let mut ranges = Vec::new();
    for range in &message.reserved_range {
        let start = range.start.context("missing reserved RPC range start")?;
        let end = range.end.context("missing reserved RPC range end")?;
        ensure!(
            start >= 1 && start < end && end <= 536_870_912,
            "invalid reserved RPC field range"
        );
        ranges.push((start, end));
    }
    ranges.sort_unstable();
    ensure!(
        ranges.windows(2).all(|pair| pair[0].1 <= pair[1].0),
        "overlapping reserved RPC field ranges"
    );
    for field in &message.field {
        ensure!(
            !names.contains(field.name.as_deref().unwrap_or_default()),
            "RPC field reuses a reserved name"
        );
        if let Some(number) = field.number {
            let index = ranges.partition_point(|(start, _)| *start <= number);
            ensure!(
                index == 0 || number >= ranges[index - 1].1,
                "RPC field reuses a reserved number"
            );
        }
    }
    Ok(())
}

fn map_field(
    parent: &str,
    field: &FieldDescriptorProto,
    target: &str,
    entry: &DescriptorProto,
) -> Result<()> {
    ensure!(
        field.r#type == Some(Type::Message as i32)
            && field.label == Some(Label::Repeated as i32)
            && field.oneof_index.is_none()
            && !field.proto3_optional(),
        "RPC map must be a repeated message field"
    );
    let field_name = field.name.as_deref().context("unnamed RPC map field")?;
    let mut entry_name = String::new();
    let mut uppercase = true;
    // Protoc's map-entry spelling capitalizes after underscores, retaining all
    // other letters. General language case converters change names such as IDs.
    for character in field_name.chars() {
        if character == '_' {
            uppercase = true;
        } else {
            entry_name.push(if uppercase {
                character.to_ascii_uppercase()
            } else {
                character
            });
            uppercase = false;
        }
    }
    entry_name.push_str("Entry");
    ensure!(
        target == format!("{parent}.{entry_name}"),
        "RPC map entry must be nested in its owner with protoc's generated name"
    );
    ensure!(
        entry.field.len() == 2
            && entry.nested_type.is_empty()
            && entry.enum_type.is_empty()
            && entry.oneof_decl.is_empty()
            && entry.extension.is_empty()
            && entry.extension_range.is_empty(),
        "invalid RPC map entry structure"
    );
    for (field, number, name) in [(&entry.field[0], 1, "key"), (&entry.field[1], 2, "value")] {
        ensure!(
            field.number == Some(number)
                && field.name.as_deref() == Some(name)
                && field.label == Some(Label::Optional as i32)
                && field.oneof_index.is_none()
                && !field.proto3_optional(),
            "invalid RPC map key/value field"
        );
    }
    let key = Type::try_from(entry.field[0].r#type.context("missing RPC map key type")?)?;
    ensure!(
        matches!(
            key,
            Type::Int32
                | Type::Int64
                | Type::Uint32
                | Type::Uint64
                | Type::Sint32
                | Type::Sint64
                | Type::Fixed32
                | Type::Fixed64
                | Type::Sfixed32
                | Type::Sfixed64
                | Type::Bool
                | Type::String
        ),
        "unsupported RPC map key type"
    );
    Ok(())
}

pub(super) fn enumeration(definition: &EnumDescriptorProto) -> Result<()> {
    ensure!(
        definition.value.first().and_then(|value| value.number) == Some(0),
        "proto3 RPC enums must start with zero"
    );
    let aliases = definition
        .options
        .as_ref()
        .is_some_and(|options| options.allow_alias());
    let mut names = BTreeSet::new();
    let mut numbers = BTreeSet::new();
    let mut reserved_names = BTreeSet::new();
    for name in &definition.reserved_name {
        ensure!(
            valid_identifier(name, false) && reserved_names.insert(name.as_str()),
            "invalid or duplicate reserved RPC enum name"
        );
    }
    let mut ranges = Vec::new();
    for range in &definition.reserved_range {
        let start = range
            .start
            .context("missing reserved RPC enum range start")?;
        let end = range.end.context("missing reserved RPC enum range end")?;
        ensure!(start <= end, "invalid reserved RPC enum range");
        ranges.push((start, end));
    }
    ranges.sort_unstable();
    ensure!(
        ranges.windows(2).all(|pair| pair[0].1 < pair[1].0),
        "overlapping reserved RPC enum ranges"
    );
    for value in &definition.value {
        let name = value.name.as_deref().context("unnamed RPC enum value")?;
        let number = value.number.context("RPC enum value has no number")?;
        ensure!(
            valid_identifier(name, false) && names.insert(name),
            "invalid or duplicate RPC enum value name"
        );
        ensure!(
            numbers.insert(number) || aliases,
            "duplicate RPC enum number requires allow_alias"
        );
        ensure!(
            !reserved_names.contains(name),
            "RPC enum value reuses a reserved name"
        );
        let index = ranges.partition_point(|(start, _)| *start <= number);
        ensure!(
            index == 0 || number > ranges[index - 1].1,
            "RPC enum value reuses a reserved number"
        );
    }
    Ok(())
}

#[cfg(test)]
#[path = "schema_tests.rs"]
mod tests;
