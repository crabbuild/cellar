// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{Code, Failure};
use anyhow::{ensure, Context as _, Result};
use prost::Message;
use prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, FieldDescriptorProto, FileDescriptorSet,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

/// Explicit per-message validation budgets, shared by all language backends.
#[derive(Clone, Copy, Debug, Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Limits {
    pub max_message_bytes: usize,
    pub max_depth: usize,
    pub max_elements: usize,
    /// Estimated materialized slots: message layout fields/objects plus wire
    /// occurrences, including omitted map-message defaults. Not an RSS cap.
    pub max_decoded_slots: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_message_bytes: 1024 * 1024,
            max_depth: 64,
            max_elements: 100_000,
            max_decoded_slots: 1_000_000,
        }
    }
}

impl Limits {
    pub fn validate(self) -> Result<Self> {
        ensure!(
            (128..=16 * 1024 * 1024).contains(&self.max_message_bytes),
            "invalid RPC byte limit"
        );
        ensure!(
            (1..=64).contains(&self.max_depth),
            "invalid RPC depth limit"
        );
        ensure!(
            (1..=1_000_000).contains(&self.max_elements),
            "invalid RPC element limit"
        );
        ensure!(
            (1..=4_000_000).contains(&self.max_decoded_slots),
            "invalid RPC decoded-slot limit"
        );
        Ok(self)
    }

    pub(super) fn reply_limit(self) -> usize {
        self.max_message_bytes.saturating_add(1024)
    }
}

/// A fully qualified unary operation and its descriptor-owned message types.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Method {
    pub name: String,
    pub input: String,
    pub output: String,
}

#[derive(Clone, Debug)]
pub(super) struct Field {
    pub kind: Type,
    pub type_name: String,
    pub repeated: bool,
}

/// Immutable, validated descriptor inventory. Its digest identifies the exact
/// normalized artifact used for every generated language, not a message hash.
#[derive(Clone, Debug)]
pub struct Contract {
    descriptor: Vec<u8>,
    digest: String,
    methods: BTreeMap<String, Method>,
    pub(super) messages: BTreeMap<String, BTreeMap<u32, Field>>,
    pub(super) map_defaults: BTreeMap<String, String>,
    limits: Limits,
    codec_shapes: serde_json::Value,
}

impl Contract {
    // Build-time artifact readers apply this before allocating descriptor input.
    pub(crate) const MAX_DESCRIPTOR_BYTES: usize = 4 * 1024 * 1024;

    /// Load an application descriptor set at startup, with no I/O or runtime
    /// schema lookup. Unsupported features and unresolved imports are refused.
    /// Cross-file symbol collisions, undeclared type references and cyclic
    /// imports are rejected within bounded expanded-name inventories.
    pub fn from_descriptor(bytes: &[u8], limits: Limits) -> Result<Self> {
        let limits = limits.validate()?;
        ensure!(
            bytes.len() <= Self::MAX_DESCRIPTOR_BYTES,
            "RPC descriptor exceeds limit"
        );
        let mut set = FileDescriptorSet::decode(bytes).context("invalid RPC descriptor")?;
        ensure!(
            !set.file.is_empty() && set.file.len() <= 256,
            "invalid RPC descriptor file count"
        );
        set.file.sort_by(|a, b| a.name.cmp(&b.name));
        // Validate the complete namespace and import graph before cloning any
        // message subtrees or projecting language codec inventories.
        super::descriptor::validate(&set)?;
        let mut message_defs = BTreeMap::new();
        let mut enums = BTreeSet::new();
        for file in &mut set.file {
            file.source_code_info = None;
            let package = file.package.as_deref().unwrap_or_default();
            for definition in &file.message_type {
                collect_messages(package, definition, 0, &mut message_defs, &mut enums)?;
            }
            for definition in &file.enum_type {
                super::schema::enumeration(definition)?;
                let name = qualify(
                    package,
                    definition.name.as_deref().context("unnamed RPC enum")?,
                );
                ensure!(valid_identifier(&name, true), "invalid RPC enum name");
                ensure!(enums.insert(name), "duplicate RPC enum");
            }
        }
        ensure!(
            message_defs.len() + enums.len() <= 4096,
            "too many RPC message and enum types"
        );
        super::schema::messages(&message_defs)?;
        let mut messages = BTreeMap::new();
        let mut map_defaults = BTreeMap::new();
        let mut field_count = 0;
        for (name, definition) in &message_defs {
            if super::schema::is_map(definition) && definition.field[1].r#type() == Type::Message {
                map_defaults.insert(
                    name.clone(),
                    definition.field[1]
                        .type_name()
                        .trim_start_matches('.')
                        .to_string(),
                );
            }
            ensure!(!enums.contains(name), "RPC type name collision");
            ensure!(
                name != "google.protobuf.Any",
                "RPC Any messages are unsupported"
            );
            let mut fields = BTreeMap::new();
            let mut oneof_names = BTreeSet::new();
            for oneof in &definition.oneof_decl {
                let name = oneof.name.as_deref().context("unnamed RPC oneof")?;
                ensure!(
                    valid_identifier(name, false) && oneof_names.insert(name),
                    "invalid or duplicate RPC oneof"
                );
            }
            let mut field_names = BTreeSet::new();
            for field in &definition.field {
                ensure!(
                    field.default_value.is_none(),
                    "proto3 RPC fields cannot declare defaults"
                );
                field_count += 1;
                ensure!(field_count <= 65_536, "too many RPC fields");
                let number = field.number.context("RPC field has no number")?;
                ensure!(
                    (1..=536_870_911).contains(&number) && !(19000..=19999).contains(&number),
                    "invalid RPC field number"
                );
                let field_name = field.name.as_deref().context("unnamed RPC field")?;
                ensure!(
                    valid_identifier(field_name, false)
                        && field_names.insert(field_name)
                        && !oneof_names.contains(field_name),
                    "invalid or duplicate RPC field name"
                );
                let field_type = convert_field(field, &message_defs, &enums)?;
                ensure!(
                    fields.insert(number as u32, field_type).is_none(),
                    "duplicate RPC field number"
                );
            }
            messages.insert(name.clone(), fields);
        }
        let mut methods = BTreeMap::new();
        let mut service_names = BTreeSet::new();
        for file in &set.file {
            for service in &file.service {
                let service_name = qualify(
                    file.package.as_deref().unwrap_or_default(),
                    service.name.as_deref().context("unnamed RPC service")?,
                );
                ensure!(
                    valid_identifier(&service_name, true)
                        && !messages.contains_key(&service_name)
                        && !enums.contains(&service_name)
                        && service_names.insert(service_name.clone()),
                    "invalid or duplicate RPC service"
                );
                for method in &service.method {
                    ensure!(
                        !method.client_streaming.unwrap_or(false)
                            && !method.server_streaming.unwrap_or(false),
                        "streaming RPC methods are unsupported"
                    );
                    let short = method.name.as_deref().context("unnamed RPC method")?;
                    ensure!(valid_identifier(short, false), "invalid RPC method name");
                    let name = format!("{service_name}/{short}");
                    ensure!(name.len() <= 256, "RPC method exceeds name limit");
                    let input = type_name(
                        method
                            .input_type
                            .as_deref()
                            .context("missing RPC input type")?,
                    )?;
                    let output = type_name(
                        method
                            .output_type
                            .as_deref()
                            .context("missing RPC output type")?,
                    )?;
                    ensure!(
                        messages.contains_key(&input) && messages.contains_key(&output),
                        "unresolved RPC method type"
                    );
                    ensure!(
                        !super::schema::is_map(&message_defs[&input])
                            && !super::schema::is_map(&message_defs[&output]),
                        "RPC methods cannot use synthetic map entries as messages"
                    );
                    ensure!(
                        methods
                            .insert(
                                name.clone(),
                                Method {
                                    name,
                                    input,
                                    output
                                }
                            )
                            .is_none(),
                        "duplicate RPC method"
                    );
                }
            }
        }
        ensure!(
            !methods.is_empty() && methods.len() <= 4096,
            "invalid RPC method count"
        );
        let codec_shapes = super::shape::project(&set);
        let descriptor = set.encode_to_vec();
        let digest = hex::encode(Sha256::digest(&descriptor));
        Ok(Self {
            descriptor,
            digest,
            methods,
            messages,
            map_defaults,
            limits,
            codec_shapes,
        })
    }

    pub fn digest(&self) -> &str {
        &self.digest
    }

    pub fn descriptor(&self) -> &[u8] {
        &self.descriptor
    }

    pub fn limits(&self) -> Limits {
        self.limits
    }

    pub fn methods(&self) -> impl Iterator<Item = &Method> {
        self.methods.values()
    }

    pub fn method(&self, name: &str) -> Option<&Method> {
        self.methods.get(name)
    }

    /// Check a message before handing its bytes to an allocating language codec.
    pub fn validate_message(&self, message: &str, bytes: &[u8]) -> Result<(), Failure> {
        super::wire::validate(self, message.trim_start_matches('.'), bytes).map_err(|error| {
            Failure::refused(if error.is::<super::wire::ResourceLimit>() {
                Code::ResourceExhausted
            } else {
                Code::InvalidArgument
            })
        })
    }

    /// Stable startup metadata for native registrations and guest export tables.
    pub fn inventory(&self) -> serde_json::Value {
        serde_json::json!({"digest": self.digest, "limits": self.limits, "methods": self.methods.values().collect::<Vec<_>>(), "types":self.codec_shapes})
    }
}

fn collect_messages(
    prefix: &str,
    definition: &DescriptorProto,
    depth: usize,
    messages: &mut BTreeMap<String, DescriptorProto>,
    enums: &mut BTreeSet<String>,
) -> Result<()> {
    ensure!(depth <= 64, "RPC descriptor nesting exceeds limit");
    let name = definition.name.as_deref().context("unnamed RPC message")?;
    ensure!(valid_identifier(name, false), "invalid RPC message name");
    let name = qualify(prefix, name);
    ensure!(
        definition.extension.is_empty() && definition.extension_range.is_empty(),
        "RPC extensions are unsupported"
    );
    ensure!(
        messages.insert(name.clone(), definition.clone()).is_none(),
        "duplicate RPC message"
    );
    for nested in &definition.nested_type {
        collect_messages(&name, nested, depth + 1, messages, enums)?;
    }
    for nested in &definition.enum_type {
        super::schema::enumeration(nested)?;
        let name = qualify(&name, nested.name.as_deref().context("unnamed RPC enum")?);
        ensure!(valid_identifier(&name, true), "invalid RPC enum name");
        ensure!(enums.insert(name), "duplicate RPC enum");
    }
    Ok(())
}

fn convert_field(
    field: &FieldDescriptorProto,
    messages: &BTreeMap<String, DescriptorProto>,
    enums: &BTreeSet<String>,
) -> Result<Field> {
    ensure!(
        field.options.as_ref().and_then(|options| options.jstype).unwrap_or(0) == 0,
        "RPC contracts require the portable bigint representation; jstype overrides are unsupported"
    );
    let kind = Type::try_from(field.r#type.context("RPC field has no type")?)?;
    ensure!(kind != Type::Group, "RPC groups are unsupported");
    let label = Label::try_from(field.label.context("RPC field has no label")?)?;
    ensure!(
        label != Label::Required && field.extendee.is_none(),
        "RPC required fields/extensions are unsupported"
    );
    let type_name = if matches!(kind, Type::Message | Type::Enum) {
        let name = type_name(
            field
                .type_name
                .as_deref()
                .context("RPC field has no referenced type")?,
        )?;
        ensure!(
            if kind == Type::Message {
                messages.contains_key(&name)
            } else {
                enums.contains(&name)
            },
            "unresolved RPC field type"
        );
        name
    } else {
        ensure!(
            field.type_name.is_none(),
            "scalar RPC field has a type name"
        );
        String::new()
    };
    if field
        .options
        .as_ref()
        .and_then(|options| options.packed)
        .is_some()
    {
        ensure!(
            label == Label::Repeated && !matches!(kind, Type::Message | Type::String | Type::Bytes),
            "packed RPC option requires a repeated packable scalar"
        );
    }
    Ok(Field {
        kind,
        type_name,
        repeated: label == Label::Repeated,
    })
}

fn type_name(name: &str) -> Result<String> {
    ensure!(
        name.starts_with('.') && valid_identifier(&name[1..], true),
        "RPC types must be fully qualified"
    );
    Ok(name[1..].to_string())
}

fn qualify(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}.{name}")
    }
}

pub(super) fn valid_identifier(name: &str, qualified: bool) -> bool {
    !name.is_empty()
        && name.split('.').all(|part| {
            let mut bytes = part.bytes();
            bytes
                .next()
                .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
                && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
        })
        && (qualified || !name.contains('.'))
}
