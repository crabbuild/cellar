// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{
    imports::Imports, valid_identifier, MAX_MESSAGE_DEPTH, MAX_NAME_BYTES, MAX_SYMBOLS,
    MAX_SYMBOL_BYTES,
};
use anyhow::{ensure, Context, Result};
use prost_types::{
    field_descriptor_proto::Type, DescriptorProto, EnumDescriptorProto, FileDescriptorProto,
};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Package,
    Message,
    Enum,
    EnumValue,
    Field,
    Oneof,
    Service,
    Method,
}

struct Symbol {
    kind: Kind,
    file: usize,
}

struct Reference<'a> {
    name: &'a str,
    kind: Kind,
    file: usize,
}

/// One namespace includes packages, types and members. Enum values are siblings
/// of their enum, as required by Protobuf's enclosing-scope naming rules.
#[derive(Default)]
pub(super) struct Symbols<'a> {
    entries: BTreeMap<String, Symbol>,
    references: Vec<Reference<'a>>,
    name_bytes: usize,
    types: usize,
    fields: usize,
    methods: usize,
}

impl<'a> Symbols<'a> {
    pub(super) fn package(&mut self, package: &str) -> Result<()> {
        for end in package
            .match_indices('.')
            .map(|(index, _)| index)
            .chain([package.len()])
        {
            let name = &package[..end];
            if let Some(existing) = self.entries.get(name) {
                ensure!(existing.kind == Kind::Package, "RPC package name collision");
                continue;
            }
            self.charge(name.len())?;
            self.entries.insert(
                name.to_owned(),
                Symbol {
                    kind: Kind::Package,
                    file: 0,
                },
            );
        }
        Ok(())
    }

    pub(super) fn file(&mut self, index: usize, file: &'a FileDescriptorProto) -> Result<()> {
        let package = file.package.as_deref().unwrap_or_default();
        for message in &file.message_type {
            self.message(package, message, index, 0)?;
        }
        for enumeration in &file.enum_type {
            self.enumeration(package, enumeration, index)?;
        }
        for service in &file.service {
            let name = self.insert(package, service.name.as_deref(), Kind::Service, index)?;
            for method in &service.method {
                self.insert(&name, method.name.as_deref(), Kind::Method, index)?;
                self.reference(method.input_type.as_deref(), Kind::Message, index)?;
                self.reference(method.output_type.as_deref(), Kind::Message, index)?;
            }
        }
        Ok(())
    }

    fn message(
        &mut self,
        scope: &str,
        message: &'a DescriptorProto,
        file: usize,
        depth: usize,
    ) -> Result<()> {
        ensure!(
            depth <= MAX_MESSAGE_DEPTH,
            "RPC descriptor nesting exceeds limit"
        );
        ensure!(
            message.extension.is_empty() && message.extension_range.is_empty(),
            "RPC extensions are unsupported"
        );
        let name = self.insert(scope, message.name.as_deref(), Kind::Message, file)?;
        for field in &message.field {
            self.insert(&name, field.name.as_deref(), Kind::Field, file)?;
            match field.r#type.and_then(|kind| Type::try_from(kind).ok()) {
                Some(Type::Message) => {
                    self.reference(field.type_name.as_deref(), Kind::Message, file)?
                }
                Some(Type::Enum) => self.reference(field.type_name.as_deref(), Kind::Enum, file)?,
                _ => {}
            }
        }
        for oneof in &message.oneof_decl {
            self.insert(&name, oneof.name.as_deref(), Kind::Oneof, file)?;
        }
        for nested in &message.nested_type {
            self.message(&name, nested, file, depth + 1)?;
        }
        for enumeration in &message.enum_type {
            self.enumeration(&name, enumeration, file)?;
        }
        Ok(())
    }

    fn enumeration(
        &mut self,
        scope: &str,
        enumeration: &EnumDescriptorProto,
        file: usize,
    ) -> Result<()> {
        self.insert(scope, enumeration.name.as_deref(), Kind::Enum, file)?;
        for value in &enumeration.value {
            self.insert(scope, value.name.as_deref(), Kind::EnumValue, file)?;
        }
        Ok(())
    }

    fn insert(
        &mut self,
        scope: &str,
        short: Option<&str>,
        kind: Kind,
        file: usize,
    ) -> Result<String> {
        let short = short.context("unnamed RPC descriptor symbol")?;
        ensure!(
            valid_identifier(short, false),
            "invalid RPC descriptor identifier"
        );
        let size = scope.len().saturating_add(1).saturating_add(short.len());
        ensure!(
            size <= MAX_NAME_BYTES,
            "RPC symbol exceeds qualified name limit"
        );
        self.charge(size)?;
        let name = format!("{scope}.{short}");
        ensure!(
            !self.entries.contains_key(&name),
            "duplicate RPC descriptor symbol: {name}"
        );
        match kind {
            Kind::Message | Kind::Enum => {
                self.types += 1;
                ensure!(self.types <= 4096, "too many RPC message and enum types");
            }
            Kind::Field => {
                self.fields += 1;
                ensure!(self.fields <= 65_536, "too many RPC fields");
            }
            Kind::Method => {
                self.methods += 1;
                ensure!(self.methods <= 4096, "too many RPC methods");
            }
            _ => {}
        }
        self.entries.insert(name.clone(), Symbol { kind, file });
        Ok(name)
    }

    fn charge(&mut self, bytes: usize) -> Result<()> {
        ensure!(
            self.entries.len() < MAX_SYMBOLS,
            "too many RPC descriptor symbols"
        );
        self.name_bytes = self.name_bytes.saturating_add(bytes);
        ensure!(
            self.name_bytes <= MAX_SYMBOL_BYTES,
            "RPC expanded symbol inventory exceeds byte limit"
        );
        Ok(())
    }

    fn reference(&mut self, name: Option<&'a str>, kind: Kind, file: usize) -> Result<()> {
        let name = name
            .and_then(|name| name.strip_prefix('.'))
            .context("RPC types must be fully qualified")?;
        ensure!(
            name.len() <= MAX_NAME_BYTES && valid_identifier(name, true),
            "invalid or oversized RPC type reference"
        );
        self.references.push(Reference { name, kind, file });
        Ok(())
    }

    pub(super) fn validate_references(&self, imports: &Imports) -> Result<()> {
        for reference in &self.references {
            let symbol = self
                .entries
                .get(reference.name)
                .context("unresolved RPC descriptor type")?;
            ensure!(
                symbol.kind == reference.kind,
                "RPC reference has the wrong type kind"
            );
            ensure!(
                imports.can_reference(reference.file, symbol.file),
                "RPC type requires a visible descriptor import: {}",
                reference.name
            );
        }
        Ok(())
    }
}
