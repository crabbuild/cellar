// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded HTTP metadata carried beside a real native RPC lease.
use anyhow::{bail, ensure, Context as _, Result};
use cellar::native::clone::{Object, StructuredValue, Value};
use std::collections::BTreeMap;

fn object<'a>(graph: &'a StructuredValue, value: &Value) -> Result<&'a Object> {
    let Value::Reference(id) = value else {
        bail!("RPC HTTP metadata requires an object")
    };
    graph
        .objects
        .get(*id as usize)
        .context("invalid HTTP metadata reference")
}

pub(super) fn array<'a>(graph: &'a StructuredValue, value: &Value) -> Result<&'a [Value]> {
    match object(graph, value)? {
        Object::DenseArray {
            elements,
            properties,
        } if properties.is_empty() => Ok(elements),
        _ => bail!("RPC HTTP metadata requires a dense array"),
    }
}

pub(super) fn string(value: &Value, maximum: usize) -> Result<String> {
    let Value::String(value) = value else {
        bail!("RPC HTTP metadata requires a string")
    };
    ensure!(value.len() <= maximum, "RPC HTTP metadata exceeds limit");
    let value = String::from_utf16(value)?;
    ensure!(value.len() <= maximum, "RPC HTTP metadata exceeds limit");
    Ok(value)
}

pub(super) struct Head<'a> {
    graph: &'a StructuredValue,
    fields: BTreeMap<String, &'a Value>,
}

impl<'a> Head<'a> {
    pub(super) fn new(graph: &'a StructuredValue, value: &Value, names: &[&str]) -> Result<Self> {
        let Object::Object(properties) = object(graph, value)? else {
            bail!("invalid RPC HTTP marker")
        };
        ensure!(
            properties.len() == names.len(),
            "invalid RPC HTTP metadata fields"
        );
        let mut fields = BTreeMap::new();
        for (name, value) in properties {
            let name = string(name, 32)?;
            ensure!(
                names.contains(&name.as_str()),
                "unknown RPC HTTP metadata field"
            );
            ensure!(
                fields.insert(name, value).is_none(),
                "duplicate RPC HTTP field"
            );
        }
        Ok(Self { graph, fields })
    }

    pub(super) fn field(&self, name: &str) -> Result<&'a Value> {
        self.fields
            .get(name)
            .copied()
            .context("missing RPC HTTP field")
    }

    pub(super) fn text(&self, name: &str, maximum: usize) -> Result<String> {
        string(self.field(name)?, maximum)
    }

    pub(super) fn boolean(&self, name: &str) -> Result<bool> {
        match self.field(name)? {
            Value::Boolean(value) => Ok(*value),
            _ => bail!("invalid RPC HTTP boolean"),
        }
    }

    pub(super) fn headers(&self) -> Result<usize> {
        let headers = array(self.graph, self.field("headers")?)?;
        ensure!(headers.len() <= 128, "RPC HTTP headers exceed limit");
        let checked = cellar::sdk::http::Headers::new();
        let mut bytes = 0;
        for header in headers {
            let pair = array(self.graph, header)?;
            ensure!(pair.len() == 2, "invalid RPC HTTP header pair");
            let name = string(&pair[0], 32768)?;
            let value = string(&pair[1], 32768)?;
            bytes += name.len() + value.len();
            ensure!(bytes <= 32768, "RPC HTTP headers exceed limit");
            checked.append(&name, &value)?;
        }
        Ok(headers.len())
    }
}
