//! Bounded read replies built on the original native invocation, one value at a time.
use anyhow::{ensure, Context as _, Result};
use serde_json::{Map, Value};
use std::{collections::BTreeSet, io::Write};

/// Only this module can construct an encoded, depth-checked success envelope.
pub(crate) struct Reply(String);

impl Reply {
    pub(crate) fn into_string(self) -> String {
        self.0
    }
}

pub(crate) fn execute(
    context: &cellar::sdk::Context,
    value: Value,
    options: &crate::runtime::Options,
) -> Result<Reply> {
    let keys = keys(&value, options.max_batch_operations).context(crate::failure::Validation)?;
    let storage = context.storage()?;
    encode(keys, options.max_value_bytes, |key| storage.get_json(key))
}

pub(crate) fn append_initial(
    context: &cellar::sdk::Context,
    payload: String,
    keys: &[String],
    maximum: usize,
) -> Result<String> {
    let storage = context.storage()?;
    append(payload, keys, maximum, |key| storage.get_json(key))
}

fn append(
    mut payload: String,
    keys: &[String],
    maximum: usize,
    read: impl FnMut(&str) -> Result<Option<Value>>,
) -> Result<String> {
    ensure!(payload.pop() == Some('}'), "expected invocation object");
    ensure!(
        payload.len() <= maximum,
        "Python invocation exceeds value budget"
    );
    let mut output = Bounded {
        bytes: payload.into_bytes(),
        limit: maximum,
    };
    // The caller supplies a native-generated invocation object. Keep its
    // existing fields and stream snapshots into its final field, without
    // collecting all decoded rows or reparsing their encoded representation.
    output.write_all(b",\"initial_storage\":{")?;
    rows(keys.iter().map(String::as_str), &mut output, read)?;
    output.write_all(b"}}")?;
    Ok(String::from_utf8(output.bytes)?)
}

fn keys(value: &Value, limit: usize) -> Result<BTreeSet<&str>> {
    let object = value.as_object().context("read batch must be an object")?;
    ensure!(object.len() == 1, "read batch requires only keys");
    let keys = value["keys"].as_array().context("keys must be an array")?;
    // Count supplied keys before deduplication, so duplicates cannot bypass admission.
    ensure!(keys.len() <= limit, "read batch exceeds operation limit");
    keys.iter()
        .map(|key| key.as_str().context("read key must be a string"))
        .collect()
}

struct Bounded {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for Bounded {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("SDK result exceeds value limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn encode(
    keys: BTreeSet<&str>,
    limit: usize,
    read: impl FnMut(&str) -> Result<Option<Value>>,
) -> Result<Reply> {
    let mut output = Bounded {
        bytes: Vec::new(),
        limit,
    };
    output.write_all(b"{\"ok\":true,\"value\":{")?;
    rows(keys, &mut output, read)?;
    output.write_all(b"}}")?;
    Ok(Reply(String::from_utf8(output.bytes)?))
}

fn rows<'a>(
    keys: impl IntoIterator<Item = &'a str>,
    output: &mut Bounded,
    mut read: impl FnMut(&str) -> Result<Option<Value>>,
) -> Result<()> {
    for (index, key) in keys.into_iter().enumerate() {
        let value = read(key)?;
        // Include the result-map level in the depth check. Keep only one decoded
        // value alive; collecting the whole batch could multiply storage memory
        // by max_batch_operations before the encoded reply limit is enforced.
        let snapshot = Value::Object(Map::from_iter([
            ("present".into(), Value::Bool(value.is_some())),
            ("value".into(), value.unwrap_or(Value::Null)),
        ]));
        let row = Value::Object(Map::from_iter([(key.to_owned(), snapshot)]));
        cellar_guest::values::check_depth(&row)?;
        if index != 0 {
            output.write_all(b",")?;
        }
        serde_json::to_writer(&mut *output, key)?;
        output.write_all(b":")?;
        serde_json::to_writer(&mut *output, &row[key])?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "read_batch_tests.rs"]
mod tests;
