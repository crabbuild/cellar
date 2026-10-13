//! Bounded operator commands mapped to the existing stream owner protocol.

use crate::application::event_stream::{
    self as events, authority::Action, Binding, Change, Cursor, Retirement,
};
use anyhow::Result;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub(crate) const MAX_REQUEST_BYTES: usize = 16 << 10;
pub(crate) const MAX_REPLY_BYTES: usize = 1 << 20;
pub(crate) const MAX_READ_BYTES: usize = 256 << 10;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    pub binding: Binding,
    pub command: Command,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Command {
    Describe,
    Read {
        cursor: Option<String>,
        limit: usize,
        max_bytes: usize,
        payload: bool,
    },
    Subscription {
        name: String,
    },
    Change {
        change: Change,
    },
    Status {
        operation: String,
    },
    Audit {
        after: String,
        limit: usize,
    },
    Retire {
        request: Retirement,
    },
    RetirementStatus {
        operation: String,
    },
}

impl Request {
    pub(crate) fn scope(&self) -> Result<String> {
        if self.catalog() {
            events::catalog_scope(&self.binding.resource)
        } else {
            events::cell_scope(&self.binding.resource)
        }
    }

    fn catalog(&self) -> bool {
        matches!(
            self.command,
            Command::Retire { .. } | Command::RetirementStatus { .. }
        )
    }

    pub(crate) fn name(&self) -> Result<String> {
        if self.catalog() {
            events::catalog_name(&self.binding.resource)
        } else {
            events::cell_name(&self.binding.resource)
        }
    }

    pub(crate) fn prepare(
        &self,
        operator: String,
    ) -> Result<(events::Authority, &'static str, Value)> {
        self.binding.validate()?;
        let (action, method, body) = match &self.command {
            Command::Describe => (Action::Administer, "__streamDescribe", json!({})),
            Command::Read {
                cursor,
                limit,
                max_bytes,
                ..
            } => {
                anyhow::ensure!(
                    (1..=100).contains(limit) && (1..=MAX_READ_BYTES).contains(max_bytes),
                    "invalid stream read bounds"
                );
                let cursor = cursor.as_deref().map(Cursor::decode).transpose()?;
                anyhow::ensure!(
                    cursor
                        .as_ref()
                        .is_none_or(|cursor| cursor.resource == self.binding.resource.id),
                    "cursor names another stream"
                );
                (
                    Action::Read,
                    "__streamRead",
                    json!({"next":cursor.map(|cursor|cursor.next.to_string()),"max_records":limit,"max_bytes":max_bytes}),
                )
            }
            Command::Subscription { name } => {
                anyhow::ensure!(
                    !name.is_empty() && name.len() <= 128,
                    "invalid subscription name"
                );
                (
                    Action::Administer,
                    "__streamSubscriptionStatus",
                    json!({"subscription":name}),
                )
            }
            Command::Change { change } => {
                change.validate()?;
                anyhow::ensure!(
                    change.expected.resource == self.binding.resource.id,
                    "change names another stream"
                );
                (
                    Action::Administer,
                    "__streamSubscriptionChange",
                    json!({"subscription":change.expected.name,"change":change}),
                )
            }
            Command::Status { operation } => {
                anyhow::ensure!(
                    operation.len() == 32
                        && operation
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
                    "invalid operation identity"
                );
                (
                    Action::Administer,
                    "__streamOperationStatus",
                    json!({"operation":operation}),
                )
            }
            Command::Audit { after, limit } => {
                let position = after.parse::<i64>()?;
                anyhow::ensure!(
                    position >= 0 && position.to_string() == *after && (1..=100).contains(limit),
                    "invalid stream audit page bounds"
                );
                (
                    Action::Administer,
                    "__streamAudit",
                    json!({"after":after,"max_records":limit}),
                )
            }
            Command::Retire { request } => {
                request.validate()?;
                (
                    Action::Administer,
                    "__catalogRetire",
                    json!({"request":base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(request)?)}),
                )
            }
            Command::RetirementStatus { operation } => {
                anyhow::ensure!(
                    operation.len() == 32
                        && operation
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
                    "invalid retirement operation identity"
                );
                (
                    Action::Administer,
                    "__catalogRetirementStatus",
                    json!({"operation":operation}),
                )
            }
        };
        Ok((
            events::Authority::operator(&self.binding, action, operator)?,
            method,
            json!([body]),
        ))
    }

    pub(crate) fn project(&self, mut reply: Value) -> Result<Value> {
        if let Command::Read { payload, .. } = self.command {
            if let Some(page) = reply.get_mut("ok") {
                let page = page
                    .as_object_mut()
                    .ok_or_else(|| anyhow::anyhow!("invalid stream read page"))?;
                anyhow::ensure!(
                    page.get("resource").and_then(Value::as_str)
                        == Some(self.binding.resource.id.as_str()),
                    "read response names another stream"
                );
                for record in page
                    .get_mut("records")
                    .and_then(Value::as_array_mut)
                    .ok_or_else(|| anyhow::anyhow!("invalid stream read response"))?
                {
                    let encoded = record["envelope"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("read envelope missing"))?;
                    let bytes = base64::engine::general_purpose::STANDARD.decode(encoded)?;
                    let mut event: Value = serde_json::from_slice(&bytes)?;
                    if !payload {
                        event
                            .as_object_mut()
                            .ok_or_else(|| anyhow::anyhow!("invalid event envelope"))?
                            .remove("payload");
                    }
                    let record = record
                        .as_object_mut()
                        .ok_or_else(|| anyhow::anyhow!("invalid read record"))?;
                    record.insert("event".into(), event);
                    record.remove("envelope");
                }
                let cursor = Cursor {
                    resource: self.binding.resource.id.clone(),
                    partition: 0,
                    next: page
                        .get("next")
                        .and_then(Value::as_str)
                        .ok_or_else(|| anyhow::anyhow!("read cursor missing"))?
                        .parse()?,
                };
                page.insert("cursor".into(), json!(cursor.encode()?));
            }
        }
        if let Some(object) = reply.as_object_mut() {
            if let Some(value) = object.remove("ok") {
                object.insert("result".into(), value);
            }
        }
        anyhow::ensure!(
            serde_json::to_vec(&reply)?.len() <= MAX_REPLY_BYTES,
            "stream operator response exceeds limit"
        );
        Ok(reply)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_projection_rejects_malformed_or_foreign_pages() -> Result<()> {
        let request: Request = serde_json::from_value(json!({
            "binding":{"environment":"EVENTS","resource":{"tenant":"tenant","application":"app","name":"events","family":"events","id":"00000000000000000000000000000001"},"principal":"app","grant":"reader","read":true},
            "command":{"kind":"read","cursor":null,"limit":1,"max_bytes":4096,"payload":false}
        }))?;
        for page in [
            json!(0),
            json!({"resource":"00000000000000000000000000000002","records":[],"next":"1"}),
            json!({"resource":"00000000000000000000000000000001","records":0,"next":"1"}),
        ] {
            assert!(request.project(json!({"ok":page})).is_err());
        }
        Ok(())
    }
}
