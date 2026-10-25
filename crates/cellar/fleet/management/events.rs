// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Console adapters for existing event operator protocols. No cell files are opened.

use super::*;
use crate::application::event_stream::{
    self as events, BindingConfig, Change, ChangeAction, SubscriptionIdentity,
};
use crate::application::{OutboxOptions, OutboxPage, OutboxRevision};
use crate::event_inventory::{Descriptor, InventoryReply, InventoryRequest};
use crate::runtime::{source_control, stream_control};
use anyhow::Context as _;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutboxAction {
    Inspect {
        limit: usize,
        cursor: Option<String>,
    },
    Retry {
        expected: OutboxRevision,
        reason: String,
    },
    Destroy {
        incarnation: String,
        reason: String,
        data_loss_acknowledgment: String,
    },
    Recreate {
        incarnation: String,
        reason: String,
    },
    Status {
        operation: String,
    },
    Audit {
        limit: usize,
        after: Option<String>,
    },
}

impl OutboxAction {
    fn request(&self, scope: &str, operation: &str) -> source_control::Request {
        match self {
            Self::Inspect { limit, cursor } => source_control::Request::Inspect {
                limit: *limit,
                cursor: cursor.clone(),
            },
            Self::Retry { expected, reason } => source_control::Request::Retry {
                change: source_control::Retry {
                    operation: operation.into(),
                    expected: expected.clone(),
                    reason: reason.clone(),
                },
            },
            Self::Destroy {
                incarnation,
                reason,
                data_loss_acknowledgment,
            } => source_control::Request::Destroy {
                change: crate::runtime::source_destruction::Destroy {
                    source: scope.into(),
                    incarnation: incarnation.clone(),
                    operation: operation.into(),
                    reason: reason.clone(),
                    data_loss_acknowledgment: data_loss_acknowledgment.clone(),
                },
            },
            Self::Recreate {
                incarnation,
                reason,
            } => source_control::Request::Recreate {
                change: crate::runtime::source_destruction::Recreate {
                    source: scope.into(),
                    incarnation: incarnation.clone(),
                    operation: operation.into(),
                    reason: reason.clone(),
                },
            },
            Self::Status { operation } => source_control::Request::Status {
                operation: operation.clone(),
            },
            Self::Audit { limit, after } => source_control::Request::Audit {
                limit: *limit,
                after: after.clone(),
            },
        }
    }

    pub fn validate(&self, scope: &str, operation: &str) -> Result<()> {
        ensure!(
            cellar_logic::cell::valid_cell_scope(scope)
                && scope.contains(':')
                && !events::is_event_scope(scope),
            "invalid outbox source scope"
        );
        self.request(scope, operation).validate()?;
        if let Self::Retry { expected, .. } = self {
            ensure!(expected.source == scope, "retry names another source");
        }
        Ok(())
    }

    pub fn is_write(&self) -> bool {
        matches!(
            self,
            Self::Retry { .. } | Self::Destroy { .. } | Self::Recreate { .. }
        )
    }

    pub fn requires_data_write(&self) -> bool {
        matches!(self, Self::Destroy { .. } | Self::Recreate { .. })
    }

    pub fn audit_action(&self) -> &'static str {
        match self {
            Self::Destroy { .. } => "outbox_destroy",
            Self::Recreate { .. } => "outbox_recreate",
            _ => "outbox_retry",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EventStreamAction {
    Describe,
    Retire {
        revision: String,
        reason: String,
        mode: events::RetirementMode,
    },
    RetirementStatus {
        operation: String,
    },
    RetirementProof,
    Read {
        cursor: Option<String>,
        limit: usize,
        max_bytes: usize,
        payload: bool,
    },
    Subscription {
        name: String,
    },
    Subscriptions {
        cursor: Option<String>,
        limit: usize,
    },
    Sources {
        cursor: Option<String>,
        limit: usize,
    },
    Change {
        expected: SubscriptionIdentity,
        action: ChangeAction,
        reason: String,
    },
    Status {
        operation: String,
    },
    Audit {
        after: String,
        limit: usize,
    },
}

impl EventStreamAction {
    fn command(&self, operation: &str) -> Result<stream_control::Command> {
        Ok(match self {
            Self::Describe => stream_control::Command::Describe,
            Self::Retire {
                revision,
                reason,
                mode,
            } => stream_control::Command::Retire {
                request: events::Retirement {
                    operation: operation.into(),
                    revision: revision.parse()?,
                    reason: reason.clone(),
                    mode: mode.clone(),
                },
            },
            Self::RetirementStatus { operation } => stream_control::Command::RetirementStatus {
                operation: operation.clone(),
            },
            Self::RetirementProof => {
                anyhow::bail!("platform retirement proof has its own catalog route")
            }
            Self::Read {
                cursor,
                limit,
                max_bytes,
                payload,
            } => stream_control::Command::Read {
                cursor: cursor.clone(),
                limit: *limit,
                max_bytes: *max_bytes,
                payload: *payload,
            },
            Self::Subscription { name } => {
                stream_control::Command::Subscription { name: name.clone() }
            }
            Self::Subscriptions { cursor, limit } => stream_control::Command::Subscriptions {
                cursor: cursor.clone(),
                limit: *limit,
            },
            Self::Sources { cursor, limit } => stream_control::Command::Sources {
                cursor: cursor.clone(),
                limit: *limit,
            },
            Self::Change {
                expected,
                action,
                reason,
            } => stream_control::Command::Change {
                change: Change {
                    operation: operation.into(),
                    expected: expected.clone(),
                    action: action.clone(),
                    reason: reason.clone(),
                },
            },
            Self::Status { operation } => stream_control::Command::Status {
                operation: operation.clone(),
            },
            Self::Audit { after, limit } => stream_control::Command::Audit {
                after: after.clone(),
                limit: *limit,
            },
        })
    }

    pub fn is_write(&self) -> bool {
        matches!(self, Self::Change { .. } | Self::Retire { .. })
    }

    pub fn requires_data_write(&self) -> bool {
        matches!(
            self,
            Self::Retire { .. }
                | Self::Change {
                    action: ChangeAction::Reset { .. } | ChangeAction::Delete { .. },
                    ..
                }
        )
    }

    pub fn audit_action(&self) -> &'static str {
        if matches!(self, Self::Retire { .. }) {
            "stream_retire"
        } else {
            "subscription_change"
        }
    }

    pub fn validate(&self, resource: &str, operation: &str) -> Result<()> {
        validate_resource_id(resource)?;
        match self {
            Self::Read {
                cursor,
                limit,
                max_bytes,
                ..
            } => {
                ensure!(
                    (1..=100).contains(limit)
                        && (1..=stream_control::MAX_READ_BYTES).contains(max_bytes),
                    "invalid stream read bounds"
                );
                if let Some(cursor) = cursor {
                    ensure!(
                        events::Cursor::decode(cursor)?.resource == resource,
                        "cursor names another stream"
                    );
                }
            }
            Self::Subscriptions { cursor, limit } => {
                events::SubscriptionListOptions {
                    cursor: cursor.clone(),
                    max_records: *limit,
                }
                .validate(resource)?;
            }
            Self::Sources { cursor, limit } => {
                ensure!(
                    (1..=100).contains(limit) && cursor.as_ref().is_none_or(|c| c.len() <= 2048),
                    "invalid producer page bounds"
                );
            }
            Self::Subscription { name } => ensure!(
                !name.is_empty() && name.len() <= 128,
                "invalid subscription name"
            ),
            Self::Change {
                expected,
                action,
                reason,
            } => {
                ensure!(expected.resource == resource, "change names another stream");
                Change {
                    operation: operation.into(),
                    expected: expected.clone(),
                    action: action.clone(),
                    reason: reason.clone(),
                }
                .validate()?;
            }
            Self::Status { operation } => validate_resource_id(operation)?,
            Self::Audit { after, limit } => {
                let n: i64 = after.parse()?;
                ensure!(
                    n >= 0 && n.to_string() == *after && (1..=100).contains(limit),
                    "invalid audit bounds"
                );
            }
            Self::Retire {
                revision,
                reason,
                mode,
            } => {
                let n: i64 = revision.parse()?;
                ensure!(
                    n.to_string() == *revision,
                    "noncanonical retirement revision"
                );
                events::Retirement {
                    operation: operation.into(),
                    revision: n,
                    reason: reason.clone(),
                    mode: mode.clone(),
                }
                .validate()?;
            }
            Self::RetirementStatus { operation } => validate_resource_id(operation)?,
            Self::RetirementProof | Self::Describe => {}
        }
        Ok(())
    }
}

fn validate_resource_id(id: &str) -> Result<()> {
    ensure!(
        id.len() == 32
            && id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "identity must be 32 lowercase hexadecimal digits"
    );
    Ok(())
}

impl FleetClient {
    async fn event_control(
        &self,
        scope: &str,
        route: &str,
        operator: &str,
        body: Value,
    ) -> Result<Value> {
        ensure!(
            serde_json::to_vec(&body)?.len() <= 16 << 10,
            "event operator request exceeds bounds"
        );
        Reachable::from_entrances_route(
            &self.bucket,
            Subject {
                noun: "event resource",
                source: "dashboard",
                timeout: Duration::from_secs(30),
            },
            scope.into(),
            None,
            self.routing_entrances(scope).await?,
            route,
        )
        .await?
        .with_operator(&self.bucket, operator)
        .await?
        .call(body)
        .await
    }

    pub async fn outbox(
        &self,
        scope: &str,
        action: OutboxAction,
        operator: &str,
        operation: &str,
    ) -> Result<Value> {
        action.validate(scope, operation)?;
        let reply = self
            .event_control(
                scope,
                "outbox",
                operator,
                serde_json::to_value(action.request(scope, operation))?,
            )
            .await?;
        match &action {
            OutboxAction::Inspect { limit, cursor } => {
                let page: OutboxPage = serde_json::from_value(reply.clone())?;
                page.validate(
                    scope,
                    &OutboxOptions {
                        limit: *limit,
                        cursor: cursor.clone(),
                    },
                )?;
            }
            OutboxAction::Retry { expected, reason } => {
                ensure!(
                    reply["operation"] == operation
                        && reply["operator"] == operator
                        && reply["expected"] == serde_json::to_value(expected)?
                        && reply["reason"] == *reason
                        && reply["rearmed"].is_boolean(),
                    "invalid outbox retry receipt"
                );
            }
            OutboxAction::Destroy {
                incarnation,
                reason,
                data_loss_acknowledgment,
            } => {
                validate_source_receipt(&reply, scope, operator, operation)?;
                ensure!(
                    reply["incarnation"] == *incarnation
                        && reply["reason"] == *reason
                        && reply["data_loss_acknowledgment"] == *data_loss_acknowledgment
                        && reply["state"] != "live",
                    "source destruction receipt differs from request"
                );
            }
            OutboxAction::Recreate {
                incarnation,
                reason,
            } => {
                validate_source_receipt(&reply, scope, operator, operation)?;
                ensure!(
                    reply["reason"] == *reason
                        && reply["retired_incarnation"] == *incarnation
                        && reply["incarnation"] != *incarnation
                        && reply["state"] == "live",
                    "source lifecycle receipt differs from request"
                );
            }
            OutboxAction::Status { operation } if reply.get("state").is_some() => {
                validate_source_receipt(&reply, scope, operator, operation)?
            }
            OutboxAction::Status { operation } => {
                ensure!(
                    reply["operation"] == *operation
                        && reply["operator"] == operator
                        && reply["expected"]["source"] == scope
                        && reply["rearmed"].is_boolean(),
                    "invalid outbox operation receipt"
                );
            }
            OutboxAction::Audit { limit, .. } => ensure!(
                reply["records"]
                    .as_array()
                    .is_some_and(|records| records.len() <= *limit),
                "invalid outbox audit page"
            ),
        }
        Ok(reply)
    }

    pub async fn event_inventory(
        &self,
        scope: &str,
        request: InventoryRequest,
    ) -> Result<InventoryReply> {
        request.validate(scope)?;
        let reply: InventoryReply = serde_json::from_value(
            self.event_control(
                scope,
                "event-inventory",
                "dashboard",
                serde_json::to_value(&request)?,
            )
            .await?,
        )?;
        reply.validate(scope, &request)?;
        Ok(reply)
    }

    pub async fn cell_event_state(&self, scope: &str) -> Result<crate::event_state::StateSample> {
        ensure!(
            cellar_logic::cell::valid_cell_scope(scope) && scope.contains(':'),
            "invalid event state scope"
        );
        let sample: crate::event_state::StateSample = serde_json::from_value(
            self.event_control(scope, "event-state", "dashboard", json!({}))
                .await?,
        )?;
        sample.values(scope)?;
        Ok(sample)
    }

    async fn event_binding(&self, application: &str, environment: &str) -> Result<BindingConfig> {
        ensure!(
            !environment.is_empty() && environment.len() <= 256,
            "invalid stream binding name"
        );
        let (manifest, _, _) = self.application_manifest(application).await?;
        let declarations = manifest.raw_metadata["stream_bindings"]
            .as_array()
            .context("event stream bindings unavailable")?;
        ensure!(
            declarations.len() <= 128,
            "event binding count exceeds bounds"
        );
        let mut selected = None;
        for declaration in declarations {
            ensure!(
                serde_json::to_vec(declaration)?.len() <= 64 << 10,
                "event binding exceeds bounds"
            );
            let binding: BindingConfig = serde_json::from_value(declaration.clone())?;
            binding.validate()?;
            ensure!(
                binding.principal == manifest.script_name,
                "event binding principal mismatch"
            );
            if binding.environment == environment {
                ensure!(selected.is_none(), "duplicate event binding name");
                selected = Some(binding);
            }
        }
        selected.context("event stream binding not found")
    }

    /// Platform metadata only. The returned catalog cannot mint resource grants.
    pub async fn event_resources(
        &self,
        application: &str,
        environment: &str,
        cursor: Option<String>,
        limit: usize,
    ) -> Result<events::CatalogPage> {
        let binding = self.event_binding(application, environment).await?;
        let request = crate::runtime::catalog_control::Request {
            namespace: events::CatalogNamespace {
                tenant: binding.resource.tenant,
                application: binding.resource.application,
            },
            cursor,
            limit,
            retired_resource: None,
        };
        request.prepare("dashboard".into())?;
        let page: events::CatalogPage = serde_json::from_value(
            self.event_control(
                &request.scope()?,
                "catalogs",
                "dashboard",
                serde_json::to_value(&request)?,
            )
            .await?,
        )?;
        page.validate(
            &request.namespace,
            &events::CatalogListOptions {
                cursor: request.cursor,
                max_records: limit,
            },
        )?;
        Ok(page)
    }

    /// The UUID is selected from observed metadata, never used as delegated authority.
    /// Named selectors are checked against the owner's full resource identity.
    pub async fn event_stream(
        &self,
        application: &str,
        environment: &str,
        resource_id: &str,
        action: EventStreamAction,
        operator: &str,
        operation: &str,
    ) -> Result<Value> {
        action.validate(resource_id, operation)?;
        let declaration = self.event_binding(application, environment).await?;
        let binding = if declaration.resource.id.is_some() {
            let binding = declaration.explicit()?;
            ensure!(
                binding.resource.id == resource_id,
                "stream binding changed; reopen the workspace"
            );
            binding
        } else {
            let candidate = events::Resource {
                tenant: declaration.resource.tenant.clone(),
                application: declaration.resource.application.clone(),
                id: resource_id.into(),
                name: declaration.resource.name.clone(),
                family: declaration.resource.family.clone(),
            };
            let Descriptor::Stream {
                resource: Some(resource),
                ..
            } = (match self
                .event_inventory(&events::cell_scope(&candidate)?, InventoryRequest::Describe)
                .await?
            {
                InventoryReply::Descriptor(descriptor) => descriptor,
                _ => anyhow::bail!("stream identity unavailable"),
            })
            else {
                anyhow::bail!("stream identity unavailable")
            };
            declaration.resolve(resource)?
        };
        if matches!(action, EventStreamAction::RetirementProof) {
            return self
                .event_retirement_proof(binding.resource, operator)
                .await;
        }
        let bound_resource = binding.resource.clone();
        let request = stream_control::Request {
            binding,
            command: action.command(operation)?,
        };
        request.prepare(operator.into())?;
        let reply = self
            .event_control(
                &request.scope()?,
                "streams",
                operator,
                serde_json::to_value(&request)?,
            )
            .await?;
        match &action {
            EventStreamAction::Change {
                expected,
                action,
                reason,
            } => {
                let receipt: events::LifecycleReceipt = serde_json::from_value(reply.clone())?;
                receipt.validate()?;
                ensure!(
                    receipt.previous == *expected
                        && receipt.action == *action
                        && receipt.reason == *reason
                        && receipt.operation == operation
                        && receipt.operator.as_deref() == Some(operator),
                    "invalid subscription change receipt"
                );
            }
            EventStreamAction::Status { operation } => {
                let receipt: events::LifecycleReceipt = serde_json::from_value(reply.clone())?;
                receipt.validate()?;
                ensure!(
                    receipt.current.resource == resource_id
                        && receipt.operation == *operation
                        && receipt.operator.as_deref() == Some(operator),
                    "invalid subscription operation receipt"
                );
            }
            EventStreamAction::Retire { .. } | EventStreamAction::RetirementStatus { .. } => {
                let id = if let EventStreamAction::RetirementStatus { operation } = &action {
                    operation.as_str()
                } else {
                    operation
                };
                let status: events::RetirementStatus = serde_json::from_value(reply.clone())?;
                status.validate(
                    &bound_resource,
                    id,
                    if status.state == "retiring" && status.operator.is_none() {
                        None
                    } else {
                        Some(operator)
                    },
                )?;
                if status.state == "deleted" {
                    events::validate_retirement_acceptance(&reply, Some(operator))?;
                }
            }
            _ => {}
        }
        Ok(reply)
    }

    /// Read-only platform evidence for an exact identity, independent of resource grants.
    pub async fn event_retirement_proof(
        &self,
        resource: events::Resource,
        operator: &str,
    ) -> Result<Value> {
        let request = crate::runtime::catalog_control::Request {
            namespace: events::CatalogNamespace {
                tenant: resource.tenant.clone(),
                application: resource.application.clone(),
            },
            cursor: None,
            limit: 1,
            retired_resource: Some(resource.clone()),
        };
        request.prepare(operator.into())?;
        let proof: events::RetirementProof = serde_json::from_value(
            self.event_control(
                &request.scope()?,
                "catalogs",
                operator,
                serde_json::to_value(request)?,
            )
            .await?,
        )?;
        proof.validate(&resource)?;
        Ok(serde_json::to_value(proof)?)
    }

    /// Host-lifetime counters, not resident-cell aggregates. Two signed probes
    /// bracket sampling so a process restart cannot be attributed to an old lease.
    pub async fn node_event_metrics(&self, node: &str, generation: &str) -> Result<Value> {
        ensure!(super::valid_node_id(node), "invalid node ID");
        let lease = crate::fleet::live_node_lease(&self.bucket, node)
            .await?
            .context("node has no current lease")?;
        ensure!(
            lease.generation == generation,
            "node generation changed; refresh before continuing"
        );
        let address = crate::startup::parse_advertise(&lease.addr)?;
        ensure!(
            self.allow_public_peers || !address.is_public_ip(),
            "public peer addresses are disabled"
        );
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let auth = crate::peer_auth::PeerAuth::new(
            crate::peer_auth::load_existing(&self.bucket).await?,
            "dashboard",
        )?;
        crate::peer_probe::probe(&http, &lease, &auth).await?;
        let path = "/event-metrics?format=json";
        let response = auth
            .sign(
                http.get(format!("http://{}{path}", lease.addr)),
                "GET",
                path,
                &[],
                node,
            )?
            .send()
            .await?;
        crate::peer_auth::validate_response(response.headers())?;
        ensure!(
            response.status().is_success(),
            "node event metrics unavailable"
        );
        let metrics: Value =
            serde_json::from_str(&crate::operator_cell::bounded_reply(response, 64 << 10).await?)?;
        validate_metrics(
            &metrics,
            &serde_json::to_value(crate::event_metrics::Registry::default().snapshot(0, 0))?,
        )?;
        crate::peer_probe::probe(&http, &lease, &auth).await?;
        Ok(
            json!({"node":node,"generation":generation,"observed_at":crate::ownership_store::now_ms(),"metrics":metrics}),
        )
    }
}

// Validate the fixed host schema and preserve every u64 as a canonical decimal.
fn validate_metrics(value: &Value, schema: &Value) -> Result<()> {
    let valid = match (value, schema) {
        (Value::Object(value), Value::Object(schema)) if value.len() == schema.len() => {
            for (key, expected) in schema {
                validate_metrics(
                    value.get(key).context("event metric field missing")?,
                    expected,
                )?;
            }
            true
        }
        (Value::Array(value), Value::Array(schema)) if value.len() == schema.len() => {
            for (value, expected) in value.iter().zip(schema) {
                validate_metrics(value, expected)?;
            }
            true
        }
        (Value::String(value), Value::String(expected)) if expected == "0" => {
            value.parse::<u64>().is_ok_and(|n| n.to_string() == *value)
        }
        _ => value == schema,
    };
    ensure!(valid, "invalid or unsupported event metrics response");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_actions_reject_cross_target_and_unbounded_requests() {
        let resource = "a".repeat(32);
        assert!(EventStreamAction::Read {
            cursor: None,
            limit: 101,
            max_bytes: 1024,
            payload: false
        }
        .validate(&resource, "")
        .is_err());
        assert!(EventStreamAction::Audit {
            after: "00".into(),
            limit: 10
        }
        .validate(&resource, "")
        .is_err());
        assert!(OutboxAction::Inspect {
            limit: 1,
            cursor: None
        }
        .validate("__EventStream:abc", "")
        .is_err());
        let revision = OutboxRevision {
            source: "Order:other".into(),
            incarnation: "b".repeat(32),
            lane: "c".repeat(64),
            dispatch_generation: 0,
            next_sequence: 2,
            settled_through: 0,
        };
        assert!(OutboxAction::Retry {
            expected: revision,
            reason: "retry".into()
        }
        .validate("Order:one", &resource)
        .is_err());
    }

    #[test]
    fn host_metrics_are_exact_and_fixed_schema() -> Result<()> {
        let schema =
            serde_json::to_value(crate::event_metrics::Registry::default().snapshot(0, 0))?;
        let mut value = schema.clone();
        value["counters"][0]["value"] = json!(u64::MAX.to_string());
        validate_metrics(&value, &schema)?;
        value["counters"][0]["value"] = json!("01");
        assert!(validate_metrics(&value, &schema).is_err());
        value["counters"][0]["value"] = json!(1);
        assert!(validate_metrics(&value, &schema).is_err());
        value = schema.clone();
        value["payload"] = json!("not a metric");
        assert!(validate_metrics(&value, &schema).is_err());
        Ok(())
    }
}

fn validate_source_receipt(
    reply: &Value,
    scope: &str,
    operator: &str,
    operation: &str,
) -> Result<()> {
    ensure!(
        reply["operation"] == operation
            && reply["operator"] == operator
            && reply["source"] == scope
            && reply["incarnation"]
                .as_str()
                .is_some_and(|s| validate_resource_id(s).is_ok())
            && matches!(
                reply["state"].as_str(),
                Some("checking" | "verified" | "cleaning" | "deleted" | "live")
            ),
        "invalid source lifecycle receipt"
    );
    let fields = [
        "operation",
        "operator",
        "source",
        "incarnation",
        "generation",
        "reason",
        "state",
        "data_loss_acknowledgment",
        "retired_incarnation",
        "captured_rows",
        "captured_bytes",
        "discarded_rows",
        "discarded_bytes",
    ];
    ensure!(
        reply
            .as_object()
            .is_some_and(|object| object.keys().all(|key| fields.contains(&key.as_str()))),
        "unexpected source receipt fields"
    );
    let number = |key: &str| -> Result<i64> {
        let text = reply[key]
            .as_str()
            .context("source receipt counter missing")?;
        let value: i64 = text.parse()?;
        ensure!(
            value >= 0 && value.to_string() == text,
            "invalid source receipt counter"
        );
        Ok(value)
    };
    ensure!(
        number("generation")? > 0
            && reply["reason"]
                .as_str()
                .is_some_and(|s| !s.trim().is_empty() && s.len() <= 512),
        "invalid source receipt generation or reason"
    );
    if reply["state"] == "live" {
        let retired = reply["retired_incarnation"]
            .as_str()
            .context("retired incarnation missing")?;
        validate_resource_id(retired)?;
        ensure!(
            reply["incarnation"] != retired,
            "recreation reused a retired incarnation"
        );
    } else {
        ensure!(
            reply["data_loss_acknowledgment"]
                .as_str()
                .is_some_and(|s| !s.trim().is_empty() && s.len() <= 1024),
            "source destruction acknowledgment missing"
        );
        let rows = number("captured_rows")?;
        let bytes = number("captured_bytes")?;
        let discarded_rows = number("discarded_rows")?;
        let discarded_bytes = number("discarded_bytes")?;
        ensure!(
            discarded_rows <= rows
                && discarded_bytes <= bytes
                && (reply["state"] != "deleted"
                    || (rows == discarded_rows && bytes == discarded_bytes)),
            "invalid source discard accounting"
        );
    }
    Ok(())
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    #[test]
    fn source_lifecycle_receipts_reject_foreign_payloads_and_false_completion() -> Result<()> {
        let valid = json!({"operation":"a".repeat(32),"operator":"console","source":"Order:one","incarnation":"b".repeat(32),"generation":"1","reason":"discard test source","data_loss_acknowledgment":"discard fixture","state":"deleted","captured_rows":"2","captured_bytes":"200","discarded_rows":"2","discarded_bytes":"200"});
        validate_source_receipt(&valid, "Order:one", "console", &"a".repeat(32))?;
        for (key, value) in [
            ("discarded_rows", json!("1")),
            ("generation", json!("01")),
            ("payload", json!({"must":"not leak"})),
        ] {
            let mut forged = valid.clone();
            forged[key] = value;
            assert!(
                validate_source_receipt(&forged, "Order:one", "console", &"a".repeat(32)).is_err()
            );
        }
        Ok(())
    }

    #[test]
    fn lifecycle_actions_pin_scope_incarnation_and_exact_revision() -> Result<()> {
        let id = "a".repeat(32);
        let destroy = OutboxAction::Destroy {
            incarnation: id.clone(),
            reason: "retire test source".into(),
            data_loss_acknowledgment: "discard test data".into(),
        };
        assert!(destroy.is_write() && destroy.requires_data_write());
        destroy.validate("Order:one", &id)?;
        assert!(destroy.validate("__EventStream:one", &id).is_err());
        let encoded = serde_json::to_value(destroy.request("Order:one", &id))?;
        assert_eq!(encoded["change"]["source"], "Order:one");
        assert_eq!(encoded["change"]["incarnation"], id);
        assert_eq!(encoded["op"], "destroy");
        for revision in ["0", "01", "9223372036854775808"] {
            let action = EventStreamAction::Retire {
                revision: revision.into(),
                reason: "retire".into(),
                mode: events::RetirementMode::Coordinated {
                    procedure: "all producers stopped".into(),
                },
            };
            assert!(action.validate(&id, &id).is_err());
        }
        let action = EventStreamAction::Retire {
            revision: "9223372036854775807".into(),
            reason: "retire".into(),
            mode: events::RetirementMode::Coordinated {
                procedure: "all producers stopped".into(),
            },
        };
        action.validate(&id, &id)?;
        assert!(action.is_write() && action.requires_data_write());
        assert!(!EventStreamAction::RetirementProof.is_write());
        Ok(())
    }
}
