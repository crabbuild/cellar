// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Private, peer-authenticated child execution; ordinary RPC cannot mint authority.
use crate::application::{containers::*, Context, Invocation, Output};
use crate::storage::sandbox_children as children;
use anyhow::{ensure, Context as _, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub(crate) const METHOD: &str = "__sandboxExecute";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Identity {
    pub parent_scope: String,
    pub parent_incarnation: String,
    pub name: String,
    pub generation: u64,
    pub execution_scope: String,
    pub parameters_digest: String,
    pub configuration_digest: String,
}

impl Identity {
    pub(crate) fn new(
        scope: &str,
        parent: &SandboxParent,
        allocation: &SandboxAllocation,
        configuration: &str,
    ) -> Result<Self> {
        ensure!(
            scope == parent.scope && parent.incarnation == allocation.parent_incarnation,
            "sandbox parent identity mismatch"
        );
        let identity = Self {
            parent_scope: scope.into(),
            parent_incarnation: parent.incarnation.clone(),
            name: allocation.name.clone(),
            generation: allocation.generation,
            execution_scope: allocation.execution_scope.clone(),
            parameters_digest: allocation.parameters_digest.clone(),
            configuration_digest: configuration.into(),
        };
        identity.validate()?;
        Ok(identity)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        use crate::application::sandbox_catalog::values;
        for scope in [&self.parent_scope, &self.execution_scope] {
            ensure!(
                cellar_logic::cell::valid_cell_scope(scope) && scope.contains(':'),
                "invalid sandbox authority scope"
            );
        }
        ensure!(
            self.parent_scope != self.execution_scope,
            "sandbox cannot be its own parent"
        );
        values::name(&self.name)?;
        ensure!(
            values::hex(&self.parent_incarnation, 32)
                && values::hex(&self.parameters_digest, 64)
                && values::hex(&self.configuration_digest, 64)
                && (1..=i64::MAX as u64).contains(&self.generation),
            "invalid sandbox execution identity"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Action {
    Start,
    Inspect,
    Destroy,
    Call { operation: String },
}

impl Action {
    pub(crate) fn validate(&self) -> Result<()> {
        if let Self::Call { operation } = self {
            ensure!(
                matches!(
                    operation.as_str(),
                    "container.capabilities"
                        | "container.running"
                        | "container.exec_output"
                        | "container.file.read"
                        | "container.file.write"
                        | "container.file.stat"
                        | "container.file.list"
                        | "container.port.wait_ready"
                        | "container.port.fetch"
                ),
                "unsupported bounded sandbox operation"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Kind {
    #[serde(rename = "sandbox-execution-v1")]
    Execute,
}

#[doc(hidden)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Authority {
    native_capacity: bool,
    kind: Kind,
    identity: Identity,
    action: Action,
}

impl Authority {
    pub(crate) fn new(identity: Identity, action: Action, native_capacity: bool) -> Result<Self> {
        let authority = Self {
            native_capacity,
            kind: Kind::Execute,
            identity,
            action,
        };
        authority.validate()?;
        Ok(authority)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.identity.validate()?;
        self.action.validate()
    }

    pub(crate) fn validate_target(&self, scope: &str, method: &str) -> Result<()> {
        self.validate()?;
        ensure!(
            scope == self.identity.execution_scope && method == METHOD,
            "sandbox authority does not match its target"
        );
        Ok(())
    }

    pub(crate) fn scope(&self) -> &str {
        &self.identity.execution_scope
    }

    pub(crate) fn requires_native_capacity(&self) -> bool {
        self.native_capacity && matches!(self.action, Action::Start)
    }

    pub(crate) fn action(&self) -> &Action {
        &self.action
    }
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "status",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum Reply {
    Success(Value),
    Failure(SandboxFailure),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Phase {
    Unstarted,
    Starting,
    Ready,
    Deleting,
    Deleted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Record {
    pub identity: Identity,
    pub phase: Phase,
}

async fn save(context: &Context, record: &Record) -> Result<()> {
    children::save(
        &context
            .identity()
            .context("sandbox child has no identity")?
            .scope,
        record,
    )?;
    context.storage()?.sync().await?;
    context.check_turn()?;
    Ok(())
}

async fn run(context: &Context, authority: Authority, input: Value) -> Result<Value> {
    let scope = &context
        .identity()
        .context("sandbox child has no identity")?
        .scope;
    let executor = context
        .worker_config
        .native_application
        .as_ref()
        .and_then(|app| app.sandbox_executor(&context.identity().unwrap().class))
        .context("sandbox executor is not registered")?;
    ensure!(
        executor.configuration_digest() == authority.identity.configuration_digest
            && executor.requires_native_capacity() == authority.native_capacity,
        "sandbox executor configuration changed"
    );
    let lifecycle = BindingJournal::open(context, "sandbox-execution")?;
    let _guard = lifecycle.lock_lifecycle(context).await?;
    let prior = children::load(scope)?;
    if let Some(prior) = &prior {
        ensure!(
            prior.identity == authority.identity,
            "sandbox child belongs to another parent or configuration"
        );
    }
    let attach = prior.is_none();
    let mut record = prior.unwrap_or(Record {
        identity: authority.identity,
        phase: Phase::Unstarted,
    });
    // Attach before any provider I/O. A delete arriving first installs a durable
    // tombstone, so a delayed start can never resurrect that execution identity.
    if attach {
        save(context, &record).await?;
    }
    match authority.action {
        Action::Start => {
            ensure!(
                crate::application::sandbox_catalog::execution::digest(&input)?
                    == record.identity.parameters_digest,
                "sandbox start parameters changed"
            );
            match record.phase {
                Phase::Ready => {
                    let observed = executor
                        .execute(context.clone(), "container.reconcile".into(), json!({}))
                        .await?;
                    if observed["state"] == "running" {
                        return Ok(json!({"state":"ready"}));
                    }
                    return Err(SandboxFailure {
                        kind: SandboxFailureKind::Unknown,
                    }
                    .into());
                }
                Phase::Unstarted => {}
                _ => {
                    return Err(SandboxFailure {
                        kind: SandboxFailureKind::Unknown,
                    }
                    .into())
                }
            }
            record.phase = Phase::Starting;
            save(context, &record).await?;
            match executor
                .execute(context.clone(), "container.start".into(), input)
                .await
            {
                Ok(_) => {
                    record.phase = Phase::Ready;
                    save(context, &record).await?;
                    Ok(json!({"state":"ready"}))
                }
                Err(error) => {
                    if error
                        .downcast_ref::<SandboxFailure>()
                        .is_some_and(|failure| failure.kind != SandboxFailureKind::Unknown)
                    {
                        record.phase = Phase::Unstarted;
                        save(context, &record).await?;
                    }
                    Err(error)
                }
            }
        }
        Action::Inspect => {
            if record.phase == Phase::Deleted {
                return Ok(json!({"state":"deleted"}));
            }
            let observed = executor
                .execute(context.clone(), "container.reconcile".into(), json!({}))
                .await?;
            if observed["state"] == "running" && record.phase == Phase::Starting {
                record.phase = Phase::Ready;
                save(context, &record).await?;
            }
            Ok(json!({"state":record.phase,"provider":observed}))
        }
        Action::Destroy => {
            if record.phase == Phase::Deleted {
                return Ok(json!({"state":"deleted"}));
            }
            record.phase = Phase::Deleting;
            save(context, &record).await?;
            executor
                .execute(context.clone(), "container.destroy".into(), json!({}))
                .await?;
            record.phase = Phase::Deleted;
            save(context, &record).await?;
            Ok(json!({"state":"deleted"}))
        }
        Action::Call { operation } => {
            ensure!(
                record.phase == Phase::Ready,
                "sandbox is not ready for commands"
            );
            executor.execute(context.clone(), operation, input).await
        }
    }
}

pub(crate) async fn invoke(
    context: Context,
    invocation: Invocation,
    authority: Authority,
) -> Result<Output> {
    context.check_turn()?;
    let Invocation::CellRpc { method, args } = invocation else {
        anyhow::bail!("sandbox execution requires RPC");
    };
    authority.validate_target(
        &context
            .identity()
            .context("sandbox child identity missing")?
            .scope,
        &method,
    )?;
    ensure!(
        args.payload_bytes() <= MAX_SANDBOX_MESSAGE_BYTES,
        "sandbox request exceeds byte limit"
    );
    let input = crate::application::decode_rpc(&args)?.to_json()?;
    crate::application::sandbox_catalog::execution::digest(&input)?;
    let reply = match run(&context, authority, input).await {
        Ok(value) => Reply::Success(value),
        Err(error) => Reply::Failure(error.downcast_ref::<SandboxFailure>().cloned().unwrap_or(
            SandboxFailure {
                kind: SandboxFailureKind::Unknown,
            },
        )),
    };
    let value = serde_json::to_value(reply)?;
    crate::application::sandbox_catalog::execution::digest(&value)?;
    Ok(Output::Rpc(crate::application::encode_rpc(
        &cellar_native::clone::StructuredValue::from_json(value)?,
    )?))
}

#[cfg(test)]
#[path = "sandbox_execution_tests.rs"]
mod tests;
