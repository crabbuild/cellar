// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// This module is an explicit process boundary for environment, files and exit.
#![allow(clippy::disallowed_methods)]
//! Human operator commands use authenticated management, not bucket listing.
mod http;
mod lifecycle;
mod nodes;
mod options;
use crate::sdk::fleet_operations::*;
use serde_json::{json, Value};
use std::time::Duration;

type Result<T> = std::result::Result<T, RaftFleetFailure>;

fn invalid(message: &str) -> RaftFleetFailure {
    RaftFleetFailure::invalid(message)
}

fn unavailable() -> RaftFleetFailure {
    RaftFleetFailure {
        code: RaftFleetFailureCode::Unavailable,
        outcome: RaftFleetOutcome::Unknown,
        message:
            "The management call was not confirmed; retain the exact request for reconciliation."
                .into(),
    }
}

/// Execute in an application execution domain. This function never exits the
/// embedding process and does not automatically retry a mutation.
pub async fn run(arguments: Vec<String>) -> Result<Value> {
    let options = options::parse(arguments)?;
    execute(&options).await
}

async fn execute(options: &options::Options) -> Result<Value> {
    use options::Action;
    let client = http::Client::new(options)?;
    let timeout = Duration::from_secs(30);
    match &options.action {
        Action::EvacuationStatus(operation) => {
            lifecycle::status(&client, options.catalog_id.as_deref(), operation, timeout).await
        }
        Action::Evacuate(node, request) => {
            lifecycle::execute(
                &client,
                lifecycle::Scope {
                    node,
                    catalog: &request.catalog_id,
                    request: &request.request_id,
                    operation: &request.operation_id,
                    revision: request.expected_revision,
                    verb: "evacuate",
                    process: None,
                    stop: None,
                    fence: None,
                },
                request.as_ref(),
                false,
                options.timeout,
            )
            .await
        }
        Action::PrepareRemoval(node, request) => {
            lifecycle::execute(
                &client,
                lifecycle::Scope {
                    node,
                    catalog: &request.catalog_id,
                    request: &request.request_id,
                    operation: &request.operation_id,
                    revision: request.expected_revision,
                    verb: "prepare-removal",
                    process: None,
                    stop: None,
                    fence: None,
                },
                request.as_ref(),
                options.wait,
                options.timeout,
            )
            .await
        }
        Action::StopNode(node, request) => {
            lifecycle::execute(
                &client,
                lifecycle::Scope {
                    node,
                    catalog: &request.operation.catalog_id,
                    request: &request.operation.request_id,
                    operation: &request.operation.operation_id,
                    revision: request.operation.expected_revision,
                    verb: "stop",
                    process: Some(&request.expected_process),
                    stop: Some(&request.operation.request_id),
                    fence: None,
                },
                request.as_ref(),
                false,
                options.timeout,
            )
            .await
        }
        Action::RemoveNode(node, request) => {
            lifecycle::execute(
                &client,
                lifecycle::Scope {
                    node,
                    catalog: &request.operation.catalog_id,
                    request: &request.operation.request_id,
                    operation: &request.operation.operation_id,
                    revision: request.operation.expected_revision,
                    verb: "remove",
                    process: None,
                    stop: Some(&request.stop_id),
                    fence: None,
                },
                request.as_ref(),
                options.wait,
                options.timeout,
            )
            .await
        }
        Action::CancelEvacuation(node, request) => {
            lifecycle::execute(
                &client,
                lifecycle::Scope {
                    node,
                    catalog: &request.operation.catalog_id,
                    request: &request.operation.request_id,
                    operation: &request.operation.operation_id,
                    revision: request.operation.expected_revision,
                    verb: "cancel-evacuation",
                    process: request.expected_process.as_deref(),
                    stop: None,
                    fence: request.fence_id.as_deref(),
                },
                request.as_ref(),
                false,
                options.timeout,
            )
            .await
        }
        Action::NodeStatus(node) => {
            nodes::status(&client, options.catalog_id.as_deref(), node, timeout).await
        }
        Action::NodeRegister(request) => nodes::register(&client, request).await,
        Action::NodeCapacity(node, request, activate) => {
            nodes::capacity(&client, node, request, *activate).await
        }
        Action::Status => {
            let view = client
                .get::<RaftFleetStatus>("status", None, None, timeout)
                .await?;
            check_catalog(options.catalog_id.as_deref(), &view.catalog_id)?;
            if view.observed_revision == 0 || view.active.as_ref().is_some_and(|v| v.terminal) {
                return Err(unavailable());
            }
            for operation in view.active.iter().chain(view.placement.iter()) {
                operation.validate().map_err(|_| unavailable())?;
                check_catalog(Some(&view.catalog_id), &operation.catalog_id)?;
                if operation.observed_revision != view.observed_revision {
                    return Err(unavailable());
                }
            }
            serde_json::to_value(view).map_err(|_| unavailable())
        }
        Action::Operation(id) => {
            let view = client
                .get::<RaftFleetOperation>(&format!("operations/{id}"), None, None, timeout)
                .await?;
            view.validate().map_err(|_| unavailable())?;
            check_catalog(options.catalog_id.as_deref(), &view.catalog_id)?;
            if view.operation_id != *id {
                return Err(unavailable());
            }
            serde_json::to_value(view).map_err(|_| unavailable())
        }
        Action::Progress(id, cursor, limit) => {
            let page = client
                .get::<RaftFleetMovements>(
                    &format!("operations/{id}/movements"),
                    cursor.as_deref(),
                    Some(*limit),
                    timeout,
                )
                .await?;
            check_catalog(options.catalog_id.as_deref(), &page.catalog_id)?;
            if page.operation_id != *id
                || page.observed_revision == 0
                || page.entries.len() > *limit
            {
                return Err(unavailable());
            }
            serde_json::to_value(page).map_err(|_| unavailable())
        }
        Action::Wait(id) => serde_json::to_value(
            http::wait(
                &client,
                id,
                options
                    .catalog_id
                    .as_deref()
                    .ok_or_else(|| invalid("missing catalog ID"))?,
                options.timeout,
                false,
            )
            .await?,
        )
        .map_err(|_| unavailable()),
        Action::Begin(request) => {
            let accepted = client
                .post::<RaftFleetAcceptance>("operations", request)
                .await?;
            accepted.operation.validate().map_err(|_| unavailable())?;
            check_catalog(Some(&request.catalog_id), &accepted.operation.catalog_id)?;
            if accepted.request_id != request.operation_id
                || accepted.operation.operation_id != request.operation_id
            {
                return Err(unavailable());
            }
            if options.wait {
                serde_json::to_value(
                    http::wait(
                        &client,
                        &request.operation_id,
                        &request.catalog_id,
                        options.timeout,
                        false,
                    )
                    .await?,
                )
                .map_err(|_| unavailable())
            } else {
                serde_json::to_value(accepted).map_err(|_| unavailable())
            }
        }
        Action::Cancel(id, request) => {
            let accepted = client
                .post::<RaftFleetAcceptance>(&format!("operations/{id}/cancel"), request)
                .await?;
            accepted.operation.validate().map_err(|_| unavailable())?;
            check_catalog(Some(&request.catalog_id), &accepted.operation.catalog_id)?;
            if accepted.request_id != request.request_id || accepted.operation.operation_id != *id {
                return Err(unavailable());
            }
            if options.wait {
                serde_json::to_value(
                    http::wait(&client, id, &request.catalog_id, options.timeout, true).await?,
                )
                .map_err(|_| unavailable())
            } else {
                serde_json::to_value(accepted).map_err(|_| unavailable())
            }
        }
    }
}

/// Dedicated `cellar` process entry. The runtime/domain drop before termination;
/// library users call `run` and receive a typed result instead.
pub(crate) fn entry(arguments: Vec<String>) -> ! {
    use crate::cli_output::{Format, Output};
    if arguments
        .iter()
        .any(|a| matches!(a.as_str(), "--help" | "-h"))
        || arguments.first().is_some_and(|a| a == "help")
        || arguments.get(1).is_some_and(|a| a == "help")
    {
        let result = Output::new(Format::Text).line(format_args!("{}", options::HELP));
        std::process::exit(if result.is_ok() { 0 } else { 5 });
    }
    let json_output = arguments.iter().any(|a| a == "--json");
    let result = match options::parse(arguments) {
        Err(error) => Err(error),
        Ok(options) => {
            rustls::crypto::ring::default_provider()
                .install_default()
                .ok();
            match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Err(_) => Err(unavailable()),
                Ok(runtime) => runtime.block_on(async {
                    let domain =
                        crate::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
                    let result = domain
                        .run(execute(&options))
                        .await
                        .map_err(|_| unavailable())
                        .and_then(|r| r);
                    domain.shutdown().await;
                    result
                }),
            }
        }
    };
    let code = match result {
        Ok(value) => {
            let result = Output::new(if json_output {
                Format::Json
            } else {
                Format::Text
            })
            .row(&Record(value));
            if result.is_ok() {
                0
            } else {
                crate::note!("Unable to write the command result.");
                5
            }
        }
        Err(error) => {
            if json_output {
                if Output::new(Format::Json)
                    .row(&Record(json!({"error":error,"phase":null,"terminal":null})))
                    .is_err()
                {
                    crate::note!("Unable to write the command result.");
                }
            } else {
                crate::note!("{error}");
            }
            error.exit_code()
        }
    };
    std::process::exit(code)
}

struct Record(Value);

impl crate::cli_output::Record for Record {
    fn json(&self) -> Value {
        self.0.clone()
    }

    fn text(&self) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Owned(format!("{:#}", self.0))
    }
}

fn catalog_mismatch() -> RaftFleetFailure {
    RaftFleetFailure { code: RaftFleetFailureCode::Conflict, outcome: RaftFleetOutcome::Unknown,
    message: "The response belongs to another catalog generation; the retained operation was not confirmed.".into() }
}

fn check_catalog(expected: Option<&str>, actual: &str) -> Result<()> {
    let mut digest = [0; 32];
    if actual.len() != 64
        || hex::decode_to_slice(actual, &mut digest).is_err()
        || hex::encode(digest) != actual
        || digest == [0; 32]
    {
        return Err(unavailable());
    }
    if expected.is_some_and(|expected| expected != actual) {
        return Err(catalog_mismatch());
    }
    Ok(())
}
