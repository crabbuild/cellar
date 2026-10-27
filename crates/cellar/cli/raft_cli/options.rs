// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Process-boundary input; credentials never enter canonical fleet requests.
use super::*;
use std::io::Read;

pub(super) const HELP: &str = "cellar raft fleet nodes evacuate NODE_HEX --operation-id OP --request-id REQUEST --expected-revision R
cellar raft fleet nodes cancel-evacuation NODE_HEX --operation-id OP --request-id REQUEST --expected-revision R [--expected-process PROCESS --fence-id FENCE]
cellar raft fleet nodes prepare-removal NODE_HEX --operation-id OP --request-id REQUEST --expected-revision R [--wait]
cellar raft fleet nodes stop NODE_HEX --operation-id OP --request-id STOP --expected-process PROCESS --expected-revision R
cellar raft fleet nodes remove NODE_HEX --operation-id OP --request-id REQUEST --stop-id STOP --expected-revision R [--wait]
cellar raft fleet nodes evacuation-status OP
cellar raft fleet nodes add --descriptor NODE.json --operation-id REQUEST_HEX --expected-revision R
cellar raft fleet nodes status NODE_HEX
cellar raft fleet nodes qualify NODE_HEX --attestation CAPACITY.json --operation-id REQUEST_HEX --expected-revision R
cellar raft fleet nodes activate NODE_HEX --attestation CAPACITY.json --operation-id REQUEST_HEX --expected-revision R
cellar raft fleet status --fleet F --admin-url URL [--json]
cellar raft fleet rebalance --keep NODE_HEX,... --policy POLICY.json --operation-id OP_HEX --expected-revision R [--wait]
cellar raft fleet operation status OP_HEX
cellar raft fleet operation progress OP_HEX [--cursor TOKEN] [--limit 64]
cellar raft fleet operation wait OP_HEX [--timeout-seconds 600]
cellar raft fleet operation cancel OP_HEX --operation-id CANCEL_REQUEST_HEX --expected-revision R [--wait]

All commands require --fleet and --admin-url (or CELLAR_ADMIN_URL).
All operation commands require --catalog-id DIGEST from fleet status; retain it
with the request so a replacement fleet cannot receive an old command.
Read the bearer credential from CELLAR_ADMIN_TOKEN or --token-file FILE.
Mutations retain the exact ID, revision and payload across unknown outcomes.
Wait expiry leaves the operation running. Rebalancing verifies replica placement;
it does not certify leader balance or authorize node/storage deletion.
Exit codes: 0 success, 2 invalid input, 3 conflict, 4 blocked/cancelled,
5 unavailable/unknown outcome, 6 wait timeout.";

pub(super) enum Action {
    Status,
    NodeStatus(String),
    EvacuationStatus(String),
    Evacuate(String, Box<RaftFleetNodeOperation>),
    PrepareRemoval(String, Box<RaftFleetNodeOperation>),
    StopNode(String, Box<RaftFleetNodeStop>),
    RemoveNode(String, Box<RaftFleetNodeRemove>),
    CancelEvacuation(String, Box<RaftFleetNodeCancel>),
    NodeRegister(Box<RaftFleetRegisterNode>),
    NodeCapacity(String, Box<RaftFleetNodeCapacity>, bool),
    Begin(RaftFleetBegin),
    Operation(String),
    Progress(String, Option<String>, usize),
    Wait(String),
    Cancel(String, RaftFleetCancel),
}

pub(super) struct Options {
    pub action: Action,
    pub fleet: String,
    pub catalog_id: Option<String>,
    pub admin_url: String,
    pub token: String,
    pub wait: bool,
    pub timeout: Duration,
}

pub(super) fn parse(arguments: Vec<String>) -> Result<Options> {
    let mut args = arguments.into_iter();
    if args.next().as_deref() != Some("fleet") {
        return Err(invalid("raft requires the fleet command"));
    }
    let command = args
        .next()
        .ok_or_else(|| invalid("missing fleet command"))?;
    let (command, target) = if command == "nodes" {
        let verb = args.next().ok_or_else(|| invalid("missing node command"))?;
        let target = if verb == "add" {
            None
        } else {
            Some(args.next().ok_or_else(|| invalid("missing node ID"))?)
        };
        (format!("nodes_{verb}"), target)
    } else if command == "operation" {
        let command = args
            .next()
            .ok_or_else(|| invalid("missing operation command"))?;
        let target = args.next().ok_or_else(|| invalid("missing operation ID"))?;
        (command, Some(target))
    } else {
        (command, None)
    };
    let mut fleet = None;
    let mut catalog_id = None;
    let mut admin_url = None;
    let mut token_file = None;
    let mut operation_id = None;
    let mut expected_revision = None;
    let mut request_id = None;
    let mut expected_process = None;
    let mut stop_id = None;
    let mut fence_id = None;
    let mut keep = None;
    let mut policy = None;
    let mut descriptor = None;
    let mut attestation = None;
    let mut cursor = None;
    let mut limit = 64usize;
    let mut wait = false;
    let mut seconds = 600u64;
    while let Some(flag) = args.next() {
        if flag == "--json" {
            continue;
        }
        if flag == "--wait" {
            wait = true;
            continue;
        }
        let value = args
            .next()
            .ok_or_else(|| invalid("option requires a value"))?;
        match flag.as_str() {
            "--fleet" => set(&mut fleet, value)?,
            "--catalog-id" => set(&mut catalog_id, value)?,
            "--admin-url" => set(&mut admin_url, value)?,
            "--token-file" => set(&mut token_file, value)?,
            "--operation-id" => set(&mut operation_id, value)?,
            "--request-id" => set(&mut request_id, value)?,
            "--expected-process" => set(&mut expected_process, value)?,
            "--stop-id" => set(&mut stop_id, value)?,
            "--fence-id" => set(&mut fence_id, value)?,
            "--expected-revision" => set(
                &mut expected_revision,
                value
                    .parse::<u64>()
                    .map_err(|_| invalid("invalid expected revision"))?,
            )?,
            "--keep" => set(
                &mut keep,
                value.split(',').map(str::to_owned).collect::<Vec<_>>(),
            )?,
            "--descriptor" => set(
                &mut descriptor,
                json_file::<RaftFleetNodeDescriptor>(&value)?,
            )?,
            "--attestation" => set(
                &mut attestation,
                json_file::<RaftFleetCapacityAttestation>(&value)?,
            )?,
            "--policy" => set(
                &mut policy,
                json_file::<crate::raft::fleet_catalog::FleetTopologyPolicy>(&value)?,
            )?,
            "--cursor" => set(&mut cursor, value)?,
            "--limit" => limit = value.parse().map_err(|_| invalid("invalid page limit"))?,
            "--timeout-seconds" => {
                seconds = value.parse().map_err(|_| invalid("invalid wait timeout"))?
            }
            _ => return Err(invalid("unsupported raft fleet option")),
        }
    }
    if seconds == 0
        || seconds > 86_400
        || !(1..=128).contains(&limit)
        || cursor.as_ref().is_some_and(|s| s.len() > 2048)
    {
        return Err(invalid("invalid wait or pagination bounds"));
    }
    let fleet = fleet.ok_or_else(|| invalid("--fleet is required"))?;
    if fleet.is_empty()
        || fleet.len() > 128
        || !fleet
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
    {
        return Err(invalid("invalid fleet ID"));
    }
    if let Some(pin) = &catalog_id {
        validate_catalog(pin)?;
    }
    let action = match (command.as_str(), target) {
        ("nodes_evacuation-status", Some(operation)) if !wait && cursor.is_none() => {
            validate_id(&operation)?;
            Action::EvacuationStatus(operation)
        }
        (
            verb @ ("nodes_evacuate"
            | "nodes_prepare-removal"
            | "nodes_stop"
            | "nodes_remove"
            | "nodes_cancel-evacuation"),
            Some(node),
        ) if cursor.is_none() => {
            validate_id(&node)?;
            let request = RaftFleetNodeOperation {
                catalog_id: catalog_id
                    .clone()
                    .ok_or_else(|| invalid("--catalog-id from fleet status is required"))?,
                operation_id: operation_id
                    .take()
                    .ok_or_else(|| invalid("--operation-id is required"))?,
                request_id: request_id
                    .take()
                    .ok_or_else(|| invalid("--request-id is required"))?,
                expected_revision: expected_revision
                    .take()
                    .ok_or_else(|| invalid("--expected-revision is required"))?,
            };
            request.validate()?;
            match verb {
                "nodes_evacuate" if !wait => Action::Evacuate(node, Box::new(request)),
                "nodes_prepare-removal" => Action::PrepareRemoval(node, Box::new(request)),
                "nodes_stop" if !wait => {
                    let request = RaftFleetNodeStop {
                        operation: request,
                        expected_process: expected_process
                            .take()
                            .ok_or_else(|| invalid("--expected-process is required"))?,
                    };
                    request.validate()?;
                    Action::StopNode(node, Box::new(request))
                }
                "nodes_cancel-evacuation" if !wait => {
                    let request = RaftFleetNodeCancel {
                        operation: request,
                        expected_process: expected_process.take(),
                        fence_id: fence_id.take(),
                    };
                    request.validate()?;
                    Action::CancelEvacuation(node, Box::new(request))
                }
                "nodes_remove" => {
                    let request = RaftFleetNodeRemove {
                        operation: request,
                        stop_id: stop_id
                            .take()
                            .ok_or_else(|| invalid("--stop-id is required"))?,
                    };
                    request.validate()?;
                    Action::RemoveNode(node, Box::new(request))
                }
                _ => return Err(invalid("--wait is only supported for verification steps")),
            }
        }
        ("nodes_status", Some(node)) if !wait && cursor.is_none() => {
            validate_id(&node)?;
            Action::NodeStatus(node)
        }
        ("nodes_add", None) if !wait && cursor.is_none() => {
            let request = RaftFleetRegisterNode {
                catalog_id: catalog_id
                    .clone()
                    .ok_or_else(|| invalid("--catalog-id from fleet status is required"))?,
                request_id: operation_id
                    .take()
                    .ok_or_else(|| invalid("node add requires --operation-id"))?,
                expected_revision: expected_revision
                    .take()
                    .ok_or_else(|| invalid("node add requires --expected-revision"))?,
                descriptor: descriptor
                    .take()
                    .ok_or_else(|| invalid("node add requires --descriptor"))?,
            };
            request.validate()?;
            Action::NodeRegister(Box::new(request))
        }
        ("nodes_qualify" | "nodes_activate", Some(node)) if !wait && cursor.is_none() => {
            let request = RaftFleetNodeCapacity {
                catalog_id: catalog_id
                    .clone()
                    .ok_or_else(|| invalid("--catalog-id from fleet status is required"))?,
                request_id: operation_id
                    .take()
                    .ok_or_else(|| invalid("node capacity requires --operation-id"))?,
                expected_revision: expected_revision
                    .take()
                    .ok_or_else(|| invalid("node capacity requires --expected-revision"))?,
                attestation: attestation
                    .take()
                    .ok_or_else(|| invalid("node capacity requires --attestation"))?,
            };
            request.validate(&node)?;
            Action::NodeCapacity(node, Box::new(request), command == "nodes_activate")
        }
        ("status", None)
            if operation_id.is_none()
                && expected_revision.is_none()
                && keep.is_none()
                && policy.is_none()
                && cursor.is_none()
                && !wait =>
        {
            Action::Status
        }
        ("rebalance", None) if cursor.is_none() => Action::Begin(
            RaftFleetBegin {
                catalog_id: catalog_id
                    .clone()
                    .ok_or_else(|| invalid("--catalog-id from fleet status is required"))?,
                operation_id: operation_id
                    .take()
                    .ok_or_else(|| invalid("rebalance requires --operation-id"))?,
                expected_revision: expected_revision
                    .take()
                    .ok_or_else(|| invalid("rebalance requires --expected-revision"))?,
                keep: keep
                    .take()
                    .ok_or_else(|| invalid("rebalance requires an explicit --keep set"))?,
                policy: policy
                    .take()
                    .ok_or_else(|| invalid("rebalance requires --policy"))?,
            }
            .canonicalize()?,
        ),
        ("status", Some(id)) if !wait && cursor.is_none() => Action::Operation(id),
        ("progress", Some(id)) if !wait => Action::Progress(id, cursor.take(), limit),
        ("wait", Some(id)) if cursor.is_none() => Action::Wait(id),
        ("cancel", Some(id)) if cursor.is_none() => {
            let request = RaftFleetCancel {
                catalog_id: catalog_id
                    .clone()
                    .ok_or_else(|| invalid("--catalog-id from fleet status is required"))?,
                request_id: operation_id
                    .take()
                    .ok_or_else(|| invalid("cancel requires --operation-id"))?,
                expected_revision: expected_revision
                    .take()
                    .ok_or_else(|| invalid("cancel requires --expected-revision"))?,
            };
            request.validate(&id)?;
            Action::Cancel(id, request)
        }
        _ => return Err(invalid("unsupported fleet command or options")),
    };
    if !matches!(&action, Action::Status) && catalog_id.is_none() {
        return Err(invalid("--catalog-id from fleet status is required"));
    }
    if operation_id.is_some()
        || expected_revision.is_some()
        || keep.is_some()
        || policy.is_some()
        || cursor.is_some()
        || descriptor.is_some()
        || attestation.is_some()
        || request_id.is_some()
        || expected_process.is_some()
        || stop_id.is_some()
        || fence_id.is_some()
    {
        return Err(invalid("options do not apply to this command"));
    }
    if let Action::Operation(id)
    | Action::Progress(id, ..)
    | Action::Wait(id)
    | Action::Cancel(id, _) = &action
    {
        validate_id(id)?;
    }
    let admin_url = admin_url
        .or_else(|| std::env::var("CELLAR_ADMIN_URL").ok())
        .ok_or_else(|| invalid("--admin-url or CELLAR_ADMIN_URL is required"))?;
    let token = if let Some(file) = token_file {
        String::from_utf8(read_file(&file, 8192)?).map_err(|_| invalid("invalid token file"))?
    } else {
        std::env::var("CELLAR_ADMIN_TOKEN")
            .map_err(|_| invalid("CELLAR_ADMIN_TOKEN or --token-file is required"))?
    };
    let token = token.trim().to_owned();
    if token.len() < 32
        || token.len() > 8192
        || reqwest::header::HeaderValue::from_str(&format!("Bearer {token}")).is_err()
    {
        return Err(invalid("invalid management credential"));
    }
    Ok(Options {
        action,
        fleet,
        catalog_id,
        admin_url,
        token,
        wait,
        timeout: Duration::from_secs(seconds),
    })
}

fn set<T>(slot: &mut Option<T>, value: T) -> Result<()> {
    if slot.replace(value).is_some() {
        Err(invalid("duplicate option"))
    } else {
        Ok(())
    }
}

fn read_file(path: &str, limit: usize) -> Result<Vec<u8>> {
    let file =
        std::fs::File::open(path).map_err(|_| invalid("cannot open the requested input file"))?;
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid("cannot read the requested input file"))?;
    if bytes.len() > limit {
        return Err(invalid("input file exceeds its byte limit"));
    }
    Ok(bytes)
}

fn json_file<T: serde::de::DeserializeOwned>(path: &str) -> Result<T> {
    serde_json::from_slice(&read_file(path, 16 * 1024)?).map_err(|_| invalid("invalid JSON input"))
}

fn validate_id(value: &str) -> Result<()> {
    let mut bytes = [0; 16];
    if value.len() != 32
        || hex::decode_to_slice(value, &mut bytes).is_err()
        || hex::encode(bytes) != value
        || bytes == [0; 16]
    {
        Err(invalid(
            "operation IDs require 32 nonzero lowercase hexadecimal digits",
        ))
    } else {
        Ok(())
    }
}

fn validate_catalog(value: &str) -> Result<()> {
    let mut bytes = [0; 32];
    if value.len() != 64
        || hex::decode_to_slice(value, &mut bytes).is_err()
        || hex::encode(bytes) != value
        || bytes == [0; 32]
    {
        Err(invalid(
            "catalog ID requires 64 nonzero lowercase hexadecimal digits",
        ))
    } else {
        Ok(())
    }
}
