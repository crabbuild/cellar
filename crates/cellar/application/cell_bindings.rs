// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ordinary cell bindings retain their host, incarnation and child identity
//! contracts separately from the source invocation's own durability proof.

use super::storage::RaftDispatch;
use super::{consensus::*, HandlerFuture};
use super::{Context, Storage};
use crate::engine_api::RpcData;
use anyhow::Result;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct Route {
    pub cluster: ClusterId,
    pub namespace: NamespaceId,
    pub cell: CellId,
    pub schema: [u8; 32],
    pub contract: OperationContract,
    pub durability: Durability,
    pub max_input_bytes: usize,
    pub max_result_bytes: usize,
    pub max_retry_window_ms: u64,
    pub json_only: bool,
}

/// The node installs an erased weak owner. A cached application object or a
/// parked invocation cannot keep the node's external SDK lifetime alive.
pub(crate) type BufferLease = Arc<dyn Send + Sync>;

pub(crate) trait Host: Send + Sync {
    fn admit(&self, group: GroupHistory, bytes: usize) -> Result<BufferLease>;

    fn route(
        &self,
        source: &CellHistory,
        configuration: [u8; 32],
        scope: &str,
        method: &str,
    ) -> Result<Route>;

    fn resolve(&self, route: Route, timeout: Duration) -> HandlerFuture<Option<CellHistory>>;

    fn enroll(
        &self,
        history: CellHistory,
        creation: ProposalId,
        durability: Durability,
        timeout: Duration,
    ) -> HandlerFuture<()>;

    fn command(
        &self,
        history: CellHistory,
        contract: OperationContract,
        input: Vec<u8>,
        options: CommandOptions,
    ) -> HandlerFuture<CommandReceipt>;
}

#[derive(Clone)]
pub(crate) struct Source {
    pub host: Arc<dyn Host>,
    pub history: CellHistory,
    pub group: GroupHistory,
    pub parent: CommandId,
    pub parent_fingerprint: [u8; 32],
    pub configuration: [u8; 32],
    pub retry_until_ms: u64,
    pub now_unix_millis: u64,
}

#[derive(Clone)]
pub(crate) struct Intent {
    pub parent: CommandId,
    pub ordinal: u64,
    pub epoch: [u8; 16],
    pub lane: u64,
    pub fingerprint: [u8; 32],
    pub route: Route,
    pub candidate: CellHistory,
    pub creation: ProposalId,
    pub child: CommandId,
    pub retry_until_ms: u64,
    pub input: Vec<u8>,
}

impl Intent {
    pub(crate) fn prepare(
        source: &Source,
        route: Route,
        input: Vec<u8>,
        ordinal: u64,
        lane: u64,
        sequence: u64,
    ) -> Result<Self> {
        anyhow::ensure!(
            input.len() <= route.max_input_bytes,
            CommandFailure::Overloaded
        );
        anyhow::ensure!(
            source
                .retry_until_ms
                .checked_sub(source.now_unix_millis)
                .is_some_and(|window| window <= route.max_retry_window_ms),
            CommandFailure::Overloaded
        );
        let mut hash = Sha256::new();
        hash.update(b"cellar-native-binding-client-1");
        hash.update(crate::raft::cell_groups::cell_image_history_digest(
            &source.history,
        ));
        hash.update(lane.to_be_bytes());
        let mut client: [u8; 32] = hash.finalize().into();
        client[..4].copy_from_slice(b"RCB1");
        let child = CommandId::new(ClientId::new(client[..16].try_into()?)?, sequence)?;
        let mut hash = Sha256::new();
        hash.update(b"cellar-native-binding-enrollment-1");
        hash.update(client);
        hash.update(child.sequence().to_be_bytes());
        let mut creation: [u8; 32] = hash.finalize().into();
        creation[0] |= 1;
        creation[16] |= 1;
        let candidate = CellHistory {
            cluster: route.cluster,
            namespace: route.namespace,
            cell: route.cell.clone(),
            incarnation: IncarnationId::new(creation[16..].try_into()?)?,
        };
        let mut hash = Sha256::new();
        hash.update(b"cellar-native-binding-intent-1");
        hash.update(source.parent.client.bytes());
        hash.update(source.parent.sequence().to_be_bytes());
        hash.update(ordinal.to_be_bytes());
        hash.update(source.parent_fingerprint);
        hash.update(source.configuration);
        hash.update(client);
        hash.update(creation);
        hash.update(child.sequence().to_be_bytes());
        hash.update(source.retry_until_ms.to_be_bytes());
        hash.update(route.cluster.bytes());
        hash.update(route.namespace.bytes());
        hash.update((route.cell.as_str().len() as u64).to_be_bytes());
        hash.update(route.cell.as_str().as_bytes());
        hash.update(route.schema);
        hash.update(route.contract.operation.as_bytes());
        hash.update(route.contract.digest);
        hash.update([match route.durability {
            Durability::Quorum => 1,
            Durability::S3 => 2,
        }]);
        hash.update(command_request_digest(&input));
        Ok(Self {
            parent: source.parent,
            ordinal,
            epoch: source.history.incarnation.bytes(),
            lane,
            fingerprint: hash.finalize().into(),
            route,
            candidate,
            creation: ProposalId::new(creation[..16].try_into()?)?,
            child,
            retry_until_ms: source.retry_until_ms,
            input,
        })
    }
}

/// An admitted ordinary call can outlive its response. Preserve this identity
/// when reconciling; a source failure may leave a durable, unfinished intent.
#[derive(Debug)]
pub struct CallError {
    pub command: CommandId,
    pub target: Option<CellHistory>,
    pub ambiguous: bool,
    pub cause: anyhow::Error,
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cell binding call {:?}, ambiguous={}: {}",
            self.command, self.ambiguous, self.cause
        )
    }
}

impl std::error::Error for CallError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause.as_ref())
    }
}

pub(crate) fn rpc(
    context: &Context,
    scope: String,
    name: Option<String>,
    method: String,
    args: RpcData,
    call_scope: crate::host_rpc::CallScope,
) -> HandlerFuture<RpcData> {
    let signal = call_scope.signal.clone();
    let prepared = (|| -> Result<_> {
        context.check_binding_turn()?;
        context.io.charge_subrequest().map_err(anyhow::Error::msg)?;
        if let Some(signal) = &call_scope.signal {
            signal.throw_if_aborted()?;
        }
        let mut dispatch = crate::storage::capture_raft_dispatch()?
            .ok_or(CommandFailure::UnsupportedCapability)?;
        if let Some(deadline) = call_scope.deadline_ms {
            let remaining = deadline
                .checked_sub(crate::asyncrt::mono_ms())
                .filter(|remaining| *remaining != 0)
                .ok_or(RuntimeError::DeadlineExceeded)?;
            dispatch = dispatch.limit_source(Duration::from_millis(remaining))?;
        }
        let source = dispatch.binding_source()?;
        let route = source
            .host
            .route(&source.history, source.configuration, &scope, &method)?;
        let (class, _) = scope
            .split_once(':')
            .ok_or(CommandFailure::UnsupportedCapability)?;
        super::consensus::native_rpc::validate_identity(
            &context.worker_config.script_name,
            class,
            &scope,
            name.as_deref(),
        )?;
        anyhow::ensure!(
            !route.json_only || matches!(&args, RpcData::Json(_)),
            CommandFailure::UnsupportedCapability
        );
        // The source SQL stage does not own buffers retained by the child
        // dispatch or its response channel. Charge them to the same node/group
        // budget before copying input, and retain admission through delivery.
        let bytes = route
            .max_input_bytes
            .checked_mul(2)
            .and_then(|n| {
                route
                    .max_result_bytes
                    .checked_mul(4)
                    .and_then(|m| n.checked_add(m))
            })
            .and_then(|n| n.checked_add(8192))
            .ok_or(CommandFailure::Overloaded)?;
        let lease = source.host.admit(source.group, bytes)?;
        let input = super::consensus::native_rpc::encode_request(
            name.as_deref(),
            &args,
            route.max_input_bytes,
        )?;
        anyhow::ensure!(
            source
                .retry_until_ms
                .checked_sub(source.now_unix_millis)
                .is_some_and(|window| window <= route.max_retry_window_ms),
            CommandFailure::Overloaded
        );
        let ordinal = crate::storage::next_raft_binding_ordinal()?;
        let selected = Arc::new(Mutex::new(None));
        let observed = selected.clone();
        let storage = context.storage()?;
        let action = dispatch.clone();
        let action_source = source.clone();
        let (identity, call) = crate::storage::raft_bindings::prepare(
            &source,
            route,
            input,
            ordinal,
            move |intent| {
                let identity = intent.child;
                let call = dispatch.try_schedule(move |_| {
                    run(action_source, storage, intent, action, observed, lease)
                })?;
                Ok((identity, call))
            },
        )?;
        Ok((identity, selected, call))
    })();
    Box::pin(async move {
        let (command, selected, call) = prepared?;
        let reply = async move {
            call.future
                .await
                .and_then(std::convert::identity)
                .map(|(value, _lease)| value)
        };
        let result = if let Some(signal) = signal {
            crate::asyncrt::select_biased! { "binding abort preserves its admitted child identity";
                reason=signal.cancelled()=>Err(reason.into()), result=reply=>result,
            }
        } else {
            reply.await
        };
        result.map_err(|cause| {
            if cause.is::<CallError>() {
                cause
            } else {
                CallError {
                    command,
                    target: selected.lock().unwrap().clone(),
                    ambiguous: true,
                    cause,
                }
                .into()
            }
        })
    })
}

async fn run(
    source: Source,
    storage: Storage,
    intent: Intent,
    dispatch: RaftDispatch,
    selected: Arc<Mutex<Option<CellHistory>>>,
    lease: BufferLease,
) -> Result<(RpcData, BufferLease)> {
    // The owned dispatch task already proved the Prepared source intent.
    let found = source
        .host
        .resolve(intent.route.clone(), dispatch.remaining_timeout()?)
        .await?;
    let history = match found {
        Some(history) => history,
        None => {
            let candidate = intent.candidate.clone();
            crate::storage::raft_bindings::select(&intent, &candidate, true)?;
            storage.sync().await?;
            *selected.lock().unwrap() = Some(candidate.clone());
            match source
                .host
                .enroll(
                    candidate.clone(),
                    intent.creation,
                    intent.route.durability,
                    dispatch.remaining_timeout()?,
                )
                .await
            {
                Ok(()) => candidate,
                Err(error) => {
                    // Enrollment has not authorized a business callback. A
                    // concurrent creator may have won this logical name.
                    match source
                        .host
                        .resolve(intent.route.clone(), dispatch.remaining_timeout()?)
                        .await
                    {
                        Ok(Some(history)) => history,
                        _ => return Err(error),
                    }
                }
            }
        }
    };
    crate::storage::raft_bindings::select(&intent, &history, false)?;
    storage.sync().await?;
    *selected.lock().unwrap() = Some(history.clone());
    let mut options = CommandOptions::new(intent.child, intent.retry_until_ms);
    options.timeout = dispatch.remaining_timeout()?;
    options.durability = intent.route.durability;
    let result = source
        .host
        .command(
            history.clone(),
            intent.route.contract.clone(),
            intent.input.clone(),
            options,
        )
        .await;
    match result {
        Ok(receipt) => {
            anyhow::ensure!(
                receipt.id == intent.child
                    && receipt.version.history == history
                    && receipt.durability >= intent.route.durability,
                CommandFailure::CorruptState
            );
            crate::storage::raft_bindings::complete(&intent, &history, &receipt.result)?;
            storage.sync().await?;
            Ok((
                super::consensus::native_rpc::decode_response(&receipt.result)?,
                lease,
            ))
        }
        Err(cause) => {
            let known = cause
                .downcast_ref::<Arc<CellCommandError>>()
                .is_some_and(|error| !error.ambiguous);
            if known {
                crate::storage::raft_bindings::complete(
                    &intent,
                    &history,
                    b"known-target-refusal-1",
                )?;
                storage.sync().await?;
            }
            Err(CallError {
                command: intent.child,
                target: Some(history),
                ambiguous: !known,
                cause,
            }
            .into())
        }
    }
}
