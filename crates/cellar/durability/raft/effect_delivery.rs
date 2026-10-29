// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Authenticated, immutable delivery envelopes. Transport success alone cannot
//! settle an intent: the destination signs its transaction result and releases
//! it through the ordinary command durability gate.
use crate::application::consensus::{
    protocol::WireScope, ClientId, CommandFailure, CommandId, Durability, GroupScope,
    OperationContract, OperationKind, RaftCellHandle,
};
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use futures_util::future::BoxFuture;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fmt,
    sync::{Arc, Weak},
    time::Duration,
};

#[cfg(all(test, not(cellar_internal_tests)))]
#[path = "effect_delivery_tests.rs"]
mod tests;

pub(crate) const MAX_REQUEST: usize = 60 * 1024;
pub(crate) const MAX_RECEIPT: usize = 16 * 1024;
pub(crate) const MAX_PAYLOAD: usize = 32 * 1024;
pub(crate) const MAX_RESULT: usize = 4 * 1024;
const REQUEST_DOMAIN: &[u8] = b"cellar-raft-delivery-request-1";
const RECEIPT_DOMAIN: &[u8] = b"cellar-raft-delivery-receipt-1";
const CLIENT_PREFIX: &[u8; 4] = b"RFX1";

pub(crate) fn protocol_identity() -> [u8; 32] {
    Sha256::digest(b"cellar-raft-delivery-1/definition-target-1").into()
}

/// Deployment-owned material shared by compatible replicas. Debug never
/// exposes secrets. Rotation requires a new compatible capability definition.
pub struct RaftEffectSigningKey(SigningKey);

impl RaftEffectSigningKey {
    pub fn new(mut secret: [u8; 32]) -> Result<Self> {
        ensure!(secret != [0; 32], "invalid effect signing key");
        let key = SigningKey::from_bytes(&secret);
        secret.fill(0);
        Ok(Self(key))
    }

    pub fn verification_key(&self) -> RaftEffectVerificationKey {
        RaftEffectVerificationKey(self.0.verifying_key().to_bytes())
    }
}

impl fmt::Debug for RaftEffectSigningKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RaftEffectSigningKey([redacted])")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RaftEffectVerificationKey([u8; 32]);

impl RaftEffectVerificationKey {
    pub fn new(public: [u8; 32]) -> Result<Self> {
        let key = VerifyingKey::from_bytes(&public)?;
        ensure!(!key.is_weak(), "weak effect verification key");
        Ok(Self(public))
    }

    pub fn public_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// A logical destination, never a physical leader address. Its signed receipt
/// policy is fixed; a source cannot upgrade an old Quorum receipt to S3.
/// The definition digest is RaftClass::definition_digest, paired with the exact
/// command contract. The target's immutable namespace separately binds its full
/// compiled schema; that manifest can contain self/cyclic delivery bindings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftEffectTarget {
    pub(crate) scope: GroupScope,
    pub(crate) definition: [u8; 32],
    pub(crate) contract: OperationContract,
    pub(crate) key: RaftEffectVerificationKey,
    pub(crate) durability: Durability,
}

impl RaftEffectTarget {
    pub fn new(
        scope: GroupScope,
        definition: [u8; 32],
        contract: OperationContract,
        receipt_key: RaftEffectVerificationKey,
        durability: Durability,
    ) -> Result<Self> {
        scope.validate()?;
        ensure!(
            definition != [0; 32]
                && contract.kind == OperationKind::Command
                && contract.digest != [0; 32]
                && !contract.operation.starts_with("__sys_raft_")
                && cellar_logic::cell::valid_cell_scope(&contract.operation)
                && contract.operation.len() <= 128,
            "invalid effect target"
        );
        Ok(Self {
            scope,
            definition,
            contract,
            key: receipt_key,
            durability,
        })
    }

    pub fn scope(&self) -> &GroupScope {
        &self.scope
    }

    pub fn definition_digest(&self) -> [u8; 32] {
        self.definition
    }

    pub fn contract(&self) -> &OperationContract {
        &self.contract
    }

    pub fn durability(&self) -> Durability {
        self.durability
    }

    pub(crate) fn wire(&self) -> Target {
        Target {
            scope: WireScope::from(&self.scope),
            definition: self.definition,
            operation: self.contract.operation.clone(),
            contract: self.contract.digest,
            key: self.key.0,
            durability: policy(self.durability),
        }
    }
}

/// Receiver authorization for one source class and compiled lane. Every source
/// cell/history is separately identified and signed; grants do not confer Raft
/// membership or permit the source to choose a different destination contract.
#[derive(Clone, Debug)]
pub struct RaftEffectSourceGrant {
    pub(crate) cluster: crate::application::consensus::ClusterId,
    pub(crate) namespace: crate::application::consensus::NamespaceId,
    pub(crate) class: String,
    pub(crate) binding: String,
    pub(crate) digest: [u8; 32],
    pub(crate) key: RaftEffectVerificationKey,
}

impl RaftEffectSourceGrant {
    pub fn new(
        cluster: crate::application::consensus::ClusterId,
        namespace: crate::application::consensus::NamespaceId,
        class: impl Into<String>,
        binding: impl Into<String>,
        binding_digest: [u8; 32],
        sender_key: RaftEffectVerificationKey,
    ) -> Result<Self> {
        let (class, binding) = (class.into(), binding.into());
        ensure!(
            valid_name(&class) && valid_name(&binding) && binding_digest != [0; 32],
            "invalid effect source grant"
        );
        Ok(Self {
            cluster,
            namespace,
            class,
            binding,
            digest: binding_digest,
            key: sender_key,
        })
    }

    pub(crate) fn identity(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.cluster.bytes());
        bytes.extend_from_slice(&self.namespace.bytes());
        for name in [&self.class, &self.binding] {
            bytes.extend_from_slice(&(name.len() as u16).to_be_bytes());
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.extend_from_slice(&self.digest);
        bytes.extend_from_slice(&self.key.0);
        bytes
    }

    fn accepts(&self, request: &Request) -> Result<bool> {
        let scope = request.origin.clone().decode()?;
        scope.validate()?;
        Ok(scope.cluster == self.cluster
            && scope.namespace == self.namespace
            && scope
                .cell
                .split_once(':')
                .is_some_and(|(class, _)| class == self.class)
            && request.binding == self.binding
            && request.binding_digest == self.digest
            && request.sender == self.key.0)
    }
}

pub(crate) fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && cellar_logic::cell::valid_cell_scope(name)
        && !name.contains(':')
        && !name.starts_with("__sys_raft_")
}

pub(crate) fn policy(value: Durability) -> u8 {
    if value == Durability::S3 {
        1
    } else {
        0
    }
}

fn message(domain: &[u8], body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(domain.len() + 8 + body.len());
    bytes.extend_from_slice(domain);
    bytes.extend_from_slice(&(body.len() as u64).to_be_bytes());
    bytes.extend_from_slice(body);
    bytes
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Signed<T> {
    body: T,
    signature: String,
}

fn sign<T: Serialize>(
    body: T,
    key: &RaftEffectSigningKey,
    domain: &[u8],
    max: usize,
) -> Result<Vec<u8>> {
    let canonical = serde_json::to_vec(&body)?;
    let signature = URL_SAFE_NO_PAD.encode(key.0.sign(&message(domain, &canonical)).to_bytes());
    let bytes = serde_json::to_vec(&Signed { body, signature })?;
    ensure!(bytes.len() <= max, CommandFailure::Overloaded);
    Ok(bytes)
}

fn parse<T: Serialize + DeserializeOwned>(bytes: &[u8], max: usize) -> Result<Signed<T>> {
    ensure!(bytes.len() <= max, CommandFailure::Overloaded);
    let signed: Signed<T> = serde_json::from_slice(bytes)?;
    ensure!(
        serde_json::to_vec(&signed)? == bytes,
        CommandFailure::CommandConflict
    );
    Ok(signed)
}

fn verify<T: Serialize>(
    signed: &Signed<T>,
    key: RaftEffectVerificationKey,
    domain: &[u8],
) -> Result<()> {
    let signature = URL_SAFE_NO_PAD.decode(&signed.signature)?;
    ensure!(
        URL_SAFE_NO_PAD.encode(&signature) == signed.signature,
        CommandFailure::CommandConflict
    );
    let signature = Signature::from_slice(&signature)?;
    let verifier = VerifyingKey::from_bytes(&key.0)?;
    verifier.verify_strict(
        &message(domain, &serde_json::to_vec(&signed.body)?),
        &signature,
    )?;
    Ok(())
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Target {
    scope: WireScope,
    definition: [u8; 32],
    operation: String,
    contract: [u8; 32],
    key: [u8; 32],
    durability: u8,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    marker: u8,
    pub(crate) origin: WireScope,
    origin_schema: [u8; 32],
    pub(crate) binding: String,
    binding_digest: [u8; 32],
    sequence: String,
    target: Target,
    payload_digest: [u8; 32],
    payload: String,
    sender: [u8; 32],
}

/// Immutable logical request. A new leader/attempt recreates exactly these
/// bytes. Terms, clocks and process identities never enter the receiver input.
#[derive(Clone)]
pub struct RaftDeliveryRequest {
    inner: Arc<DeliveryData>,
}

struct DeliveryData {
    bytes: Vec<u8>,
    request: Request,
    target: RaftEffectTarget,
    command: CommandId,
}

impl fmt::Debug for RaftDeliveryRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RaftDeliveryRequest")
            .field("command", &self.inner.command)
            .field("bytes", &self.inner.bytes.len())
            .finish()
    }
}

pub(crate) struct DeliveryOrigin<'a> {
    pub scope: &'a GroupScope,
    pub schema: [u8; 32],
    pub binding: &'a str,
    pub digest: [u8; 32],
    pub sequence: u64,
}

impl RaftDeliveryRequest {
    pub(crate) fn new(
        origin: DeliveryOrigin<'_>,
        payload: &[u8],
        target: RaftEffectTarget,
        key: &RaftEffectSigningKey,
    ) -> Result<Self> {
        let DeliveryOrigin {
            scope: origin,
            schema,
            binding,
            digest,
            sequence,
        } = origin;
        origin.validate()?;
        ensure!(
            schema != [0; 32]
                && valid_name(binding)
                && digest != [0; 32]
                && sequence > 0
                && payload.len() <= MAX_PAYLOAD,
            CommandFailure::UnsupportedCapability
        );
        let request = Request {
            marker: 1,
            origin: WireScope::from(origin),
            origin_schema: schema,
            binding: binding.into(),
            binding_digest: digest,
            sequence: sequence.to_string(),
            target: target.wire(),
            payload_digest: Sha256::digest(payload).into(),
            payload: URL_SAFE_NO_PAD.encode(payload),
            sender: key.verification_key().0,
        };
        let command = command_id(&request)?;
        let bytes = sign(request.clone(), key, REQUEST_DOMAIN, MAX_REQUEST)?;
        Ok(Self {
            inner: Arc::new(DeliveryData {
                bytes,
                request,
                target,
                command,
            }),
        })
    }

    pub fn encoded(&self) -> &[u8] {
        &self.inner.bytes
    }

    pub fn command_id(&self) -> CommandId {
        self.inner.command
    }

    pub fn target(&self) -> &RaftEffectTarget {
        &self.inner.target
    }

    /// Verify an opaque destination receipt before replicating source cleanup.
    /// This authenticates a configured receiver's durability assertion; it is
    /// not a Byzantine consensus certificate from the destination voters.
    pub fn verify_receipt(&self, bytes: &[u8]) -> Result<()> {
        let signed: Signed<Receipt> = parse(bytes, MAX_RECEIPT)?;
        verify(&signed, self.inner.target.key, RECEIPT_DOMAIN)?;
        ensure!(
            signed.body.marker == 1
                && signed.body.request_digest
                    == <[u8; 32]>::from(Sha256::digest(&self.inner.bytes))
                && signed.body.target == self.inner.request.target
                && signed.body.identity == identity(&self.inner.request)?,
            CommandFailure::CommandConflict
        );
        let result = URL_SAFE_NO_PAD.decode(&signed.body.result)?;
        ensure!(
            result.len() <= MAX_RESULT && URL_SAFE_NO_PAD.encode(&result) == signed.body.result,
            CommandFailure::CommandConflict
        );
        Ok(())
    }
}

pub(crate) fn reserved_client(client: ClientId) -> bool {
    client.bytes().starts_with(CLIENT_PREFIX)
}

pub(crate) fn restore_request(
    bytes: &[u8],
    origin: DeliveryOrigin<'_>,
    target: RaftEffectTarget,
    key: &RaftEffectSigningKey,
) -> Result<RaftDeliveryRequest> {
    let signed: Signed<Request> = parse(bytes, MAX_REQUEST)?;
    verify(&signed, key.verification_key(), REQUEST_DOMAIN)?;
    let payload = URL_SAFE_NO_PAD.decode(&signed.body.payload)?;
    let expected = RaftDeliveryRequest::new(origin, &payload, target, key)?;
    ensure!(expected.encoded() == bytes, CommandFailure::CorruptState);
    Ok(expected)
}

fn identity(request: &Request) -> Result<[u8; 32]> {
    // Payload and source schema are deliberately separate from logical ID.
    // Reusing that ID with changed bytes is a conflict, never a new delivery.
    let body = serde_json::to_vec(&(
        &request.origin,
        &request.binding,
        &request.binding_digest,
        &request.target,
    ))?;
    Ok(Sha256::digest(message(b"cellar-raft-delivery-lane-1", &body)).into())
}

fn command_id(request: &Request) -> Result<CommandId> {
    let digest = identity(request)?;
    let mut client = [0; 16];
    client[..4].copy_from_slice(CLIENT_PREFIX);
    client[4..].copy_from_slice(&digest[..12]);
    let sequence: u64 = request.sequence.parse()?;
    ensure!(
        sequence.to_string() == request.sequence,
        CommandFailure::CommandConflict
    );
    Ok(CommandId::new(ClientId::new(client)?, sequence)?)
}

pub(crate) struct Incoming {
    pub command: CommandId,
    pub lane: [u8; 32],
    pub digest: [u8; 32],
    pub payload: Vec<u8>,
    request: Request,
}

pub(crate) fn authenticate(
    bytes: &[u8],
    grants: &[RaftEffectSourceGrant],
    target: &GroupScope,
    definition: [u8; 32],
    contract: &OperationContract,
    key: RaftEffectVerificationKey,
    durability: Durability,
) -> Result<Incoming> {
    let signed: Signed<Request> = parse(bytes, MAX_REQUEST)?;
    let request = &signed.body;
    ensure!(
        request.marker == 1 && request.origin_schema != [0; 32] && valid_name(&request.binding),
        CommandFailure::CommandConflict
    );
    let grant = grants
        .iter()
        .find(|grant| grant.accepts(request).unwrap_or(false))
        .ok_or(CommandFailure::UnsupportedCapability)?;
    verify(&signed, grant.key, REQUEST_DOMAIN)?;
    let expected = RaftEffectTarget::new(
        target.clone(),
        definition,
        contract.clone(),
        key,
        durability,
    )?;
    ensure!(
        request.target == expected.wire(),
        CommandFailure::CommandConflict
    );
    let payload = URL_SAFE_NO_PAD.decode(&request.payload)?;
    ensure!(
        payload.len() <= MAX_PAYLOAD
            && URL_SAFE_NO_PAD.encode(&payload) == request.payload
            && <[u8; 32]>::from(Sha256::digest(&payload)) == request.payload_digest,
        CommandFailure::CommandConflict
    );
    Ok(Incoming {
        command: command_id(request)?,
        lane: identity(request)?,
        digest: Sha256::digest(bytes).into(),
        payload,
        request: signed.body,
    })
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    marker: u8,
    request_digest: [u8; 32],
    identity: [u8; 32],
    target: Target,
    result: String,
}

pub(crate) fn receipt(
    incoming: &Incoming,
    result: &[u8],
    key: &RaftEffectSigningKey,
) -> Result<Vec<u8>> {
    ensure!(result.len() <= MAX_RESULT, CommandFailure::Overloaded);
    sign(
        Receipt {
            marker: 1,
            request_digest: incoming.digest,
            identity: incoming.lane,
            target: incoming.request.target.clone(),
            result: URL_SAFE_NO_PAD.encode(result),
        },
        key,
        RECEIPT_DOMAIN,
        MAX_RECEIPT,
    )
}

/// The host owns and bounds the returned future. An adapter must not detach
/// tasks. A lost response remains ambiguous and is retried with the same bytes.
pub trait RaftEffectSink: Send + Sync {
    fn deliver(
        &self,
        request: RaftDeliveryRequest,
    ) -> BoxFuture<'static, Result<Vec<u8>, CommandFailure>>;
}

/// Concrete in-process adapter for independently replicated native cells.
/// Endpoints are a bounded routing hint; only signed logical identity and the
/// receiver's ordinary command output gate supply the delivery contract.
pub struct RaftNativeEffectSink {
    endpoints: Vec<NativeEndpoint>,
    timeout: Duration,
    retry_window_ms: u64,
}

#[derive(Clone)]
struct NativeEndpoint {
    handle: Weak<crate::application::consensus::client::Handle>,
    scope: GroupScope,
}

impl RaftNativeEffectSink {
    pub fn new(
        endpoints: Vec<RaftCellHandle>,
        timeout: Duration,
        retry_window_ms: u64,
    ) -> Result<Arc<Self>> {
        ensure!(
            !endpoints.is_empty()
                && endpoints.len() <= 8
                && timeout.as_millis() > 0
                && timeout <= Duration::from_secs(60)
                && retry_window_ms > timeout.as_millis() as u64
                && retry_window_ms <= 600_000,
            "invalid native effect routing bounds"
        );
        Ok(Arc::new(Self {
            endpoints: endpoints
                .into_iter()
                .map(|endpoint| NativeEndpoint {
                    handle: Arc::downgrade(&endpoint.handle),
                    scope: endpoint.scope().clone(),
                })
                .collect(),
            timeout,
            retry_window_ms,
        }))
    }
}

impl RaftEffectSink for RaftNativeEffectSink {
    fn deliver(
        &self,
        request: RaftDeliveryRequest,
    ) -> BoxFuture<'static, Result<Vec<u8>, CommandFailure>> {
        let (endpoints, timeout, retry_window_ms) =
            (self.endpoints.clone(), self.timeout, self.retry_window_ms);
        Box::pin(async move {
            let now = u64::try_from(crate::asyncrt::wall_ms())
                .map_err(|_| CommandFailure::Unavailable)?;
            let retry_until = now
                .checked_add(retry_window_ms)
                .ok_or(CommandFailure::Unavailable)?;
            let mut last = CommandFailure::Unavailable;
            for hint in endpoints {
                let Some(handle) = hint.handle.upgrade() else {
                    continue;
                };
                let binding = match handle
                    .bindings
                    .get(&(hint.scope.namespace, hint.scope.cell))
                {
                    Ok(binding) => binding,
                    Err(CommandFailure::Unavailable) => continue,
                    Err(error) => return Err(error),
                };
                let endpoint = RaftCellHandle { handle, binding };
                if endpoint.scope() != request.inner.target.scope()
                    || endpoint.binding.class.definition_digest() != request.inner.target.definition
                {
                    continue;
                }
                let mut options = crate::application::consensus::CommandOptions::new(
                    request.inner.command,
                    retry_until,
                );
                options.timeout = timeout;
                options.durability = request.inner.target.durability;
                match endpoint
                    .command(
                        request.inner.target.contract.clone(),
                        request.inner.bytes.clone(),
                        options,
                    )
                    .await
                {
                    Ok(receipt) => {
                        request
                            .verify_receipt(&receipt.result)
                            .map_err(|_| CommandFailure::CommandConflict)?;
                        return Ok(receipt.result);
                    }
                    Err(
                        error @ (CommandFailure::NotLeader { .. }
                        | CommandFailure::Unavailable
                        | CommandFailure::Overloaded
                        | CommandFailure::Ambiguous { .. }
                        | CommandFailure::ArchiveUnavailable { .. }),
                    ) => last = error,
                    // A different endpoint cannot repair an identity, contract
                    // or expired-receipt refusal. Preserve that typed result.
                    Err(error) => return Err(error),
                }
            }
            Err(last)
        })
    }
}
