// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded, authenticated history observations. Tokens confer no authorization
//! and contain no leader identity: a normal election preserves their meaning.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use cellar_logic::cell_group::{
    CellHistory, CellId, CellImage, CellImageCommitment, CellVersion, CommitWitness, GroupHistory,
    MutationSequence,
};
use cellar_logic::consensus::*;
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fmt, sync::Arc, time::Duration};

const MAGIC: &[u8; 8] = b"CLRVRS01";
const DOMAIN: &[u8] = b"cellar-raft-version-1";
const MAX_BYTES: usize = 1408;
const MAX_ENCODED: usize = 1878;
const MAX_KEYS: usize = 64;
const MAX_LIFETIME_MS: u64 = 7 * 24 * 60 * 60 * 1000;
const CELL_MAGIC: &[u8; 8] = b"CLRCVS01";
const CELL_DOMAIN: &[u8] = b"cellar-cell-group-version-1";

/// Largest issuer wall-clock lead accepted for a signed version's start time.
///
/// A token observes an already committed version and confers no authority.
/// Accepting it before its issuer's timestamp cannot grant an older cut:
/// callers still enforce the observed version as a floor, and expiry, key
/// retention and the maximum lifetime remain exact. Without an allowance, a
/// verifier clock stepping backwards, or a leader clock leading an ingress node
/// by more than the one-way RPC latency, refuses valid replies and turns
/// committed remote writes into ambiguous outcomes.
pub const MAX_ISSUER_CLOCK_LEAD_MS: u64 = 500;

/// Signed validity check shared by group- and cell-scoped tokens: the start
/// tolerates [`MAX_ISSUER_CLOCK_LEAD_MS`]; the end is never extended. An empty
/// or inverted interval always refuses, so callers may subtract its bounds.
fn within_signed_validity(issued_ms: u64, expires_ms: u64, now_ms: u64) -> bool {
    issued_ms < expires_ms
        && now_ms < expires_ms
        && now_ms.saturating_add(MAX_ISSUER_CLOCK_LEAD_MS) >= issued_ms
}

// Framing/key/time, cell identity, sequence/image, schema, witness and signature.
const CELL_FIXED_BYTES: usize = 8 + 32 + 16 + 48 + 2 + 8 + 24 + 37 + 32 + 64 + 16 + 16 + 64;
const CELL_MAX_BYTES: usize = CELL_FIXED_BYTES + cellar_logic::cell_group::MAX_CELL_ID_BYTES;
const CELL_MAX_ENCODED: usize = CELL_MAX_BYTES.div_ceil(3) * 4;

/// Cell-scoped token for the group runtime. Old group-scoped tokens cannot be
/// interpreted in this domain. The original witness is provenance, not routing.
#[derive(Clone, PartialEq, Eq)]
pub struct CellVersionToken(String);

impl CellVersionToken {
    pub fn from_encoded(encoded: &str) -> Result<Self, VersionError> {
        if encoded.len() > CELL_MAX_ENCODED {
            return Err(VersionError::Limit);
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| VersionError::Invalid)?;
        if bytes.len() < CELL_FIXED_BYTES + 1
            || bytes.len() > CELL_MAX_BYTES
            || !bytes.starts_with(CELL_MAGIC)
            || URL_SAFE_NO_PAD.encode(&bytes) != encoded
        {
            return Err(VersionError::Invalid);
        }
        Ok(Self(encoded.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for CellVersionToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CellVersionToken")
            .field("bytes", &self.0.len())
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellVersionObservation {
    pub version: CellVersion,
    pub witness: CommitWitness,
}

/// A version already authenticated against the request's history and schema.
/// Its signed time bounds remain enforceable after handing off the result.
pub(in crate::raft) struct AuthenticatedCellVersion {
    observation: CellVersionObservation,
    validity: std::ops::Range<u64>,
}

impl AuthenticatedCellVersion {
    pub(in crate::raft) fn observation(&self) -> &CellVersionObservation {
        &self.observation
    }

    pub(in crate::raft) fn check_time(&self, now_ms: u64) -> Result<(), VersionError> {
        check_cell_version_time(&self.validity, now_ms, "caller")
    }
}

fn check_cell_version_time(
    validity: &std::ops::Range<u64>,
    now_ms: u64,
    boundary: &'static str,
) -> Result<(), VersionError> {
    if within_signed_validity(validity.start, validity.end, now_ms) {
        return Ok(());
    }
    tracing::debug!(
        target: "cellar::raft::version_clock",
        boundary,
        now_ms,
        issued_ms = validity.start,
        expires_ms = validity.end,
        "signed cell version outside its validity interval"
    );
    Err(VersionError::Expired)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VersionError {
    Invalid,
    Expired,
    WrongHistory,
    WrongSchema,
    KeyRetention,
    Limit,
}

impl fmt::Display for VersionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Raft version token: {self:?}")
    }
}

impl std::error::Error for VersionError {}

/// Opaque wire token. Parsing is bounded but does not authenticate it. Only the
/// receiving server's configured verification keys establish a usable version.
#[derive(Clone, PartialEq, Eq)]
pub struct VersionToken(String);

impl VersionToken {
    pub fn from_encoded(encoded: &str) -> Result<Self, VersionError> {
        if encoded.len() > MAX_ENCODED {
            return Err(VersionError::Limit);
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| VersionError::Invalid)?;
        if bytes.len() > MAX_BYTES
            || bytes.len() < 235
            || !bytes.starts_with(MAGIC)
            || URL_SAFE_NO_PAD.encode(&bytes) != encoded
        {
            return Err(VersionError::Invalid);
        }
        Ok(Self(encoded.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for VersionToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VersionToken")
            .field("bytes", &self.0.len())
            .finish()
    }
}

impl fmt::Display for VersionToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A trusted public key and its final verification deadline, distributed in
/// server configuration. The key ID binds its public bytes and is never taken
/// from an untrusted token as a trust root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionVerificationKey {
    public: [u8; 32],
    verify_until_ms: u64,
}

impl VersionVerificationKey {
    pub fn new(public: [u8; 32], verify_until_ms: u64) -> Result<Self, VersionError> {
        let key = VerifyingKey::from_bytes(&public).map_err(|_| VersionError::Invalid)?;
        if key.is_weak() || verify_until_ms == 0 {
            return Err(VersionError::Invalid);
        }
        Ok(Self {
            public,
            verify_until_ms,
        })
    }

    pub fn id(&self) -> [u8; 32] {
        Sha256::digest(self.public).into()
    }

    pub fn public_bytes(&self) -> [u8; 32] {
        self.public
    }

    pub fn verify_until_ms(&self) -> u64 {
        self.verify_until_ms
    }
}

/// Signing material is injected by the owner of the deployment. Never expose
/// it in Debug, persist it in the object-store catalog or derive it from a
/// process identity that changes on restart.
pub struct VersionSigningKey {
    secret: SigningKey,
    verifier: VersionVerificationKey,
}

impl VersionSigningKey {
    pub fn new(mut secret: [u8; 32], verify_until_ms: u64) -> Result<Self, VersionError> {
        if secret == [0; 32] {
            return Err(VersionError::Invalid);
        }
        let key = SigningKey::from_bytes(&secret);
        secret.fill(0);
        let verifier =
            VersionVerificationKey::new(key.verifying_key().to_bytes(), verify_until_ms)?;
        Ok(Self {
            secret: key,
            verifier,
        })
    }

    pub fn verification_key(&self) -> VersionVerificationKey {
        self.verifier.clone()
    }
}

impl fmt::Debug for VersionSigningKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VersionSigningKey")
            .field("verifier", &self.verifier)
            .finish_non_exhaustive()
    }
}

struct Keys {
    active: VersionSigningKey,
    verification: BTreeMap<[u8; 32], VersionVerificationKey>,
    lifetime_ms: u64,
}

/// Immutable key configuration. Rotate by constructing a successor with the
/// old verification keys retained, distributing it to all replicas, and then
/// activating the new signer. Namespace startup checks durable retention
/// metadata so a restart cannot silently remove an unexpired verifier.
#[derive(Clone)]
pub struct VersionKeys(Arc<Keys>);

impl fmt::Debug for VersionKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VersionKeys")
            .field("active", &self.0.active.verifier.id())
            .field("verification_keys", &self.0.verification.len())
            .field("lifetime_ms", &self.0.lifetime_ms)
            .finish()
    }
}

impl VersionKeys {
    pub fn issue_cell(
        &self,
        observation: &CellVersionObservation,
        schema: [u8; 32],
        now_ms: u64,
    ) -> Result<CellVersionToken, VersionError> {
        let version = &observation.version;
        let witness = &observation.witness;
        witness
            .validate(&version.history)
            .map_err(|_| VersionError::Invalid)?;
        crate::application::consensus::LtxImage::try_from(version.image)
            .map_err(|_| VersionError::Invalid)?;
        self.validate_retention(&[], now_ms)?;
        let mut bytes = Vec::with_capacity(CELL_FIXED_BYTES + version.history.cell.as_str().len());
        bytes.extend_from_slice(CELL_MAGIC);
        bytes.extend_from_slice(&self.0.active.verifier.id());
        bytes.extend_from_slice(&now_ms.to_be_bytes());
        bytes.extend_from_slice(&(now_ms + self.0.lifetime_ms).to_be_bytes());
        for id in [
            version.history.cluster.bytes(),
            version.history.namespace.bytes(),
            version.history.incarnation.bytes(),
        ] {
            bytes.extend_from_slice(&id);
        }
        let cell = version.history.cell.as_str().as_bytes();
        bytes.extend_from_slice(&(cell.len() as u16).to_be_bytes());
        bytes.extend_from_slice(cell);
        bytes.extend_from_slice(&version.sequence.get().to_be_bytes());
        for value in [
            version.image.epoch,
            version.image.txid,
            version.image.checksum,
        ] {
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        let (tag, digest, length) = match version.image.commitment {
            CellImageCommitment::Materialized(digest) => (1, digest, 0),
            CellImageCommitment::Extents {
                root,
                manifest_bytes,
            } => (2, root, manifest_bytes),
        };
        bytes.push(tag);
        bytes.extend_from_slice(&digest);
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes.extend_from_slice(&schema);
        for id in [
            witness.group.cluster.bytes(),
            witness.group.namespace.bytes(),
            witness.group.group.bytes(),
            witness.group.incarnation.bytes(),
        ] {
            bytes.extend_from_slice(&id);
        }
        bytes.extend_from_slice(&witness.position.index.get().to_be_bytes());
        bytes.extend_from_slice(&witness.position.term.get().to_be_bytes());
        bytes.extend_from_slice(&witness.proposal.bytes());
        let signature = self.0.active.secret.sign(&[CELL_DOMAIN, &bytes].concat());
        bytes.extend_from_slice(&signature.to_bytes());
        CellVersionToken::from_encoded(&URL_SAFE_NO_PAD.encode(bytes))
    }

    /// Authenticate against logical history. The caller must separately check
    /// installed transfer provenance and current quorum/read authority.
    pub fn verify_cell(
        &self,
        token: &CellVersionToken,
        history: &CellHistory,
        schema: [u8; 32],
        now_ms: u64,
    ) -> Result<CellVersionObservation, VersionError> {
        self.authenticate_cell(token, history, schema, now_ms)
            .map(|proof| proof.observation)
    }

    pub(in crate::raft) fn authenticate_cell(
        &self,
        token: &CellVersionToken,
        history: &CellHistory,
        schema: [u8; 32],
        now_ms: u64,
    ) -> Result<AuthenticatedCellVersion, VersionError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(token.as_str())
            .map_err(|_| VersionError::Invalid)?;
        let (payload, signature) =
            bytes.split_at(bytes.len().checked_sub(64).ok_or(VersionError::Invalid)?);
        let mut input = Input(payload);
        if input.take(8)? != CELL_MAGIC {
            return Err(VersionError::Invalid);
        }
        let key = self
            .0
            .verification
            .get(&input.array()?)
            .ok_or(VersionError::Invalid)?;
        VerifyingKey::from_bytes(&key.public)
            .map_err(|_| VersionError::Invalid)?
            .verify_strict(
                &[CELL_DOMAIN, payload].concat(),
                &Signature::from_slice(signature).map_err(|_| VersionError::Invalid)?,
            )
            .map_err(|_| VersionError::Invalid)?;
        let validity = input.u64()?..input.u64()?;
        check_cell_version_time(&validity, now_ms, "authentication")?;
        if validity.end > key.verify_until_ms || validity.end - validity.start > MAX_LIFETIME_MS {
            tracing::debug!(
                target: "cellar::raft::version_clock",
                expires_ms = validity.end,
                issued_ms = validity.start,
                verify_until_ms = key.verify_until_ms,
                "signed cell version exceeds key retention or maximum lifetime"
            );
            return Err(VersionError::Expired);
        }
        let cluster = ClusterId::new(input.array()?).map_err(|_| VersionError::Invalid)?;
        let namespace = NamespaceId::new(input.array()?).map_err(|_| VersionError::Invalid)?;
        let incarnation = IncarnationId::new(input.array()?).map_err(|_| VersionError::Invalid)?;
        let n = u16::from_be_bytes(input.array()?) as usize;
        if n > cellar_logic::cell_group::MAX_CELL_ID_BYTES {
            return Err(VersionError::Limit);
        }
        let cell =
            CellId::new(std::str::from_utf8(input.take(n)?).map_err(|_| VersionError::Invalid)?)
                .map_err(|_| VersionError::Invalid)?;
        let decoded = CellHistory {
            cluster,
            namespace,
            cell,
            incarnation,
        };
        if &decoded != history {
            return Err(VersionError::WrongHistory);
        }
        let sequence = MutationSequence::new(input.u64()?).map_err(|_| VersionError::Invalid)?;
        let epoch = input.u64()?;
        let txid = input.u64()?;
        let checksum = input.u64()?;
        let tag = input.array::<1>()?[0];
        let root = input.array()?;
        let manifest_bytes = u32::from_be_bytes(input.array()?);
        let commitment = match (tag, manifest_bytes) {
            (1, 0) => CellImageCommitment::Materialized(root),
            (2, _) => CellImageCommitment::Extents {
                root,
                manifest_bytes,
            },
            _ => return Err(VersionError::Invalid),
        };
        let image = CellImage {
            epoch,
            txid,
            checksum,
            commitment,
        };
        crate::application::consensus::LtxImage::try_from(image)
            .map_err(|_| VersionError::Invalid)?;
        if input.array::<32>()? != schema {
            return Err(VersionError::WrongSchema);
        }
        let group = GroupHistory {
            cluster: ClusterId::new(input.array()?).map_err(|_| VersionError::Invalid)?,
            namespace: NamespaceId::new(input.array()?).map_err(|_| VersionError::Invalid)?,
            group: GroupId::new(input.array()?).map_err(|_| VersionError::Invalid)?,
            incarnation: IncarnationId::new(input.array()?).map_err(|_| VersionError::Invalid)?,
        };
        let position = LogPosition {
            index: LogIndex::new(input.u64()?),
            term: Term::new(input.u64()?),
        };
        let proposal = ProposalId::new(input.array()?).map_err(|_| VersionError::Invalid)?;
        let witness = CommitWitness {
            group,
            position,
            proposal,
        };
        witness
            .validate(&decoded)
            .map_err(|_| VersionError::Invalid)?;
        if !input.0.is_empty() {
            return Err(VersionError::Invalid);
        }
        Ok(AuthenticatedCellVersion {
            observation: CellVersionObservation {
                version: CellVersion {
                    history: decoded,
                    sequence,
                    image,
                },
                witness,
            },
            validity,
        })
    }

    pub fn new(
        active: VersionSigningKey,
        retained: Vec<VersionVerificationKey>,
        lifetime: Duration,
    ) -> Result<Self, VersionError> {
        let lifetime_ms = u64::try_from(lifetime.as_millis()).map_err(|_| VersionError::Limit)?;
        if lifetime_ms == 0 || lifetime_ms > MAX_LIFETIME_MS || retained.len() >= MAX_KEYS {
            return Err(VersionError::Limit);
        }
        let mut verification = BTreeMap::new();
        for key in retained.into_iter().chain([active.verification_key()]) {
            if let Some(old) = verification.insert(key.id(), key.clone()) {
                if old != key {
                    return Err(VersionError::KeyRetention);
                }
            }
        }
        Ok(Self(Arc::new(Keys {
            active,
            verification,
            lifetime_ms,
        })))
    }

    pub fn verification_keys(&self) -> Vec<VersionVerificationKey> {
        self.0.verification.values().cloned().collect()
    }

    pub(crate) fn validate_retention(
        &self,
        previous: &[VersionVerificationKey],
        now_ms: u64,
    ) -> Result<(), VersionError> {
        if now_ms
            .checked_add(self.0.lifetime_ms)
            .is_none_or(|until| until > self.0.active.verifier.verify_until_ms)
        {
            return Err(VersionError::Expired);
        }
        for key in previous {
            if key.verify_until_ms > now_ms && self.0.verification.get(&key.id()) != Some(key) {
                return Err(VersionError::KeyRetention);
            }
        }
        Ok(())
    }

    pub fn issue(
        &self,
        version: &CommittedVersion,
        schema: [u8; 32],
        now_ms: u64,
    ) -> Result<VersionToken, VersionError> {
        version
            .scope
            .validate()
            .map_err(|_| VersionError::Invalid)?;
        version
            .position
            .validate()
            .map_err(|_| VersionError::Invalid)?;
        if version.position.index.get() == 0 {
            return Err(VersionError::Invalid);
        }
        self.validate_retention(&[], now_ms)?;
        let mut bytes = Vec::with_capacity(512);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&self.0.active.verifier.id());
        bytes.extend_from_slice(&now_ms.to_be_bytes());
        bytes.extend_from_slice(&(now_ms + self.0.lifetime_ms).to_be_bytes());
        for id in [
            version.scope.cluster.bytes(),
            version.scope.namespace.bytes(),
            version.scope.group.bytes(),
            version.scope.incarnation.bytes(),
        ] {
            bytes.extend_from_slice(&id);
        }
        bytes.extend_from_slice(&(version.scope.cell.len() as u16).to_be_bytes());
        bytes.extend_from_slice(version.scope.cell.as_bytes());
        bytes.extend_from_slice(&schema);
        bytes.extend_from_slice(&version.position.index.get().to_be_bytes());
        bytes.extend_from_slice(&version.position.term.get().to_be_bytes());
        let signature = self.0.active.secret.sign(&[DOMAIN, &bytes].concat());
        bytes.extend_from_slice(&signature.to_bytes());
        VersionToken::from_encoded(&URL_SAFE_NO_PAD.encode(bytes))
    }

    pub fn verify(
        &self,
        token: &VersionToken,
        scope: &GroupScope,
        schema: [u8; 32],
        now_ms: u64,
    ) -> Result<CommittedVersion, VersionError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(token.as_str())
            .map_err(|_| VersionError::Invalid)?;
        let (payload, signature) =
            bytes.split_at(bytes.len().checked_sub(64).ok_or(VersionError::Invalid)?);
        let mut input = Input(payload);
        if input.take(8)? != MAGIC {
            return Err(VersionError::Invalid);
        }
        let id = input.array()?;
        let key = self.0.verification.get(&id).ok_or(VersionError::Invalid)?;
        VerifyingKey::from_bytes(&key.public)
            .map_err(|_| VersionError::Invalid)?
            .verify_strict(
                &[DOMAIN, payload].concat(),
                &Signature::from_slice(signature).map_err(|_| VersionError::Invalid)?,
            )
            .map_err(|_| VersionError::Invalid)?;
        let issued = input.u64()?;
        let expires = input.u64()?;
        // A bounded issuer clock lead preserves availability across ordinary
        // skew. Skew or rollback beyond it refuses; neither case can grant a
        // read an older cut, and expiry is never extended.
        if !within_signed_validity(issued, expires, now_ms)
            || expires > key.verify_until_ms
            || expires <= issued
            || expires - issued > MAX_LIFETIME_MS
        {
            return Err(VersionError::Expired);
        }
        let decoded = GroupScope {
            cluster: ClusterId::new(input.array()?).map_err(|_| VersionError::Invalid)?,
            namespace: NamespaceId::new(input.array()?).map_err(|_| VersionError::Invalid)?,
            group: GroupId::new(input.array()?).map_err(|_| VersionError::Invalid)?,
            incarnation: IncarnationId::new(input.array()?).map_err(|_| VersionError::Invalid)?,
            cell: {
                let n = u16::from_be_bytes(input.array()?) as usize;
                if n > 1024 {
                    return Err(VersionError::Limit);
                }
                std::str::from_utf8(input.take(n)?)
                    .map_err(|_| VersionError::Invalid)?
                    .into()
            },
        };
        decoded.validate().map_err(|_| VersionError::Invalid)?;
        if decoded != *scope {
            return Err(VersionError::WrongHistory);
        }
        if input.array::<32>()? != schema {
            return Err(VersionError::WrongSchema);
        }
        let position = LogPosition {
            index: LogIndex::new(input.u64()?),
            term: Term::new(input.u64()?),
        };
        position.validate().map_err(|_| VersionError::Invalid)?;
        if !input.0.is_empty() || position.index.get() == 0 {
            return Err(VersionError::Invalid);
        }
        Ok(CommittedVersion {
            scope: decoded,
            position,
        })
    }
}

pub(crate) struct Input<'a>(pub &'a [u8]);

impl<'a> Input<'a> {
    pub fn take(&mut self, n: usize) -> Result<&'a [u8], VersionError> {
        let (value, rest) = self.0.split_at_checked(n).ok_or(VersionError::Invalid)?;
        self.0 = rest;
        Ok(value)
    }

    pub fn array<const N: usize>(&mut self) -> Result<[u8; N], VersionError> {
        self.take(N)?.try_into().map_err(|_| VersionError::Invalid)
    }

    pub fn u64(&mut self) -> Result<u64, VersionError> {
        Ok(u64::from_be_bytes(self.array()?))
    }
}
