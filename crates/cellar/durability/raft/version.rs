//! Bounded, authenticated history observations. Tokens confer no authorization
//! and contain no leader identity: a normal election preserves their meaning.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
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
        // No future-clock allowance is needed for correctness. A clock skew or
        // rollback can refuse a token, but cannot grant a read an older cut.
        if issued > now_ms
            || now_ms >= expires
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
