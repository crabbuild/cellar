// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The surrounding host signs only after actual Node/domain/engine joins.
//! These receipts establish physical closure for one boot, not archive health,
//! application completion, terminal removal or permission to delete a volume.
use super::records::StopSelector;
use crate::bucket::Bucket;
use crate::node_lifecycle::NodeOutcome;
use crate::raft::catalog_wire::{decode, encode};
use crate::raft::fleet_catalog::FleetCatalogError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

type Result<T> = std::result::Result<T, FleetCatalogError>;

const KIND: u8 = 59;
const LIMIT: usize = 4096;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Body {
    format: u8,
    selector: StopSelector,
    groups: u64,
    omitted: u64,
    archive_lag: u64,
    unknown_archive: u64,
}

impl Body {
    fn validate(&self) -> Result<()> {
        self.selector
            .validate()
            .map_err(|_| FleetCatalogError::Corrupt("closure selector"))?;
        if self.format != 1
            || self.groups > 4096
            || self.omitted > self.groups
            || self.archive_lag > self.groups
            || self.unknown_archive > self.groups
        {
            return Err(FleetCatalogError::Corrupt("closure counts"));
        }
        Ok(())
    }

    fn digest(&self) -> Result<[u8; 32]> {
        self.validate()?;
        Ok(Sha256::digest(encode(KIND, self, LIMIT)?).into())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    body: Body,
    signature: String,
}

/// Signature-verified physical Raft/Node-domain closure for one exact stop.
/// It grants no terminal node transition, slot release or deletion authority.
pub struct VerifiedFleetClosure {
    body: Body,
    digest: [u8; 32],
}

impl VerifiedFleetClosure {
    pub fn operation(&self) -> [u8; 16] {
        self.body.selector.operation
    }

    pub fn request(&self) -> [u8; 16] {
        self.body.selector.request
    }

    pub fn process(&self) -> [u8; 16] {
        self.body.selector.boot.process
    }

    pub fn node(&self) -> [u8; 16] {
        self.body.selector.boot.node
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Clone)]
pub(crate) struct Sink {
    pub bucket: Bucket,
    pub key: String,
}

impl Sink {
    pub async fn verify(&self, expected: &StopSelector) -> Result<VerifiedFleetClosure> {
        let Some((bytes, _)) = self
            .bucket
            .get_bounded(&self.key, LIMIT)
            .await
            .map_err(FleetCatalogError::Storage)?
        else {
            return Err(FleetCatalogError::Unconfirmed);
        };
        let record: Record = decode(KIND, &bytes, LIMIT)?;
        if record.body.selector != *expected || record.signature.len() != 128 {
            return Err(FleetCatalogError::Corrupt("closure scope/signature"));
        }
        let signature: [u8; 64] = hex::decode(&record.signature)
            .map_err(|_| FleetCatalogError::Corrupt("closure signature encoding"))?
            .try_into()
            .map_err(|_| FleetCatalogError::Corrupt("closure signature length"))?;
        if hex::encode(signature) != record.signature {
            return Err(FleetCatalogError::Corrupt("noncanonical closure signature"));
        }
        let digest = record.body.digest()?;
        let mut payload = b"cellar-fleet-physical-closure-1\0".to_vec();
        payload.extend_from_slice(&digest);
        let key = ed25519_dalek::VerifyingKey::from_bytes(&expected.boot.join_key)
            .map_err(|_| FleetCatalogError::Corrupt("closure public key"))?;
        key.verify_strict(&payload, &ed25519_dalek::Signature::from_bytes(&signature))
            .map_err(|_| FleetCatalogError::Corrupt("closure signature"))?;
        Ok(VerifiedFleetClosure {
            body: record.body,
            digest,
        })
    }

    /// Only the outer host calls after joining the Node's complete domain.
    pub async fn joined(
        &self,
        selector: StopSelector,
        signer: &crate::peer_probe::PeerProbeSigner,
        outcome: &anyhow::Result<NodeOutcome>,
    ) -> Result<()> {
        let report = match outcome {
            Ok(NodeOutcome::Stopped(report)) => report,
            _ => return Err(FleetCatalogError::Unconfirmed),
        };
        let raft = report.raft.as_ref().ok_or(FleetCatalogError::Unconfirmed)?;
        let closed = raft
            .result
            .as_ref()
            .map_err(|_| FleetCatalogError::Unconfirmed)?;
        if raft.startup_incomplete
            || !closed.unfinished.is_empty()
            || closed.node.node.bytes() != selector.boot.node
            || closed.node.disk.bytes() != selector.boot.disk
            || closed.node.process.bytes() != selector.boot.process
            || closed.storage_instance.bytes() != selector.boot.engine
            || signer.public_key_bytes() != selector.boot.join_key
        {
            return Err(FleetCatalogError::Unconfirmed);
        }
        let body = Body {
            format: 1,
            selector,
            groups: closed.groups_total as u64,
            omitted: closed.omitted_groups as u64,
            archive_lag: closed.groups_with_archive_lag as u64,
            unknown_archive: closed.groups_with_unknown_archive as u64,
        };
        let signature = hex::encode(signer.fleet_closure_signature(body.digest()?));
        let bytes = encode(KIND, &Record { body, signature }, LIMIT)?;
        let submitted = self.bucket.put_cas(&self.key, bytes.clone(), None).await;
        let observed = self
            .bucket
            .get_bounded(&self.key, LIMIT)
            .await
            .map_err(FleetCatalogError::Storage)?;
        match observed {
            Some((found, _)) if found.as_ref() == bytes.as_slice() => Ok(()),
            Some(_) => Err(FleetCatalogError::PayloadConflict),
            None => match submitted {
                Err(error) => Err(FleetCatalogError::Storage(error)),
                _ => Err(FleetCatalogError::Unconfirmed),
            },
        }
    }
}
