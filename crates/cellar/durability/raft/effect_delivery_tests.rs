// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::consensus::{ClusterId, GroupId, IncarnationId, NamespaceId};

fn scope(id: u8, class: &str) -> Result<GroupScope> {
    Ok(GroupScope {
        cluster: ClusterId::new([id; 16])?,
        namespace: NamespaceId::new([id + 1; 16])?,
        cell: format!("{class}:one"),
        group: GroupId::new([id + 2; 16])?,
        incarnation: IncarnationId::new([id + 3; 16])?,
    })
}

struct Fixture {
    origin: GroupScope,
    target: RaftEffectTarget,
    source: RaftEffectSigningKey,
    receiver: RaftEffectSigningKey,
    grants: Vec<RaftEffectSourceGrant>,
}

impl Fixture {
    fn new() -> Result<Self> {
        let origin = scope(1, "Source")?;
        let source = RaftEffectSigningKey::new([9; 32])?;
        let receiver = RaftEffectSigningKey::new([10; 32])?;
        let target = RaftEffectTarget::new(
            scope(11, "Receiver")?,
            [12; 32],
            OperationContract {
                operation: "Receive".into(),
                kind: OperationKind::Command,
                digest: [13; 32],
            },
            receiver.verification_key(),
            Durability::S3,
        )?;
        let grants = vec![RaftEffectSourceGrant::new(
            origin.cluster,
            origin.namespace,
            "Source",
            "Events",
            [14; 32],
            source.verification_key(),
        )?];
        Ok(Self {
            origin,
            target,
            source,
            receiver,
            grants,
        })
    }

    fn request(&self, sequence: u64, payload: &[u8]) -> Result<RaftDeliveryRequest> {
        RaftDeliveryRequest::new(
            DeliveryOrigin {
                scope: &self.origin,
                schema: [15; 32],
                binding: "Events",
                digest: [14; 32],
                sequence,
            },
            payload,
            self.target.clone(),
            &self.source,
        )
    }

    fn incoming(&self, bytes: &[u8]) -> Result<Incoming> {
        authenticate(
            bytes,
            &self.grants,
            &self.target.scope,
            self.target.definition,
            &self.target.contract,
            self.receiver.verification_key(),
            self.target.durability,
        )
    }
}

#[test]
fn immutable_wire_and_payload_conflict_identity() -> Result<()> {
    let f = Fixture::new()?;
    let first = f.request(1, b"first")?;
    let retry = f.request(1, b"first")?;
    let conflict = f.request(1, b"changed")?;
    ensure!(first.encoded() == retry.encoded());
    ensure!(first.command_id() == conflict.command_id() && first.encoded() != conflict.encoded());
    ensure!(f.incoming(first.encoded())?.payload == b"first");
    ensure!(
        restore_request(
            first.encoded(),
            DeliveryOrigin {
                scope: &f.origin,
                schema: [15; 32],
                binding: "Events",
                digest: [14; 32],
                sequence: 1
            },
            f.target.clone(),
            &f.source
        )?
        .encoded()
            == first.encoded()
    );
    let mut history = f.origin.clone();
    history.incarnation = IncarnationId::new([33; 16])?;
    ensure!(restore_request(
        first.encoded(),
        DeliveryOrigin {
            scope: &history,
            schema: [15; 32],
            binding: "Events",
            digest: [14; 32],
            sequence: 1
        },
        f.target.clone(),
        &f.source
    )
    .is_err());
    Ok(())
}

#[test]
fn receipt_binds_exact_request_destination_and_policy() -> Result<()> {
    let f = Fixture::new()?;
    let first = f.request(1, b"first")?;
    let incoming = f.incoming(first.encoded())?;
    let proof = receipt(&incoming, b"accepted", &f.receiver)?;
    first.verify_receipt(&proof)?;
    ensure!(f.request(2, b"first")?.verify_receipt(&proof).is_err());
    ensure!(f.request(1, b"changed")?.verify_receipt(&proof).is_err());
    let other = RaftEffectSigningKey::new([20; 32])?;
    ensure!(first
        .verify_receipt(&receipt(&incoming, b"accepted", &other)?)
        .is_err());
    let mut downgraded: Signed<Receipt> = parse(&proof, MAX_RECEIPT)?;
    downgraded.body.target.durability = 0;
    ensure!(first
        .verify_receipt(&sign(
            downgraded.body,
            &f.receiver,
            RECEIPT_DOMAIN,
            MAX_RECEIPT
        )?)
        .is_err());
    Ok(())
}

#[test]
fn request_requires_source_grant_and_exact_target() -> Result<()> {
    let mut f = Fixture::new()?;
    let first = f.request(1, b"first")?;
    f.grants[0].namespace = NamespaceId::new([50; 16])?;
    ensure!(f.incoming(first.encoded()).is_err());
    let f = Fixture::new()?;
    let mut other = f.target.clone();
    other.scope.incarnation = IncarnationId::new([51; 16])?;
    ensure!(authenticate(
        first.encoded(),
        &f.grants,
        &other.scope,
        other.definition,
        &other.contract,
        f.receiver.verification_key(),
        other.durability
    )
    .is_err());
    let mut signed: Signed<Request> = parse(first.encoded(), MAX_REQUEST)?;
    signed.body.sender = RaftEffectSigningKey::new([52; 32])?
        .verification_key()
        .public_bytes();
    ensure!(f
        .incoming(&sign(signed.body, &f.source, REQUEST_DOMAIN, MAX_REQUEST)?)
        .is_err());
    Ok(())
}

#[test]
fn canonical_and_semantic_wire_validation() -> Result<()> {
    let f = Fixture::new()?;
    let first = f.request(1, b"first")?;
    let mut spaced = first.encoded().to_vec();
    spaced.push(b' ');
    ensure!(f.incoming(&spaced).is_err());
    for alteration in 0..5 {
        let mut signed: Signed<Request> = parse(first.encoded(), MAX_REQUEST)?;
        match alteration {
            0 => signed.body.sequence = "01".into(),
            1 => signed.body.marker = 2,
            2 => signed.body.payload_digest = [0; 32],
            3 => signed.body.payload.push('='),
            _ => signed.body.origin_schema = [0; 32],
        }
        ensure!(f
            .incoming(&sign(signed.body, &f.source, REQUEST_DOMAIN, MAX_REQUEST)?)
            .is_err());
    }
    let mut json: serde_json::Value = serde_json::from_slice(first.encoded())?;
    json["body"]["unexpected"] = true.into();
    ensure!(f.incoming(&serde_json::to_vec(&json)?).is_err());
    let duplicate = String::from_utf8(first.encoded().to_vec())?
        .replace("\"marker\":1,", "\"marker\":1,\"marker\":1,");
    ensure!(f.incoming(duplicate.as_bytes()).is_err());
    Ok(())
}

#[test]
fn delivery_bounds_and_secret_debug() -> Result<()> {
    let f = Fixture::new()?;
    ensure!(f.request(1, &vec![1; MAX_PAYLOAD])?.encoded().len() < MAX_REQUEST);
    ensure!(f.request(1, &vec![1; MAX_PAYLOAD + 1]).is_err());
    ensure!(f.incoming(&vec![0; MAX_REQUEST + 1]).is_err());
    let incoming = f.incoming(f.request(1, b"first")?.encoded())?;
    ensure!(receipt(&incoming, &vec![1; MAX_RESULT + 1], &f.receiver).is_err());
    ensure!(format!("{:?}", f.source) == "RaftEffectSigningKey([redacted])");
    ensure!(RaftEffectVerificationKey::new([0; 32]).is_err());
    ensure!(reserved_client(f.request(1, b"first")?.command_id().client));
    Ok(())
}
