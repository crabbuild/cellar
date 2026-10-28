// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(clippy::disallowed_methods)]
use super::*;
use crate::application::consensus::{protocol::WireScope, EnvelopeLimits};
use crate::raft::{archive::ArchiveReference, manifest, member_state::GroupMembership};
use anyhow::{ensure, Result};
use cellar_logic::consensus::*;
use cellar_ltx::{apply::ImageState, stream_snapshot, DirectFileSystem, TXID};
use raft::prelude::{ConfState, Entry};

struct Fixture {
    set: MaterializedSet,
    _root: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Result<Self> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("input.sqlite");
        let db = rusqlite::Connection::open(&path)?;
        db.pragma_update(None, "page_size", 4096)?;
        db.execute_batch("CREATE TABLE counter(value); INSERT INTO counter VALUES(7)")?;
        drop(db);
        let bytes = std::fs::read(path)?;
        let state = ImageState {
            epoch: 1,
            txid: TXID(1),
            checksum: 0,
            page_size: 4096,
            length: bytes.len() as u64,
            digest: Sha256::digest(&bytes).into(),
        };
        let snapshot = stream_snapshot::encode_admitted_in(
            &DirectFileSystem,
            root.path(),
            state,
            stream_snapshot::Limits::default(),
            &stream_snapshot::ScratchPool::default(),
            |offset, length| Ok(bytes[offset as usize..offset as usize + length].to_vec()),
        )?;
        let scope = GroupScope {
            cluster: ClusterId::new([1; 16])?,
            namespace: NamespaceId::new([2; 16])?,
            cell: "Counter:proof".into(),
            group: GroupId::new([3; 16])?,
            incarnation: IncarnationId::new([4; 16])?,
        };
        let conf = ConfState {
            voters: vec![1],
            ..Default::default()
        };
        let conf_state = manifest::encode_conf(&conf)?;
        let membership = GroupMembership::initial(&conf);
        let position = manifest::Position { index: 1, term: 1 };
        let manifest = manifest::Manifest {
            scope: WireScope::from(&scope),
            root_digest: [5; 32],
            birth_digest: [6; 32],
            schema_digest: [7; 32],
            snapshot: manifest::Snapshot {
                position,
                image: state.into(),
                conf_state: conf_state.clone(),
                membership: membership.clone(),
                object: manifest::ObjectRef::new(manifest::ObjectKind::Snapshot, b"index", None),
            },
            tails: vec![],
            position,
            image: state.into(),
            conf_state,
            membership,
            dependencies: vec![],
        };
        let mut set = MaterializedSet {
            validated: None,
            activity: None,
            _retention: None,
            reference: ArchiveReference(manifest::ObjectRef::new(
                manifest::ObjectKind::Manifest,
                b"manifest",
                None,
            )),
            scope,
            position: position.decode()?,
            manifest,
            snapshot,
            entries: vec![],
            dependencies: vec![],
            envelope: EnvelopeLimits::default(),
            max_image_bytes: 64 * 1024 * 1024,
        };
        set.validate_payloads()?;
        Ok(Self { set, _root: root })
    }

    fn checked(&self) -> Result<Key, ArchiveError> {
        self.set
            .validated
            .as_ref()
            .ok_or(ArchiveError::Invalid(
                "archive replay input is not validated",
            ))?
            .checked_key(&self.set)
    }
}

#[test]
fn current_metadata_requires_its_own_validation_even_when_sql_is_unchanged() -> Result<()> {
    let mut fixture = Fixture::new()?;
    let original = fixture.checked()?;
    fixture.set.position.index = LogIndex::new(2);
    fixture.set.manifest.position.index = 2;
    fixture.set.entries.push(Entry {
        index: 2,
        term: 1,
        ..Default::default()
    });
    ensure!(
        fixture.checked().is_err(),
        "stale validation accepted a new cut"
    );
    fixture.set.validate_payloads()?;
    ensure!(
        fixture.checked()? == original,
        "protocol-only cut changed SQL projection"
    );
    fixture.set.entries[0].context = b"new context".to_vec().into();
    ensure!(
        fixture.checked().is_err(),
        "metadata-only entry mutation retained validation"
    );
    fixture.set.validate_payloads()?;
    ensure!(fixture.checked()? == original);
    fixture.set.manifest.conf_state.push(255);
    ensure!(
        fixture.checked().is_err(),
        "configuration mutation retained validation"
    );
    ensure!(fixture.set.validate_payloads().is_err());
    ensure!(
        fixture.set.validated.is_none(),
        "failed validation retained its old record"
    );
    Ok(())
}

#[test]
fn physical_identity_binds_history_policy_image_and_ordered_data() -> Result<()> {
    for field in 0..12 {
        let mut f = Fixture::new()?;
        let original = Key::new(&f.set)?;
        match field {
            0 => f.set.scope.cell.push('x'),
            1 => f.set.scope.incarnation = IncarnationId::new([99; 16])?,
            2 => f.set.manifest.root_digest = [99; 32],
            3 => f.set.manifest.birth_digest = [99; 32],
            4 => f.set.manifest.schema_digest = [99; 32],
            5 => f.set.manifest.image.digest = [99; 32],
            6 => f.set.manifest.snapshot.image.epoch += 1,
            7 => f.set.envelope.max_files += 1,
            8 => f.set.max_image_bytes += 4096,
            _ => f.set.entries.push(Entry {
                index: field,
                term: 1,
                data: vec![1, 2, 3].into(),
                ..Default::default()
            }),
        }
        ensure!(
            Key::new(&f.set)? != original,
            "changed field {field} reused projection"
        );
        ensure!(
            f.checked().is_err(),
            "changed field {field} retained validation"
        );
    }
    Ok(())
}

#[test]
fn framed_entry_hashes_do_not_alias_context_and_payload_boundaries() -> Result<()> {
    let mut f = Fixture::new()?;
    f.set.entries.push(Entry {
        index: 2,
        term: 1,
        context: b"a".to_vec().into(),
        data: b"bc".to_vec().into(),
        ..Default::default()
    });
    let first = Key::new(&f.set)?;
    f.set.entries[0].context = b"ab".to_vec().into();
    f.set.entries[0].data = b"c".to_vec().into();
    ensure!(Key::new(&f.set)? != first);
    let second = Key::new(&f.set)?;
    f.set.entries[0].index += 1;
    ensure!(Key::new(&f.set)? != second);
    let third = Key::new(&f.set)?;
    f.set.entries[0].term += 1;
    ensure!(Key::new(&f.set)? != third);
    Ok(())
}

#[test]
fn dependency_records_bind_actual_bytes_and_exact_metadata() -> Result<()> {
    let mut fixture = Fixture::new()?;
    let object = manifest::ObjectRef::new(manifest::ObjectKind::Dependency, b"payload", None);
    fixture.set.manifest.dependencies.push(object.clone());
    fixture.set.dependencies.push((object, b"payload".to_vec()));
    fixture.set.validate_payloads()?;
    fixture.checked()?;
    fixture.set.dependencies[0].1[0] ^= 1;
    ensure!(
        fixture.checked().is_err(),
        "changed dependency bytes retained validation"
    );
    ensure!(fixture.set.validate_payloads().is_err());
    fixture.set.dependencies[0].1[0] ^= 1;
    fixture.set.validate_payloads()?;
    fixture.set.dependencies[0].0.kind = manifest::ObjectKind::Tail;
    ensure!(
        fixture.checked().is_err(),
        "dependency reference mismatch retained validation"
    );
    ensure!(fixture.set.validate_payloads().is_err());
    Ok(())
}

#[test]
fn replay_work_survives_a_memo_hit_without_minting_a_new_physical_proof() -> Result<()> {
    let fixture = Fixture::new()?;
    let images = GenerationStore::open(
        fixture._root.path().join("verifier"),
        Arc::new(DirectFileSystem),
        cellar_ltx::generation::GenerationLimits::default(),
    )?;
    let verifier = ArchiveVerifier::new(images.clone(), ImageId::new([40; 16])?)?;
    let performed = verifier.verify_with_work(&fixture.set)?;
    ensure!(performed.replay == Replay::Performed);
    ensure!(performed.work.images == 1);
    ensure!(performed.work.image_bytes == fixture.set.manifest.image.length);
    let reused = verifier.verify_with_work(&fixture.set)?;
    ensure!(reused.replay == Replay::Reused && reused.work == performed.work);
    ensure!(images.usage()?.generations == 0);
    Ok(())
}

#[test]
fn unvalidated_inputs_cannot_use_or_create_a_physical_replay_memo() -> Result<()> {
    let mut fixture = Fixture::new()?;
    let images = GenerationStore::open(
        fixture._root.path().join("verifier"),
        Arc::new(DirectFileSystem),
        cellar_ltx::generation::GenerationLimits::default(),
    )?;
    let verifier = ArchiveVerifier::new(images.clone(), ImageId::new([40; 16])?)?;
    ensure!(verifier.verify(&fixture.set)? == Replay::Performed);
    fixture.set.validated = None;
    ensure!(matches!(
        verifier.verify(&fixture.set),
        Err(ArchiveError::Invalid(
            "archive replay input is not validated"
        ))
    ));
    ensure!(images.usage()?.generations == 0);
    fixture.set.validate_payloads()?;
    ensure!(
        verifier.verify(&fixture.set)? == Replay::Performed,
        "unvalidated input did not invalidate the memo"
    );
    ensure!(images.usage()?.generations == 0);
    Ok(())
}
