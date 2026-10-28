// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(clippy::disallowed_methods)]
use super::*;
use cellar_logic::consensus::*;

fn archive(id: u8) -> CellArchive {
    CellArchive {
        coordination: None,
        activity: None,
        scratch_directory: None,
        snapshot_scratch: Default::default(),
        store: Arc::new(object_store::memory::InMemory::new()),
        prefix: format!("test/archives/{id}"),
        bundle_prefix: "test/archives/bundles".into(),
        scope: GroupScope {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            cell: format!("Counter:{id}"),
            group: GroupId::new([id; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        },
        root: [5; 32],
        birth: [id; 32],
        schema: [6; 32],
        initial_membership: super::super::super::member_state::GroupMembership::initial(
            &ConfState {
                voters: vec![1],
                ..Default::default()
            },
        ),
        limits: ArchiveLimits::default(),
        host: LtxHost::default(),
        cancellation: None,
        io_domain: None,
        read_retention: None,
        bundle_client: None,
        verification_deadline: None,
        root_store: None,
        image_storage: super::super::super::catalog::RaftImageStorage::Materialized,
    }
}

fn part(archive: &CellArchive, bytes: &[u8]) -> Part {
    Part {
        member: Member {
            scope: (&archive.scope).into(),
            root: archive.root,
            birth: archive.birth,
            schema: archive.schema,
            object: ObjectRef::new(
                ObjectKind::Snapshot,
                bytes,
                Some(PositionRange {
                    first: Position { index: 5, term: 2 },
                    last: Position { index: 5, term: 2 },
                }),
            ),
            offset: 0,
        },
        bytes: bytes.to_vec(),
    }
}

fn rewrite(
    bytes: &[u8],
    expected: &ObjectRef,
    edit: impl FnOnce(&mut Index),
) -> (Vec<u8>, ObjectRef) {
    let length = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let mut index: Index = manifest::decode(3, &bytes[12..12 + length], MAX_INDEX_BYTES).unwrap();
    edit(&mut index);
    let header = manifest::encode(3, &index, MAX_INDEX_BYTES).unwrap();
    let mut changed = MAGIC.to_vec();
    changed.extend_from_slice(&(header.len() as u32).to_be_bytes());
    changed.extend_from_slice(&header);
    changed.extend_from_slice(&bytes[12 + length..]);
    let mut expected = expected.clone();
    let reference = expected.bundle.as_mut().unwrap();
    reference.digest = manifest::digest(&changed);
    reference.length = changed.len() as u64;
    (changed, expected)
}

#[test]
fn bundle_selects_exact_scope_and_payload() -> anyhow::Result<()> {
    let a = archive(10);
    let b = archive(11);
    let (bytes, refs) = encode(vec![part(&a, b"first"), part(&b, b"second")])?;
    assert_eq!(extract(&a, &refs[0], &bytes)?, b"first");
    assert_eq!(extract(&b, &refs[1], &bytes)?, b"second");
    assert!(extract(&b, &refs[0], &bytes).is_err());
    let mut other_member = refs[0].clone();
    other_member.bundle.as_mut().unwrap().member = 1;
    assert!(extract(&a, &other_member, &bytes).is_err());
    for field in 0..5 {
        let mut wrong = archive(10);
        match field {
            0 => wrong.root = [77; 32],
            1 => wrong.birth = [77; 32],
            2 => wrong.schema = [77; 32],
            3 => wrong.scope.incarnation = IncarnationId::new([77; 16])?,
            _ => wrong.scope.cell = "Counter:other".into(),
        }
        assert!(extract(&wrong, &refs[0], &bytes).is_err());
    }
    let mut corrupt = bytes.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    assert!(extract(&a, &refs[0], &corrupt).is_err());
    assert!(extract(&a, &refs[0], &bytes[..bytes.len() - 1]).is_err());
    Ok(())
}

#[test]
fn batching_excludes_incompatible_recovery_domains() -> anyhow::Result<()> {
    let a = archive(10);
    let mut b = archive(11);
    b.store = a.store.clone();
    assert!(a.can_batch_with(&b));
    for field in 0..6 {
        let mut incompatible = b.clone();
        match field {
            0 => incompatible.root = [77; 32],
            1 => incompatible.schema = [77; 32],
            2 => incompatible.scope.cluster = ClusterId::new([77; 16])?,
            3 => incompatible.scope.namespace = NamespaceId::new([77; 16])?,
            4 => incompatible.bundle_prefix = "another/archives/bundles".into(),
            _ => incompatible.store = Arc::new(object_store::memory::InMemory::new()),
        }
        assert!(!a.can_batch_with(&incompatible), "domain field {field}");
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_bundle_extraction_retains_its_read_reservation() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut a = archive(10);
    let b = archive(11);
    a.io_domain = domain
        .run(async { crate::asyncrt::services().domain_token() })
        .await?;
    let (bytes, refs) = encode(vec![part(&a, b"first"), part(&b, b"second")])?;
    a.store
        .put(
            &a.bundle_key(refs[0].bundle.as_ref().unwrap()),
            bytes.into(),
        )
        .await?;
    // Hold the physical job independently of its observing future, matching a
    // blocking executor whose work survives cancellation of its awaiter.
    let (send, mut jobs) = mpsc::unbounded_channel();
    a.host = LtxHost::new(
        || 0,
        |_| unreachable!(),
        |_| async { unreachable!() },
        move |job| {
            send.send(job).unwrap();
            std::future::pending()
        },
    );
    let bytes = a.read_memory_bound();
    let budget = crate::raft::budget::Budget::new(MAX_RECOVERY_READ_BYTES, 1);
    let a = a.with_read_retention(Arc::new(budget.acquire(bytes).unwrap()));
    let mut read = Box::pin(async move { a.get_object(&refs[0]).await });
    let job = crate::asyncrt::select! {
        result = &mut read => panic!("extraction completed before physical job: {result:?}"),
        job = jobs.recv() => job.expect("physical extraction was queued"),
    };
    drop(read);
    assert_eq!(budget.usage(), (bytes, 1));
    assert!(budget.acquire(bytes).is_none());
    job();
    assert_eq!(budget.usage(), (0, 0));
    domain.shutdown().await;
    Ok(())
}

#[test]
fn canonical_checksums_do_not_authorize_bad_bundle_indexes() -> anyhow::Result<()> {
    let a = archive(10);
    let b = archive(11);
    let (bytes, refs) = encode(vec![part(&a, b"first"), part(&b, b"second")])?;
    for mutation in 0..9 {
        let (changed, expected) = rewrite(&bytes, &refs[0], |index| match mutation {
            0 => index.members[1].offset = 0,
            1 => index.members[1].object.length = u64::MAX,
            2 => index.members[1].object.kind = ObjectKind::Manifest,
            3 => index.members[1].object.bundle = refs[1].bundle.clone(),
            4 => index.members[1].scope = index.members[0].scope.clone(),
            5 => index.members.swap(0, 1),
            6 => index.members[1].root = [77; 32],
            7 => index.members[1].object.range = None,
            _ => index.members[1].object.range.as_mut().unwrap().last.index = 4,
        });
        assert!(
            extract(&a, &expected, &changed).is_err(),
            "mutation {mutation}"
        );
    }
    let mut expected = refs[0].clone();
    expected.bundle.as_mut().unwrap().member = u32::MAX;
    assert!(extract(&a, &expected, &bytes).is_err());
    Ok(())
}

#[tokio::test]
async fn member_limit_keeps_small_objects_on_direct_path() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut a = archive(10);
    let mut b = archive(11);
    a.io_domain = domain
        .run(async { crate::asyncrt::runtime_services().domain_token() })
        .await?;
    b.io_domain = a.io_domain.clone();
    b.store = a.store.clone();
    // Each object fits, but the complete bundle cannot fit member B's limit.
    b.limits.max_object_bytes = 1024;
    let payloads = [vec![1; 512], vec![2; 512]];
    let mut requests = Vec::new();
    let mut results = Vec::new();
    for (archive, bytes) in [a, b].into_iter().zip(payloads) {
        let object = part(&archive, &bytes).member.object;
        let (reply, result) = oneshot::channel();
        results.push((archive.clone(), bytes.clone(), result));
        requests.push(Request {
            archive,
            object,
            bytes,
            reply,
        });
    }
    flush(requests).await;
    for (archive, bytes, result) in results {
        let object = result.await??;
        assert!(object.bundle.is_none());
        assert_eq!(archive.get_object(&object).await?, bytes);
    }
    domain.shutdown().await;
    Ok(())
}
