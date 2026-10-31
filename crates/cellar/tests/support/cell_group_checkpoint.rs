// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Source-side checkpoint qualification over the existing real three-voter
//! fixture. This does not qualify a receiver, learner promotion or snapshot RPC.

use super::*;
use cellar_ltx::internal::{
    ExtentId, ExtentManifest, ExtentRootReference, Geometry, ImageBinding, NodeExtentStore,
    EXTENT_BYTES,
};
use std::collections::BTreeMap;

pub(super) fn limits() -> CellGroupCheckpointLimits {
    CellGroupCheckpointLimits {
        max_sources: 1,
        max_retained_bytes: 32 * 1024 * 1024,
        max_cells: 64,
        max_transfer_bytes: 32 * 1024 * 1024,
        max_chunk_bytes: 4093,
    }
}

async fn populated(rooted: bool) -> Result<(Cluster, BTreeMap<String, Vec<u8>>)> {
    populated_with_lagging(rooted, false).await
}

pub(super) async fn populated_with_lagging(
    rooted: bool,
    lagging: bool,
) -> Result<(Cluster, BTreeMap<String, Vec<u8>>)> {
    populate(rooted, lagging, false).await
}

pub(super) async fn populated_partition(
    rooted: bool,
) -> Result<(Cluster, BTreeMap<String, Vec<u8>>)> {
    populate(rooted, false, true).await
}

async fn populate(
    rooted: bool,
    lagging: bool,
    partition: bool,
) -> Result<(Cluster, BTreeMap<String, Vec<u8>>)> {
    populate_images(rooted, lagging, partition, 0, 4_202_496).await
}

pub(super) async fn populate_images(
    rooted: bool,
    lagging: bool,
    partition: bool,
    additional: usize,
    padding: usize,
) -> Result<(Cluster, BTreeMap<String, Vec<u8>>)> {
    let mut cluster = Cluster::new().await?;
    cluster.elect().await?;
    cluster.voters[2].paused = lagging;
    let expected = enroll_images(&mut cluster, rooted, partition, additional, padding).await?;
    Ok((cluster, expected))
}

/// Enroll into an existing elected group so archive tests can capture its empty
/// checkpoint first. Existing source/snapshot fixtures call this same path.
pub(super) async fn enroll_images(
    cluster: &mut Cluster,
    rooted: bool,
    partition: bool,
    additional: usize,
    padding: usize,
) -> Result<BTreeMap<String, Vec<u8>>> {
    let route = cluster.birth.route().clone();
    if rooted {
        for n in 0..3 {
            cluster.voters[n].extents = Some(NodeExtentStore::open(
                cluster._root.path().join(format!("extents-{n}")),
                Arc::new(DirectFileSystem),
                Default::default(),
            )?);
        }
    }
    let mut expected = BTreeMap::new();
    let names = ["alpha", "beta", "empty"]
        .into_iter()
        .map(str::to_owned)
        .chain((0..additional).map(|n| format!("cell-{n:03}")));
    for (ordinal, name) in names.enumerate() {
        let cell = history(&route, &name)?;
        let bytes = if name == "empty" {
            Vec::new()
        } else {
            // Keep the original two-image fixture unchanged. Pressure cohorts
            // use unique contents so other cells cannot accidentally retain an
            // old reader's extent after that cell advances.
            let value = if additional == 0 { 1 } else { ordinal + 1 };
            images::image(None, &format!("CREATE TABLE value(n); INSERT INTO value VALUES({value}); CREATE TABLE padding(bytes); INSERT INTO padding VALUES(zeroblob({padding}));"))?
        };
        let encoded = if bytes.is_empty() {
            CellGroupEntry {
                binding: GroupEntryBinding {
                    group: route.history,
                    route_epoch: route.epoch,
                    birth_digest: route.birth_digest,
                    schema_digest: route.schema_digest,
                    proposal: ProposalId::new([99; 16])?,
                },
                command: GroupCommand::Enroll {
                    cell: cell.clone(),
                    image: LtxImage {
                        epoch: 1,
                        txid: 0,
                        checksum: 0,
                        commitment: ImageCommitment::Sha256(Sha256::digest([]).into()),
                    },
                    files: vec![],
                },
            }
            .encode(&route, GroupEntryLimits::default())?
        } else {
            let cut = images::Cut::new(&[], &bytes, 0, true, false);
            let encoded = request(&route, cell.clone(), 1, &cut)?;
            let mut entry = CellGroupEntry::decode(&encoded, &route, GroupEntryLimits::default())?;
            if rooted {
                let commitment = extent_commitment(&route, &cell, cut.result, &bytes)?;
                let GroupCommand::Enroll { image, .. } = &mut entry.command else {
                    unreachable!()
                };
                image.commitment = commitment;
            }
            entry.encode(&route, GroupEntryLimits::default())?
        };
        cluster.voters[0].owner.as_mut().unwrap().propose(encoded)?;
        cluster
            .drive_partition(false, partition.then_some(2))
            .await?;
        expected.insert(name.to_owned(), bytes);
    }
    Ok(expected)
}

fn extent_commitment(
    route: &GroupRoute,
    cell: &CellHistory,
    state: ImageState,
    bytes: &[u8],
) -> Result<ImageCommitment> {
    let manifest = ExtentManifest::new(
        ImageBinding {
            history: cell_image_history_digest(cell),
            schema: route.schema_digest,
            epoch: state.epoch,
            txid: state.txid.0,
            checksum: state.checksum,
        },
        Geometry::new(state.page_size, state.length)?,
        &bytes
            .chunks(EXTENT_BYTES as usize)
            .map(ExtentId::of)
            .collect::<std::result::Result<Vec<_>, _>>()?,
    )?;
    Ok(ImageCommitment::Extents(ExtentRootReference::new(
        manifest.root(),
        manifest.encode()?.len() as u32,
    )?))
}

async fn advance(cluster: &mut Cluster, name: &str, before: &[u8], rooted: bool) -> Result<()> {
    advance_partition(cluster, name, before, rooted, None).await
}

pub(super) async fn advance_partition(
    cluster: &mut Cluster,
    name: &str,
    before: &[u8],
    rooted: bool,
    excluded: Option<usize>,
) -> Result<()> {
    let route = cluster.birth.route().clone();
    let cell = history(&route, name)?;
    let after = images::image(Some(before), "UPDATE value SET n=n+1")?;
    let cut = images::Cut::new(before, &after, 1, true, false);
    let encoded = request(&route, cell.clone(), 2, &cut)?;
    let mut entry = CellGroupEntry::decode(&encoded, &route, GroupEntryLimits::default())?;
    if rooted {
        let GroupCommand::Mutation(mutation) = &mut entry.command else {
            unreachable!()
        };
        mutation.base.commitment = extent_commitment(&route, &cell, cut.base, before)?;
        mutation.result.commitment = extent_commitment(&route, &cell, cut.result, &after)?;
    }
    cluster.voters[0]
        .owner
        .as_mut()
        .unwrap()
        .propose(entry.encode(&route, GroupEntryLimits::default())?)?;
    cluster.drive_partition(false, excluded).await?;
    Ok(())
}

#[derive(Default)]
pub(super) struct Received {
    header: Vec<u8>,
    pages: BTreeMap<[u8; 32], Vec<u8>>,
    images: BTreeMap<String, Vec<u8>>,
    manifests: BTreeMap<String, Vec<u8>>,
    extents: BTreeMap<(String, u32), (ExtentId, Vec<u8>)>,
    largest_chunk: usize,
}

impl Received {
    pub(super) fn append(
        &mut self,
        chunk: CellGroupCheckpointChunk<'_>,
    ) -> std::result::Result<(), CellGroupCheckpointError> {
        self.largest_chunk = self.largest_chunk.max(chunk.bytes.len());
        let output = match chunk.part {
            CellGroupCheckpointPart::Header => &mut self.header,
            CellGroupCheckpointPart::Page(key) => self.pages.entry(key).or_default(),
            CellGroupCheckpointPart::Materialized(cell) => {
                self.images.entry(cell.as_str().to_owned()).or_default()
            }
            CellGroupCheckpointPart::ExtentManifest(cell) => {
                self.manifests.entry(cell.as_str().to_owned()).or_default()
            }
            CellGroupCheckpointPart::Extent { cell, ordinal, id } => {
                let output = self
                    .extents
                    .entry((cell.as_str().to_owned(), ordinal))
                    .or_insert_with(|| (id, vec![]));
                assert_eq!(output.0, id);
                &mut output.1
            }
        };
        assert_eq!(chunk.offset, output.len() as u64);
        assert!(chunk.offset + chunk.bytes.len() as u64 <= chunk.length);
        output.extend_from_slice(chunk.bytes);
        Ok(())
    }

    pub(super) fn verify(
        mut self,
        marker: &GroupManifest,
        leaves: &BTreeMap<String, CellManifestLeaf>,
        expected: &BTreeMap<String, Vec<u8>>,
        rooted: bool,
    ) -> Result<()> {
        assert!(self.largest_chunk <= limits().max_chunk_bytes);
        let header: serde_json::Value = serde_json::from_slice(&self.header)?;
        assert_eq!(header["domain"], "cellar-cell-group-checkpoint-1");
        assert_eq!(header["transition"], "active");
        assert_eq!(
            header["membership"]["revision"],
            marker.membership_revision()
        );
        assert_eq!(header["root"], serde_json::to_value(marker)?);
        let conf = header["conf"]
            .as_array()
            .context("configuration")?
            .iter()
            .map(|n| n.as_u64().unwrap() as u8)
            .collect::<Vec<_>>();
        let conf: raft::prelude::ConfState = protobuf::Message::parse_from_bytes(&conf)?;
        assert_eq!(conf.voters, vec![1, 2, 3]);
        assert_eq!(self.pages.len(), expected.len());
        assert!(self.pages.contains_key(&marker.root().digest().unwrap()));
        for (digest, bytes) in &self.pages {
            let mut hash = Sha256::new();
            hash.update(b"cellar-cell-group-manifest-page-1\0");
            hash.update(bytes);
            assert_eq!(*digest, <[u8; 32]>::from(hash.finalize()));
        }
        for (name, wanted) in expected {
            let leaf = &leaves[name];
            assert_eq!(leaf.version.sequence.get(), 1);
            if wanted.is_empty() {
                assert_eq!(leaf.version.image.txid, 0);
                assert!(!self.images.contains_key(name));
                assert!(!self.manifests.contains_key(name));
                continue;
            }
            if rooted {
                let CellImageCommitment::Extents {
                    root,
                    manifest_bytes,
                } = leaf.version.image.commitment
                else {
                    unreachable!()
                };
                let bytes = self.manifests.remove(name).context("manifest")?;
                assert_eq!(bytes.len(), manifest_bytes as usize);
                let manifest = ExtentManifest::decode(
                    &bytes,
                    cellar_ltx::internal::ImageRoot::from_bytes(root),
                )?;
                let mut assembled = Vec::new();
                for n in 0..manifest.geometry().extent_count() {
                    let (id, bytes) = self.extents.remove(&(name.clone(), n)).context("extent")?;
                    assert_eq!(Some(id), manifest.extent(n));
                    id.verify(&bytes)?;
                    assembled.extend(bytes);
                }
                assert_eq!(&assembled, wanted);
            } else {
                assert_eq!(self.images.remove(name).as_ref(), Some(wanted));
            }
        }
        assert!(self.images.is_empty() && self.extents.is_empty() && self.manifests.is_empty());
        Ok(())
    }
}

pub(super) async fn coherent() -> Result<()> {
    for rooted in [false, true] {
        let (mut cluster, expected) = populated(rooted).await?;
        let group = cluster.voters[0]
            .log
            .cell_group(&cluster.birth.route().history)?;
        let root = cluster.voters[0].owner.as_ref().unwrap().manifest().clone();
        let old_page = cluster._root.path().join("node-1/pages").join(format!(
            "{}.page",
            hex::encode(root.marker().root().digest().unwrap())
        ));
        let pool = CellGroupCheckpointPool::new(limits())?;
        let mut source = pool.capture(&group, &root)?;
        let marker = root.marker().clone();
        let leaves = expected
            .keys()
            .map(|name| {
                let cell = CellId::new(name.clone()).unwrap();
                (name.clone(), root.pin_cell(&cell).unwrap().0.clone())
            })
            .collect::<BTreeMap<_, _>>();
        drop(root); // Only the stream retains the old physical closure after apply.
        let (entered, entered_rx) = tokio::sync::oneshot::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let worker = cluster.domain.blocking(move || -> Result<_> {
            let mut entered = Some(entered);
            let mut received = Received::default();
            source.stream(|chunk| {
                if matches!(chunk.part, CellGroupCheckpointPart::Page(_)) {
                    if let Some(entered) = entered.take() {
                        let _ = entered.send(());
                        release_rx.recv().map_err(|_| {
                            CellGroupCheckpointError::Sink(std::io::Error::other("released"))
                        })?;
                    }
                }
                received.append(chunk)
            })?;
            Ok((source, received))
        })?;
        tokio::time::timeout(Duration::from_secs(10), entered_rx).await??;
        // Real quorum publication continues while the transfer owner is held.
        advance(&mut cluster, "alpha", &expected["alpha"], rooted).await?;
        advance(&mut cluster, "beta", &expected["beta"], rooted).await?;
        for voter in &mut cluster.voters {
            let current = voter.owner.as_ref().unwrap().manifest();
            assert!(current.marker().position().index > marker.position().index);
            assert_eq!(
                current
                    .pin_cell(&CellId::new("alpha")?)
                    .unwrap()
                    .0
                    .version
                    .sequence
                    .get(),
                2
            );
            voter.pages.as_mut().unwrap().reconcile_retained(&[])?;
            voter.pages.as_mut().unwrap().reclaim(1024)?;
            voter.images.retain_only(&[])?;
            voter.images.reclaim(1024)?;
            if let Some(store) = &voter.extents {
                store.retain_only(&[])?;
                store.reclaim(1024)?;
            }
        }
        assert!(old_page.exists(), "live source must pin its manifest page");
        release.send(())?;
        let (source, received) = worker.await??;
        received.verify(&marker, &leaves, &expected, rooted)?;
        assert_eq!(source.marker(), &marker);
        assert_eq!(pool.usage().sources, 1);
        drop(source);
        assert_eq!(pool.usage().sources, 0);
        cluster.voters[0].pages.as_mut().unwrap().reclaim(1024)?;
        cluster.voters[0].pages.as_mut().unwrap().reclaim(1024)?;
        assert!(
            !old_page.exists(),
            "released obsolete source page can be reclaimed"
        );
        cluster.close().await?;
    }
    Ok(())
}

pub(super) async fn refusals() -> Result<()> {
    let mut cluster = Cluster::new().await?;
    let group = cluster.voters[0]
        .log
        .cell_group(&cluster.birth.route().history)?;
    let original = cluster.voters[0].owner.as_ref().unwrap().manifest().clone();
    let pool = CellGroupCheckpointPool::new(limits())?;
    // A recovered empty genesis is a complete closure, with no cache image pins yet.
    let mut empty = pool.capture(&group, &original)?;
    let mut parts = 0;
    empty.stream(|chunk| {
        assert_eq!(chunk.part, CellGroupCheckpointPart::Header);
        parts += 1;
        Ok(())
    })?;
    assert!(parts > 0);
    drop(empty);
    let foreign = cluster.voters[1].owner.as_ref().unwrap().manifest();
    assert!(matches!(
        pool.capture(&group, foreign),
        Err(CellGroupCheckpointError::Invalid(_))
    ));
    let prospective = cluster.voters[0].tree.prepare_unchanged(
        cluster.voters[0].pages.as_ref().unwrap(),
        &original,
        LogPosition {
            index: LogIndex::new(2),
            term: Term::new(1),
        },
    )?;
    assert!(matches!(
        pool.capture(&group, &prospective),
        Err(CellGroupCheckpointError::Invalid(_))
    ));
    assert_eq!(pool.usage().retained_bytes, 0);
    cluster.elect().await?;
    assert!(matches!(
        pool.capture(&group, &original),
        Err(CellGroupCheckpointError::Invalid(_))
    ));
    let current = cluster.voters[0].owner.as_ref().unwrap().manifest();
    let source = pool.capture(&group, current)?;
    let bytes = pool.usage().retained_bytes;
    // Two slots, but only enough bytes for one source: exercise byte admission
    // independently of the item ceiling, with no refund on a failed acquire.
    let small = CellGroupCheckpointPool::new(CellGroupCheckpointLimits {
        max_sources: 2,
        max_retained_bytes: bytes,
        ..limits()
    })?;
    let first = small.capture(&group, current)?;
    assert!(matches!(
        small.capture(&group, current),
        Err(CellGroupCheckpointError::Busy)
    ));
    assert_eq!(small.usage().retained_bytes, bytes);
    drop(first);
    assert_eq!(small.usage().retained_bytes, 0);
    drop(source);
    drop(prospective);
    drop(original);
    cluster.close().await?;
    Ok(())
}

pub(super) async fn ownership() -> Result<()> {
    let (cluster, _) = populated(false).await?;
    let group = cluster.voters[0]
        .log
        .cell_group(&cluster.birth.route().history)?;
    let root = cluster.voters[0].owner.as_ref().unwrap().manifest();
    let pool = CellGroupCheckpointPool::new(limits())?;
    let mut source = pool.capture(&group, root)?;
    let usage = pool.usage();
    let mut limited = limits();
    limited.max_cells = 2;
    assert!(matches!(
        CellGroupCheckpointPool::new(limited)?.capture(&group, root),
        Err(CellGroupCheckpointError::Invalid(_))
    ));
    limited = limits();
    limited.max_transfer_bytes = 64 * 1024;
    assert!(matches!(
        CellGroupCheckpointPool::new(limited)?.capture(&group, root),
        Err(CellGroupCheckpointError::Limit)
    ));
    assert!(matches!(
        pool.capture(&group, root),
        Err(CellGroupCheckpointError::Busy)
    ));
    assert_eq!(pool.usage(), usage);
    let (entered, rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    let worker = cluster.domain.blocking(move || {
        let mut entered = Some(entered);
        let result = source.stream(|_| {
            if let Some(entered) = entered.take() {
                let _ = entered.send(());
                let _ = release_rx.recv();
            }
            Err(CellGroupCheckpointError::Sink(std::io::Error::other(
                "cancelled sink",
            )))
        });
        assert!(matches!(result, Err(CellGroupCheckpointError::Sink(_))));
        assert!(matches!(
            source.stream(|_| Ok(())),
            Err(CellGroupCheckpointError::Invalid(_))
        ));
    })?;
    tokio::time::timeout(Duration::from_secs(10), rx).await??;
    drop(worker); // Losing the result waiter must not refund the physical source.
    assert_eq!(pool.usage(), usage);
    assert!(matches!(
        pool.capture(&group, root),
        Err(CellGroupCheckpointError::Busy)
    ));
    release.send(())?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while pool.usage().sources != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    assert_eq!(pool.usage().retained_bytes, 0);
    cluster.close().await?;
    Ok(())
}

pub(super) async fn corruption() -> Result<()> {
    for rooted in [false, true] {
        let (mut cluster, _) = populated(rooted).await?;
        let group = cluster.voters[0]
            .log
            .cell_group(&cluster.birth.route().history)?;
        let root = cluster.voters[0].owner.as_ref().unwrap().manifest().clone();
        let pool = CellGroupCheckpointPool::new(limits())?;
        let mut source = pool.capture(&group, &root)?;
        let (_, pin) = root.pin_cell(&CellId::new("alpha")?).unwrap();
        let path = match pin {
            CellImagePin::Materialized(pin) => cluster
                ._root
                .path()
                .join("node-1/images")
                .join(format!("{}.sqlite", hex::encode(pin.id().bytes()))),
            CellImagePin::Extents(pin) => {
                let id = pin.manifest().extent(0).unwrap();
                cluster._root.path().join("extents-0").join(format!(
                    "e-{:08x}-{}",
                    id.length(),
                    hex::encode(id.bytes())
                ))
            }
            CellImagePin::Empty => unreachable!(),
        };
        let mut bytes = std::fs::read(&path)?;
        bytes[100] ^= 0x80;
        std::fs::write(path, bytes)?;
        let result = cluster
            .domain
            .blocking(move || {
                let result = source.stream(|_| Ok(()));
                (source, result)
            })?
            .await?;
        assert!(matches!(
            result.1,
            Err(CellGroupCheckpointError::Invalid(
                "materialized image digest"
            )) | Err(CellGroupCheckpointError::Extent(_))
        ));
        assert_eq!(pool.usage().sources, 1);
        drop(result);
        let mut closed = pool.capture(&group, &root)?;
        drop(cluster.voters[0].owner.take());
        cluster.voters[0].log.close().await?;
        let mut emissions = 0;
        assert!(matches!(
            closed.stream(|_| {
                emissions += 1;
                Ok(())
            }),
            Err(CellGroupCheckpointError::Log(_))
        ));
        assert_eq!(emissions, 0);
        drop(closed);
        drop(root);
        cluster.close().await?;
    }
    Ok(())
}
