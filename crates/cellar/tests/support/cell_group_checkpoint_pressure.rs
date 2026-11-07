// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded physical transfer and read views; quorum admission is tested separately.
use super::*;
use cellar_ltx::stream_snapshot::{ScratchLimits, ScratchPool};

fn digest(pin: &CellImagePin) -> Result<[u8; 32]> {
    let mut hash = Sha256::new();
    match pin {
        CellImagePin::Empty => {}
        CellImagePin::Materialized(pin) => {
            let mut reader = pin.open_reader()?;
            let mut offset = 0;
            while offset < reader.length() {
                let size = (reader.length() - offset).min(64 * 1024) as usize;
                hash.update(reader.read_exact_at(offset, size)?);
                offset += size as u64;
            }
        }
        CellImagePin::Extents(pin) => {
            for n in 0..pin.manifest().geometry().extent_count() {
                hash.update(
                    pin.view()
                        .read_extent(pin.manifest().extent(n).context("extent")?)?,
                );
            }
        }
    }
    Ok(hash.finalize().into())
}

pub(crate) async fn transfer_pressure(rooted: bool) -> Result<()> {
    // 31 populated cells plus empty enrollment: aggregate payload exceeds both
    // source workspace and one receiver image spool, without collecting a stream.
    let (mut cluster, expected) =
        checkpoint_fixture::populate_images(rooted, true, false, 29, 524_288).await?;
    let source_group = cluster.voters[0]
        .log
        .cell_group(&cluster.birth.route().history)?;
    let root = cluster.voters[0].owner.as_ref().unwrap().manifest().clone();
    assert_eq!(root.pinned_images(), 32);
    let marker = root.marker().clone();
    let (_, reader) = root
        .pin_cell(&CellId::new("alpha")?)
        .context("reader pin")?;
    let old_image = match &reader {
        CellImagePin::Materialized(pin) => cluster
            ._root
            .path()
            .join("node-1/images")
            .join(format!("{}.sqlite", hex::encode(pin.id().bytes()))),
        CellImagePin::Extents(pin) => {
            let id = pin.manifest().extent(0).context("reader extent")?;
            cluster._root.path().join("extents-0").join(format!(
                "e-{:08x}-{}",
                id.length(),
                hex::encode(id.bytes())
            ))
        }
        CellImagePin::Empty => anyhow::bail!("populated reader"),
    };
    let old_page = cluster._root.path().join("node-1/pages").join(format!(
        "{}.page",
        hex::encode(marker.root().digest().unwrap())
    ));
    let source_pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let mut source = source_pool.capture(&source_group, &root)?;
    let source_usage = source_pool.usage();
    let payload: usize = expected.values().map(Vec::len).sum();
    assert!(payload > source_usage.retained_bytes);
    assert!(matches!(
        source_pool.capture(&source_group, &root),
        Err(CellGroupCheckpointError::Busy)
    ));
    drop(root);

    let applier = checkpoint_receive::applier(&mut cluster)?;
    let destination = cluster.voters[2]
        .log
        .cell_group(&cluster.birth.route().history)?;
    let before = destination.group_manifest()?.context("receiver base")?;
    let limits = CellGroupCheckpointLimits {
        max_sources: 2,
        ..checkpoint_fixture::limits()
    };
    let pool = CellGroupCheckpointPool::new(limits)?;
    let image_bytes = expected["alpha"].len() as u64;
    let scratch = ScratchPool::new(ScratchLimits {
        max_files: 1,
        max_bytes: image_bytes,
    })?;
    let directory = cluster._root.path().to_owned();
    let mut receiver = pool.receive(applier.clone(), scratch.clone(), directory.clone())?;
    let receive_usage = pool.usage();
    assert!(receive_usage.retained_bytes <= limits.max_retained_bytes);
    // Verify byte admission independently of item admission, including rollback
    // when a second full receiver cannot reserve its metadata/workspace.
    let bytes_pool = CellGroupCheckpointPool::new(CellGroupCheckpointLimits {
        max_sources: 2,
        max_retained_bytes: receive_usage.retained_bytes,
        ..limits
    })?;
    let reserved = bytes_pool.receive(applier.clone(), scratch.clone(), directory.clone())?;
    assert!(matches!(
        bytes_pool.receive(applier.clone(), scratch.clone(), directory.clone()),
        Err(CellGroupCheckpointError::Busy)
    ));
    assert_eq!(bytes_pool.usage(), receive_usage);
    drop(reserved);
    assert_eq!(bytes_pool.usage().retained_bytes, 0);

    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    let observed_pool = pool.clone();
    let observed_scratch = scratch.clone();
    let worker = cluster.domain.blocking(move || -> Result<_> {
        let mut entered = Some(entered);
        let mut transferred = 0;
        let mut peak_scratch = 0;
        source.stream(|chunk| {
            let image = matches!(
                chunk.part,
                CellGroupCheckpointPart::Materialized(_) | CellGroupCheckpointPart::Extent { .. }
            );
            assert!(chunk.bytes.len() <= limits.max_chunk_bytes);
            transferred += chunk.bytes.len();
            receiver.append(chunk)?;
            let usage = observed_scratch.usage().unwrap();
            assert!(usage.reserved_files <= 1 && usage.reserved_bytes <= image_bytes);
            peak_scratch = peak_scratch.max(usage.reserved_bytes);
            assert_eq!(observed_pool.usage(), receive_usage);
            if image {
                if let Some(entered) = entered.take() {
                    let _ = entered.send(());
                    release_rx.recv().map_err(|_| {
                        CellGroupCheckpointError::Sink(std::io::Error::other("released"))
                    })?;
                }
            }
            Ok(())
        })?;
        Ok((receiver.finish()?, transferred, peak_scratch))
    })?;
    tokio::time::timeout(Duration::from_secs(10), entered_rx).await??;
    assert_eq!(pool.usage(), receive_usage);
    assert_eq!(source_pool.usage(), source_usage);
    let spare = pool.receive(applier.clone(), scratch.clone(), directory.clone())?;
    assert!(matches!(
        pool.receive(applier.clone(), scratch.clone(), directory.clone()),
        Err(CellGroupCheckpointError::Busy)
    ));
    drop(spare);
    assert_eq!(pool.usage(), receive_usage);
    if !rooted {
        // A different node can produce another valid stream, but this destination
        // has only one shared image spool. Refusal must not refund the first one.
        let peer_pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        let mut peer_source = peer_pool.capture(
            &cluster.voters[1]
                .log
                .cell_group(&cluster.birth.route().history)?,
            cluster.voters[1].owner.as_ref().unwrap().manifest(),
        )?;
        let mut competing = pool.receive(applier.clone(), scratch.clone(), directory.clone())?;
        cluster
            .domain
            .blocking(move || {
                assert!(matches!(
                    peer_source.stream(|chunk| competing.append(chunk)),
                    Err(CellGroupCheckpointError::Image(
                        cellar_ltx::generation::GenerationError::Snapshot(
                            cellar_ltx::stream_snapshot::Error::ScratchFull
                        )
                    ))
                ));
            })?
            .await?;
        assert_eq!(pool.usage(), receive_usage);
        assert_eq!(peer_pool.usage().sources, 0);
        assert_eq!(scratch.usage()?.reserved_files, 1);
        assert_eq!(scratch.usage()?.reserved_bytes, image_bytes);
        assert_eq!(destination.group_manifest()?.as_ref(), Some(&before));
    }
    assert_eq!(
        scratch.usage()?.reserved_bytes,
        if rooted { 0 } else { image_bytes }
    );
    assert_eq!(destination.group_manifest()?.as_ref(), Some(&before));
    // Receiver is deliberately detached. The other two real voters still commit
    // mutations while its physical stream is blocked midway through an image.
    for name in ["alpha", "beta"] {
        checkpoint_fixture::advance_partition(&mut cluster, name, &expected[name], rooted, Some(2))
            .await?;
    }
    for voter in &mut cluster.voters[..2] {
        assert!(
            voter
                .owner
                .as_ref()
                .unwrap()
                .manifest()
                .marker()
                .position()
                .index
                > marker.position().index
        );
        for name in ["alpha", "beta"] {
            assert_eq!(
                voter
                    .owner
                    .as_ref()
                    .unwrap()
                    .manifest()
                    .pin_cell(&CellId::new(name)?)
                    .context("advanced cell")?
                    .0
                    .version
                    .sequence
                    .get(),
                2
            );
        }
        voter.pages.as_mut().unwrap().reconcile_retained(&[])?;
        voter.pages.as_mut().unwrap().reclaim(1024)?;
        voter.images.retain_only(&[])?;
        voter.images.reclaim(1024)?;
        if let Some(store) = &voter.extents {
            store.retain_only(&[])?;
            store.reclaim(1024)?;
        }
    }
    assert!(old_page.exists());
    release.send(())?;
    let (prepared, transferred, peak_scratch) = worker.await??;
    assert!(transferred >= payload);
    assert_eq!(peak_scratch, if rooted { 0 } else { image_bytes });
    assert_eq!(source_pool.usage().sources, 0);
    assert_eq!(source_pool.usage().retained_bytes, 0);
    assert_eq!(scratch.usage()?.reserved_bytes, 0);
    assert_eq!(scratch.usage()?.reserved_files, 0);
    assert_eq!(
        pool.usage(),
        receive_usage,
        "prepared closure still owns admission"
    );
    assert_eq!(prepared.marker(), &marker);
    assert_eq!(destination.group_manifest()?.as_ref(), Some(&before));
    let source_voter = &mut cluster.voters[0];
    source_voter.pages.as_mut().unwrap().reclaim(1024)?;
    source_voter.pages.as_mut().unwrap().reclaim(1024)?;
    assert!(
        !old_page.exists(),
        "released transfer no longer pins its old tree"
    );
    source_voter.images.reclaim(1024)?;
    if let Some(store) = &source_voter.extents {
        store.reclaim(1024)?;
    }
    assert!(
        old_image.exists(),
        "reader alone retains its obsolete image"
    );
    cluster
        .domain
        .blocking(move || -> Result<()> {
            // The old physical reader survives even after the transfer and its tree
            // pin are gone. Check all destination images independently, in bounded reads.
            assert_eq!(
                digest(&reader)?,
                <[u8; 32]>::from(Sha256::digest(&expected["alpha"]))
            );
            for (name, bytes) in expected {
                let (leaf, pin) = prepared
                    .manifest()
                    .pin_cell(&CellId::new(name)?)
                    .context("received cell")?;
                assert_eq!(leaf.version.sequence.get(), 1);
                assert_eq!(digest(&pin)?, <[u8; 32]>::from(Sha256::digest(bytes)));
            }
            Ok(())
        })?
        .await??;
    assert_eq!(pool.usage().sources, 0);
    assert_eq!(pool.usage().retained_bytes, 0);
    let source_voter = &mut cluster.voters[0];
    source_voter.images.reclaim(1024)?;
    if let Some(store) = &source_voter.extents {
        store.reclaim(1024)?;
    }
    assert!(
        !old_image.exists(),
        "released reader permits obsolete image reclamation"
    );
    drop(applier);
    cluster.close().await?;
    Ok(())
}
