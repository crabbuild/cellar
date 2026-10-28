// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical replay amplification, independently of encoded tail byte limits.
use super::*;

fn restored_images(set: &ArchiveRecoverySet, expected: &[u8], value: i64) -> Result<usize> {
    let root = tempfile::tempdir()?;
    let faults = image_fault::Faults::new(root.path().join("images"), None);
    let images = GenerationStore::open(
        faults.directory.clone(),
        Arc::new(image_fault::FaultFileSystem {
            faults: faults.clone(),
        }),
        GenerationLimits::default(),
    )?;
    faults.events.lock().unwrap().clear();
    let pin = set.restore(&images, images::id(95))?;
    ensure!(images::read(&pin)? == expected);
    let query = images::query(pin)?;
    ensure!(
        query
            .connection()
            .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?
            == value
    );
    let seals = faults
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| **event == "image-sync")
        .count();
    Ok(seals)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_budget_rotates_a_tail_before_advertising_an_extra_image_rewrite() -> Result<()> {
    for max_replay_images in [1, 2] {
        let mut fixture = Fixture::new(ArchiveLimits {
            max_tail_image_ratio: 128,
            max_replay_images,
            ..Default::default()
        })
        .await?;
        let first = fixture.commit(1, vec![]).await?;
        fixture.publish(first).await?;
        let mut latest = None;
        for value in 2..=65 {
            latest = Some(fixture.commit(value, vec![]).await?);
        }
        let set = fixture
            .publish(latest.context("missing committed tail")?)
            .await?;
        let tails = set.tail_entries();
        let seals = restored_images(&set, &fixture.bytes, 65)?;
        fixture.close().await?;
        ensure!(
            tails == if max_replay_images == 1 { 0 } else { 64 },
            "budget {max_replay_images} advertised {tails} tail entries"
        );
        ensure!(
            seals == max_replay_images as usize,
            "budget did not change physical restore work"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_budget_can_checkpoint_the_same_cut_after_rechecking_prior_objects() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits {
        max_tail_image_ratio: 128,
        max_replay_images: 2,
        ..Default::default()
    })
    .await?;
    let first = fixture.commit(1, vec![]).await?;
    fixture.publish(first.clone()).await?;
    let mut latest = None;
    for value in 2..=65 {
        latest = Some(fixture.commit(value, vec![]).await?);
    }
    let cut = latest.context("missing committed tail")?;
    let before = fixture.publish(cut.clone()).await?;
    ensure!(before.tail_entries() == 64);
    let archive = fixture.namespace.cell_archive(
        &fixture.birth,
        LtxHost::default(),
        ArchiveLimits {
            max_tail_image_ratio: 128,
            max_replay_images: 1,
            ..Default::default()
        },
    )?;
    // A stale capture cannot replace a newer committed cut with its own bytes.
    let covered = archive.publish(first, &fixture.verifier).await?;
    ensure!(covered.reference() == before.reference());
    let objects = fixture
        .store
        .inner
        .list(None)
        .try_collect::<Vec<_>>()
        .await?;
    for kind in ["/snapshots/", "/tails/"] {
        let object = objects
            .iter()
            .find(|object| object.location.as_ref().contains(kind))
            .context("retained archive payload")?;
        let bytes = fixture
            .store
            .inner
            .get(&object.location)
            .await?
            .bytes()
            .await?;
        for missing in [true, false] {
            if missing {
                fixture.store.inner.delete(&object.location).await?;
            } else {
                let mut corrupt = bytes.to_vec();
                corrupt[0] ^= 1;
                fixture
                    .store
                    .inner
                    .put(&object.location, corrupt.into())
                    .await?;
            }
            ensure!(matches!(
                archive.publish(cut.clone(), &fixture.verifier).await,
                Err(ArchiveError::Corrupt(_))
            ));
            ensure!(
                archive
                    .recovery_metadata()
                    .await?
                    .context("prior head disappeared")?
                    .reference()
                    == before.reference()
            );
            fixture
                .store
                .inner
                .put(&object.location, bytes.clone().into())
                .await?;
        }
    }
    let after = archive.publish(cut.clone(), &fixture.verifier).await?;
    ensure!(after.reference() != before.reference() && after.position() == before.position());
    ensure!(after.tail_entries() == 0 && restored_images(&after, &fixture.bytes, 65)? == 1);
    ensure!(archive.publish(cut, &fixture.verifier).await?.reference() == after.reference());
    fixture.close().await
}

#[test]
fn replay_image_budget_has_explicit_bounds() {
    for max_replay_images in [0, 1, 2, 4096, 4097, u32::MAX] {
        let result = ArchiveLimits {
            max_replay_images,
            ..Default::default()
        }
        .validate();
        assert_eq!(result.is_ok(), (1..=4096).contains(&max_replay_images));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_budget_never_checkpoints_over_an_invalid_intermediate_image() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits {
        max_tail_image_ratio: 128,
        max_replay_images: 1,
        ..Default::default()
    })
    .await?;
    let first = fixture.commit(1, vec![]).await?;
    let initial = fixture.publish(first).await?;
    for value in 2..=63 {
        fixture.commit(value, vec![]).await?;
    }
    let middle = images::image(Some(&fixture.bytes), "UPDATE counter SET value=64")?;
    let mut false_digest = ImageCommitment::Sha256([0; 32]);
    fixture
        .commit_bytes(middle, vec![], |update| {
            let ImageCommitment::Sha256(digest) = &mut update.result.commitment else {
                panic!("materialized fixture returned a root commitment");
            };
            digest[0] ^= 1;
            false_digest = update.result.commitment;
        })
        .await?;
    let final_image = images::image(Some(&fixture.bytes), "UPDATE counter SET value=65")?;
    let cut = fixture
        .commit_bytes(final_image, vec![], |update| {
            update.base.commitment = false_digest;
        })
        .await?;
    let failed = fixture.publish(cut).await;
    ensure!(matches!(
        failed,
        Err(ArchiveError::Images(
            cellar_ltx::generation::GenerationError::Apply(
                cellar_ltx::apply::ApplyError::ResultDigest
            )
        ))
    ));
    let selected = fixture
        .archive
        .recovery_set()
        .await?
        .context("prior head disappeared")?;
    ensure!(selected.reference() == initial.reference());
    restored(&selected, 1)?;
    fixture.close().await
}
