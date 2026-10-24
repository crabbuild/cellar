//! Bounded physical replay batches retain every intermediate image proof.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_ltx_fault.rs"]
#[allow(dead_code)]
mod fault;
#[path = "support/raft_ltx_fixture.rs"]
#[allow(dead_code)]
mod fixture;

use anyhow::Result;
use cellar_ltx::apply::ApplyError;
use cellar_ltx::generation::{GenerationError, GenerationLimits, GenerationStore};
use cellar_ltx::DirectFileSystem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn history(checksums: bool) -> Result<(Vec<fixture::Cut>, Vec<u8>)> {
    let first = fixture::image(
        None,
        "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(1)",
    )?;
    let second = fixture::image(Some(&first), "UPDATE counter SET value=2")?;
    let third = fixture::image(Some(&second), "UPDATE counter SET value=3")?;
    Ok((
        vec![
            fixture::Cut::new(&[], &first, 0, checksums, true),
            fixture::Cut::new(&first, &second, 1, checksums, false),
            fixture::Cut::new(&second, &third, 2, checksums, true),
        ],
        third,
    ))
}

#[test]
fn batch_replay_checks_intermediate_digests_before_later_overwrites() -> Result<()> {
    for checksums in [false, true] {
        let root = tempfile::tempdir()?;
        let directory = root.path().join("images");
        let faults = fault::Faults::new(directory.clone(), None);
        let store = GenerationStore::open(
            directory,
            Arc::new(fault::FaultFileSystem {
                faults: faults.clone(),
            }),
            GenerationLimits::default(),
        )?;
        let (cuts, expected) = history(checksums)?;
        let mut updates: Vec<_> = cuts.iter().map(fixture::Cut::update).collect();
        // A metadata-contiguous chain with a false intermediate image digest
        // must fail even though the final entry overwrites it with valid bytes.
        updates[1].result.digest[0] ^= 1;
        updates[2].base.digest = updates[1].result.digest;
        faults.events.lock().unwrap().clear();
        assert!(matches!(
            store.prepare_chain(fixture::id(1), None, &updates),
            Err(GenerationError::Apply(ApplyError::ResultDigest))
        ));
        assert_eq!(store.usage()?.generations, 0);
        assert!(!faults.events.lock().unwrap().contains(&"image-sync"));

        let updates: Vec<_> = cuts.iter().map(fixture::Cut::update).collect();
        faults.events.lock().unwrap().clear();
        let creates_before = faults.image_creates.load(Ordering::Acquire);
        let observed_store = store.clone();
        let expected_state = cuts.last().unwrap().result;
        let barrier_observed = Arc::new(AtomicBool::new(false));
        let observed = barrier_observed.clone();
        *faults.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
            assert!(
                matches!(
                    observed_store.adopt(fixture::id(1), expected_state),
                    Err(GenerationError::Busy)
                ),
                "a verified batch must remain inaccessible before directory sync"
            );
            observed.store(true, Ordering::Release);
        }));
        let pin = store.prepare_chain(fixture::id(1), None, &updates)?;
        assert_eq!(fixture::read(&pin)?, expected);
        assert!(barrier_observed.load(Ordering::Acquire));
        // The exclusive final name is not publication authority. Both sync
        // barriers must complete before the batch returns its only image pin.
        assert_eq!(
            *faults.events.lock().unwrap(),
            ["image-sync", "directory-sync"]
        );
        assert_eq!(
            faults.image_creates.load(Ordering::Acquire) - creates_before,
            1
        );
        assert_eq!(faults.image_renames.load(Ordering::Acquire), 0);
    }
    Ok(())
}

#[test]
fn batch_replay_preserves_the_pinned_base_and_refuses_chain_gaps() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = GenerationStore::open(
        root.path().join("images"),
        Arc::new(DirectFileSystem),
        GenerationLimits::default(),
    )?;
    let (cuts, expected) = history(false)?;
    let base = store.prepare(fixture::id(1), None, &cuts[0].update())?;
    let original = fixture::read(&base)?;
    let mut updates: Vec<_> = cuts[1..].iter().map(fixture::Cut::update).collect();
    updates[1].base.digest[0] ^= 1;
    assert!(matches!(
        store.prepare_chain(fixture::id(2), Some(&base), &updates),
        Err(GenerationError::Apply(ApplyError::Chain))
    ));
    assert_eq!(store.usage()?.generations, 1);
    let updates: Vec<_> = cuts[1..].iter().map(fixture::Cut::update).collect();
    let final_pin = store.prepare_chain(fixture::id(2), Some(&base), &updates)?;
    assert_eq!(fixture::read(&final_pin)?, expected);
    assert_eq!(fixture::read(&base)?, original);
    Ok(())
}

#[test]
fn batch_replay_enforces_aggregate_file_byte_and_page_limits() -> Result<()> {
    let (cuts, _) = history(true)?;
    let updates: Vec<_> = cuts.iter().map(fixture::Cut::update).collect();
    for bound in 0..4 {
        let root = tempfile::tempdir()?;
        let mut limits = GenerationLimits::default();
        match bound {
            0 => limits.apply.max_files = 2,
            1 => {
                limits.apply.max_total_encoded_bytes =
                    cuts.iter().map(|cut| cut.bytes.len()).max().unwrap()
            }
            2 => {
                limits.apply.max_total_decoded_bytes = cuts
                    .iter()
                    .map(|cut| cut.pages.len() as u64 * 512)
                    .max()
                    .unwrap()
            }
            _ => {
                limits.apply.max_total_pages = cuts.iter().map(|cut| cut.pages.len()).max().unwrap()
            }
        }
        let store = GenerationStore::open(
            root.path().join("images"),
            Arc::new(DirectFileSystem),
            limits,
        )?;
        assert!(
            matches!(
                store.prepare_chain(fixture::id(1), None, &updates),
                Err(GenerationError::Apply(ApplyError::Limit))
            ),
            "batch ignored aggregate bound {bound}"
        );
        assert_eq!(store.usage()?.generations, 0);
    }
    Ok(())
}

#[test]
fn batch_replay_reserves_peak_growth_before_truncating_to_the_final_image() -> Result<()> {
    for checksums in [false, true] {
        let initial = fixture::image(
            None,
            "CREATE TABLE payload(value BLOB); INSERT INTO payload VALUES(zeroblob(4096))",
        )?;
        let grown = fixture::image(
            Some(&initial),
            "INSERT INTO payload VALUES(zeroblob(32768))",
        )?;
        let shrunk = fixture::image(Some(&grown), "DELETE FROM payload; VACUUM")?;
        assert!(grown.len() > initial.len() && grown.len() > shrunk.len());
        let cuts = [
            fixture::Cut::new(&[], &initial, 0, checksums, true),
            fixture::Cut::new(&initial, &grown, 1, checksums, false),
            fixture::Cut::new(&grown, &shrunk, 2, checksums, true),
        ];
        let updates: Vec<_> = cuts.iter().map(fixture::Cut::update).collect();
        for enough in [false, true] {
            let root = tempfile::tempdir()?;
            let limits = GenerationLimits {
                max_generations: 1,
                max_bytes: 2 * grown.len() as u64 - u64::from(!enough),
                ..Default::default()
            };
            let store = GenerationStore::open(
                root.path().join("images"),
                Arc::new(DirectFileSystem),
                limits,
            )?;
            let result = store.prepare_chain(fixture::id(1), None, &updates);
            if enough {
                assert_eq!(fixture::read(&result?)?, shrunk);
            } else {
                assert!(matches!(result, Err(GenerationError::Limit)));
                assert_eq!(store.usage()?.generations, 0);
            }
        }
    }
    Ok(())
}

#[test]
fn batch_replay_sync_failure_keeps_the_original_pin_and_refunds_capacity() -> Result<()> {
    use std::sync::atomic::Ordering;
    for mode in [fault::FAIL_IMAGE_SYNC, fault::FAIL_DIRECTORY_SYNC] {
        let root = tempfile::tempdir()?;
        let directory = root.path().join("images");
        let faults = fault::Faults::new(directory.clone(), None);
        let store = GenerationStore::open(
            directory,
            Arc::new(fault::FaultFileSystem {
                faults: faults.clone(),
            }),
            GenerationLimits::default(),
        )?;
        let (cuts, expected) = history(false)?;
        let base = store.prepare(fixture::id(1), None, &cuts[0].update())?;
        let original = fixture::read(&base)?;
        let usage = store.usage()?;
        let updates: Vec<_> = cuts[1..].iter().map(fixture::Cut::update).collect();
        faults.mode.store(mode, Ordering::Release);
        assert!(store
            .prepare_chain(fixture::id(2), Some(&base), &updates)
            .is_err());
        assert_eq!(store.usage()?, usage);
        assert_eq!(fixture::read(&base)?, original);
        let pin = store.prepare_chain(fixture::id(2), Some(&base), &updates)?;
        assert_eq!(fixture::read(&pin)?, expected);
        assert_eq!(fixture::read(&base)?, original);
    }
    Ok(())
}
