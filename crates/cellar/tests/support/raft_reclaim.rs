// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared image publication/deletion barriers, capacity and crash boundaries.
use super::*;
use cellar_ltx::generation::GenerationPin;
use fixture::{id, image, load_marker, read, save_marker, Cut};
use std::sync::atomic::Ordering;

fn initial() -> Result<Vec<u8>> {
    image(
        None,
        "CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(1)",
    )
}

fn two_images(store: &GenerationStore, bytes: &[u8]) -> Result<(GenerationPin, GenerationPin)> {
    let first = store.prepare(id(1), None, &Cut::new(&[], bytes, 0, true, true).update())?;
    let next = image(Some(bytes), "UPDATE counter SET value=2")?;
    let second = store.prepare(
        id(2),
        Some(&first),
        &Cut::new(bytes, &next, 1, true, true).update(),
    )?;
    Ok((first, second))
}

#[test]
fn publication_reclaims_without_refunding_before_the_shared_barrier() -> Result<()> {
    let root = tempfile::tempdir()?;
    let bytes = initial()?;
    let faults = fault::Faults::new(root.path().join("images"), None);
    let store = GenerationStore::open(
        faults.directory.clone(),
        Arc::new(fault::FaultFileSystem {
            faults: faults.clone(),
        }),
        GenerationLimits {
            max_generations: 3,
            max_bytes: 4 * bytes.len() as u64,
            ..Default::default()
        },
    )?;
    let (first, second) = two_images(&store, &bytes)?;
    let first_state = first.state();
    store.retire(first.id())?;
    drop(first);
    let old = read(&second)?;
    let next = image(Some(&old), "UPDATE counter SET value=3")?;
    faults.synced_paths.lock().unwrap().clear();
    let inspect = store.clone();
    let base = second.clone();
    let old_bytes = old.clone();
    let new_bytes = next.clone();
    *faults.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
        let usage = inspect.usage().unwrap();
        assert_eq!(usage.generations, 3);
        assert_eq!(usage.bytes, 4 * new_bytes.len() as u64);
        assert_eq!(
            usage.busy, 2,
            "new image and removed image keep their reservations"
        );
        assert!(matches!(
            inspect.adopt(id(1), first_state),
            Err(GenerationError::Busy)
        ));
        assert!(
            matches!(
                inspect.prepare(
                    id(4),
                    Some(&base),
                    &Cut::new(&old_bytes, &new_bytes, 2, true, true).update()
                ),
                Err(GenerationError::Limit)
            ),
            "unsynced deletion must not fund another preparation"
        );
    }));
    let third = store.prepare(
        id(3),
        Some(&second),
        &Cut::new(&old, &next, 2, true, true).update(),
    )?;
    assert_eq!(read(&third)?, next);
    assert_eq!(store.usage()?.generations, 2);
    assert_eq!(store.usage()?.bytes, 2 * next.len() as u64);
    assert_eq!(store.usage()?.busy, 0);
    assert_eq!(
        faults
            .synced_paths
            .lock()
            .unwrap()
            .iter()
            .filter(|p| **p == faults.directory)
            .count(),
        1
    );
    assert_eq!(faults.removed_paths.lock().unwrap().len(), 1);
    assert!(matches!(
        store.adopt(id(1), first_state),
        Err(GenerationError::Missing)
    ));
    Ok(())
}

#[test]
fn admission_reclaims_first_when_capacity_is_tight_but_never_steals_pins() -> Result<()> {
    for by_bytes in [false, true] {
        let root = tempfile::tempdir()?;
        let bytes = initial()?;
        let faults = fault::Faults::new(root.path().join("images"), None);
        let store = GenerationStore::open(
            faults.directory.clone(),
            Arc::new(fault::FaultFileSystem {
                faults: faults.clone(),
            }),
            GenerationLimits {
                max_generations: if by_bytes { 3 } else { 2 },
                max_bytes: (if by_bytes { 3 } else { 4 }) * bytes.len() as u64,
                ..Default::default()
            },
        )?;
        let (first, second) = two_images(&store, &bytes)?;
        store.retire(first.id())?;
        let old = read(&second)?;
        let next = image(Some(&old), "UPDATE counter SET value=3")?;
        let cut = Cut::new(&old, &next, 2, true, true);
        assert!(matches!(
            store.prepare(id(3), Some(&second), &cut.update()),
            Err(GenerationError::Limit)
        ));
        assert_eq!(read(&first)?, bytes);
        assert_eq!(store.usage()?.generations, 2);
        drop(first);
        faults.synced_paths.lock().unwrap().clear();
        let third = store.prepare(id(3), Some(&second), &cut.update())?;
        assert_eq!(read(&third)?, next);
        assert_eq!(store.usage()?.generations, 2);
        assert_eq!(
            faults
                .synced_paths
                .lock()
                .unwrap()
                .iter()
                .filter(|p| **p == faults.directory)
                .count(),
            2,
            "tight admission must sync deletion before reserving and sealing the next image"
        );
    }
    Ok(())
}

#[test]
fn reclamation_batches_both_recovered_names_and_respects_file_budget() -> Result<()> {
    let root = tempfile::tempdir()?;
    let bytes = initial()?;
    let directory = root.path().join("images");
    std::fs::create_dir(&directory)?;
    for generation in 1..=2 {
        for extension in ["stage", "sqlite"] {
            std::fs::write(
                directory.join(format!(
                    "{}.{}",
                    hex::encode(id(generation).bytes()),
                    extension
                )),
                &bytes,
            )?;
        }
    }
    let faults = fault::Faults::new(directory, None);
    let store = GenerationStore::open(
        faults.directory.clone(),
        Arc::new(fault::FaultFileSystem {
            faults: faults.clone(),
        }),
        GenerationLimits::default(),
    )?;
    store.retain_only(&[])?;
    faults.synced_paths.lock().unwrap().clear();
    let report = store.reclaim(3)?;
    assert_eq!(report.files, 3);
    assert_eq!(report.bytes, 3 * bytes.len() as u64);
    assert_eq!(store.usage()?.generations, 1);
    assert_eq!(store.usage()?.bytes, bytes.len() as u64);
    assert_eq!(faults.synced_paths.lock().unwrap().len(), 1);
    assert_eq!(store.reclaim(3)?.files, 1);
    assert_eq!(store.usage()?.generations, 0);
    assert_eq!(faults.synced_paths.lock().unwrap().len(), 2);
    assert_eq!(store.reclaim(3)?.files, 0);
    assert_eq!(
        faults.synced_paths.lock().unwrap().len(),
        2,
        "empty cleanup needs no barrier"
    );
    Ok(())
}

#[test]
fn failed_shared_reclamation_quarantines_and_recovers_only_the_durable_marker() -> Result<()> {
    for mode in [fault::FAIL_REMOVE, fault::FAIL_DIRECTORY_SYNC] {
        let root = tempfile::tempdir()?;
        let bytes = initial()?;
        let faults = fault::Faults::new(root.path().join("images"), None);
        let store = GenerationStore::open(
            faults.directory.clone(),
            Arc::new(fault::FaultFileSystem {
                faults: faults.clone(),
            }),
            GenerationLimits::default(),
        )?;
        let (first, second) = two_images(&store, &bytes)?;
        save_marker(root.path(), &second)?;
        store.retire(first.id())?;
        drop(first);
        let old = read(&second)?;
        let next = image(Some(&old), "UPDATE counter SET value=3")?;
        faults.mode.store(mode, Ordering::Release);
        assert!(store
            .prepare(
                id(3),
                Some(&second),
                &Cut::new(&old, &next, 2, true, true).update()
            )
            .is_err());
        assert!(matches!(store.usage(), Err(GenerationError::Quarantined)));
        assert_eq!(load_marker(root.path())?.0, id(2));
        assert_eq!(read(&second)?, old);
        drop(second);
        drop(store);
        let recovered = GenerationStore::open(
            faults.directory.clone(),
            Arc::new(DirectFileSystem),
            GenerationLimits::default(),
        )?;
        let (marker, state) = load_marker(root.path())?;
        let pin = recovered.adopt(marker, state)?;
        assert_eq!(read(&pin)?, old);
        recovered.retain_only(&[marker])?;
        recovered.reclaim(8)?;
        let next_pin = recovered.prepare(
            id(3),
            Some(&pin),
            &Cut::new(&old, &next, 2, true, true).update(),
        )?;
        assert_eq!(read(&next_pin)?, next);
    }
    Ok(())
}

#[test]
fn process_loss_during_shared_reclamation_preserves_the_authoritative_image() -> Result<()> {
    for mode in [
        fault::AFTER_REMOVE,
        fault::AFTER_DIRECTORY_SYNC,
        fault::AFTER_MARKER_SYNC,
    ] {
        let root = tempfile::tempdir()?;
        let bytes = initial()?;
        let store = GenerationStore::open(
            root.path().join("images"),
            Arc::new(DirectFileSystem),
            GenerationLimits::default(),
        )?;
        let (first, second) = two_images(&store, &bytes)?;
        let old = read(&second)?;
        let next = image(Some(&old), "UPDATE counter SET value=3")?;
        std::fs::write(root.path().join("source.sqlite"), &old)?;
        std::fs::write(root.path().join("next.sqlite"), &next)?;
        save_marker(root.path(), &second)?;
        drop(first);
        drop(second);
        drop(store);

        struct Child(std::process::Child);

        impl Drop for Child {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let log = std::fs::File::create(root.path().join("child.log"))?;
        let mut child = Child(
            std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--ignored",
                    "--exact",
                    "reclaim::shared_reclamation_crash_process",
                    "--nocapture",
                ])
                .env("CELLAR_IMAGE_CRASH_ROOT", root.path())
                .env("CELLAR_IMAGE_CRASH_MODE", mode.to_string())
                .stdout(std::process::Stdio::from(log))
                .stderr(std::process::Stdio::inherit())
                .spawn()?,
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !root.path().join("reached").exists() {
            if child.0.try_wait()?.is_some() || std::time::Instant::now() > deadline {
                anyhow::bail!(
                    "shared cleanup missed crash cut {mode}: {}",
                    std::fs::read_to_string(root.path().join("child.log"))?
                );
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        child.0.kill()?;
        child.0.wait()?;
        let (marker, state) = load_marker(root.path())?;
        assert_eq!(
            marker,
            if mode == fault::AFTER_MARKER_SYNC {
                id(3)
            } else {
                id(2)
            }
        );
        let recovered = GenerationStore::open(
            root.path().join("images"),
            Arc::new(DirectFileSystem),
            GenerationLimits::default(),
        )?;
        let pin = recovered.adopt(marker, state)?;
        assert_eq!(
            read(&pin)?,
            if mode == fault::AFTER_MARKER_SYNC {
                next
            } else {
                old
            }
        );
        recovered.retain_only(&[marker])?;
        recovered.reclaim(8)?;
        assert_eq!(recovered.usage()?.generations, 1);
    }
    Ok(())
}

#[test]
#[ignore = "child entry point invoked and killed by process_loss_during_shared_reclamation_preserves_the_authoritative_image"]
fn shared_reclamation_crash_process() -> Result<()> {
    let root = std::path::PathBuf::from(std::env::var("CELLAR_IMAGE_CRASH_ROOT")?);
    let mode: u8 = std::env::var("CELLAR_IMAGE_CRASH_MODE")?.parse()?;
    let faults = fault::Faults::new(root.join("images"), Some(root.join("reached")));
    let store = GenerationStore::open(
        faults.directory.clone(),
        Arc::new(fault::FaultFileSystem {
            faults: faults.clone(),
        }),
        GenerationLimits::default(),
    )?;
    let (marker, state) = load_marker(&root)?;
    let pin = store.adopt(marker, state)?;
    store.retain_only(&[marker])?;
    let old = std::fs::read(root.join("source.sqlite"))?;
    let next = std::fs::read(root.join("next.sqlite"))?;
    faults.mode.store(mode, Ordering::Release);
    let next_pin = store.prepare(
        id(3),
        Some(&pin),
        &Cut::new(&old, &next, 2, true, true).update(),
    )?;
    save_marker(&root, &next_pin)?;
    if mode == fault::AFTER_MARKER_SYNC {
        faults.pause()?;
    }
    anyhow::bail!("shared reclamation crash fixture did not stop")
}
