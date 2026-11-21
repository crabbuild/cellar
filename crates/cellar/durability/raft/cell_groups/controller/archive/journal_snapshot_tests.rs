// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::{cell_group::*, consensus::*};

#[path = "journal_test_fs.rs"]
mod filesystem;
use filesystem::{Filesystem, Phase};

#[allow(clippy::disallowed_methods)]
async fn with_journal(
    seed: bool,
    test: impl FnOnce(Arc<Journal>, Arc<Filesystem>, CellGroupArchiveIntent) -> anyhow::Result<()>
        + Send
        + 'static,
) -> anyhow::Result<()> {
    let domain = crate::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let history = GroupHistory {
        cluster: ClusterId::new([61; 16])?,
        namespace: NamespaceId::new([62; 16])?,
        group: GroupId::new([63; 16])?,
        incarnation: IncarnationId::new([64; 16])?,
    };
    let archive = CellGroupArchivePool::new(Default::default())?.for_group(
        Arc::new(object_store::memory::InMemory::new()),
        object_store::path::Path::from("journal-snapshot"),
        history,
    )?;
    let prepared = archive
        .prepare_registry_creation(
            &domain,
            GroupRoute {
                history,
                range: CellRange::all(),
                epoch: RouteEpoch::new(1)?,
                birth_digest: [65; 32],
                schema_digest: [66; 32],
            },
        )
        .await?;
    let creation = prepared.intent().clone();
    let registry = prepared.commit(&domain).await?;
    let intent = registry.prepare_upload(&domain).await?;
    let disk = tempfile::tempdir()?;
    let path = disk.path().join("owner");
    let tested = domain
        .blocking(move || -> anyhow::Result<()> {
            let fs = Arc::new(Filesystem::new(path.join("state")));
            let journal = Arc::new(if seed {
                Journal::open_seed(fs.clone(), path, [67; 16], creation)?
            } else {
                Journal::open(fs.clone(), path, [67; 16], creation)?
            });
            test(journal, fs, intent)
        })?
        .await;
    domain.shutdown().await;
    tested??;
    Ok(())
}

fn record(intent: &CellGroupArchiveIntent) -> Record {
    Record {
        pending: Pending::Reserve(intent.clone()),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn journal_load_selects_complete_generation_replaced_before_open() -> anyhow::Result<()> {
    for seed in [false, true] {
        with_journal(seed, |journal, fs, intent| {
            let empty = Record::default();
            let pending = record(&intent);
            assert_ne!(encode(&empty)?.len(), encode(&pending)?.len());
            for (previous, next) in [(empty.clone(), pending.clone()), (pending, empty)] {
                journal.store(&previous)?;
                let publisher = Arc::downgrade(&journal);
                let expected = next.clone();
                fs.arm(Phase::BeforeOpen, move || {
                    publisher
                        .upgrade()
                        .unwrap()
                        .store(&next)
                        .map_err(std::io::Error::other)
                });
                let selected = journal.load()?;
                assert_eq!(encode(&selected)?, encode(&expected)?);
            }
            Ok(())
        })
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn journal_load_keeps_open_generation_during_later_publication() -> anyhow::Result<()> {
    for seed in [false, true] {
        with_journal(seed, |journal, fs, intent| {
            let previous = record(&intent);
            journal.store(&previous)?;
            let publisher = Arc::downgrade(&journal);
            fs.arm(Phase::AfterOpen, move || {
                publisher
                    .upgrade()
                    .unwrap()
                    .store(&Record::default())
                    .map_err(std::io::Error::other)
            });
            assert_eq!(encode(&journal.load()?)?, encode(&previous)?);
            assert_eq!(encode(&journal.load()?)?, encode(&Record::default())?);
            Ok(())
        })
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn journal_load_bounds_replacement_descriptor_before_reading() -> anyhow::Result<()> {
    for seed in [false, true] {
        with_journal(seed, |journal, fs, intent| {
            for length in [0, LIMIT + 1] {
                journal.store(&record(&intent))?;
                let publisher = fs.clone();
                fs.arm(Phase::BeforeOpen, move || {
                    publisher.replace(&vec![b' '; length])
                });
                assert!(matches!(journal.load(), Err(Error::Invalid(_))));
                assert!(
                    fs.reads().is_empty(),
                    "invalid descriptor consumed journal bytes"
                );
            }
            Ok(())
        })
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn journal_load_rejects_foreign_or_noncanonical_replacement() -> anyhow::Result<()> {
    for seed in [false, true] {
        with_journal(seed, |journal, fs, intent| {
            let mut engine = journal.identity.clone();
            engine.engine = [68; 16];
            let mut purpose = journal.identity.clone();
            purpose.domain = "foreign-journal-1".into();
            let mut history = journal.identity.clone();
            let mut creation = serde_json::to_value(&history.creation)?;
            creation["group"] = serde_json::to_value([69_u8; 16])?;
            history.creation = serde_json::from_value(creation)?;
            assert_ne!(
                history.creation.route().history,
                journal.identity.creation.route().history
            );
            let pending = record(&intent);
            let mut noncanonical = encode(&Envelope {
                identity: journal.identity.clone(),
                record: pending.clone(),
            })?;
            noncanonical.push(b'\n');
            let replacements = [
                encode(&Envelope {
                    identity: engine,
                    record: pending.clone(),
                })?,
                encode(&Envelope {
                    identity: purpose,
                    record: pending.clone(),
                })?,
                encode(&Envelope {
                    identity: history,
                    record: pending.clone(),
                })?,
                noncanonical,
                b"{".to_vec(),
            ];
            for bytes in replacements {
                journal.store(&pending)?;
                let publisher = fs.clone();
                fs.arm(Phase::BeforeOpen, move || publisher.replace(&bytes));
                assert!(matches!(journal.load(), Err(Error::Invalid(_))));
                assert!(fs.reads().iter().all(|n| *n <= LIMIT));
            }
            Ok(())
        })
        .await?;
    }
    Ok(())
}
