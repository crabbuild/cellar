// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Node authority remains durable and visible through every logical client.
#![allow(clippy::disallowed_methods)]
#[path = "../peer_faults/storage.rs"]
mod storage;

use anyhow::{ensure, Result};
use cellar::sdk::ExecutionDomain;
use futures_util::TryStreamExt as _;
use object_store::{path::Path, PutMode, PutOptions, UpdateVersion};
use std::time::Duration;

#[tokio::test]
async fn shared_node_records_progress_with_a_held_data_writer_and_survive_reopen() -> Result<()> {
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let ordinary = storage::open(root.path(), &["owner", "source"])?;
            let leases = ordinary.clone();
            ordinary
                .store
                .put(&Path::from("data/counter"), b"7".to_vec().into())
                .await?;
            let (entered, ready) = tokio::sync::oneshot::channel();
            let (release, released) = std::sync::mpsc::channel();
            let file = root.path().join("fleet.sqlite");
            let held = cellar::asyncrt::try_blocking(move || -> Result<()> {
                let mut connection = rusqlite::Connection::open(file)?;
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let _ = entered.send(());
                released.recv_timeout(Duration::from_secs(5))?;
                transaction.rollback()?;
                Ok(())
            })?;
            ready.await?;
            let outcome = cellar::asyncrt::timeout(Duration::from_secs(1), async {
                for node in ["owner", "source"] {
                    let key = Path::from(format!("nodes/{node}.json"));
                    let logical = format!("nodes/{node}.json");
                    let first = leases.put_cas(&logical, b"initial".to_vec(), None).await?;
                    ensure!(first.is_some());
                    ensure!(ordinary.get(&logical).await?.is_some());
                    let renewed = leases
                        .put_cas(&logical, b"renewed".to_vec(), first.as_deref())
                        .await?;
                    ensure!(renewed.is_some() && renewed != first);
                    ensure!(ordinary.get(&logical).await?.unwrap().0.as_ref() == b"renewed");
                    ensure!(leases
                        .put_cas(&logical, b"wrong".to_vec(), Some("wrong"))
                        .await?
                        .is_none());
                    leases.store.delete(&key).await?;
                    let receipt = leases
                        .cas_store
                        .put_opts(
                            &key,
                            node.as_bytes().to_vec().into(),
                            PutOptions {
                                mode: PutMode::Create,
                                ..Default::default()
                            },
                        )
                        .await?;
                    let read = ordinary.store.get(&key).await?;
                    ensure!(read.meta.e_tag == receipt.e_tag);
                    ensure!(read.bytes().await?.as_ref() == node.as_bytes());
                    ensure!(matches!(
                        ordinary
                            .cas_store
                            .put_opts(
                                &key,
                                b"forged".to_vec().into(),
                                PutOptions {
                                    mode: PutMode::Update(UpdateVersion {
                                        e_tag: Some("wrong".into()),
                                        version: None
                                    }),
                                    ..Default::default()
                                }
                            )
                            .await,
                        Err(object_store::Error::Precondition { .. })
                    ));
                }
                let nodes = ordinary
                    .store
                    .list(Some(&Path::from("nodes/")))
                    .try_collect::<Vec<_>>()
                    .await?;
                ensure!(nodes.len() == 2);
                ensure!(
                    ordinary
                        .store
                        .get(&Path::from("data/counter"))
                        .await?
                        .bytes()
                        .await?
                        .as_ref()
                        == b"7"
                );
                Ok::<_, anyhow::Error>(())
            })
            .await;
            // Release and join the native writer before propagating any failure.
            let _ = release.send(());
            held.await??;
            outcome??;
            drop(leases);
            drop(ordinary);
            let reopened = storage::open(root.path(), &["source", "owner"])?;
            for node in ["owner", "source"] {
                let key = Path::from(format!("nodes/{node}.json"));
                ensure!(reopened.store.get(&key).await?.bytes().await?.as_ref() == node.as_bytes());
                ensure!(matches!(
                    reopened
                        .store
                        .put_opts(
                            &key,
                            b"duplicate".to_vec().into(),
                            PutOptions {
                                mode: PutMode::Create,
                                ..Default::default()
                            }
                        )
                        .await,
                    Err(object_store::Error::AlreadyExists { .. })
                ));
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result??;
    Ok(())
}

#[tokio::test]
async fn missing_canonical_node_record_never_falls_back_to_a_stale_data_copy() -> Result<()> {
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let ordinary = storage::open(root.path(), &["owner"])?;
            let key = Path::from("nodes/owner.json");
            ordinary.store.put(&key, b"current".to_vec().into()).await?;
            let data = cellar::bucket::Bucket::open_dev(&root.path().join("fleet.sqlite"))?;
            data.store.put(&key, b"stale".to_vec().into()).await?;
            ensure!(ordinary.store.get(&key).await?.bytes().await?.as_ref() == b"current");
            ensure!(
                ordinary
                    .store
                    .list(None)
                    .try_collect::<Vec<_>>()
                    .await?
                    .len()
                    == 1
            );
            let page = ordinary.common_prefixes_page("", None, None, 1).await?;
            ensure!(page.prefixes == vec!["nodes".to_string()]);
            ordinary.store.delete(&key).await?;
            ensure!(matches!(
                ordinary.store.get(&key).await,
                Err(object_store::Error::NotFound { .. })
            ));
            ensure!(ordinary
                .store
                .list(None)
                .try_collect::<Vec<_>>()
                .await?
                .is_empty());
            let page = ordinary.common_prefixes_page("", None, None, 1).await?;
            ensure!(page.prefixes.is_empty() && page.page_token.is_none());
            let listing = ordinary.store.list_with_delimiter(None).await?;
            ensure!(listing.objects.is_empty() && listing.common_prefixes.is_empty());
            drop(ordinary);
            let reopened = storage::open(root.path(), &["owner"])?;
            ensure!(matches!(
                reopened.store.get(&key).await,
                Err(object_store::Error::NotFound { .. })
            ));
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result??;
    Ok(())
}

#[tokio::test]
async fn cross_shard_copy_keeps_conditional_creation_and_logical_listing() -> Result<()> {
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let bucket = storage::open(root.path(), &["owner", "source"])?;
            let data = Path::from("data/original");
            let owner = Path::from("nodes/owner.json");
            let source = Path::from("nodes/source.json");
            bucket.store.put(&data, b"original".to_vec().into()).await?;
            bucket.store.copy_if_not_exists(&data, &owner).await?;
            bucket.store.copy(&owner, &source).await?;
            bucket.store.put(&data, b"changed".to_vec().into()).await?;
            ensure!(matches!(
                bucket.store.copy_if_not_exists(&data, &owner).await,
                Err(object_store::Error::AlreadyExists { .. })
            ));
            for node in [&owner, &source] {
                ensure!(bucket.store.get(node).await?.bytes().await?.as_ref() == b"original");
            }
            let nodes = bucket
                .store
                .list_with_delimiter(Some(&Path::from("nodes")))
                .await?;
            ensure!(nodes.objects.len() == 2 && nodes.common_prefixes.is_empty());
            let roots = bucket.store.list_with_delimiter(None).await?;
            ensure!(
                roots.objects.is_empty()
                    && roots.common_prefixes == vec![Path::from("data"), Path::from("nodes")]
            );
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result??;
    Ok(())
}
