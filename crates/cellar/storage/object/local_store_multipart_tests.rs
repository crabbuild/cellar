// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(clippy::disallowed_methods)]

use super::*;
use crate::sdk::ExecutionDomain;
use anyhow::{ensure, Result};
use object_store::multipart::{MultipartStore, PartId};

#[tokio::test]
async fn receipts_replacement_and_failed_completion_preserve_the_upload() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("objects.sqlite");
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let key = Path::from("blob/data/object");
            let wrong = Path::from("blob/other/object");
            let store = LocalStore::open(&path)?;
            store
                .put_opts(&key, b"original".to_vec().into(), PutOptions::default())
                .await?;
            let mut attributes = Attributes::new();
            attributes.insert(Attribute::ContentType, "application/zstd".into());
            let id = store
                .create_multipart_opts(
                    &key,
                    PutMultipartOptions {
                        attributes,
                        ..Default::default()
                    },
                )
                .await?;
            let two =
                MultipartStore::put_part(&store, &key, &id, 1, b"second".to_vec().into()).await?;
            let obsolete =
                MultipartStore::put_part(&store, &key, &id, 0, b"obsolete".to_vec().into()).await?;
            let one =
                MultipartStore::put_part(&store, &key, &id, 0, b"first".to_vec().into()).await?;
            ensure!(!one.content_id.is_empty() && one.content_id != obsolete.content_id);
            ensure!(store
                .complete_multipart(&key, &id, vec![obsolete, two.clone()])
                .await
                .is_err());
            ensure!(
                store
                    .get_opts(&key, GetOptions::default())
                    .await?
                    .bytes()
                    .await?
                    == b"original"[..]
            );
            ensure!(
                MultipartStore::put_part(&store, &wrong, &id, 0, b"foreign".to_vec().into())
                    .await
                    .is_err()
            );
            ensure!(store.abort_multipart(&wrong, &id).await.is_err());
            ensure!(store
                .complete_multipart(&key, &id, Vec::new())
                .await
                .is_err());
            ensure!(
                MultipartStore::put_part(&store, &key, &id, 10_000, Bytes::new().into())
                    .await
                    .is_err()
            );
            let mut forged = two.clone();
            forged.content_id = "forged".into();
            ensure!(store
                .complete_multipart(&key, &id, vec![one.clone(), forged])
                .await
                .is_err());
            store.complete_multipart(&key, &id, vec![one, two]).await?;
            let object = store.get_opts(&key, GetOptions::default()).await?;
            ensure!(object
                .attributes
                .get(&Attribute::ContentType)
                .is_some_and(|value| value.as_ref() == "application/zstd"));
            ensure!(object.bytes().await? == b"firstsecond"[..]);
            ensure!(
                MultipartStore::put_part(&store, &key, &id, 0, Bytes::new().into())
                    .await
                    .is_err()
            );
            store.abort_multipart(&key, &id).await?;
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result??;
    Ok(())
}

#[tokio::test]
async fn upload_parts_survive_a_separate_process_and_complete_atomically() -> Result<()> {
    const CHILD: &str = "CELLAR_TEST_MULTIPART_REOPEN";
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    if let Some(path) = std::env::var_os(CHILD) {
        let id = std::env::var("CELLAR_TEST_MULTIPART_ID")?;
        let receipt = std::env::var("CELLAR_TEST_MULTIPART_RECEIPT")?;
        let result = domain
            .run(async {
                let store = LocalStore::open(path)?;
                let key = Path::from("blob/data/restarted");
                ensure!(
                    store
                        .get_opts(&key, GetOptions::default())
                        .await?
                        .bytes()
                        .await?
                        == b"previous"[..]
                );
                let one = MultipartStore::put_part(&store, &key, &id, 0, b"after-".to_vec().into())
                    .await?;
                store
                    .complete_multipart(
                        &key,
                        &id,
                        vec![
                            one,
                            PartId {
                                content_id: receipt,
                            },
                        ],
                    )
                    .await?;
                ensure!(
                    store
                        .get_opts(&key, GetOptions::default())
                        .await?
                        .bytes()
                        .await?
                        == b"after-restart"[..]
                );
                Ok::<_, anyhow::Error>(())
            })
            .await;
        domain.shutdown().await;
        result??;
        return Ok(());
    }
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("objects.sqlite");
    let prepared = domain
        .run(async {
            let store = LocalStore::open(&path)?;
            let key = Path::from("blob/data/restarted");
            store
                .put_opts(&key, b"previous".to_vec().into(), PutOptions::default())
                .await?;
            let id = store.create_multipart(&key).await?;
            let two =
                MultipartStore::put_part(&store, &key, &id, 1, b"restart".to_vec().into()).await?;
            Ok::<_, anyhow::Error>((id, two.content_id))
        })
        .await;
    domain.shutdown().await;
    let (id, receipt) = prepared??;
    // A newly executed process cannot recover an in-memory upload registry.
    let child = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            &format!(
                "{}::upload_parts_survive_a_separate_process_and_complete_atomically",
                module_path!().split_once("::").unwrap().1
            ),
            "--test-threads=1",
        ])
        .env(CHILD, &path)
        .env("CELLAR_TEST_MULTIPART_ID", id)
        .env("CELLAR_TEST_MULTIPART_RECEIPT", receipt)
        .output()?;
    ensure!(
        child.status.success()
            && String::from_utf8_lossy(&child.stdout).contains("1 passed; 0 failed"),
        "multipart restart child failed: {}",
        String::from_utf8_lossy(&child.stderr)
    );
    let store = LocalStore::open(&path)?;
    ensure!(store.read("blob/data/restarted")?.body == b"after-restart");
    let connection = store.connect()?;
    for table in ["__sys_local_uploads", "__sys_local_parts"] {
        ensure!(
            connection.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| row
                .get::<_, u64>(0))?
                == 0
        );
    }
    Ok(())
}

#[tokio::test]
async fn completion_and_abort_from_independent_clients_have_one_atomic_result() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("objects.sqlite");
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let store = LocalStore::open(&path)?;
            let other = LocalStore::open(&path)?;
            let key = Path::from("blob/data/race");
            for _ in 0..8 {
                store
                    .put_opts(&key, b"previous".to_vec().into(), PutOptions::default())
                    .await?;
                let id = store.create_multipart(&key).await?;
                let payload = vec![0x5a; READ_CHUNK_BYTES * 3 + 17];
                let part =
                    MultipartStore::put_part(&store, &key, &id, 0, payload.clone().into()).await?;
                let (completed, aborted) = futures_util::future::join(
                    store.complete_multipart(&key, &id, vec![part]),
                    other.abort_multipart(&key, &id),
                )
                .await;
                aborted?;
                let actual = store
                    .get_opts(&key, GetOptions::default())
                    .await?
                    .bytes()
                    .await?;
                ensure!(
                    actual.as_ref()
                        == if completed.is_ok() {
                            payload.as_slice()
                        } else {
                            b"previous"
                        }
                );
            }
            let connection = store.connect()?;
            ensure!(
                connection.query_row("SELECT count(*) FROM __sys_local_parts", [], |row| row
                    .get::<_, u64>(0))?
                    == 0
            );
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result??;
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn completion_streams_persisted_parts_without_collecting_the_object() -> Result<()> {
    const CHILD: &str = "CELLAR_TEST_MULTIPART_RSS_PATH";
    const PART_BYTES: usize = 8 << 20;
    const PARTS: usize = 8;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    if let Some(path) = std::env::var_os(CHILD) {
        let id = std::env::var("CELLAR_TEST_MULTIPART_ID")?;
        let receipts: Vec<String> =
            serde_json::from_str(&std::env::var("CELLAR_TEST_MULTIPART_RECEIPTS")?)?;
        let result = domain.run(async {
            let store = LocalStore::open(path)?;
            let key = Path::from("blob/data/large");
            let baseline = crate::memory::sample().rss_bytes;
            ensure!(baseline > 0, "multipart RSS baseline was unavailable");
            let mut peak = baseline;
            let mut publication = Box::pin(store.complete_multipart(&key, &id, receipts.into_iter().map(|content_id| PartId { content_id }).collect()));
            loop {
                crate::asyncrt::select_biased! { "observe peak memory while the native SQLite worker publishes parts";
                    result = &mut publication => { result?; break; },
                    _ = crate::asyncrt::sleep(Duration::from_millis(1)) => {
                        peak = peak.max(crate::memory::sample().rss_bytes);
                    },
                }
            }
            peak = peak.max(crate::memory::sample().rss_bytes);
            let mut total = 0;
            let mut body = store.get_opts(&key, GetOptions::default()).await?.into_stream();
            while let Some(chunk) = body.next().await {
                let chunk = chunk?;
                ensure!(chunk.len() <= READ_CHUNK_BYTES && chunk.iter().all(|byte| *byte == 0x5a));
                total += chunk.len();
            }
            ensure!(total == PART_BYTES * PARTS);
            let growth = peak.saturating_sub(baseline);
            eprintln!("multipart completion: object_bytes={total}, baseline={baseline}, peak={peak}, growth={growth}");
            ensure!(growth <= 16 << 20, "multipart completion exceeded 16 MiB RSS growth: {growth}");
            Ok::<_, anyhow::Error>(())
        }).await;
        domain.shutdown().await;
        result??;
        return Ok(());
    }
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("objects.sqlite");
    let prepared = domain
        .run(async {
            let store = LocalStore::open(&path)?;
            let key = Path::from("blob/data/large");
            let id = store.create_multipart(&key).await?;
            let mut receipts = Vec::new();
            for index in 0..PARTS {
                let part = MultipartStore::put_part(
                    &store,
                    &key,
                    &id,
                    index,
                    vec![0x5a; PART_BYTES].into(),
                )
                .await?;
                receipts.push(part.content_id);
            }
            Ok::<_, anyhow::Error>((id, receipts))
        })
        .await;
    domain.shutdown().await;
    let (id, receipts) = prepared??;
    let child = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            &format!(
                "{}::completion_streams_persisted_parts_without_collecting_the_object",
                module_path!().split_once("::").unwrap().1
            ),
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, path)
        .env("CELLAR_TEST_MULTIPART_ID", id)
        .env(
            "CELLAR_TEST_MULTIPART_RECEIPTS",
            serde_json::to_string(&receipts)?,
        )
        .output()?;
    ensure!(
        child.status.success()
            && String::from_utf8_lossy(&child.stdout).contains("1 passed; 0 failed")
            && String::from_utf8_lossy(&child.stderr).contains("object_bytes=67108864"),
        "multipart memory child failed: {}",
        String::from_utf8_lossy(&child.stderr)
    );
    eprintln!("{}", String::from_utf8_lossy(&child.stderr));
    Ok(())
}
