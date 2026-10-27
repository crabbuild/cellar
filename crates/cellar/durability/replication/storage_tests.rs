// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::disallowed_methods)] // The external driver deliberately has no Cellar domain.
async fn default_paged_reader_retains_its_owner_and_refuses_after_owner_closure(
) -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let bytes: Vec<u8> = (0..256 * 1024).map(|n| (n % 251) as u8).collect();
    let file = cellar_ltx::FileInfo {
        min_txid: cellar_ltx::TXID(1),
        max_txid: cellar_ltx::TXID(1),
        size: bytes.len() as i64,
        ..Default::default()
    };
    let reader = domain
        .run(async {
            let bucket = crate::bucket::Bucket::open_dev(&directory.path().join("bucket.sqlite"))?;
            let filename = cellar_ltx::ltx::format_filename(file.min_txid, file.max_txid);
            bucket
                .put(
                    &format!("test/cells/Cell:range/ltx/e1/0000/{filename}"),
                    bytes.clone(),
                )
                .await?;
            let storage = ReplicaStorage::new(
                bucket.store,
                ReplicaStorageOptions {
                    prefix: "test".into(),
                    ..Default::default()
                },
            )?;
            storage.range_reader("Cell:range", 1).await
        })
        .await??;
    let selected = file.clone();
    // SQLite can fault on an arbitrary thread. The test driver's blocking
    // thread deliberately has no installed Cellar execution domain.
    let (reader, received) = tokio::task::spawn_blocking(move || {
        let received = reader.read_range(&selected, 129 * 1024, 1024);
        (reader, received)
    })
    .await?;
    domain.shutdown().await;
    assert_eq!(received?, bytes[129 * 1024..130 * 1024]);
    let unrelated = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let late = unrelated
        .run(async { reader.read_range(&file, 0, 16) })
        .await?;
    unrelated.shutdown().await;
    assert!(
        late.is_err(),
        "a closed page-reader owner rebound to another domain"
    );
    Ok(())
}
