// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fault-adapter contracts only; in-memory bytes never certify fleet durability.
use super::*;

#[tokio::test]
async fn root_observer_preserves_real_bytes_and_cas_metadata() -> anyhow::Result<()> {
    let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let observer = Store::new(
        inner.clone(),
        "prefix/",
        "Counter:alice",
        false,
        "fresh".into(),
    );
    let path = Path::from(observer.root.as_str());
    let bytes = bytes::Bytes::from_static(br#"{"generation":"fresh","preserve":17}"#);
    inner
        .put_opts(&path, bytes.clone().into(), Default::default())
        .await?;
    let original = inner.get_opts(&path, Default::default()).await?;
    let observed = observer.get_opts(&path, Default::default()).await?;
    assert_eq!(observed.meta.e_tag, original.meta.e_tag);
    assert_eq!(observed.meta.version, original.meta.version);
    assert_eq!(observed.range, original.range);
    assert_eq!(observed.attributes, original.attributes);
    assert_eq!(observed.bytes().await?, bytes);
    assert_eq!(observer.replaced_reads.load(Ordering::Acquire), 1);
    observer.observing.store(false, Ordering::Release);
    assert_eq!(
        observer
            .get_opts(&path, Default::default())
            .await?
            .bytes()
            .await?,
        bytes
    );
    assert_eq!(observer.replaced_reads.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn witness_claim_exclusion_keeps_its_own_lease_and_proof_reads_real() -> anyhow::Result<()> {
    let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let observer = Store::new(
        inner.clone(),
        "prefix/",
        "Counter:alice",
        true,
        String::new(),
    );
    for key in [&observer.root, &observer.owner] {
        let path = Path::from(key.as_str());
        inner
            .put_opts(
                &path,
                bytes::Bytes::from_static(b"original").into(),
                Default::default(),
            )
            .await?;
        assert!(observer
            .put_opts(
                &path,
                bytes::Bytes::from_static(b"wrong claimant").into(),
                Default::default()
            )
            .await
            .is_err());
        assert_eq!(
            observer
                .get_opts(&path, Default::default())
                .await?
                .bytes()
                .await?
                .as_ref(),
            b"original"
        );
    }
    let own = Path::from("prefix/nodes/recovery-witness.json");
    observer
        .put_opts(
            &own,
            bytes::Bytes::from_static(b"real witness lease").into(),
            Default::default(),
        )
        .await?;
    assert_eq!(
        inner
            .get_opts(&own, Default::default())
            .await?
            .bytes()
            .await?
            .as_ref(),
        b"real witness lease"
    );
    assert_eq!(observer.denied_claims.load(Ordering::Acquire), 2);
    Ok(())
}

#[tokio::test]
async fn missing_root_observation_does_not_fabricate_a_response_or_count_other_keys(
) -> anyhow::Result<()> {
    let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let observer = Store::new(inner, "", "Counter:alice", false, String::new());
    assert!(matches!(
        observer
            .get_opts(&Path::from(observer.root.as_str()), Default::default())
            .await,
        Err(Error::NotFound { .. })
    ));
    assert!(matches!(
        observer
            .get_opts(&Path::from("nodes/other.json"), Default::default())
            .await,
        Err(Error::NotFound { .. })
    ));
    assert_eq!(observer.missing_reads.load(Ordering::Acquire), 1);
    assert_eq!(observer.replaced_reads.load(Ordering::Acquire), 0);
    Ok(())
}
