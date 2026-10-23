#![allow(clippy::disallowed_methods)]
use super::*;
use futures_util::{StreamExt, TryStreamExt};
use object_store::UpdateVersion;

async fn setup() -> anyhow::Result<(crate::asyncrt::ExecutionDomain, Arc<Registry>)> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let registry = domain
        .run(async { Registry::new(crate::asyncrt::services().domain_token().unwrap()) })
        .await?;
    Ok((domain, registry))
}

fn operation(
    registry: &Registry,
    name: &str,
) -> crate::application::consensus::RaftOperationPerformance {
    registry
        .snapshot()
        .operations
        .into_iter()
        .find(|operation| operation.name == name)
        .unwrap()
}

fn counter(registry: &Registry, name: &str) -> u64 {
    registry
        .snapshot()
        .counters
        .into_iter()
        .find(|counter| counter.name == name)
        .unwrap()
        .value
}

#[tokio::test]
async fn conditional_writes_and_streamed_reads_keep_actual_outcomes() -> anyhow::Result<()> {
    let (domain, registry) = setup().await?;
    let store = wrap(
        Arc::new(object_store::memory::InMemory::new()),
        registry.clone(),
    );
    let key = Path::from("value");
    let created = store
        .put_opts(
            &key,
            Bytes::from_static(b"abc").into(),
            PutMode::Create.into(),
        )
        .await?;
    assert!(matches!(
        store
            .put_opts(
                &key,
                Bytes::from_static(b"bad").into(),
                PutMode::Create.into()
            )
            .await,
        Err(object_store::Error::AlreadyExists { .. })
    ));
    let update = PutMode::Update(UpdateVersion {
        e_tag: created.e_tag,
        version: created.version,
    });
    store
        .put_opts(
            &key,
            Bytes::from_static(b"12345").into(),
            update.clone().into(),
        )
        .await?;
    assert!(matches!(
        store
            .put_opts(&key, Bytes::from_static(b"stale").into(), update.into())
            .await,
        Err(object_store::Error::Precondition { .. })
    ));
    assert_eq!(counter(&registry, "object_put_requested_bytes"), 16);
    assert_eq!(counter(&registry, "object_put_confirmed_bytes"), 8);
    for name in ["object_put_create", "object_put_update"] {
        let op = operation(&registry, name);
        assert_eq!(
            (op.started, op.succeeded, op.failed, op.abandoned),
            (2, 1, 1, 0)
        );
    }
    let mut response = store.get(&key).await?.into_stream();
    let op = operation(&registry, "object_get");
    assert_eq!((op.started, op.count), (1, 0));
    assert_eq!(response.try_next().await?.unwrap(), b"12345"[..]);
    assert_eq!(counter(&registry, "object_get_body_bytes"), 5);
    assert_eq!(operation(&registry, "object_get").count, 0);
    assert!(response.try_next().await?.is_none());
    assert_eq!(operation(&registry, "object_get").succeeded, 1);
    drop(store.get(&key).await?);
    assert_eq!(operation(&registry, "object_get").abandoned, 1);
    assert_eq!(counter(&registry, "object_get_body_bytes"), 5);
    assert_eq!(store.head(&key).await?.size, 5);
    assert_eq!(operation(&registry, "object_get").succeeded, 2);
    assert!(matches!(
        store.get(&Path::from("missing")).await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert_eq!(operation(&registry, "object_get").failed, 1);
    assert!(registry.snapshot().available);
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn stream_errors_and_dropped_bodies_retain_only_delivered_bytes() -> anyhow::Result<()> {
    let (domain, registry) = setup().await?;
    let failure = object_store::Error::Generic {
        store: "fault",
        source: "body failed".into(),
    };
    let mut body = Body {
        source: futures_util::stream::iter([Ok(Bytes::from_static(b"abc")), Err(failure)]).boxed(),
        performance: registry.clone(),
        timer: Some(registry.timer(Operation::ObjectGet)),
    };
    assert_eq!(body.try_next().await?.unwrap().len(), 3);
    assert!(matches!(
        body.try_next().await,
        Err(object_store::Error::Generic { store: "fault", .. })
    ));
    assert!(body.try_next().await?.is_none());
    drop(body);
    let mut body = Body {
        source: futures_util::stream::once(async { Ok(Bytes::from_static(b"xy")) })
            .chain(futures_util::stream::pending())
            .boxed(),
        performance: registry.clone(),
        timer: Some(registry.timer(Operation::ObjectGet)),
    };
    assert_eq!(body.try_next().await?.unwrap().len(), 2);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), body.try_next())
            .await
            .is_err()
    );
    assert_eq!(operation(&registry, "object_get").count, 1);
    drop(body);
    let op = operation(&registry, "object_get");
    assert_eq!(
        (op.started, op.succeeded, op.failed, op.abandoned, op.count),
        (2, 0, 1, 1, 2)
    );
    assert_eq!(counter(&registry, "object_get_body_bytes"), 5);
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn unmeasured_store_operation_preserves_result_and_marks_evidence_unavailable(
) -> anyhow::Result<()> {
    let (domain, registry) = setup().await?;
    let store = wrap(
        Arc::new(object_store::memory::InMemory::new()),
        registry.clone(),
    );
    let a = Path::from("a");
    let b = Path::from("b");
    store.put(&a, Bytes::from_static(b"value").into()).await?;
    store.copy(&a, &b).await?;
    assert_eq!(store.get(&b).await?.bytes().await?, b"value"[..]);
    assert!(!registry.snapshot().available);
    domain.shutdown().await;
    Ok(())
}

#[derive(Debug, Default)]
struct Overrides(object_store::memory::InMemory);

impl std::fmt::Display for Overrides {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("method override fixture")
    }
}

fn overridden(method: &'static str) -> object_store::Error {
    object_store::Error::Generic {
        store: method,
        source: "intentional override".into(),
    }
}

#[async_trait::async_trait]
impl ObjectStore for Overrides {
    async fn put(&self, _: &Path, _: PutPayload) -> object_store::Result<PutResult> {
        Err(overridden("put"))
    }

    async fn get(&self, _: &Path) -> object_store::Result<GetResult> {
        Err(overridden("get"))
    }

    async fn head(&self, _: &Path) -> object_store::Result<ObjectMeta> {
        Err(overridden("head"))
    }

    async fn rename(&self, _: &Path, _: &Path) -> object_store::Result<()> {
        Err(overridden("rename"))
    }

    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.0.put_opts(path, payload, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.0.get_opts(path, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(path, options).await
    }

    async fn delete(&self, path: &Path) -> object_store::Result<()> {
        self.0.delete(path).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.0.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.0.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.0.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.0.copy_if_not_exists(from, to).await
    }
}

#[tokio::test]
async fn observation_preserves_backend_method_overrides() -> anyhow::Result<()> {
    let (domain, registry) = setup().await?;
    let store = wrap(Arc::new(Overrides::default()), registry.clone());
    let key = Path::from("key");
    store
        .put_opts(
            &key,
            Bytes::from_static(b"original").into(),
            PutMode::Create.into(),
        )
        .await?;
    assert!(matches!(
        store
            .put(&key, Bytes::from_static(b"replacement").into())
            .await,
        Err(object_store::Error::Generic { store: "put", .. })
    ));
    assert!(matches!(
        store.get(&key).await,
        Err(object_store::Error::Generic { store: "get", .. })
    ));
    assert!(matches!(
        store.head(&key).await,
        Err(object_store::Error::Generic { store: "head", .. })
    ));
    assert_eq!(
        store
            .get_opts(&key, GetOptions::default())
            .await?
            .bytes()
            .await?,
        b"original"[..]
    );
    assert_eq!(counter(&registry, "object_put_confirmed_bytes"), 8);
    assert_eq!(operation(&registry, "object_get").failed, 2);
    assert!(matches!(
        store.rename(&key, &Path::from("destination")).await,
        Err(object_store::Error::Generic {
            store: "rename",
            ..
        })
    ));
    assert!(!registry.snapshot().available);
    domain.shutdown().await;
    Ok(())
}
