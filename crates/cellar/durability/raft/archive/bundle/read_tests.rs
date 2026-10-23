//! Exercise the actual object read/extraction path with two batch members.
use super::*;
use futures_util::stream::BoxStream;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions,
    PutPayload, PutResult,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug, Default)]
struct Store {
    inner: object_store::memory::InMemory,
    held: AtomicBool,
    reads: AtomicUsize,
    active: AtomicUsize,
    resume: tokio::sync::Notify,
}

impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("held bundle read")
    }
}

impl Store {
    fn release(&self) {
        self.held.store(false, Ordering::Release);
        self.resume.notify_waiters();
    }
}

#[async_trait::async_trait]
impl ObjectStore for Store {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(path, payload, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        struct Active<'a>(&'a AtomicUsize);

        impl Drop for Active<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::AcqRel);
            }
        }
        self.reads.fetch_add(1, Ordering::AcqRel);
        self.active.fetch_add(1, Ordering::AcqRel);
        let _active = Active(&self.active);
        loop {
            let resumed = self.resume.notified();
            tokio::pin!(resumed);
            resumed.as_mut().enable();
            if !self.held.load(Ordering::Acquire) {
                break;
            }
            resumed.await;
        }
        self.inner.get_opts(path, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn delete(&self, path: &Path) -> object_store::Result<()> {
        self.inner.delete(path).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

struct Fixture {
    domain: crate::asyncrt::ExecutionDomain,
    archives: [CellArchive; 2],
    objects: Vec<ObjectRef>,
    store: Arc<Store>,
    performance: Arc<crate::raft::performance::Registry>,
    _requests: mpsc::Receiver<Request>,
}

impl Fixture {
    async fn new() -> anyhow::Result<Self> {
        let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let (token, performance) = domain
            .run(async {
                let token = crate::asyncrt::services().domain_token().unwrap();
                let performance = crate::raft::performance::Registry::new(token.clone());
                (Some(token), performance)
            })
            .await?;
        let store = Arc::new(Store::default());
        let mut archives = [archive(10), archive(11)];
        let (sender, requests) = mpsc::channel(MAX_GROUPS);
        let client = Client::new(sender);
        for archive in &mut archives {
            archive.store = store.clone();
            archive.io_domain = token.clone();
            archive.bundle_client = Some(client.clone());
            archive.bundle_read_performance = Some(performance.clone());
        }
        let (bytes, objects) = encode(vec![
            part(&archives[0], b"first"),
            part(&archives[1], b"second"),
        ])?;
        store
            .inner
            .put(
                &archives[0].bundle_key(objects[0].bundle.as_ref().unwrap()),
                bytes.into(),
            )
            .await?;
        store.held.store(true, Ordering::Release);
        Ok(Self {
            domain,
            archives,
            objects,
            store,
            performance,
            _requests: requests,
        })
    }
}

#[tokio::test]
async fn overlapping_members_share_one_physical_bundle_read() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let mut first = Box::pin(fixture.archives[0].get_object(&fixture.objects[0]));
    let mut second = Box::pin(fixture.archives[1].get_object(&fixture.objects[1]));
    // Each actual read path is polled until the store gate blocks it. No sleep
    // or scheduler timing is used to establish overlapping readers.
    let first_pending = futures_util::poll!(&mut first).is_pending();
    let second_pending = futures_util::poll!(&mut second).is_pending();
    let physical_reads = fixture.store.reads.load(Ordering::Acquire);
    fixture.store.release();
    let (first, second) = tokio::join!(first, second);
    let counters = fixture.performance.snapshot().counters;
    fixture.domain.shutdown().await;
    assert!(first_pending && second_pending);
    assert_eq!(first?, b"first");
    assert_eq!(second?, b"second");
    assert_eq!(fixture.store.active.load(Ordering::Acquire), 0);
    assert_eq!(physical_reads, 1, "same bundle was downloaded twice");
    for name in ["archive_bundle_read_owner", "archive_bundle_read_joined"] {
        assert_eq!(counters.iter().find(|c| c.name == name).unwrap().value, 1);
    }
    Ok(())
}

#[tokio::test]
async fn completed_bundle_reads_are_never_cached() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    fixture.store.release();
    let first = fixture.archives[0].get_object(&fixture.objects[0]).await;
    fixture
        .store
        .inner
        .delete(&fixture.archives[0].bundle_key(fixture.objects[0].bundle.as_ref().unwrap()))
        .await?;
    let second = fixture.archives[1].get_object(&fixture.objects[1]).await;
    fixture.domain.shutdown().await;
    assert_eq!(first?, b"first");
    assert!(matches!(
        second,
        Err(ArchiveError::Corrupt("missing archive bundle"))
    ));
    assert_eq!(fixture.store.reads.load(Ordering::Acquire), 2);
    Ok(())
}

#[tokio::test]
async fn shared_bundle_bytes_still_require_each_members_binding() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let mut first = Box::pin(fixture.archives[0].get_object(&fixture.objects[0]));
    // Same physical bundle, but the second member asks for the first history.
    let mut wrong = Box::pin(fixture.archives[1].get_object(&fixture.objects[0]));
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(futures_util::poll!(&mut wrong).is_pending());
    fixture.store.release();
    let (first, wrong) = tokio::join!(first, wrong);
    fixture.domain.shutdown().await;
    assert_eq!(first?, b"first");
    assert!(matches!(
        wrong,
        Err(ArchiveError::Corrupt(
            "archive bundle history/object binding"
        ))
    ));
    assert_eq!(fixture.store.reads.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn corrupt_shared_bytes_refuse_every_member_then_allow_a_fresh_read() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let (bytes, _) = encode(vec![
        part(&fixture.archives[0], b"first"),
        part(&fixture.archives[1], b"second"),
    ])?;
    let path = fixture.archives[0].bundle_key(fixture.objects[0].bundle.as_ref().unwrap());
    let mut corrupt = bytes.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    fixture.store.inner.put(&path, corrupt.into()).await?;
    let mut first = Box::pin(fixture.archives[0].get_object(&fixture.objects[0]));
    let mut second = Box::pin(fixture.archives[1].get_object(&fixture.objects[1]));
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(futures_util::poll!(&mut second).is_pending());
    fixture.store.release();
    let (first, second) = tokio::join!(first, second);
    fixture.store.inner.put(&path, bytes.into()).await?;
    let fresh = fixture.archives[1].get_object(&fixture.objects[1]).await;
    fixture.domain.shutdown().await;
    for outcome in [first, second] {
        assert!(matches!(
            outcome,
            Err(ArchiveError::Corrupt("archive bundle digest/length"))
        ));
    }
    assert_eq!(fresh?, b"second");
    assert_eq!(fixture.store.reads.load(Ordering::Acquire), 2);
    Ok(())
}

#[tokio::test]
async fn cancelling_the_read_owner_refuses_waiters_without_stranding_them() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let mut first = Box::pin(fixture.archives[0].get_object(&fixture.objects[0]));
    let mut second = Box::pin(fixture.archives[1].get_object(&fixture.objects[1]));
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(futures_util::poll!(&mut second).is_pending());
    drop(first);
    let active = fixture.store.active.load(Ordering::Acquire);
    let second = second.await;
    fixture.store.release();
    let fresh = fixture.archives[0].get_object(&fixture.objects[0]).await;
    fixture.domain.shutdown().await;
    assert_eq!(active, 0);
    assert!(matches!(second, Err(ArchiveError::Unconfirmed)));
    assert_eq!(fresh?, b"first");
    assert_eq!(fixture.store.reads.load(Ordering::Acquire), 2);
    Ok(())
}

#[tokio::test]
async fn cancelling_a_waiter_does_not_cancel_the_physical_owner() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let mut first = Box::pin(fixture.archives[0].get_object(&fixture.objects[0]));
    let mut second = Box::pin(fixture.archives[1].get_object(&fixture.objects[1]));
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(futures_util::poll!(&mut second).is_pending());
    drop(second);
    let active = fixture.store.active.load(Ordering::Acquire);
    fixture.store.release();
    let first = first.await;
    let fresh = fixture.archives[1].get_object(&fixture.objects[1]).await;
    fixture.domain.shutdown().await;
    assert_eq!(active, 1);
    assert_eq!(first?, b"first");
    assert_eq!(fresh?, b"second");
    assert_eq!(fixture.store.reads.load(Ordering::Acquire), 2);
    Ok(())
}

#[tokio::test]
async fn joined_reader_keeps_its_own_deadline() -> anyhow::Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.archives[1].limits.io_timeout_ms = 20;
    let mut first = Box::pin(fixture.archives[0].get_object(&fixture.objects[0]));
    let mut second = Box::pin(fixture.archives[1].get_object(&fixture.objects[1]));
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(futures_util::poll!(&mut second).is_pending());
    let timed = tokio::time::timeout(std::time::Duration::from_secs(1), &mut second).await;
    drop(second);
    let active = fixture.store.active.load(Ordering::Acquire);
    fixture.store.release();
    let first = first.await;
    fixture.domain.shutdown().await;
    assert!(matches!(timed?, Err(ArchiveError::Timeout)));
    assert_eq!(active, 1);
    assert_eq!(first?, b"first");
    assert_eq!(fixture.store.reads.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn shared_payload_keeps_each_extraction_reservation_until_physical_return(
) -> anyhow::Result<()> {
    let Fixture {
        domain,
        mut archives,
        objects,
        store,
        _requests,
        ..
    } = Fixture::new().await?;
    let (send, mut jobs) = mpsc::unbounded_channel();
    let host = LtxHost::new(
        || 0,
        |_| unreachable!(),
        |_| async { unreachable!() },
        move |job| {
            send.send(job).unwrap();
            std::future::pending()
        },
    );
    let budget = crate::raft::budget::Budget::new(2 * MAX_RECOVERY_READ_BYTES, 2);
    let bytes = archives[0].read_memory_bound();
    for archive in &mut archives {
        archive.host = host.clone();
        archive.read_retention = Some(Arc::new(budget.acquire(bytes).unwrap()));
    }
    let [a, b] = archives;
    let first_object = objects[0].clone();
    let second_object = objects[1].clone();
    let mut first = Box::pin(async move { a.get_object(&first_object).await });
    let mut second = Box::pin(async move { b.get_object(&second_object).await });
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(futures_util::poll!(&mut second).is_pending());
    store.release();
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(futures_util::poll!(&mut second).is_pending());
    let first_job = jobs.try_recv()?;
    let second_job = jobs.try_recv()?;
    drop(first);
    drop(second);
    let both = budget.usage();
    first_job();
    let one = budget.usage();
    second_job();
    let none = budget.usage();
    domain.shutdown().await;
    assert_eq!(both, (bytes * 2, 2));
    assert_eq!(one, (bytes, 1));
    assert_eq!(none, (0, 0));
    assert_eq!(store.reads.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn pending_bundle_read_capacity_is_bounded_and_cancelled_waiters_release_slots(
) -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let mut readers: Vec<_> = (0..MAX_GROUPS * READ_WINDOW)
        .map(|_| Box::pin(fixture.archives[0].get_object(&fixture.objects[0])))
        .collect();
    for reader in &mut readers {
        assert!(futures_util::poll!(reader).is_pending());
    }
    let refused = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        fixture.archives[1].get_object(&fixture.objects[1]),
    )
    .await;
    drop(readers.pop());
    let mut replacement = Box::pin(fixture.archives[1].get_object(&fixture.objects[1]));
    let admitted = futures_util::poll!(&mut replacement).is_pending();
    fixture.store.release();
    let (results, replacement) = tokio::join!(futures_util::future::join_all(readers), replacement);
    fixture.domain.shutdown().await;
    assert!(matches!(refused?, Err(ArchiveError::Limit)));
    assert!(admitted);
    for result in results {
        assert_eq!(result?, b"first");
    }
    assert_eq!(replacement?, b"second");
    assert_eq!(fixture.store.reads.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn cancelled_waiter_cannot_extract_an_already_delivered_shared_body() -> anyhow::Result<()> {
    let mut fixture = Fixture::new().await?;
    let (cancel, receiver) = tokio::sync::watch::channel(false);
    fixture.archives[1].cancellation = Some(receiver);
    let mut first = Box::pin(fixture.archives[0].get_object(&fixture.objects[0]));
    let mut second = Box::pin(fixture.archives[1].get_object(&fixture.objects[1]));
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(futures_util::poll!(&mut second).is_pending());
    fixture.store.release();
    let first = first.await;
    cancel.send_replace(true);
    let second = second.await;
    fixture.domain.shutdown().await;
    assert_eq!(first?, b"first");
    assert!(matches!(second, Err(ArchiveError::Stopped)));
    assert_eq!(fixture.store.reads.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn unpolled_joined_reader_refuses_bytes_delivered_after_its_deadline() -> anyhow::Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.archives[1].limits.io_timeout_ms = 20;
    let mut first = Box::pin(fixture.archives[0].get_object(&fixture.objects[0]));
    let mut second = Box::pin(fixture.archives[1].get_object(&fixture.objects[1]));
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(futures_util::poll!(&mut second).is_pending());
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    fixture.store.release();
    let first = first.await;
    // The shared response and the expired timer are now both ready. The old
    // result-first I/O select must not let a newly shared read extend its lease.
    let second = second.await;
    fixture.domain.shutdown().await;
    assert_eq!(first?, b"first");
    assert!(matches!(second, Err(ArchiveError::Timeout)));
    assert_eq!(fixture.store.reads.load(Ordering::Acquire), 1);
    Ok(())
}
