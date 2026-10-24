//! Conditional writes are real InMemory operations; faults lose responses only.
use futures_util::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{
    path::Path, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug, Default)]
pub struct Store {
    pub hold_next_fleet_read: AtomicBool,
    pub fleet_read_hang: AtomicBool,
    pub active_fleet_reads: AtomicUsize,
    pub fleet_read_resume: tokio::sync::Notify,
    /// Hold one exact ordinal initial-member claim while the host initializes.
    pub hold_nth_claim: AtomicUsize,
    pub claim_hang: AtomicBool,
    pub claim_waiting: tokio::sync::Notify,
    pub claim_resume: tokio::sync::Notify,
    pub inner: object_store::memory::InMemory,
    pub fail_before: AtomicBool,
    pub fail_after: AtomicBool,
    pub fail_read: AtomicBool,
    pub fail_confirmation: AtomicBool,
    pub hold_next_birth: AtomicBool,
    pub hold_birth_confirmation: AtomicBool,
    pub birth_confirmation_pending: AtomicBool,
    pub birth_hang: AtomicBool,
    pub birth_waiting: AtomicBool,
    pub birth_resume: tokio::sync::Notify,
    pub fail_birth_after: AtomicBool,
    pub fail_birth_confirmation: AtomicBool,
    pub fail_birth_read: AtomicBool,
    pub fail_head_before: AtomicBool,
    pub fail_head_after: AtomicBool,
    pub fail_bundle_after: AtomicBool,
    pub fail_head_confirmation: AtomicBool,
    pub fail_join_claim_before: AtomicBool,
    pub fail_join_claim_after: AtomicBool,
    pub fail_join_claim_confirmation: AtomicBool,
    pub fail_join_claim_read: AtomicBool,
    pub archive_unavailable: AtomicBool,
    pub dependency_unavailable: AtomicBool,
    pub archive_hang: AtomicBool,
    pub archive_resume: tokio::sync::Notify,
    pub archive_attempts: AtomicUsize,
    pub archive_heads_hang: AtomicBool,
    pub archive_heads_waiting: AtomicUsize,
    pub archive_heads_resume: tokio::sync::Notify,
    pub archive_payload_reads: AtomicUsize,
    pub hold_archive_payloads: AtomicBool,
    pub active_payload_reads: AtomicUsize,
    pub peak_payload_reads: AtomicUsize,
    pub payload_window_resume: tokio::sync::Notify,
    pub payload_size_delta: AtomicUsize,
    pub payload_chunks: std::sync::Arc<AtomicUsize>,
    pub refuse_archive_payload_reads: AtomicBool,
    pub hold_next_payload: AtomicBool,
    pub payload_hang: AtomicBool,
    pub payload_waiting: AtomicBool,
    pub payload_resume: tokio::sync::Notify,
    pub held_payload_path: std::sync::Mutex<Option<Path>>,
    pub non_create: AtomicUsize,
    pub hold_next_view: AtomicBool,
    pub view_hang: AtomicBool,
    pub view_waiting: AtomicBool,
    pub view_resume: tokio::sync::Notify,
    pub fail_directory_reads: AtomicBool,
    pub hold_view_reads: AtomicBool,
    pub view_reads: AtomicUsize,
    pub active_view_reads: AtomicUsize,
    pub view_reads_resume: tokio::sync::Notify,
}

impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "catalog fault store")
    }
}

fn failure() -> object_store::Error {
    object_store::Error::Generic {
        store: "catalog-test",
        source: Box::new(std::io::Error::other("lost store response")),
    }
}

struct BirthWaiting<'a>(&'a AtomicBool);

impl Drop for BirthWaiting<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl Store {
    async fn wait_birth(&self) {
        self.birth_waiting.store(true, Ordering::Release);
        let _waiting = BirthWaiting(&self.birth_waiting);
        loop {
            let resumed = self.birth_resume.notified();
            tokio::pin!(resumed);
            resumed.as_mut().enable();
            if !self.birth_hang.load(Ordering::Acquire) {
                break;
            }
            resumed.await;
        }
    }

    // Shared with targets that do not arm the payload gate.
    #[allow(dead_code)]
    pub fn release_payload(&self) {
        self.hold_next_payload.store(false, Ordering::Release);
        self.payload_hang.store(false, Ordering::Release);
        self.payload_resume.notify_waiters();
    }

    async fn archive_fault(&self, path: &Path) -> object_store::Result<()> {
        if path.as_ref().contains("/archives/") {
            self.archive_attempts.fetch_add(1, Ordering::AcqRel);
            loop {
                let resumed = self.archive_resume.notified();
                tokio::pin!(resumed);
                resumed.as_mut().enable();
                if !self.archive_hang.load(Ordering::Acquire) {
                    break;
                }
                resumed.await;
            }
            if self.archive_unavailable.load(Ordering::Acquire) {
                return Err(failure());
            }
        }
        Ok(())
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
        if path.as_ref().contains("/dependencies/")
            && self.dependency_unavailable.load(Ordering::Acquire)
        {
            return Err(failure());
        }
        tokio::task::yield_now().await;
        if path.filename() == Some("birth") && self.hold_next_birth.swap(false, Ordering::AcqRel) {
            self.wait_birth().await;
        }
        if path.as_ref().contains("/claims/")
            && self
                .hold_nth_claim
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok_and(|remaining| remaining == 1)
        {
            self.claim_waiting.notify_one();
            loop {
                let resumed = self.claim_resume.notified();
                tokio::pin!(resumed);
                resumed.as_mut().enable();
                if !self.claim_hang.load(Ordering::Acquire) {
                    break;
                }
                resumed.await;
            }
        }
        if path.filename() == Some("view") && self.hold_next_view.swap(false, Ordering::AcqRel) {
            self.view_waiting.store(true, Ordering::Release);
            loop {
                let resumed = self.view_resume.notified();
                tokio::pin!(resumed);
                resumed.as_mut().enable();
                if !self.view_hang.load(Ordering::Acquire) {
                    break;
                }
                resumed.await;
            }
            self.view_waiting.store(false, Ordering::Release);
        }
        self.archive_fault(path).await?;
        if !matches!(options.mode, PutMode::Create) {
            self.non_create.fetch_add(1, Ordering::AcqRel);
        }
        if self.fail_before.swap(false, Ordering::AcqRel) {
            return Err(failure());
        }
        let joining_claim = path.as_ref().ends_with("/claims/0000000000000004");
        if joining_claim && self.fail_join_claim_before.swap(false, Ordering::AcqRel) {
            return Err(failure());
        }
        if path.filename() == Some("head") && self.fail_head_before.swap(false, Ordering::AcqRel) {
            return Err(failure());
        }
        let result = self.inner.put_opts(path, payload, options).await?;
        if path.as_ref().contains("/archives/bundles/")
            && self.fail_bundle_after.swap(false, Ordering::AcqRel)
        {
            return Err(failure());
        }
        if joining_claim
            && self
                .fail_join_claim_confirmation
                .swap(false, Ordering::AcqRel)
        {
            self.fail_join_claim_read.store(true, Ordering::Release);
        }
        if joining_claim && self.fail_join_claim_after.swap(false, Ordering::AcqRel) {
            return Err(failure());
        }

        if path.filename() == Some("birth") {
            if self.fail_birth_confirmation.swap(false, Ordering::AcqRel) {
                self.fail_birth_read.store(true, Ordering::Release);
                return Err(failure());
            }
            if self.fail_birth_after.swap(false, Ordering::AcqRel) {
                return Err(failure());
            }
            if self.hold_birth_confirmation.swap(false, Ordering::AcqRel) {
                self.birth_confirmation_pending
                    .store(true, Ordering::Release);
            }
        }
        let head = path.filename() == Some("head");
        if head && path.as_ref().contains("/archives/") {
            self.archive_heads_waiting.fetch_add(1, Ordering::AcqRel);
            loop {
                let resumed = self.archive_heads_resume.notified();
                tokio::pin!(resumed);
                resumed.as_mut().enable();
                if !self.archive_heads_hang.load(Ordering::Acquire) {
                    break;
                }
                resumed.await;
            }
        }
        if head && self.fail_head_confirmation.swap(false, Ordering::AcqRel) {
            self.fail_read.store(true, Ordering::Release);
        }
        if head && self.fail_head_after.swap(false, Ordering::AcqRel) {
            return Err(failure());
        }
        if self.fail_confirmation.swap(false, Ordering::AcqRel) {
            self.fail_read.store(true, Ordering::Release);
        }
        if self.fail_after.swap(false, Ordering::AcqRel) {
            return Err(failure());
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        tokio::task::yield_now().await;
        if path.as_ref().ends_with("/fleet/head")
            && self.hold_next_fleet_read.swap(false, Ordering::AcqRel)
        {
            struct Reading<'a>(&'a AtomicUsize);

            impl Drop for Reading<'_> {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::AcqRel);
                }
            }
            self.active_fleet_reads.fetch_add(1, Ordering::AcqRel);
            let _reading = Reading(&self.active_fleet_reads);
            loop {
                let resumed = self.fleet_read_resume.notified();
                tokio::pin!(resumed);
                resumed.as_mut().enable();
                if !self.fleet_read_hang.load(Ordering::Acquire) {
                    break;
                }
                resumed.await;
            }
        }
        if path.filename() == Some("birth")
            && self
                .birth_confirmation_pending
                .swap(false, Ordering::AcqRel)
        {
            self.wait_birth().await;
        }
        if path.filename() == Some("birth") && self.fail_birth_read.swap(false, Ordering::AcqRel) {
            return Err(failure());
        }
        if matches!(path.filename(), Some("pin" | "view"))
            && self.fail_directory_reads.load(Ordering::Acquire)
        {
            return Err(failure());
        }
        if path.filename() == Some("view") {
            self.view_reads.fetch_add(1, Ordering::AcqRel);

            struct Reading<'a>(&'a AtomicUsize);

            impl Drop for Reading<'_> {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::AcqRel);
                }
            }
            self.active_view_reads.fetch_add(1, Ordering::AcqRel);
            let _reading = Reading(&self.active_view_reads);
            loop {
                let resumed = self.view_reads_resume.notified();
                tokio::pin!(resumed);
                resumed.as_mut().enable();
                if !self.hold_view_reads.load(Ordering::Acquire) {
                    break;
                }
                resumed.await;
            }
        }
        self.archive_fault(path).await?;
        let payload = path.as_ref().contains("/archives/")
            && ["/snapshots/", "/tails/", "/dependencies/", "/bundles/"]
                .iter()
                .any(|part| path.as_ref().contains(part));

        struct PayloadRead<'a>(&'a AtomicUsize);

        impl Drop for PayloadRead<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::AcqRel);
            }
        }
        let _reading = payload.then(|| {
            let active = self.active_payload_reads.fetch_add(1, Ordering::AcqRel) + 1;
            self.peak_payload_reads.fetch_max(active, Ordering::AcqRel);
            PayloadRead(&self.active_payload_reads)
        });
        if payload {
            self.archive_payload_reads.fetch_add(1, Ordering::AcqRel);
            loop {
                let resumed = self.payload_window_resume.notified();
                tokio::pin!(resumed);
                resumed.as_mut().enable();
                if !self.hold_archive_payloads.load(Ordering::Acquire) {
                    break;
                }
                resumed.await;
            }
            if self.hold_next_payload.swap(false, Ordering::AcqRel) {
                *self.held_payload_path.lock().map_err(|_| failure())? = Some(path.clone());
                self.payload_waiting.store(true, Ordering::Release);
                loop {
                    let resumed = self.payload_resume.notified();
                    tokio::pin!(resumed);
                    resumed.as_mut().enable();
                    if !self.payload_hang.load(Ordering::Acquire) {
                        break;
                    }
                    resumed.await;
                }
                self.payload_waiting.store(false, Ordering::Release);
            }

            if self.refuse_archive_payload_reads.load(Ordering::Acquire) {
                return Err(failure());
            }
        }
        if path.as_ref().ends_with("/claims/0000000000000004")
            && self.fail_join_claim_read.swap(false, Ordering::AcqRel)
        {
            return Err(failure());
        }
        if self.fail_read.swap(false, Ordering::AcqRel) {
            return Err(failure());
        }
        let mut result = self.inner.get_opts(path, options).await?;
        if payload {
            let delta = self.payload_size_delta.load(Ordering::Acquire) as u64;
            result.meta.size += delta;
            result.range.end += delta;
            let GetResultPayload::Stream(stream) = result.payload;
            let chunks = self.payload_chunks.clone();
            result.payload = GetResultPayload::Stream(
                stream
                    .inspect_ok(move |_| {
                        chunks.fetch_add(1, Ordering::AcqRel);
                    })
                    .boxed(),
            );
        }
        Ok(result)
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
