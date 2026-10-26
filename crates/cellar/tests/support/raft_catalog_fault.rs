// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Conditional writes are real InMemory operations; faults lose responses only.
use futures_util::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{
    path::Path, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

// This test-only external store runs outside the node execution boundary.
// Its transport delay must remain observable while the node's work is idle.
#[allow(clippy::disallowed_methods)]
async fn slow_confirmation_chunk() {
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
}

#[derive(Debug, Default)]
pub struct Store {
    pub fail_closure_after: AtomicBool,
    pub fail_fleet_after: AtomicBool,
    pub fail_fleet_confirmation: AtomicBool,
    pub fail_fleet_read: AtomicBool,
    // Background trust refresh must not steal the mutator's confirmation loss.
    fleet_confirmation_task: std::sync::Mutex<Option<tokio::task::Id>>,
    pub hold_next_fleet_cas: AtomicBool,
    pub fleet_cas_hang: AtomicBool,
    pub fleet_cas_waiting: AtomicBool,
    pub fleet_cas_resume: tokio::sync::Notify,
    pub hold_next_fleet_read: AtomicBool,
    pub hold_fleet_reads: AtomicBool,
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
    pub snapshot_part_reads: AtomicUsize,
    pub snapshot_part_puts: AtomicUsize,
    pub hold_part_confirmations: AtomicBool,
    pub slow_part_confirmations: std::sync::Arc<AtomicBool>,
    pub fail_first_part_after_window_enters: AtomicBool,
    pub active_part_confirmations: AtomicUsize,
    pub peak_part_confirmations: AtomicUsize,
    pub part_confirmations_resume: tokio::sync::Notify,
    pub hold_next_publication_release: AtomicBool,
    pub publication_release_waiting: AtomicBool,
    pub publication_release_resume: tokio::sync::Notify,
    pub corrupt_scratch_after_part_read: std::sync::Mutex<Option<std::sync::Arc<AtomicBool>>>,
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
    pub hold_archive_head_until_payload: std::sync::Mutex<Option<String>>,
    pub non_create: AtomicUsize,
    pub birth_inventory_cas: AtomicUsize,
    pub hold_next_birth_inventory_cas: AtomicBool,
    pub birth_inventory_cas_hang: AtomicBool,
    pub birth_inventory_cas_waiting: AtomicBool,
    pub birth_inventory_cas_resume: tokio::sync::Notify,
    pub fail_birth_inventory_after: AtomicBool,
    pub fail_birth_inventory_confirmation: AtomicBool,
    pub fail_birth_inventory_read: AtomicBool,
    pub reject_listing: AtomicBool,
    pub listing_calls: AtomicUsize,
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
        if path.as_ref().contains("/snapshot-parts/") {
            self.snapshot_part_puts.fetch_add(1, Ordering::AcqRel);
        }
        let release = b"\"released\":true";
        if path.filename() == Some("publication")
            && payload
                .iter()
                .any(|bytes| bytes.windows(release.len()).any(|part| part == release))
            && self
                .hold_next_publication_release
                .swap(false, Ordering::AcqRel)
        {
            self.publication_release_waiting
                .store(true, Ordering::Release);
            loop {
                let resumed = self.publication_release_resume.notified();
                tokio::pin!(resumed);
                resumed.as_mut().enable();
                if !self.publication_release_waiting.load(Ordering::Acquire) {
                    break;
                }
                resumed.await;
            }
        }
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
        let inventory_cas =
            path.as_ref().ends_with("/births/head") && matches!(options.mode, PutMode::Update(_));
        if inventory_cas {
            self.birth_inventory_cas.fetch_add(1, Ordering::AcqRel);
            if self
                .hold_next_birth_inventory_cas
                .swap(false, Ordering::AcqRel)
            {
                self.birth_inventory_cas_waiting
                    .store(true, Ordering::Release);
                let _waiting = BirthWaiting(&self.birth_inventory_cas_waiting);
                loop {
                    let resumed = self.birth_inventory_cas_resume.notified();
                    tokio::pin!(resumed);
                    resumed.as_mut().enable();
                    if !self.birth_inventory_cas_hang.load(Ordering::Acquire) {
                        break;
                    }
                    resumed.await;
                }
            }
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
        if path.as_ref().ends_with("/fleet/head")
            && matches!(options.mode, PutMode::Update(_))
            && self.hold_next_fleet_cas.swap(false, Ordering::AcqRel)
        {
            self.fleet_cas_waiting.store(true, Ordering::Release);
            let _waiting = BirthWaiting(&self.fleet_cas_waiting);
            loop {
                let resumed = self.fleet_cas_resume.notified();
                tokio::pin!(resumed);
                resumed.as_mut().enable();
                if !self.fleet_cas_hang.load(Ordering::Acquire) {
                    break;
                }
                resumed.await;
            }
        }
        let fleet_cas =
            path.as_ref().ends_with("/fleet/head") && matches!(options.mode, PutMode::Update(_));
        let result = self.inner.put_opts(path, payload, options).await?;
        if path.as_ref().contains("/closures/")
            && self.fail_closure_after.swap(false, Ordering::AcqRel)
        {
            return Err(failure());
        }
        if fleet_cas && self.fail_fleet_confirmation.swap(false, Ordering::AcqRel) {
            *self.fleet_confirmation_task.lock().unwrap() = tokio::task::try_id();
            self.fail_fleet_read.store(true, Ordering::Release);
            return Err(failure());
        }
        if fleet_cas && self.fail_fleet_after.swap(false, Ordering::AcqRel) {
            return Err(failure());
        }
        if inventory_cas
            && self
                .fail_birth_inventory_confirmation
                .swap(false, Ordering::AcqRel)
        {
            self.fail_birth_inventory_read
                .store(true, Ordering::Release);
            return Err(failure());
        }
        if inventory_cas
            && self
                .fail_birth_inventory_after
                .swap(false, Ordering::AcqRel)
        {
            return Err(failure());
        }
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
        if path.as_ref().ends_with("/fleet/head")
            && *self.fleet_confirmation_task.lock().unwrap() == tokio::task::try_id()
            && self.fail_fleet_read.swap(false, Ordering::AcqRel)
        {
            return Err(failure());
        }
        tokio::task::yield_now().await;
        if path.as_ref().ends_with("/births/head")
            && self.fail_birth_inventory_read.swap(false, Ordering::AcqRel)
        {
            return Err(failure());
        }
        if path.as_ref().ends_with("/fleet/head")
            && (self.hold_fleet_reads.load(Ordering::Acquire)
                || self.hold_next_fleet_read.swap(false, Ordering::AcqRel))
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
        let wait_for_payload = {
            let mut selector = self
                .hold_archive_head_until_payload
                .lock()
                .map_err(|_| failure())?;
            if selector
                .as_ref()
                .is_some_and(|suffix| path.as_ref().ends_with(suffix))
            {
                selector.take();
                true
            } else {
                false
            }
        };
        if wait_for_payload {
            loop {
                let signaled = self.payload_resume.notified();
                tokio::pin!(signaled);
                signaled.as_mut().enable();
                if self.payload_waiting.load(Ordering::Acquire)
                    || !self.payload_hang.load(Ordering::Acquire)
                {
                    break;
                }
                signaled.await;
            }
        }
        self.archive_fault(path).await?;
        if !options.head && path.as_ref().contains("/snapshot-parts/") {
            let ordinal = self.snapshot_part_reads.fetch_add(1, Ordering::AcqRel) + 1;

            struct Confirmation<'a>(&'a AtomicUsize);

            impl Drop for Confirmation<'_> {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::AcqRel);
                }
            }
            let active = self
                .active_part_confirmations
                .fetch_add(1, Ordering::AcqRel)
                + 1;
            self.peak_part_confirmations
                .fetch_max(active, Ordering::AcqRel);
            let _confirmation = Confirmation(&self.active_part_confirmations);
            if ordinal == 1
                && self
                    .fail_first_part_after_window_enters
                    .load(Ordering::Acquire)
            {
                while self.active_part_confirmations.load(Ordering::Acquire) < 2 {
                    tokio::task::yield_now().await;
                }
                self.fail_first_part_after_window_enters
                    .store(false, Ordering::Release);
                return Err(failure());
            }
            loop {
                let resumed = self.part_confirmations_resume.notified();
                tokio::pin!(resumed);
                resumed.as_mut().enable();
                if !self.hold_part_confirmations.load(Ordering::Acquire) {
                    break;
                }
                resumed.await;
            }
            let corrupt = self
                .corrupt_scratch_after_part_read
                .lock()
                .map_err(|_| failure())?
                .take();
            if let Some(corrupt) = corrupt {
                corrupt.store(true, Ordering::Release);
            }
        }
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
                self.payload_resume.notify_waiters();
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
        if path.as_ref().contains("/snapshot-parts/")
            && self.slow_part_confirmations.load(Ordering::Acquire)
        {
            let GetResultPayload::Stream(stream) = result.payload;
            let slow = self.slow_part_confirmations.clone();
            result.payload = GetResultPayload::Stream(
                stream
                    .flat_map(move |result| {
                        let slow = slow.clone();
                        match result {
                            Ok(bytes) => futures_util::stream::unfold(
                                (bytes, 0, slow),
                                |(bytes, at, slow)| async move {
                                    if at == bytes.len() {
                                        return None;
                                    }
                                    if slow.load(Ordering::Acquire) {
                                        slow_confirmation_chunk().await;
                                    }
                                    let end = (at + 64 * 1024).min(bytes.len());
                                    Some((Ok(bytes.slice(at..end)), (bytes, end, slow)))
                                },
                            )
                            .boxed(),
                            Err(error) => {
                                futures_util::stream::once(async move { Err(error) }).boxed()
                            }
                        }
                    })
                    .boxed(),
            );
        }
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
        self.listing_calls.fetch_add(1, Ordering::AcqRel);
        if self.reject_listing.load(Ordering::Acquire) {
            return futures_util::stream::once(async { Err(failure()) }).boxed();
        }
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.listing_calls.fetch_add(1, Ordering::AcqRel);
        if self.reject_listing.load(Ordering::Acquire) {
            return Err(failure());
        }
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
