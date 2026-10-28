// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Persistent upload identities and replaceable indexed parts.

use async_trait::async_trait;
use bytes::Bytes;
use object_store::{multipart::MultipartStore, path::Path, MultipartId, PutMultipartOptions};
use std::sync::Arc;
use std::time::Duration;

/// Host policy for persistent, unpublished uploads. Deadlines are captured at
/// allocation; changing this policy never extends an existing upload's life.
#[derive(Clone, Copy, Debug)]
pub struct MultipartMaintenancePolicy {
    /// `None` disables deadlines for new uploads, not already stored deadlines.
    pub upload_ttl: Option<Duration>,
    pub idle_interval: Duration,
    pub backlog_interval: Duration,
}

impl Default for MultipartMaintenancePolicy {
    fn default() -> Self {
        Self {
            upload_ttl: Some(Duration::from_secs(7 * 24 * 60 * 60)),
            idle_interval: Duration::from_secs(60),
            backlog_interval: Duration::from_millis(10),
        }
    }
}

impl MultipartMaintenancePolicy {
    pub(crate) fn validate(self) -> object_store::Result<()> {
        if self
            .upload_ttl
            .is_some_and(|ttl| ttl.as_millis() == 0 || ttl.as_millis() > i64::MAX as u128)
            || self.idle_interval.is_zero()
            || self.backlog_interval.is_zero()
            || self.idle_interval > Duration::from_secs(24 * 60 * 60)
            || self.backlog_interval > self.idle_interval
        {
            return Err(object_store::Error::Generic {
                store: "multipart maintenance policy",
                source: "expected positive millisecond TTL, timers at most one day, and backlog interval <= idle interval".into(),
            });
        }
        Ok(())
    }
}

/// One atomic pass: at most 64 retirements, 64 cleanup headers and one stored part. Part
/// deletion can still take time proportional to that backend's maximum part.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MultipartGcProgress {
    pub retired_uploads: usize,
    pub deleted_parts: usize,
    pub deleted_uploads: usize,
    pub pending: bool,
}

/// One unpublished part, staged without retaining its complete body in memory.
/// Dropping it discards its scratch storage, never the existing part or object.
/// Entered I/O must retain the original scratch-capacity owner until it ends.
#[async_trait]
pub trait StreamedPart: Send {
    async fn write(&mut self, bytes: Bytes) -> object_store::Result<()>;

    async fn complete(self: Box<Self>) -> object_store::Result<object_store::multipart::PartId>;
}

/// Host-supplied multipart client addressing the same root as the bucket's
/// ordinary/CAS clients. Upload identifiers survive client replacement.
///
/// The pinned object_store low-level cloud API has no metadata initiation
/// operation. A backend advertises that extension explicitly; profiles that
/// require it refuse before an upload is allocated.
#[async_trait]
pub trait ResumableMultipartStore: MultipartStore {
    /// `None` leaves expiry/collection to the external provider. It does not
    /// advertise Cellar-owned cleanup for that provider.
    fn maintenance_policy(&self) -> Option<MultipartMaintenancePolicy> {
        None
    }

    /// Collect only uploads allocated in this exact host bucket scope. This
    /// convergent maintenance operation may be repeated after a lost response.
    async fn reap_expired(&self, _scope: &str) -> object_store::Result<MultipartGcProgress> {
        Err(object_store::Error::NotSupported {
            source: "host-owned multipart maintenance is unsupported by this backend".into(),
        })
    }

    /// Capture the canonical host scope separately from the full object path.
    /// Existing provider adapters may delegate when lifecycle is provider-owned.
    async fn create_scoped(
        &self,
        path: &Path,
        options: PutMultipartOptions,
        _scope: &str,
    ) -> object_store::Result<MultipartId> {
        self.create_with_options(path, options).await
    }

    fn supports_streamed_parts(&self) -> bool {
        false
    }

    /// `capacity` grants storage capacity only, never application authority.
    /// The backend retains it with its anonymous file and entered I/O. Writes
    /// are at most 64 KiB, and completion requires exactly `length` bytes.
    async fn begin_part(
        &self,
        _path: &Path,
        _id: &MultipartId,
        _index: usize,
        _length: u64,
        _capacity: Arc<dyn Send + Sync>,
    ) -> object_store::Result<Box<dyn StreamedPart>> {
        Err(object_store::Error::NotSupported {
            source: "streamed resumable parts are unsupported by this backend".into(),
        })
    }

    fn supports_metadata(&self) -> bool {
        false
    }

    async fn create_with_options(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<MultipartId> {
        if !options.attributes.is_empty() || !options.tags.encoded().is_empty() {
            return Err(object_store::Error::NotSupported {
                source: "resumable multipart metadata is unsupported by this backend".into(),
            });
        }
        self.create_multipart(path).await
    }
}

impl ResumableMultipartStore for object_store::aws::AmazonS3 {}

impl ResumableMultipartStore for object_store::gcp::GoogleCloudStorage {}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{multipart::PartId, PutPayload, PutResult};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct WithoutMetadata(AtomicUsize);

    #[async_trait]
    impl MultipartStore for WithoutMetadata {
        async fn create_multipart(&self, _: &Path) -> object_store::Result<MultipartId> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok("allocated".into())
        }

        async fn put_part(
            &self,
            _: &Path,
            _: &MultipartId,
            _: usize,
            _: PutPayload,
        ) -> object_store::Result<PartId> {
            unreachable!("negative admission control cannot upload")
        }

        async fn complete_multipart(
            &self,
            _: &Path,
            _: &MultipartId,
            _: Vec<PartId>,
        ) -> object_store::Result<PutResult> {
            unreachable!("negative admission control cannot complete")
        }

        async fn abort_multipart(&self, _: &Path, _: &MultipartId) -> object_store::Result<()> {
            unreachable!("negative admission control cannot abort")
        }
    }

    impl ResumableMultipartStore for WithoutMetadata {}

    #[tokio::test]
    async fn unsupported_metadata_refuses_before_backend_allocation() {
        let client = WithoutMetadata::default();
        let mut options = PutMultipartOptions::default();
        options.attributes.insert(
            object_store::Attribute::ContentType,
            "application/zstd".into(),
        );
        assert!(!client.supports_metadata());
        assert!(client
            .create_with_options(&Path::from("backup"), options)
            .await
            .is_err());
        assert_eq!(client.0.load(Ordering::Relaxed), 0);
        assert_eq!(
            client
                .create_with_options(&Path::from("backup"), PutMultipartOptions::default())
                .await
                .unwrap(),
            "allocated"
        );
        assert_eq!(client.0.load(Ordering::Relaxed), 1);
    }
}
