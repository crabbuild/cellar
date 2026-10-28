// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Persistent upload identities and replaceable indexed parts.

use async_trait::async_trait;
use object_store::{multipart::MultipartStore, path::Path, MultipartId, PutMultipartOptions};

/// Host-supplied multipart client addressing the same root as the bucket's
/// ordinary/CAS clients. Upload identifiers survive client replacement.
///
/// The pinned object_store low-level cloud API has no metadata initiation
/// operation. A backend advertises that extension explicitly; profiles that
/// require it refuse before an upload is allocated.
#[async_trait]
pub trait ResumableMultipartStore: MultipartStore {
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
