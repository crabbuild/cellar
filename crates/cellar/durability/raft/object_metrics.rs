//! Observe object API calls without changing preconditions, errors or payloads.
//! GET owns its timer until stream EOF, error or drop. SDK-internal retries and
//! HTTP overhead are outside this boundary; a lost PUT reply is not confirmation.
use super::performance::{Counter, Operation, Registry, Timer};
use bytes::Bytes;
use futures_util::{stream::BoxStream, Stream};
use object_store::{
    path::Path, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::{
    fmt,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

pub(crate) fn wrap(
    store: Arc<dyn ObjectStore>,
    performance: Arc<Registry>,
) -> Arc<dyn ObjectStore> {
    Arc::new(MeasuredStore { store, performance })
}

struct MeasuredStore {
    store: Arc<dyn ObjectStore>,
    performance: Arc<Registry>,
}

impl fmt::Debug for MeasuredStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MeasuredRaftStore")
    }
}

impl fmt::Display for MeasuredStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

struct Body {
    source: BoxStream<'static, object_store::Result<Bytes>>,
    performance: Arc<Registry>,
    timer: Option<Timer>,
}

impl Stream for Body {
    type Item = object_store::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = self.source.as_mut().poll_next(cx);
        match &result {
            Poll::Ready(Some(Ok(bytes))) => self
                .performance
                .increment(Counter::ObjectReadBytes, bytes.len() as u64),
            Poll::Ready(Some(Err(_))) => {
                if let Some(timer) = self.timer.take() {
                    timer.finish(false);
                }
            }
            Poll::Ready(None) => {
                if let Some(timer) = self.timer.take() {
                    timer.finish(true);
                }
            }
            Poll::Pending => (),
        }
        result
    }
}

impl MeasuredStore {
    fn read_result(
        &self,
        result: object_store::Result<GetResult>,
        timer: Timer,
        head: bool,
    ) -> object_store::Result<GetResult> {
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                timer.finish(false);
                return Err(error);
            }
        };
        if head {
            timer.finish(true);
            return Ok(result);
        }
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(Body {
                source: result.into_stream(),
                performance: self.performance.clone(),
                timer: Some(timer),
            })),
            meta,
            range,
            attributes,
        })
    }
}

#[async_trait::async_trait]
impl ObjectStore for MeasuredStore {
    async fn get(&self, path: &Path) -> object_store::Result<GetResult> {
        let timer = self.performance.timer(Operation::ObjectGet);
        self.read_result(self.store.get(path).await, timer, false)
    }

    async fn put(&self, path: &Path, payload: PutPayload) -> object_store::Result<PutResult> {
        let timer = self.performance.timer(Operation::ObjectPutOverwrite);
        let bytes = payload.content_length() as u64;
        self.performance
            .increment(Counter::ObjectPutRequestedBytes, bytes);
        let result = self.store.put(path, payload).await;
        if result.is_ok() {
            self.performance
                .increment(Counter::ObjectPutConfirmedBytes, bytes);
        }
        timer.finish(result.is_ok());
        result
    }

    async fn head(&self, path: &Path) -> object_store::Result<ObjectMeta> {
        let timer = self.performance.timer(Operation::ObjectGet);
        let result = self.store.head(path).await;
        timer.finish(result.is_ok());
        result
    }

    async fn get_range(
        &self,
        path: &Path,
        range: std::ops::Range<u64>,
    ) -> object_store::Result<Bytes> {
        let timer = self.performance.timer(Operation::ObjectGet);
        let result = self.store.get_range(path, range).await;
        if let Ok(bytes) = &result {
            self.performance
                .increment(Counter::ObjectReadBytes, bytes.len() as u64);
        }
        timer.finish(result.is_ok());
        result
    }

    async fn get_ranges(
        &self,
        path: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        // Backend coalescing and partial read progress are opaque here.
        self.performance.mark_unavailable();
        self.store.get_ranges(path, ranges).await
    }

    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let operation = match &options.mode {
            PutMode::Create => Operation::ObjectPutCreate,
            PutMode::Update(_) => Operation::ObjectPutUpdate,
            PutMode::Overwrite => Operation::ObjectPutOverwrite,
        };
        let timer = self.performance.timer(operation);
        let bytes = payload.content_length() as u64;
        self.performance
            .increment(Counter::ObjectPutRequestedBytes, bytes);
        let result = self.store.put_opts(path, payload, options).await;
        if result.is_ok() {
            self.performance
                .increment(Counter::ObjectPutConfirmedBytes, bytes);
        }
        timer.finish(result.is_ok());
        result
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let timer = self.performance.timer(Operation::ObjectGet);
        let head = options.head;
        self.read_result(self.store.get_opts(path, options).await, timer, head)
    }

    // Every method forwards to the same backend method, including overrides;
    // using trait defaults here could change atomic rename or bulk I/O behavior.
    async fn put_multipart(&self, path: &Path) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.performance.mark_unavailable();
        self.store.put_multipart(path).await
    }

    fn delete_stream<'a>(
        &'a self,
        paths: BoxStream<'a, object_store::Result<Path>>,
    ) -> BoxStream<'a, object_store::Result<Path>> {
        self.performance.mark_unavailable();
        self.store.delete_stream(paths)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.performance.mark_unavailable();
        self.store.list_with_offset(prefix, offset)
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.performance.mark_unavailable();
        self.store.rename(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.performance.mark_unavailable();
        self.store.rename_if_not_exists(from, to).await
    }

    // Raft currently uses only GET and PUT. Preserve the underlying operations
    // if that changes, but refuse measurement qualification until their streamed
    // work is instrumented; never silently count multipart/list work as zero.
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.performance.mark_unavailable();
        self.store.put_multipart_opts(path, options).await
    }

    async fn delete(&self, path: &Path) -> object_store::Result<()> {
        self.performance.mark_unavailable();
        self.store.delete(path).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.performance.mark_unavailable();
        self.store.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.performance.mark_unavailable();
        self.store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.performance.mark_unavailable();
        self.store.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.performance.mark_unavailable();
        self.store.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
#[path = "object_metrics_tests.rs"]
mod tests;
