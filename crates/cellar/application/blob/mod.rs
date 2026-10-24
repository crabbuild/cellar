// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native object storage over the fleet bucket and shared Blob host registries.

use super::Context;
use crate::blob_store::{self as store, multipart, streaming};
use crate::bucket::{BlobConditions, BlobRange, BlobRead};
use crate::engine_api::{HttpChunkStream, HttpResponse};
use anyhow::{ensure, Result};
use futures_util::{Stream, StreamExt};
use serde::Deserialize;
use std::sync::Arc;

mod types;
pub use types::{
    Algorithm, Checksum, Conditional, GetOptions, HttpMetadata, ListOptions, Object, ObjectRange,
    Page, Range, StorageClass, UploadedPart, WriteOptions,
};

pub enum GetResult {
    Missing,
    Unmet(Object),
    Body(ObjectBody),
}

pub struct ObjectBody {
    pub object: Object,
    body: Option<HttpChunkStream>,
    used: bool,
}

impl ObjectBody {
    pub fn body_used(&self) -> bool {
        self.used
    }

    pub async fn next(&mut self, context: &Context) -> Result<Option<Vec<u8>>> {
        context.check_turn()?;
        self.used = true;
        let Some(body) = &mut self.body else {
            return Ok(None);
        };
        let result = body.next().await.transpose().map_err(anyhow::Error::msg);
        context.check_turn()?;
        if result.as_ref().is_ok_and(Option::is_none) || result.is_err() {
            self.body = None;
        }
        result
    }

    pub async fn bytes(&mut self, context: &Context) -> Result<Vec<u8>> {
        context.check_turn()?;
        ensure!(!self.used, "Blob body was already consumed");
        let mut bytes = Vec::new();
        while let Some(chunk) = self.next(context).await? {
            bytes.extend(chunk);
        }
        Ok(bytes)
    }

    pub async fn text(&mut self, context: &Context) -> Result<String> {
        let bytes = self.bytes(context).await?;
        let mut decoder = cellar_native::encoding::TextDecoder::new(
            "utf-8",
            cellar_native::encoding::DecoderOptions::default(),
        )?;
        Ok(decoder.decode(&bytes, false)?)
    }

    pub async fn json(
        &mut self,
        context: &Context,
    ) -> Result<cellar_native::clone::StructuredValue> {
        cellar_native::json::parse_utf8(&self.text(context).await?)
    }

    pub fn cancel(&mut self, context: &Context) -> Result<()> {
        context.check_turn()?;
        self.used = true;
        self.body = None;
        Ok(())
    }

    pub fn into_response(
        mut self,
        context: &Context,
        status: u16,
        mut headers: Vec<(String, String)>,
    ) -> Result<HttpResponse> {
        context.check_turn()?;
        ensure!(self.body.is_some(), "Blob body was already consumed");
        self.object.write_http_metadata(&mut headers);
        Ok(HttpResponse {
            status,
            body: Vec::new(),
            stream: self.body.take(),
            headers,
            websocket: None,
            write_position: None,
            observed_position: None,
        })
    }
}

#[derive(Clone, Debug)]
pub struct Bucket {
    binding: Arc<str>,
    name: Arc<str>,
}

impl Bucket {
    pub(crate) fn new(binding: &str, name: &str) -> Self {
        Self {
            binding: binding.into(),
            name: name.into(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn key(&self, method: &str, key: &str) -> Result<()> {
        ensure!(
            !key.is_empty() && key.len() <= 1024,
            "Blob {method} (binding {}): a key is 1 to 1024 bytes, not {}",
            self.binding,
            key.len()
        );
        Ok(())
    }

    pub async fn head(&self, context: &Context, key: &str) -> Result<Option<Object>> {
        context.check_turn()?;
        self.key("head", key)?;
        store::head(&self.name, key)
            .await
            .map_err(anyhow::Error::msg)?
            .map(|meta| Object::from_record(store::object_json(key, &meta, None)))
            .transpose()
    }

    pub async fn get(
        &self,
        context: &Context,
        key: &str,
        options: GetOptions,
    ) -> Result<GetResult> {
        context.check_turn()?;
        self.key("get", key)?;
        let range = options
            .range
            .map(BlobRange::from)
            .unwrap_or(BlobRange::Whole);
        let conditions = BlobConditions::from(options.only_if.unwrap_or_default());
        Ok(
            match store::read(&self.name, key, range, &conditions)
                .await
                .map_err(anyhow::Error::msg)?
            {
                BlobRead::Missing => GetResult::Missing,
                BlobRead::Unmet(meta) => {
                    GetResult::Unmet(Object::from_record(store::object_json(key, &meta, None))?)
                }
                BlobRead::Hit(blob) => GetResult::Body(ObjectBody {
                    object: Object::from_record(store::object_json(
                        key,
                        &blob.meta,
                        Some(blob.range),
                    ))?,
                    body: Some(blob.body),
                    used: false,
                }),
            },
        )
    }

    pub async fn put(
        &self,
        context: &Context,
        key: &str,
        bytes: impl Into<Vec<u8>>,
        options: WriteOptions,
    ) -> Result<Option<Object>> {
        context.check_turn()?;
        self.key("put", key)?;
        let request: store::PutRequest = serde_json::from_value(options.wire())?;
        crate::native_worker::blob_gate(context).await?;
        stored(
            store::put_once(self.name.to_string(), key.into(), bytes.into(), request)
                .await
                .map_err(anyhow::Error::msg)?,
        )
    }

    pub async fn put_stream<S>(
        &self,
        context: &Context,
        key: &str,
        stream: S,
        options: WriteOptions,
    ) -> Result<Option<Object>>
    where
        S: Stream<Item = Result<Vec<u8>, String>> + Send,
    {
        context.check_turn()?;
        self.key("put", key)?;
        let id = streaming::begin(self.name.to_string(), key.into(), options.json()?)
            .await
            .map_err(anyhow::Error::msg)?
            .parse::<u64>()?;
        let mut cleanup = StreamCleanup(Some(id));
        let mut stream = std::pin::pin!(stream);
        while let Some(chunk) = stream.next().await {
            context.check_turn()?;
            let chunk = chunk.map_err(anyhow::Error::msg)?;
            crate::native_worker::blob_gate(context).await?;
            streaming::chunk(id, chunk)
                .await
                .map_err(anyhow::Error::msg)?;
        }
        crate::native_worker::blob_gate(context).await?;
        let result = streaming::end(id, false)
            .await
            .map_err(anyhow::Error::msg)?;
        cleanup.0 = None;
        stored(result)
    }

    pub async fn delete(&self, context: &Context, keys: &[String]) -> Result<()> {
        context.check_turn()?;
        ensure!(keys.len() <= 1000, "Blob delete accepts at most 1000 keys");
        for key in keys {
            self.key("delete", key)?;
        }
        crate::native_worker::blob_gate(context).await?;
        store::delete(&self.name, keys)
            .await
            .map_err(anyhow::Error::msg)
    }

    pub async fn list(&self, context: &Context, options: ListOptions) -> Result<Page> {
        context.check_turn()?;
        let request = serde_json::to_string(
            &serde_json::json!({"prefix":options.prefix,"cursor":options.cursor,"startAfter":options.start_after,
            "limit":options.limit,"delimiter":options.delimiter,"include":options.include_http_metadata||options.include_custom_metadata}),
        )?;

        #[derive(Deserialize)]
        struct Listing {
            objects: Vec<serde_json::Value>,
            truncated: bool,
            cursor: Option<String>,
            prefixes: Vec<String>,
        }
        let listing: Listing = serde_json::from_value(
            store::list(&self.name, &request)
                .await
                .map_err(anyhow::Error::msg)?,
        )?;
        let objects = listing
            .objects
            .into_iter()
            .map(|object| {
                let mut object = Object::from_record(object)?;
                if !options.include_http_metadata {
                    object.http_metadata = HttpMetadata::default();
                }
                if !options.include_custom_metadata {
                    object.custom_metadata.clear();
                }
                Ok(object)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Page {
            objects,
            truncated: listing.truncated,
            cursor: listing
                .cursor
                .filter(|cursor| listing.truncated && !cursor.is_empty()),
            delimited_prefixes: listing.prefixes,
        })
    }

    pub async fn create_multipart(
        &self,
        context: &Context,
        key: &str,
        options: WriteOptions,
    ) -> Result<Multipart> {
        context.check_turn()?;
        self.key("createMultipartUpload", key)?;
        ensure!(
            options.conditional().is_none(),
            "Blob multipart completion takes no precondition"
        );
        ensure!(
            options.checksums.is_empty(),
            "Blob takes a checksum on put, not createMultipartUpload"
        );
        let request: store::PutRequest = serde_json::from_value(options.wire())?;
        crate::native_worker::blob_gate(context).await?;
        let id = multipart::begin(self.name.to_string(), key.into(), request)
            .await
            .map_err(anyhow::Error::msg)?;
        let handle = self.resume_multipart(key, &id)?;
        let _ = handle.host.set(Ok(id.parse::<u64>()?));
        Ok(handle)
    }

    pub fn resume_multipart(&self, key: &str, id: &str) -> Result<Multipart> {
        self.key("resumeMultipartUpload", key)?;
        Ok(Multipart {
            bucket: self.clone(),
            key: key.into(),
            id: id.into(),
            host: Arc::new(tokio::sync::OnceCell::new()),
        })
    }
}

fn stored(result: String) -> Result<Option<Object>> {
    let result: serde_json::Value = serde_json::from_str(&result)?;
    if result["stored"] == true {
        Ok(Some(Object::from_record(result["object"].clone())?))
    } else {
        Ok(None)
    }
}

struct StreamCleanup(Option<u64>);

impl Drop for StreamCleanup {
    fn drop(&mut self) {
        if let Some(id) = self.0.take() {
            crate::asyncrt::spawn(async move {
                let _ = streaming::end(id, true).await;
            });
        }
    }
}

#[derive(Clone)]
pub struct Multipart {
    bucket: Bucket,
    key: Arc<str>,
    id: Arc<str>,
    host: Arc<tokio::sync::OnceCell<std::result::Result<u64, String>>>,
}

impl Multipart {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn upload_id(&self) -> &str {
        &self.id
    }

    async fn id(&self, context: &Context) -> Result<u64> {
        context.check_turn()?;
        self.host
            .get_or_init(|| async {
                multipart::resume(
                    self.bucket.name.to_string(),
                    self.key.to_string(),
                    self.id.to_string(),
                )
                .await?
                .parse::<u64>()
                .map_err(|error| error.to_string())
            })
            .await
            .clone()
            .map_err(anyhow::Error::msg)
    }

    pub async fn upload_part(
        &self,
        context: &Context,
        number: u32,
        bytes: impl Into<Vec<u8>>,
    ) -> Result<UploadedPart> {
        let id = self.id(context).await?;
        crate::native_worker::blob_gate(context).await?;
        multipart::part(id, number, bytes.into())
            .await
            .map_err(anyhow::Error::msg)?;
        Ok(UploadedPart {
            part_number: number,
            etag: String::new(),
        })
    }

    pub async fn complete(&self, context: &Context, parts: &[UploadedPart]) -> Result<Object> {
        let id = self.id(context).await?;
        crate::native_worker::blob_gate(context).await?;
        let result = multipart::finish(id, parts.iter().map(|part| part.part_number).collect())
            .await
            .map_err(anyhow::Error::msg)?;
        Object::from_record(serde_json::from_str(&result)?)
    }

    pub async fn abort(&self, context: &Context) -> Result<()> {
        let id = self.id(context).await?;
        if multipart::is_open(id) {
            crate::native_worker::blob_gate(context).await?;
        }
        multipart::abort(id).await.map_err(anyhow::Error::msg)?;
        Ok(())
    }
}
