// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Native object storage over the fleet bucket and shared Blob host registries.

use super::Context;
use crate::blob_store::{self as store, multipart, streaming};
use crate::bucket::{BlobConditions, BlobRange, BlobRead};
use crate::engine_api::{HttpChunkStream, HttpResponse};
use anyhow::{ensure, Result};
use futures_util::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

pub use store::{Conditional, HttpMeta as HttpMetadata, Range};

fn js_space(character: char) -> bool {
    matches!(character as u32, 0x9..=0xd | 0x20 | 0xa0 | 0x1680 | 0x2000..=0x200a | 0x2028 | 0x2029 | 0x202f | 0x3000 | 0xfeff)
}
impl Conditional {
    fn present(&self) -> bool {
        self.etag_matches.is_some()
            || self.etag_does_not_match.is_some()
            || self.uploaded_before.is_some()
            || self.uploaded_after.is_some()
    }
    pub fn from_headers(headers: &[(String, String)]) -> Option<Self> {
        let value = |name: &str| {
            headers
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.clone())
        };
        let timestamp = |name: &str| {
            value(name).and_then(|value| {
                chrono::DateTime::parse_from_rfc2822(&value)
                    .or_else(|_| chrono::DateTime::parse_from_rfc3339(&value))
                    .ok()
                    .map(|date| date.timestamp_millis())
                    .or_else(|| {
                        httpdate::parse_http_date(&value)
                            .ok()
                            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                            .and_then(|duration| i64::try_from(duration.as_millis()).ok())
                    })
            })
        };
        let conditional = Self {
            etag_matches: value("if-match"),
            etag_does_not_match: value("if-none-match"),
            uploaded_before: timestamp("if-unmodified-since"),
            uploaded_after: timestamp("if-modified-since"),
        };
        conditional.present().then_some(conditional)
    }
}
impl Range {
    pub fn from_header(header: &str) -> Result<Self> {
        let body = header
            .trim_matches(js_space)
            .strip_prefix("bytes=")
            .ok_or_else(|| anyhow::anyhow!("Blob cannot parse range {header}"))?;
        let (first, last) = body
            .split_once('-')
            .ok_or_else(|| anyhow::anyhow!("Blob cannot parse range {header}"))?;
        ensure!(
            first.bytes().all(|byte| byte.is_ascii_digit())
                && last.bytes().all(|byte| byte.is_ascii_digit()),
            "Blob cannot parse range {header}"
        );
        if first.is_empty() {
            return Ok(Self {
                suffix: Some(if last.is_empty() { 0 } else { last.parse()? }),
                ..Default::default()
            });
        }
        let offset = first.parse::<u64>()?;
        if last.is_empty() {
            return Ok(Self {
                offset: Some(offset),
                ..Default::default()
            });
        }
        let end = last.parse::<u64>()?;
        let length = end
            .checked_add(1)
            .and_then(|end| end.checked_sub(offset))
            .ok_or_else(|| anyhow::anyhow!("Blob invalid range {header}"))?;
        Ok(Self {
            offset: Some(offset),
            length: Some(length),
            suffix: None,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Algorithm {
    Md5,
    Sha1,
    Sha256,
    Sha384,
    Sha512,
}
impl Algorithm {
    fn name(self) -> &'static str {
        match self {
            Self::Md5 => "md5",
            Self::Sha1 => "sha1",
            Self::Sha256 => "sha256",
            Self::Sha384 => "sha384",
            Self::Sha512 => "sha512",
        }
    }
}
#[derive(Clone, Debug)]
pub enum Checksum {
    Hex(String),
    Bytes(Vec<u8>),
}
impl Checksum {
    fn hex(&self) -> String {
        match self {
            Self::Hex(hex) => hex.trim_matches(js_space).to_lowercase(),
            Self::Bytes(bytes) => bytes.iter().map(|byte| format!("{byte:02x}")).collect(),
        }
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum StorageClass {
    Standard,
    InfrequentAccess,
}
#[derive(Clone, Debug, Default)]
pub struct WriteOptions {
    pub http_metadata: HttpMetadata,
    pub custom_metadata: BTreeMap<String, String>,
    pub storage_class: Option<StorageClass>,
    pub only_if: Option<Conditional>,
    pub checksums: BTreeMap<Algorithm, Checksum>,
}
impl WriteOptions {
    fn conditional(&self) -> Option<&Conditional> {
        self.only_if
            .as_ref()
            .filter(|condition| condition.present())
    }
    fn wire(&self) -> serde_json::Value {
        serde_json::json!({
            "http":self.http_metadata, "custom":self.custom_metadata, "storageClass":self.storage_class,
            "onlyIf":self.conditional(), "verify":self.checksums.iter().map(|(algorithm,value)|(algorithm.name(),value.hex())).collect::<BTreeMap<_,_>>(),
        })
    }
    fn request(&self) -> Result<store::PutRequest> {
        Ok(serde_json::from_value(self.wire())?)
    }
    fn json(&self) -> Result<String> {
        Ok(serde_json::to_string(&self.wire())?)
    }
}
#[derive(Clone, Debug, Default)]
pub struct GetOptions {
    pub range: Option<Range>,
    pub only_if: Option<Conditional>,
}
#[derive(Clone, Debug, Default)]
pub struct ListOptions {
    pub prefix: String,
    pub cursor: Option<String>,
    pub start_after: Option<String>,
    pub limit: Option<i64>,
    pub delimiter: Option<String>,
    pub include_http_metadata: bool,
    pub include_custom_metadata: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ObjectRange {
    pub offset: u64,
    pub length: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Object {
    pub key: String,
    pub version: String,
    pub size: u64,
    pub etag: String,
    pub http_etag: String,
    pub uploaded_ms: i64,
    pub http_metadata: HttpMetadata,
    pub custom_metadata: BTreeMap<String, String>,
    pub checksums: BTreeMap<String, String>,
    pub storage_class: String,
    pub range: Option<ObjectRange>,
}
impl Object {
    fn from_record(value: serde_json::Value) -> Result<Self> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Record {
            key: String,
            version: Option<String>,
            size: u64,
            etag: Option<String>,
            uploaded: i64,
            http: HttpMetadata,
            custom: BTreeMap<String, String>,
            checksums: BTreeMap<String, String>,
            storage_class: String,
            range: Option<ObjectRange>,
        }
        let record: Record = serde_json::from_value(value)?;
        let etag = record.etag.unwrap_or_default();
        let etag = etag.strip_prefix('"').unwrap_or(&etag);
        let etag = etag.strip_suffix('"').unwrap_or(etag).to_string();
        let http_etag = if etag.is_empty() {
            String::new()
        } else {
            format!("\"{etag}\"")
        };
        Ok(Self {
            key: record.key,
            version: record.version.unwrap_or_else(|| etag.clone()),
            size: record.size,
            etag,
            http_etag,
            uploaded_ms: record.uploaded,
            http_metadata: record.http,
            custom_metadata: record.custom,
            checksums: record.checksums,
            storage_class: record.storage_class,
            range: record.range,
        })
    }
    pub fn checksum(&self, algorithm: Algorithm) -> Option<Vec<u8>> {
        let hex = self.checksums.get(algorithm.name())?;
        (0..hex.len() / 2)
            .map(|index| u8::from_str_radix(hex.get(index * 2..index * 2 + 2)?, 16).ok())
            .collect()
    }
    pub fn write_http_metadata(&self, headers: &mut Vec<(String, String)>) {
        let http = &self.http_metadata;
        let mut fields = vec![
            ("content-type", http.content_type.clone()),
            ("content-language", http.content_language.clone()),
            ("content-disposition", http.content_disposition.clone()),
            ("content-encoding", http.content_encoding.clone()),
            ("cache-control", http.cache_control.clone()),
        ];
        fields.push((
            "expires",
            http.cache_expiry.map(|ms| {
                chrono::DateTime::from_timestamp_millis(ms)
                    .map(|date| date.format("%a, %d %b %Y %H:%M:%S GMT").to_string())
                    .unwrap_or_else(|| "Invalid Date".into())
            }),
        ));
        for (name, value) in fields {
            if let Some(value) = value {
                headers.retain(|(existing, _)| !existing.eq_ignore_ascii_case(name));
                headers.push((name.into(), value));
            }
        }
    }
}
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
pub struct Page {
    pub objects: Vec<Object>,
    pub truncated: bool,
    pub cursor: Option<String>,
    pub delimited_prefixes: Vec<String>,
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
        let request = options.request()?;
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
        let request = options.request()?;
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UploadedPart {
    pub part_number: u32,
    pub etag: String,
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
