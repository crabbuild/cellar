//! Blob request options, object metadata and HTTP header normalization.
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Blob's `httpMetadata`. Five of the six are ordinary HTTP headers that
/// every backend stores as headers, and travel as headers. `cacheExpiry`
/// is not a header anywhere, so it travels in the the object metadata envelope with the
/// rest of the record.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct HttpMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_disposition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_encoding: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<String>,
    /// Milliseconds since the epoch, as Blob spells it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_expiry: Option<i64>,
}

/// Blob's `onlyIf`, already normalized by the harness: the `Headers` form
/// and the `BlobConditional` form arrive here the same way.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Conditional {
    pub etag_matches: Option<String>,
    pub etag_does_not_match: Option<String>,
    /// Milliseconds since the epoch.
    pub uploaded_before: Option<i64>,
    pub uploaded_after: Option<i64>,
}

/// Blob's `BlobRange`, normalized by the harness to the three fields Blob
/// documents. `suffix` wins over the other two, as it does on Blob.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Range {
    pub offset: Option<u64>,
    pub length: Option<u64>,
    pub suffix: Option<u64>,
}

fn js_space(character: char) -> bool {
    matches!(character as u32, 0x9..=0xd | 0x20 | 0xa0 | 0x1680 | 0x2000..=0x200a | 0x2028 | 0x2029 | 0x202f | 0x3000 | 0xfeff)
}

impl Conditional {
    pub(super) fn present(&self) -> bool {
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
    pub(super) fn conditional(&self) -> Option<&Conditional> {
        self.only_if
            .as_ref()
            .filter(|condition| condition.present())
    }

    pub(super) fn wire(&self) -> serde_json::Value {
        serde_json::json!({
            "http":self.http_metadata, "custom":self.custom_metadata, "storageClass":self.storage_class,
            "onlyIf":self.conditional(), "verify":self.checksums.iter().map(|(algorithm,value)|(algorithm.name(),value.hex())).collect::<BTreeMap<_,_>>(),
        })
    }

    pub(super) fn json(&self) -> Result<String> {
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
    pub(super) fn from_record(value: serde_json::Value) -> Result<Self> {
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

#[derive(Clone, Debug)]
pub struct Page {
    pub objects: Vec<Object>,
    pub truncated: bool,
    pub cursor: Option<String>,
    pub delimited_prefixes: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UploadedPart {
    pub part_number: u32,
    pub etag: String,
}

#[cfg(test)]
mod tests {
    use crate::application::blob;

    #[test]
    fn blob_range_and_condition_parsing_need_no_store() -> anyhow::Result<()> {
        assert!(blob::Range::from_header("bytes=10-8").is_err());
        assert_eq!(blob::Range::from_header("bytes=10-9")?.length, Some(0));
        assert!(blob::Range::from_header("bytes=0-18446744073709551615").is_err());
        assert_eq!(blob::Range::from_header("bytes=-5")?.suffix, Some(5));
        let condition =
            blob::Conditional::from_headers(&[("If-Match".into(), "\"version-1\"".into())])
                .unwrap();
        assert_eq!(condition.etag_matches.as_deref(), Some("\"version-1\""));
        Ok(())
    }
}
