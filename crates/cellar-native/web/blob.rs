// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable binary values. UTF-16 strings are encoded with TextEncoder's
//! replacement rules; borrowed binary parts are copied during construction.
use super::Error;
use crate::encoding::{DecoderOptions, TextDecoder};
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub struct BlobOptions {
    pub media_type: Vec<u16>,
    /// Any supplied endings value retains the existing unsupported-option error.
    pub endings: Option<Vec<u16>>,
}

impl BlobOptions {
    pub fn new(media_type: &str) -> Self {
        Self {
            media_type: media_type.encode_utf16().collect(),
            endings: None,
        }
    }
}

pub enum BlobPart<'a> {
    Bytes(&'a [u8]),
    Text(&'a str),
    Utf16(&'a [u16]),
    Blob(&'a Blob),
    File(&'a File),
}

#[derive(Debug)]
struct Data {
    bytes: Vec<u8>,
    media_type: String,
}

#[derive(Clone, Debug)]
pub struct Blob(Arc<Data>);

impl Blob {
    pub const SIZE_LIMIT: usize = 134_217_728;

    pub fn new<'a>(
        parts: impl IntoIterator<Item = BlobPart<'a>>,
        options: BlobOptions,
    ) -> Result<Self, Error> {
        if options.endings.is_some() {
            return Err(Error::generic_error(
                "The 'endings' field on 'Options' is not implemented.",
            ));
        }
        let chunks: Vec<std::borrow::Cow<'a, [u8]>> = parts
            .into_iter()
            .map(|part| match part {
                BlobPart::Bytes(bytes) => std::borrow::Cow::Borrowed(bytes),
                BlobPart::Text(text) => std::borrow::Cow::Borrowed(text.as_bytes()),
                BlobPart::Utf16(text) => {
                    std::borrow::Cow::Owned(crate::encoding::encode_utf16(text))
                }
                BlobPart::Blob(blob) => std::borrow::Cow::Borrowed(blob.as_bytes()),
                BlobPart::File(file) => std::borrow::Cow::Borrowed(file.blob().as_bytes()),
            })
            .collect();
        let size = chunks
            .iter()
            .try_fold(0usize, |size, chunk| size.checked_add(chunk.len()))
            .ok_or_else(|| Error::range_error("Blob size exceeds addressable memory"))?;
        if size > Self::SIZE_LIMIT {
            return Err(Error::range_error(format!(
                "Blob size {size} exceeds limit {}",
                Self::SIZE_LIMIT
            )));
        }
        let mut bytes = Vec::with_capacity(size);
        for chunk in chunks {
            bytes.extend_from_slice(&chunk);
        }
        Ok(Self(Arc::new(Data {
            bytes,
            media_type: media_type(&options.media_type),
        })))
    }

    pub fn from_bytes(bytes: &[u8], media_type: &str) -> Result<Self, Error> {
        Self::new([BlobPart::Bytes(bytes)], BlobOptions::new(media_type))
    }

    pub fn size(&self) -> usize {
        self.0.bytes.len()
    }

    pub fn content_type(&self) -> &str {
        &self.0.media_type
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0.bytes
    }

    pub fn bytes(&self) -> Vec<u8> {
        self.0.bytes.clone()
    }

    pub fn array_buffer(&self) -> Vec<u8> {
        self.bytes()
    }

    pub fn text(&self) -> Result<String, crate::encoding::DecodeError> {
        decode_text(self.as_bytes())
    }

    pub fn stream(&self) -> BlobStream {
        BlobStream {
            chunk: (!self.as_bytes().is_empty()).then(|| self.bytes()),
        }
    }

    pub fn slice(&self, start: Option<f64>, end: Option<f64>, media_type: &str) -> Self {
        self.slice_utf16(start, end, &media_type.encode_utf16().collect::<Vec<_>>())
    }

    pub fn slice_utf16(&self, start: Option<f64>, end: Option<f64>, kind: &[u16]) -> Self {
        let size = self.size() as f64;
        let norm = |value: f64| {
            if value.is_nan() {
                f64::NAN
            } else if value < 0.0 {
                (size + value).max(0.0)
            } else {
                value.min(size)
            }
        };
        let a = norm(start.unwrap_or(0.0));
        let b = norm(end.unwrap_or(size));
        // The retained adapter applies Math.max before subarray's integer
        // conversion. A NaN in either bound therefore produces an empty slice.
        let end = if a.is_nan() || b.is_nan() {
            0
        } else {
            a.max(b) as usize
        };
        let start = a as usize;
        let bytes = if end < start {
            Vec::new()
        } else {
            self.as_bytes()[start..end].to_vec()
        };
        Self(Arc::new(Data {
            bytes,
            media_type: media_type(kind),
        }))
    }
}

fn media_type(value: &[u16]) -> String {
    if value.iter().any(|unit| !(32..=126).contains(unit)) {
        String::new()
    } else {
        value
            .iter()
            .map(|unit| (*unit as u8 as char).to_ascii_lowercase())
            .collect()
    }
}

pub(super) fn decode_text(bytes: &[u8]) -> Result<String, crate::encoding::DecodeError> {
    TextDecoder::new("utf-8", DecoderOptions::default())?.decode(bytes, false)
}

pub struct BlobStream {
    chunk: Option<Vec<u8>>,
}

impl Iterator for BlobStream {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        self.chunk.take()
    }
}

impl std::iter::FusedIterator for BlobStream {}

#[derive(Clone, Debug, Default)]
pub struct FileOptions {
    pub blob: BlobOptions,
    pub last_modified: Option<f64>,
}

#[derive(Debug)]
struct FileData {
    blob: Blob,
    name: Vec<u16>,
    last_modified: f64,
}

#[derive(Clone, Debug)]
pub struct File(Arc<FileData>);

impl File {
    pub fn new<'a>(
        parts: impl IntoIterator<Item = BlobPart<'a>>,
        name: &str,
        options: FileOptions,
    ) -> Result<Self, Error> {
        Self::new_utf16(parts, &name.encode_utf16().collect::<Vec<_>>(), options)
    }

    pub fn new_utf16<'a>(
        parts: impl IntoIterator<Item = BlobPart<'a>>,
        name: &[u16],
        options: FileOptions,
    ) -> Result<Self, Error> {
        Ok(Self::from_blob_utf16(
            Blob::new(parts, options.blob)?,
            name,
            options.last_modified,
        ))
    }

    pub fn from_blob(blob: Blob, name: &str, last_modified: Option<f64>) -> Self {
        Self::from_blob_utf16(
            blob,
            &name.encode_utf16().collect::<Vec<_>>(),
            last_modified,
        )
    }

    pub fn from_blob_utf16(blob: Blob, name: &[u16], last_modified: Option<f64>) -> Self {
        let value = last_modified.unwrap_or_else(super::clock::now_ms);
        Self(Arc::new(FileData {
            blob,
            name: name.to_vec(),
            last_modified: if value.is_finite() {
                value.trunc()
            } else {
                0.0
            },
        }))
    }

    pub fn blob(&self) -> &Blob {
        &self.0.blob
    }

    pub fn name_utf16(&self) -> &[u16] {
        &self.0.name
    }

    pub fn name(&self) -> String {
        String::from_utf16_lossy(self.name_utf16())
    }

    pub fn last_modified(&self) -> f64 {
        self.0.last_modified
    }

    pub fn size(&self) -> usize {
        self.blob().size()
    }

    pub fn content_type(&self) -> &str {
        self.blob().content_type()
    }

    pub fn bytes(&self) -> Vec<u8> {
        self.blob().bytes()
    }

    pub fn array_buffer(&self) -> Vec<u8> {
        self.blob().array_buffer()
    }

    pub fn text(&self) -> Result<String, crate::encoding::DecodeError> {
        self.blob().text()
    }

    pub fn stream(&self) -> BlobStream {
        self.blob().stream()
    }

    pub fn slice(&self, start: Option<f64>, end: Option<f64>, kind: &str) -> Blob {
        self.blob().slice(start, end, kind)
    }

    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
