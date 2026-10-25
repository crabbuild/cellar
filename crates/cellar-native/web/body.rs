// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Typed finite HTTP bodies and their inferred Content-Type.
use super::{Blob, Error, File, FormData, SearchParams};

#[derive(Clone, Debug)]
pub enum BodyInit {
    Null,
    Bytes(Vec<u8>),
    Text(Vec<u16>),
    Blob(Blob),
    File(File),
    FormData(FormData),
    SearchParams(SearchParams),
}

impl BodyInit {
    pub fn text(value: &str) -> Self {
        Self::Text(value.encode_utf16().collect())
    }

    pub fn encode(&self) -> Result<EncodedBody, Error> {
        let (bytes, kind) = match self {
            Self::Null => {
                return Ok(EncodedBody {
                    bytes: None,
                    content_type: None,
                })
            }
            Self::Bytes(bytes) => (bytes.clone(), String::new()),
            Self::Text(text) => (crate::encoding::encode_utf16(text), String::new()),
            Self::Blob(blob) => (blob.bytes(), blob.content_type().into()),
            Self::File(file) => (file.bytes(), file.content_type().into()),
            Self::FormData(form) => {
                let encoded = form.encode()?;
                (encoded.bytes, encoded.content_type)
            }
            Self::SearchParams(params) => (
                params.serialize()?.into_bytes(),
                "application/x-www-form-urlencoded;charset=UTF-8".into(),
            ),
        };
        Ok(EncodedBody {
            bytes: Some(bytes),
            content_type: (!kind.is_empty()).then_some(kind),
        })
    }
}

#[derive(Clone, Debug)]
pub struct EncodedBody {
    pub bytes: Option<Vec<u8>>,
    pub content_type: Option<String>,
}
