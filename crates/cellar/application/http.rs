// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Owned native response bodies over the existing HTTP transport.

use super::Context;
use crate::engine_api::{HttpChunkStream, HttpResponse};
use anyhow::Result;
pub use cellar_native::web::{
    Blob, BlobOptions, BlobPart, BodyInit, File, FileOptions, FormData, FormValue, Headers,
    SearchParams, Url, UrlPattern, UrlPatternComponentResult, UrlPatternInit, UrlPatternInitUtf16,
    UrlPatternInput, UrlPatternOptions, UrlPatternResult,
};
use futures_util::StreamExt;

#[derive(Clone, Copy, Default)]
pub enum Redirect {
    #[default]
    Follow,
    Manual,
    Error,
}

impl Redirect {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Follow => "follow",
            Self::Manual => "manual",
            Self::Error => "error",
        }
    }
}

/// Outbound HTTP input. `None` omits a body; present empty bytes retain explicit
/// zero-length framing. Stream bodies must belong to the calling event.
pub struct OutboundRequest {
    pub url: String,
    pub method: String,
    pub headers: Vec<(String, String)>,
    /// None and an explicitly empty byte body retain distinct HTTP framing.
    pub body: Option<crate::engine_api::RequestBody>,
    pub redirect: Redirect,
}

impl OutboundRequest {
    pub fn get(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            method: "GET".into(),
            headers: Vec::new(),
            body: None,
            redirect: Redirect::Follow,
        }
    }
}

enum Body {
    None,
    Bytes(Vec<u8>),
    Stream(Option<HttpChunkStream>),
}

pub struct Response {
    head: HttpResponse,
    body: Body,
    used: bool,
    header_list: Option<Headers>,
}

impl Response {
    /// Serialize a Rust value as an HTTP JSON response.
    pub fn from_json(value: &impl serde::Serialize) -> Result<Self> {
        Ok(Self::new(
            200,
            serde_json::to_vec(value)?,
            vec![("content-type".into(), "application/json".into())],
        ))
    }

    pub fn accept_websocket(
        &mut self,
        context: &Context,
    ) -> super::HandlerFuture<super::websocket::WebSocket> {
        context.accept_websocket(&mut self.head)
    }

    pub fn from_body(status: u16, body: &BodyInit, headers: &Headers) -> Result<Self> {
        let encoded = body.encode()?;
        let headers = Headers::from_headers(headers);
        if let Some(kind) = encoded.content_type {
            if !headers.has("content-type")? {
                headers.set("content-type", &kind)?;
            }
        }
        let mut response = match encoded.bytes {
            Some(bytes) => Self::new(status, bytes, Vec::new()),
            None => Self::empty(status, Vec::new()),
        };
        response.set_header_list(headers);
        Ok(response)
    }

    pub fn new(status: u16, bytes: impl Into<Vec<u8>>, headers: Vec<(String, String)>) -> Self {
        Self::from_http(HttpResponse {
            status,
            body: bytes.into(),
            stream: None,
            headers,
            websocket: None,
            write_position: None,
            observed_position: None,
        })
    }

    pub fn empty(status: u16, headers: Vec<(String, String)>) -> Self {
        let mut response = Self::new(status, Vec::new(), headers);
        response.body = Body::None;
        response
    }

    pub(crate) fn empty_from_http(head: HttpResponse) -> Self {
        Self {
            head,
            body: Body::None,
            used: false,
            header_list: None,
        }
    }

    pub fn streaming(status: u16, stream: HttpChunkStream, headers: Vec<(String, String)>) -> Self {
        Self::from_http(HttpResponse {
            status,
            body: Vec::new(),
            stream: Some(stream),
            headers,
            websocket: None,
            write_position: None,
            observed_position: None,
        })
    }

    pub fn from_http(mut response: HttpResponse) -> Self {
        let body = match response.stream.take() {
            Some(stream) => Body::Stream(Some(stream)),
            None => Body::Bytes(std::mem::take(&mut response.body)),
        };
        Self {
            head: response,
            body,
            used: false,
            header_list: None,
        }
    }

    pub fn status(&self) -> u16 {
        self.head.status
    }

    pub fn headers(&self) -> Vec<(String, String)> {
        self.header_list
            .as_ref()
            .map(Headers::wire)
            .unwrap_or_else(|| self.head.headers.clone())
    }

    /// Lazily materialize a live typed list. Its aliases and iterators remain
    /// attached to this response until the list is explicitly replaced.
    pub fn header_list(&mut self) -> Result<Headers> {
        if self.header_list.is_none() {
            self.header_list = Some(Headers::from_pairs(self.head.headers.clone())?);
        }
        Ok(self.header_list.as_ref().unwrap().clone())
    }

    pub fn set_header_list(&mut self, headers: Headers) {
        self.header_list = Some(headers);
    }

    /// Constructor-style copy through the sorted Headers iteration view.
    pub fn with_headers(mut self, headers: &Headers) -> Self {
        self.header_list = Some(Headers::from_headers(headers));
        self
    }

    /// Low-level transport editing replaces the typed list and detaches its
    /// old aliases, just as assigning a new header list does.
    pub fn headers_mut(&mut self) -> &mut Vec<(String, String)> {
        if let Some(headers) = self.header_list.take() {
            self.head.headers = headers.wire();
        }
        &mut self.head.headers
    }

    pub fn body_used(&self) -> bool {
        self.used
    }

    pub fn has_body(&self) -> bool {
        !matches!(self.body, Body::None)
    }

    fn consumed() -> anyhow::Error {
        BodyError::Consumed.into()
    }

    /// Successful stream drains materialize bytes, retaining the existing
    /// adapter's repeatable reads. A failed or interrupted drain stays claimed.
    async fn consume(&mut self, context: &Context) -> Result<Vec<u8>> {
        context.check_turn()?;
        self.used = true;
        match &mut self.body {
            Body::None => Ok(Vec::new()),
            Body::Bytes(bytes) => Ok(bytes.clone()),
            Body::Stream(stream) => {
                let mut stream = stream.take().ok_or(BodyError::Locked)?;
                let mut bytes = Vec::new();
                while let Some(chunk) = stream.next().await {
                    context.check_turn()?;
                    let chunk = chunk.map_err(anyhow::Error::msg)?;
                    bytes.extend(chunk);
                }
                context.check_turn()?;
                self.body = Body::Bytes(bytes.clone());
                Ok(bytes)
            }
        }
    }

    pub async fn bytes(&mut self, context: &Context) -> Result<Vec<u8>> {
        self.consume(context).await
    }

    pub async fn array_buffer(&mut self, context: &Context) -> Result<Vec<u8>> {
        self.bytes(context).await
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

    pub async fn blob(&mut self, context: &Context) -> Result<Blob> {
        let bytes = self.bytes(context).await?;
        let kind = self
            .header_list()?
            .get_utf16(&"content-type".encode_utf16().collect::<Vec<_>>())?
            .unwrap_or_default();
        Ok(Blob::new(
            [BlobPart::Bytes(&bytes)],
            BlobOptions {
                media_type: kind,
                endings: None,
            },
        )?)
    }

    pub async fn form_data(&mut self, context: &Context) -> Result<FormData> {
        let bytes = self.bytes(context).await?;
        let kind = self
            .header_list()?
            .get_utf16(&"content-type".encode_utf16().collect::<Vec<_>>())?
            .unwrap_or_default();
        Ok(FormData::parse_utf16(&bytes, &kind)?)
    }

    pub(crate) async fn drain(&mut self, context: &Context) -> Result<()> {
        self.consume(context).await.map(|_| ())
    }

    pub fn into_http(mut self, context: &Context) -> Result<HttpResponse> {
        context.check_turn()?;
        if let Some(headers) = self.header_list.as_ref() {
            self.head.headers = headers.response_wire();
        }
        match self.body {
            Body::None => {}
            Body::Bytes(bytes) => self.head.body = bytes,
            Body::Stream(stream) => self.head.stream = Some(stream.ok_or_else(Self::consumed)?),
        }
        Ok(self.head)
    }

    pub(crate) fn rewrite_parts(
        mut self,
        context: &Context,
    ) -> Result<(HttpResponse, Option<HttpChunkStream>, Option<Headers>)> {
        context.check_turn()?;
        self.head.headers = self.headers();
        let source = match self.body {
            Body::None => None,
            Body::Bytes(bytes) => Some(Box::pin(futures_util::stream::once(
                async move { Ok(bytes) },
            )) as HttpChunkStream),
            Body::Stream(source) => Some(source.ok_or_else(Self::consumed)?),
        };
        self.head.body.clear();
        self.head.stream = None;
        Ok((
            self.head,
            source,
            self.header_list.as_ref().map(Headers::from_headers),
        ))
    }
}

/// An incoming body's owned reader. It retains the request's Content-Type
/// and transfers streamed input through the original event's ownership claim.
pub struct BodyReader {
    response: Response,
}

impl BodyReader {
    pub fn body_used(&self) -> bool {
        self.response.body_used()
    }

    pub async fn bytes(&mut self, context: &Context) -> Result<Vec<u8>> {
        self.response.bytes(context).await
    }

    pub async fn array_buffer(&mut self, context: &Context) -> Result<Vec<u8>> {
        self.bytes(context).await
    }

    pub async fn text(&mut self, context: &Context) -> Result<String> {
        self.response.text(context).await
    }

    pub async fn json(
        &mut self,
        context: &Context,
    ) -> Result<cellar_native::clone::StructuredValue> {
        self.response.json(context).await
    }

    pub async fn blob(&mut self, context: &Context) -> Result<Blob> {
        self.response.blob(context).await
    }

    pub async fn form_data(&mut self, context: &Context) -> Result<FormData> {
        self.response.form_data(context).await
    }
}

#[derive(Debug)]
pub enum BodyError {
    Consumed,
    Locked,
}

impl BodyError {
    pub fn name(&self) -> &'static str {
        "TypeError"
    }
}

impl std::fmt::Display for BodyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self{Self::Consumed=>"Body has already been used. It can only be used once. Use tee() first if you need to read it multiple times.",Self::Locked=>"ReadableStream is locked"})
    }
}

impl std::error::Error for BodyError {}

#[derive(Debug)]
pub struct MethodError(pub String);

impl MethodError {
    pub fn name(&self) -> &'static str {
        "TypeError"
    }
}

impl std::fmt::Display for MethodError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Invalid HTTP method string: {}", self.0)
    }
}

impl std::error::Error for MethodError {}

pub(crate) fn normalize_method(method: &str) -> Result<String> {
    let upper = method.to_uppercase();
    if ["GET", "HEAD", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"].contains(&upper.as_str()) {
        Ok(upper)
    } else {
        Err(MethodError(method.to_string()).into())
    }
}

impl super::Request {
    /// Encode a typed finite body and infer its Content-Type unless supplied.
    pub fn with_body(url: &str, method: &str, headers: &Headers, body: &BodyInit) -> Result<Self> {
        let method = normalize_method(method)?;
        let encoded = body.encode()?;
        let headers = Headers::from_headers(headers);
        if let Some(kind) = encoded.content_type {
            if !headers.has("content-type")? {
                headers.set("content-type", &kind)?;
            }
        }
        Ok(Self {
            url: url.into(),
            method,
            headers: headers.request_wire()?,
            body: crate::engine_api::RequestBody::Bytes(encoded.bytes.unwrap_or_default().into()),
            entrypoint: None,
        })
    }

    /// Transfer this request's body to an owned reader in its original turn.
    pub fn into_body_reader(self, context: &Context) -> Result<BodyReader> {
        context.check_turn()?;
        let response = match self.body {
            crate::engine_api::RequestBody::Bytes(bytes) => {
                Response::new(200, bytes.to_vec(), self.headers)
            }
            body @ crate::engine_api::RequestBody::Stream(_) => {
                Response::streaming(200, context.take_body(body)?, self.headers)
            }
        };
        Ok(BodyReader { response })
    }

    /// Construct a native request with the retained method normalization.
    /// URL strings remain as supplied; `parsed_url` performs explicit parsing.
    pub fn new(
        url: &str,
        method: &str,
        headers: &Headers,
        body: crate::engine_api::RequestBody,
    ) -> Result<Self> {
        let method = normalize_method(method)?;
        Ok(Self {
            url: url.into(),
            method,
            headers: Headers::from_headers(headers).request_wire()?,
            body,
            entrypoint: None,
        })
    }

    /// Request copying retains the raw wire list, including author spelling.
    pub fn header_list(&self) -> Result<Headers> {
        Ok(Headers::from_pairs(self.headers.clone())?)
    }

    pub fn set_header_list(&mut self, headers: &Headers) -> Result<()> {
        self.headers = headers.request_wire()?;
        Ok(())
    }

    pub fn parsed_url(&self) -> Result<Url> {
        Ok(Url::new(&self.url, None)?)
    }
}
