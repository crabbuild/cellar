// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Buffered HTTP values retained in ordinary cell command receipts. Process-local
//! streams, upgrades and Fleet positions never cross this wire.

use super::CommandFailure;
use crate::application::Request;
use crate::engine_api::{HttpResponse, RequestBody};
use anyhow::{ensure, Context as _, Result};

pub(crate) const OPERATION: &str = "__cellar_fetch";
const REQUEST: &[u8] = b"CHTP\x01\x01";
const RESPONSE: &[u8] = b"CHTP\x01\x02";

pub(crate) fn encode_request(
    name: Option<&str>,
    request: &Request,
    limit: usize,
) -> Result<Vec<u8>> {
    ensure!(
        request.entrypoint.is_none(),
        CommandFailure::UnsupportedCapability
    );
    let RequestBody::Bytes(body) = &request.body else {
        anyhow::bail!(CommandFailure::UnsupportedCapability);
    };
    let mut writer = Writer::new(REQUEST, limit)?;
    match name {
        Some(name) => {
            ensure!(name.len() <= 1024, "cell routing name exceeds its bound");
            writer.append(&[1])?;
            writer.bytes(name.as_bytes())?;
        }
        None => writer.append(&[0])?,
    }
    hyper::Method::from_bytes(request.method.as_bytes())?;
    writer.bytes(request.method.as_bytes())?;
    writer.bytes(request.url.as_bytes())?;
    writer.headers(&request.headers)?;
    writer.bytes(body)?;
    Ok(writer.output)
}

pub(super) fn decode_request(input: &[u8]) -> Result<(Option<String>, Request)> {
    let mut reader = Reader::new(input, REQUEST)?;
    let name = match reader.take(1)?[0] {
        0 => None,
        1 => {
            let name = reader.text()?;
            ensure!(name.len() <= 1024, "cell routing name exceeds its bound");
            Some(name)
        }
        _ => anyhow::bail!("invalid cell routing name marker"),
    };
    let method = reader.text()?;
    hyper::Method::from_bytes(method.as_bytes())?;
    let url = reader.text()?;
    let headers = reader.headers()?;
    let body = RequestBody::Bytes(bytes::Bytes::copy_from_slice(reader.bytes()?));
    reader.finish()?;
    Ok((
        name,
        Request {
            method,
            url,
            headers,
            body,
            entrypoint: None,
        },
    ))
}

pub(super) fn encode_response(response: &HttpResponse, limit: usize) -> Result<Vec<u8>> {
    ensure!(
        response.stream.is_none()
            && response.websocket.is_none()
            && response.write_position.is_none()
            && response.observed_position.is_none(),
        CommandFailure::UnsupportedCapability
    );
    validate_status(response.status)?;
    let mut writer = Writer::new(RESPONSE, limit)?;
    writer.append(&response.status.to_be_bytes())?;
    writer.headers(&response.headers)?;
    writer.bytes(&response.body)?;
    Ok(writer.output)
}

pub(crate) fn decode_response(input: &[u8]) -> Result<HttpResponse> {
    let mut reader = Reader::new(input, RESPONSE)?;
    let status = u16::from_be_bytes(reader.take(2)?.try_into()?);
    validate_status(status)?;
    let headers = reader.headers()?;
    let body = reader.bytes()?.to_vec();
    reader.finish()?;
    Ok(HttpResponse {
        status,
        headers,
        body,
        stream: None,
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}

fn validate_status(status: u16) -> Result<()> {
    ensure!(
        (200..=599).contains(&status),
        "invalid buffered HTTP response status"
    );
    Ok(())
}

fn validate_header(name: &str, value: &str) -> Result<()> {
    hyper::header::HeaderName::from_bytes(name.as_bytes())?;
    hyper::header::HeaderValue::from_str(value)?;
    Ok(())
}

struct Writer {
    output: Vec<u8>,
    limit: usize,
}

impl Writer {
    fn new(magic: &[u8], limit: usize) -> Result<Self> {
        let mut writer = Self {
            output: Vec::new(),
            limit,
        };
        writer.append(magic)?;
        Ok(writer)
    }

    fn append(&mut self, bytes: &[u8]) -> Result<()> {
        ensure!(
            self.output
                .len()
                .checked_add(bytes.len())
                .is_some_and(|n| n <= self.limit),
            CommandFailure::Overloaded
        );
        self.output.extend_from_slice(bytes);
        Ok(())
    }

    fn bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let length = u32::try_from(bytes.len()).map_err(|_| CommandFailure::Overloaded)?;
        self.append(&length.to_be_bytes())?;
        self.append(bytes)
    }

    fn headers(&mut self, headers: &[(String, String)]) -> Result<()> {
        let count = u32::try_from(headers.len()).map_err(|_| CommandFailure::Overloaded)?;
        self.append(&count.to_be_bytes())?;
        for (name, value) in headers {
            validate_header(name, value)?;
            self.bytes(name.as_bytes())?;
            self.bytes(value.as_bytes())?;
        }
        Ok(())
    }
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn new(input: &'a [u8], magic: &[u8]) -> Result<Self> {
        ensure!(input.starts_with(magic), "invalid native HTTP wire format");
        Ok(Self(&input[magic.len()..]))
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let bytes = self
            .0
            .get(..length)
            .context("truncated native HTTP value")?;
        self.0 = &self.0[length..];
        Ok(bytes)
    }

    fn length(&mut self) -> Result<usize> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into()?) as usize)
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let length = self.length()?;
        self.take(length)
    }

    fn text(&mut self) -> Result<String> {
        Ok(std::str::from_utf8(self.bytes()?)?.to_owned())
    }

    fn headers(&mut self) -> Result<Vec<(String, String)>> {
        let count = self.length()?;
        // Validate physical framing before reserving from an untrusted count.
        ensure!(count <= self.0.len() / 8, "truncated native HTTP headers");
        let mut headers = Vec::new();
        for _ in 0..count {
            let name = self.text()?;
            let value = self.text()?;
            validate_header(&name, &value)?;
            headers.push((name, value));
        }
        Ok(headers)
    }

    fn finish(self) -> Result<()> {
        ensure!(self.0.is_empty(), "trailing native HTTP bytes");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> Request {
        Request {
            method: "PATCH".into(),
            url: "https://example.test/owned".into(),
            headers: vec![
                ("x-test".into(), "one".into()),
                ("x-test".into(), "two".into()),
            ],
            body: RequestBody::Bytes(bytes::Bytes::from_static(&[0, 255, 128])),
            entrypoint: None,
        }
    }

    fn response() -> HttpResponse {
        HttpResponse {
            status: 201,
            body: vec![0, 255, 128],
            headers: vec![
                ("set-cookie".into(), "a=1".into()),
                ("set-cookie".into(), "b=2".into()),
            ],
            stream: None,
            websocket: None,
            write_position: None,
            observed_position: None,
        }
    }

    #[test]
    fn bounded_wire_preserves_owned_http_and_duplicate_headers() -> Result<()> {
        let request = request();
        let wire = encode_request(Some("target"), &request, 4096)?;
        assert!(encode_request(Some("target"), &request, wire.len() - 1)
            .unwrap_err()
            .is::<CommandFailure>());
        assert_eq!(encode_request(Some("target"), &request, wire.len())?, wire);
        let (name, decoded) = decode_request(&wire)?;
        assert_eq!(name.as_deref(), Some("target"));
        assert_eq!(decoded.url, request.url);
        assert_eq!(decoded.method, request.method);
        assert_eq!(decoded.headers, request.headers);
        let RequestBody::Bytes(body) = decoded.body else {
            anyhow::bail!("owned body required")
        };
        assert_eq!(body.as_ref(), &[0, 255, 128]);
        let response = response();
        let wire = encode_response(&response, 4096)?;
        assert!(encode_response(&response, wire.len() - 1).is_err());
        let decoded = decode_response(&wire)?;
        assert_eq!(decoded.status, response.status);
        assert_eq!(decoded.headers, response.headers);
        assert_eq!(decoded.body, response.body);
        assert!(decoded.write_position.is_none() && decoded.observed_position.is_none());
        Ok(())
    }

    #[test]
    fn truncated_and_untrusted_lengths_never_create_partial_values() -> Result<()> {
        let wire = encode_request(None, &request(), 4096)?;
        for end in 0..wire.len() {
            assert!(decode_request(&wire[..end]).is_err());
        }
        let mut trailing = wire;
        trailing.push(0);
        assert!(decode_request(&trailing).is_err());
        let wire = encode_response(&response(), 4096)?;
        for end in 0..wire.len() {
            assert!(decode_response(&wire[..end]).is_err());
        }
        let mut invalid = RESPONSE.to_vec();
        invalid.extend_from_slice(&201u16.to_be_bytes());
        invalid.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(decode_response(&invalid).is_err());
        Ok(())
    }

    #[test]
    fn sessions_and_fleet_positions_cannot_enter_a_retained_receipt() {
        let mut request = request();
        request.body = RequestBody::Stream(1);
        assert!(encode_request(None, &request, 4096)
            .unwrap_err()
            .is::<CommandFailure>());
        let mut response = response();
        response.write_position = Some(1);
        assert!(encode_response(&response, 4096)
            .unwrap_err()
            .is::<CommandFailure>());
        response.write_position = None;
        response.observed_position = Some(1);
        assert!(encode_response(&response, 4096).is_err());
        response.observed_position = None;
        response.stream = Some(Box::pin(futures_util::stream::empty()));
        assert!(encode_response(&response, 4096).is_err());
    }

    #[test]
    fn invalid_http_framing_is_refused() {
        let mut request = request();
        request.method = "GET\r\nx".into();
        assert!(encode_request(None, &request, 4096).is_err());
        request.method = "GET".into();
        request.headers.push(("x-bad".into(), "one\r\ntwo".into()));
        assert!(encode_request(None, &request, 4096).is_err());
        let mut response = response();
        response.status = 101;
        assert!(encode_response(&response, 4096).is_err());
    }
}
