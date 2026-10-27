// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fixed host transport categories carry no rollback or replay permission.
use crate::application::rpc::{Code, Failure};
use hyper::{HeaderMap, StatusCode};

const HEADER: &str = "x-cellar-rpc-failure";

pub(super) fn exhausted() -> super::HttpReply {
    let mut reply = super::peer_response(super::response(
        StatusCode::TOO_MANY_REQUESTS,
        "RPC receiver capacity exhausted",
    ));
    reply.headers_mut().insert(
        HEADER,
        hyper::header::HeaderValue::from_static("ResourceExhausted"),
    );
    reply.headers_mut().insert(
        hyper::header::CONNECTION,
        hyper::header::HeaderValue::from_static("close"),
    );
    reply
}

pub(super) fn remote(status: StatusCode, headers: &HeaderMap) -> Option<Failure> {
    let mut values = headers.get_all(HEADER).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    match (status, value.as_bytes()) {
        (StatusCode::GATEWAY_TIMEOUT, b"DeadlineExceeded") => {
            Some(Failure::new(Code::DeadlineExceeded))
        }
        (StatusCode::TOO_MANY_REQUESTS, b"ResourceExhausted") => {
            Some(Failure::new(Code::ResourceExhausted))
        }
        _ => None,
    }
}
