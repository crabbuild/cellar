// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{Code, Failure, Limits};
use crate::engine_api::RpcData;
use cellar_native::clone::{self, Object, StructuredValue, Value};
use prost::Message;

const TAG: &str = "cellar.protobuf";
const OVERHEAD: usize = 1024;

/// Encode an owned byte message in the existing native structured RPC format.
pub fn encode_envelope(digest: &str, bytes: Vec<u8>, limit: usize) -> Result<RpcData, Failure> {
    if !valid_digest(digest) {
        return Err(Failure::refused(Code::InvalidArgument));
    }
    if bytes.len() > limit {
        return Err(Failure::refused(Code::ResourceExhausted));
    }
    let graph = StructuredValue {
        root: Value::Reference(0),
        objects: vec![
            Object::DenseArray {
                elements: vec![
                    TAG.into(),
                    Value::Number(1.0),
                    digest.into(),
                    Value::Reference(1),
                ],
                properties: Vec::new(),
            },
            Object::ArrayBuffer {
                bytes,
                max_length: None,
                immutable: false,
            },
        ],
    };
    clone::encode_with_limits(&graph, clone_limits(limit))
        .map(|bytes| RpcData::Structured(bytes.into()))
        .map_err(|_| Failure::refused(Code::ResourceExhausted))
}

/// Decode only the exact typed RPC graph. Arbitrary clone graphs and capability
/// values are rejected before message decoding or application construction.
/// Metadata is bounded to 1 KiB. An oversized body returns ResourceExhausted
/// before copying; malformed metadata returns InvalidArgument.
pub fn decode_envelope(data: &RpcData, limit: usize) -> Result<(String, Vec<u8>), Failure> {
    let (digest, bytes) = view(data)?;
    if bytes.len() > limit {
        return Err(Failure::refused(Code::ResourceExhausted));
    }
    Ok((digest, bytes.to_vec()))
}

/// Inspect fixed metadata and borrow the body. Admission can reject size or
/// Protobuf work limits without allocating an owned copy of refused bytes.
pub(super) fn view(data: &RpcData) -> Result<(String, &[u8]), Failure> {
    let RpcData::Structured(bytes) = data else {
        return Err(Failure::refused(Code::InvalidArgument));
    };
    let (digest, bytes) = clone::decode_tagged_bytes(bytes, TAG, 1, 64)
        .map_err(|_| Failure::refused(Code::InvalidArgument))?;
    if !valid_digest(&digest) {
        return Err(Failure::refused(Code::InvalidArgument));
    }
    Ok((digest, bytes))
}

fn clone_limits(limit: usize) -> clone::Limits {
    clone::Limits {
        bytes: limit.saturating_add(OVERHEAD),
        objects: 2,
        values: 8,
        depth: 4,
    }
}

pub(super) fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

// Mirrors proto/cellar/rpc.proto. These plain message definitions keep protoc
// out of application builds; the schema/golden checks prevent wire drift.
#[derive(Clone, PartialEq, Message)]
struct Reply {
    #[prost(oneof = "ReplyResult", tags = "1, 2")]
    result: Option<ReplyResult>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum ReplyResult {
    #[prost(bytes, tag = "1")]
    Response(Vec<u8>),
    #[prost(message, tag = "2")]
    Failure(WireFailure),
}

#[derive(Clone, PartialEq, Message)]
struct WireFailure {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(int32, tag = "2")]
    outcome: i32,
    #[prost(string, tag = "3")]
    message: String,
}

pub(super) fn reply(
    digest: &str,
    result: Result<Vec<u8>, Failure>,
    limits: Limits,
) -> Result<RpcData, Failure> {
    let result = match result {
        Ok(bytes) => ReplyResult::Response(bytes),
        Err(failure) => ReplyResult::Failure(WireFailure {
            code: failure.code() as i32,
            outcome: failure.outcome() as i32,
            message: String::new(),
        }),
    };
    encode_envelope(
        digest,
        Reply {
            result: Some(result),
        }
        .encode_to_vec(),
        limits.reply_limit(),
    )
    .map_err(|_| Failure::new(Code::InvalidResponse))
}

/// Decode a typed result after the native call and its output gate completed.
/// Any malformed response has unknown effects, regardless of its claimed code.
pub fn response(digest: &str, data: &RpcData, limits: Limits) -> Result<Vec<u8>, Failure> {
    let (received, bytes) = decode_envelope(data, limits.reply_limit())
        .map_err(|_| Failure::new(Code::InvalidResponse))?;
    if digest != received {
        return Err(Failure::new(Code::InvalidResponse));
    }
    super::wire::validate_reply(&bytes, limits).map_err(|_| Failure::new(Code::InvalidResponse))?;
    let reply = Reply::decode(bytes.as_slice()).map_err(|_| Failure::new(Code::InvalidResponse))?;
    match reply.result {
        Some(ReplyResult::Response(bytes)) if bytes.len() <= limits.max_message_bytes => Ok(bytes),
        Some(ReplyResult::Failure(error)) if error.message.len() <= 256 => {
            Err(Failure::received(error.code, error.outcome)?)
        }
        _ => Err(Failure::new(Code::InvalidResponse)),
    }
}
