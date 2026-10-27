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
    // Borrow identity and control shape from the original frame. Going through
    // decode_envelope and Prost's Vec decoder would retain two temporary
    // body-sized buffers before returning the owned business body.
    let (received, bytes) = view(data).map_err(|_| Failure::new(Code::InvalidResponse))?;
    if digest != received {
        return Err(Failure::new(Code::InvalidResponse));
    }
    super::wire::validate_reply(bytes, limits).map_err(|_| Failure::new(Code::InvalidResponse))?;
    if let Some(body) = super::wire::response_payload(bytes) {
        if body.len() > limits.max_message_bytes {
            return Err(Failure::new(Code::InvalidResponse));
        }
        return Ok(body.to_vec());
    }
    // A valid failure has only two enums and at most 256 message bytes. Refuse
    // a large control before the failure codec allocates its text buffer.
    let payload = super::wire::failure_payload(bytes)
        .filter(|payload| payload.len() <= OVERHEAD)
        .ok_or_else(|| Failure::new(Code::InvalidResponse))?;
    let error = WireFailure::decode(payload).map_err(|_| Failure::new(Code::InvalidResponse))?;
    if error.message.len() > 256 {
        return Err(Failure::new(Code::InvalidResponse));
    }
    Err(Failure::received(error.code, error.outcome)?)
}

/// Normalize a raw typed result from entered application code, including
/// custom native cells that bypass Prepared::finish. Borrow successful payloads;
/// only a valid, fixed-size framework failure may require re-encoding.
pub(super) fn entered_reply(data: RpcData) -> Result<RpcData, Failure> {
    let Ok((digest, bytes)) = view(&data) else {
        // Malformed/non-value results already fail the caller's exact decoder
        // with unknown effects. Do not turn them into valid typed responses.
        return Ok(data);
    };
    // A valid failure contains two enums and at most 256 message bytes. Even
    // non-minimal varints fit this fixed control allowance. Larger frames are
    // either business responses or invalid failures; never copy their bodies.
    if bytes.len() > OVERHEAD {
        return Ok(data);
    }
    let Some(payload) = super::wire::failure_payload(bytes) else {
        return Ok(data);
    };
    let limits = Limits {
        max_message_bytes: 0,
        ..Default::default()
    };
    if super::wire::validate_reply(bytes, limits).is_err() {
        return Ok(data);
    }
    let Ok(error) = WireFailure::decode(payload) else {
        return Ok(data);
    };
    if error.message.len() > 256 {
        return Ok(data);
    }
    let Ok(failure) = Failure::received(error.code, error.outcome) else {
        return Ok(data);
    };
    if failure.outcome() != super::EffectOutcome::NotCommitted {
        return Ok(data);
    }
    // Entry into this parent is the authority boundary. Neither a forged
    // envelope nor an honest child's refusal can prove the parent rolled back.
    reply(&digest, Err(Failure::new(failure.code())), limits)
}
