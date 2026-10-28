// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::rpc::{decode_reply, encode_envelope, Code, EffectOutcome, Limits};
use crate::engine_api::RpcData;
use prost::encoding::encode_varint;

const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn entered(args: RpcData) -> Entered {
    Entered::new(&Invocation::CellRpc {
        method: "test.Counter/Increment".into(),
        args,
    })
}

fn finish(data: RpcData) -> RpcData {
    let request = encode_envelope(DIGEST, vec![], 128).unwrap();
    let Output::Rpc(data) = entered(request).finish(Output::Rpc(data)).unwrap() else {
        panic!("changed RPC output flavor")
    };
    data
}

fn bytes(data: &RpcData) -> &[u8] {
    let RpcData::Structured(bytes) = data else {
        panic!("changed structured output flavor")
    };
    bytes
}

#[test]
fn raw_refusals_preserve_codes_and_digest_but_never_claim_rollback() {
    for version in 10..=16 {
        for code in 1..=12 {
            // Both spellings are legal Protobuf, including non-minimal keys,
            // lengths and enum varints. Neither can preserve a forged proof.
            for wire in [
                vec![0x12, 4, 8, code, 16, 1],
                vec![0x92, 0, 0x88, 0, 0x88, 0, code | 0x80, 0, 0x90, 0, 0x81, 0],
            ] {
                let data = encode_envelope(DIGEST, wire, 1024).unwrap();
                let mut encoded = bytes(&data).to_vec();
                assert_eq!(encoded[0], 0xff);
                encoded[1] = version;
                let data = finish(RpcData::Structured(encoded.into()));
                let failure = decode_reply(DIGEST, &data, Limits::default()).unwrap_err();
                assert_eq!(failure.code(), Code::try_from(i32::from(code)).unwrap());
                assert_eq!(failure.outcome(), EffectOutcome::Unknown);
            }
        }
    }
}

#[test]
fn invalid_failure_controls_remain_invalid_with_unknown_effects() {
    for wire in [
        vec![0x12, 0],
        vec![0x12, 4, 8, 99, 16, 1],
        vec![0x12, 4, 8, 2, 16, 99],
        vec![0x12, 6, 8, 2, 8, 2, 16, 1],
        vec![0x12, 6, 8, 2, 16, 1, 32, 0],
        vec![0x12, 2, 8, 128],
        vec![0x12, 4, 8, 2, 16, 1, 10, 0],
        vec![10, 0, 0x12, 4, 8, 2, 16, 1],
    ] {
        let data = encode_envelope(DIGEST, wire, 1024).unwrap();
        let original = bytes(&data).as_ptr();
        let data = finish(data);
        assert_eq!(bytes(&data).as_ptr(), original);
        let failure = decode_reply(DIGEST, &data, Limits::default()).unwrap_err();
        assert_eq!(failure.code(), Code::InvalidResponse);
        assert_eq!(failure.outcome(), EffectOutcome::Unknown);
    }
}

#[test]
fn response_buffers_are_not_copied_and_oversized_failure_text_stays_invalid() {
    for length in [0, 128, 1024, 1024 * 1024] {
        let data = envelope::reply(DIGEST, Ok(vec![0x5a; length]), Limits::default()).unwrap();
        let original = bytes(&data).as_ptr();
        let data = finish(data);
        assert_eq!(bytes(&data).as_ptr(), original);
        assert_eq!(
            decode_reply(DIGEST, &data, Limits::default())
                .unwrap()
                .len(),
            length
        );
    }
    for length in [257, 4096] {
        let mut payload = vec![8, 2, 16, 1, 26];
        encode_varint(length, &mut payload);
        payload.extend(std::iter::repeat_n(b'x', length as usize));
        let mut wire = vec![0x12];
        encode_varint(payload.len() as u64, &mut wire);
        wire.extend(payload);
        let data = encode_envelope(DIGEST, wire, 8192).unwrap();
        let original = bytes(&data).as_ptr();
        let data = finish(data);
        assert_eq!(bytes(&data).as_ptr(), original);
        let failure = decode_reply(DIGEST, &data, Limits::default()).unwrap_err();
        assert_eq!(failure.code(), Code::InvalidResponse);
        assert_eq!(failure.outcome(), EffectOutcome::Unknown);
    }
}

#[test]
fn untyped_requests_keep_opaque_typed_looking_results_and_json_ownership() {
    let data = encode_envelope(DIGEST, vec![0x12, 4, 8, 2, 16, 1], 1024).unwrap();
    let original = bytes(&data).as_ptr();
    let Output::Rpc(data) = entered(RpcData::Json("null".into()))
        .finish(Output::Rpc(data))
        .unwrap()
    else {
        panic!("changed untyped output flavor")
    };
    assert_eq!(bytes(&data).as_ptr(), original);
    assert_eq!(
        decode_reply(DIGEST, &data, Limits::default())
            .unwrap_err()
            .outcome(),
        EffectOutcome::NotCommitted,
    );

    let text = "opaque JSON business result".repeat(4096);
    let original = text.as_ptr();
    let Output::Rpc(RpcData::Json(text)) = entered(RpcData::Json("null".into()))
        .finish(Output::Rpc(RpcData::Json(text)))
        .unwrap()
    else {
        panic!("changed JSON output flavor")
    };
    assert_eq!(text.as_ptr(), original);
}
