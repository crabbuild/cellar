// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Framework reply conversion copies one business body and bounds invalid controls.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

#[derive(Clone, Copy, Default)]
struct Observation {
    active: bool,
    live: isize,
    peak: usize,
}

thread_local! {
    // Constant TLS needs no heap allocation inside allocator callbacks. Other
    // harness threads and allocations outside the decode are excluded.
    static USAGE: Cell<Observation> = const { Cell::new(Observation {
        active: false, live: 0, peak: 0,
    }) };
}

fn record(delta: isize) {
    // Allocator callbacks must also tolerate thread-local teardown.
    let _ = USAGE.try_with(|usage| {
        let mut observation = usage.get();
        if observation.active {
            observation.live += delta;
            observation.peak = observation.peak.max(observation.live.max(0) as usize);
            usage.set(observation);
        }
    });
}

struct Measured;

// SAFETY: every allocation is forwarded to System with its original layout;
// thread-local counters do not allocate or change allocation ownership.
unsafe impl GlobalAlloc for Measured {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forward the caller's valid allocation layout unchanged.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record(layout.size() as isize);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        record(-(layout.size() as isize));
        // SAFETY: this pointer was allocated by System with this layout.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: preserve the original allocation's pointer/layout and the
        // caller's new size. Failed reallocations keep their old ownership.
        let next = unsafe { System.realloc(pointer, layout, size) };
        if !next.is_null() {
            record(size as isize - layout.size() as isize);
        }
        next
    }
}

#[global_allocator]
static ALLOCATOR: Measured = Measured;

use cellar::{engine_api::RpcData, sdk::rpc};

const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const MAX_BODY: usize = 1024 * 1024;

// Fixed metadata/error slack, independent of the business message size.
const CONTROL_SLACK: usize = 1024;

fn varint(mut value: usize, bytes: &mut Vec<u8>) {
    while value >= 128 {
        bytes.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    bytes.push(value as u8);
}

fn delimited(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut bytes = vec![tag];
    varint(body.len(), &mut bytes);
    bytes.extend_from_slice(body);
    bytes
}

fn envelope(wire: Vec<u8>) -> RpcData {
    rpc::encode_envelope(DIGEST, wire, MAX_BODY + 1024).unwrap()
}

fn measured(digest: &str, data: &RpcData) -> (Result<Vec<u8>, rpc::Failure>, Observation) {
    // Immutable schema initialization is startup work, outside per-call layouts.
    rpc::decode_reply(DIGEST, &envelope(vec![10, 0]), rpc::Limits::default()).unwrap();
    USAGE.with(|usage| {
        usage.set(Observation {
            active: true,
            ..Default::default()
        })
    });
    let result = rpc::decode_reply(digest, data, rpc::Limits::default());
    let observation = USAGE.with(|usage| {
        let observation = usage.get();
        usage.set(Observation::default());
        observation
    });
    (result, observation)
}

#[test]
fn reply_conversion_allocates_only_the_owned_business_body() {
    for length in [0, 1024, 64 * 1024, MAX_BODY] {
        let data = envelope(delimited(10, &vec![0x5a; length]));
        let (result, observation) = measured(DIGEST, &data);
        let bytes = result.unwrap();
        assert_eq!(bytes, vec![0x5a; length]);
        assert!(
            observation.peak <= length + CONTROL_SLACK,
            "body={length}: peak new allocation layouts {} exceed one body plus fixed controls",
            observation.peak
        );
        assert_eq!(observation.live, bytes.capacity() as isize);
    }
}

#[test]
fn invalid_replies_do_not_copy_large_business_buffers() {
    for length in [0, 1024, 64 * 1024, MAX_BODY] {
        let good = delimited(10, &vec![0x5a; length]);
        let mut duplicate = good.clone();
        duplicate.extend([10, 0]);
        let mut unknown = good.clone();
        unknown.extend([24, 1]);
        let mut truncated = good.clone();
        truncated.pop();
        for (digest, wire) in [
            (
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                good,
            ),
            (DIGEST, duplicate),
            (DIGEST, unknown),
            (DIGEST, truncated),
        ] {
            let (result, observation) = measured(digest, &envelope(wire));
            let error = result.unwrap_err();
            assert_eq!(error.code(), rpc::Code::InvalidResponse);
            assert_eq!(error.outcome(), rpc::EffectOutcome::Unknown);
            assert!(
                observation.peak <= CONTROL_SLACK,
                "body={length}: rejected reply copied {} allocation bytes",
                observation.peak
            );
            assert_eq!(observation.live, 0);
        }
    }
}

#[test]
fn oversized_failure_text_is_rejected_with_fixed_allocation_work() {
    for length in [257, 4096, MAX_BODY - 64] {
        let mut failure = vec![8, 2, 16, 1];
        failure.extend(delimited(26, &vec![b'x'; length]));
        let (result, observation) = measured(DIGEST, &envelope(delimited(18, &failure)));
        let error = result.unwrap_err();
        assert_eq!(error.code(), rpc::Code::InvalidResponse);
        assert_eq!(error.outcome(), rpc::EffectOutcome::Unknown);
        assert!(
            observation.peak <= CONTROL_SLACK,
            "text={length}: rejected control allocated {} bytes",
            observation.peak
        );
        assert_eq!(observation.live, 0);
    }
}

#[test]
fn borrowed_reply_parser_preserves_nonminimal_varints_and_failure_outcomes() {
    for version in 10..=16 {
        for (wire, expected) in [
            (vec![0x8a, 0, 0x83, 0, 1, 2, 3], Ok(vec![1, 2, 3])),
            (vec![0x8a, 0, 0x80, 0], Ok(vec![])),
            (
                vec![0x92, 0, 0x88, 0, 0x88, 0, 0x82, 0, 0x90, 0, 0x81, 0],
                Err(rpc::EffectOutcome::NotCommitted),
            ),
            (
                vec![0x92, 0, 0x88, 0, 0x88, 0, 0x82, 0, 0x90, 0, 0x80, 0],
                Err(rpc::EffectOutcome::Unknown),
            ),
        ] {
            let RpcData::Structured(encoded) = envelope(wire) else {
                unreachable!()
            };
            let mut encoded = encoded.to_vec();
            encoded[1] = version;
            let result = rpc::decode_reply(
                DIGEST,
                &RpcData::Structured(encoded.into()),
                rpc::Limits::default(),
            );
            match expected {
                Ok(bytes) => assert_eq!(result.unwrap(), bytes),
                Err(outcome) => {
                    let error = result.unwrap_err();
                    assert_eq!(error.code(), rpc::Code::MethodNotFound);
                    assert_eq!(error.outcome(), outcome);
                }
            }
        }
    }
}

#[test]
fn oversized_business_reply_is_rejected_before_copying_the_body() {
    for length in [1024, 64 * 1024, MAX_BODY] {
        let data = envelope(delimited(10, &vec![0x5a; length]));
        let limits = rpc::Limits {
            max_message_bytes: length - 1,
            ..Default::default()
        };
        rpc::decode_reply(DIGEST, &envelope(vec![10, 0]), limits).unwrap();
        USAGE.with(|usage| {
            usage.set(Observation {
                active: true,
                ..Default::default()
            })
        });
        let result = rpc::decode_reply(DIGEST, &data, limits);
        let observation = USAGE.with(|usage| {
            let current = usage.get();
            usage.set(Observation::default());
            current
        });
        let error = result.unwrap_err();
        assert_eq!(error.code(), rpc::Code::InvalidResponse);
        assert_eq!(error.outcome(), rpc::EffectOutcome::Unknown);
        assert!(
            observation.peak <= CONTROL_SLACK,
            "body={length}: byte-limit refusal copied {} allocation bytes",
            observation.peak
        );
        assert_eq!(observation.live, 0);
    }
}
