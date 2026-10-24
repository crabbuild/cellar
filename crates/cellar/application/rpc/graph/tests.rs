// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default, Debug)]
struct EncoderMustNotRun;

static DISCARDED: AtomicUsize = AtomicUsize::new(0);

impl Message for EncoderMustNotRun {
    fn encode_raw(&self, _: &mut impl prost::bytes::BufMut) {
        panic!("allocating encoder reached");
    }

    fn encoded_len(&self) -> usize {
        panic!("recursive sizing reached");
    }

    fn merge_field(
        &mut self,
        _: u32,
        _: prost::encoding::WireType,
        _: &mut impl prost::bytes::Buf,
        _: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        panic!("decoder reached");
    }

    fn clear(&mut self) {}
}

impl MessageGraph for EncoderMustNotRun {
    fn visit(&self, budget: &mut GraphBudget, _: usize) -> Result<(), Failure> {
        budget.depth(65)
    }

    fn discard(self: Box<Self>, _: &mut GraphDiscard) {
        DISCARDED.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn failed_graph_preflight_never_calls_prost_sizing_or_writer_and_discards_ownership() {
    DISCARDED.store(0, Ordering::SeqCst);
    let failure = encode_message(EncoderMustNotRun, Limits::default()).unwrap_err();
    assert_eq!(failure.code(), Code::ResourceExhausted);
    assert_eq!(DISCARDED.load(Ordering::SeqCst), 1);
}
