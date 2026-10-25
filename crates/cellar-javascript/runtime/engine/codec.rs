// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Pure structured-clone conversion in the optional engine, never cell I/O.
use super::{refuse, Shared};
use cellar::native::clone;
use v8::{ValueDeserializerHelper, ValueSerializerHelper};

struct Codec;

impl v8::ValueSerializerImpl for Codec {
    fn throw_data_clone_error<'s>(
        &self,
        scope: &mut v8::PinScope<'s, '_>,
        _message: v8::Local<'s, v8::String>,
    ) {
        refuse(scope, "DataCloneError");
    }
}

impl v8::ValueDeserializerImpl for Codec {}

fn limit(scope: &v8::PinScope) -> usize {
    scope
        .get_slot::<Shared>()
        .map(|state| {
            let state = state.borrow();
            state
                .runtime
                .options
                .max_body_bytes
                .min(state.runtime.options.max_value_bytes)
        })
        .unwrap_or(0)
}

pub(super) fn encode(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue,
) {
    let maximum = limit(scope);
    let serializer = v8::ValueSerializer::new(scope, Box::new(Codec));
    serializer.write_header();
    if serializer.write_value(scope.get_current_context(), args.get(0)) != Some(true) {
        return;
    }
    let bytes = serializer.release();
    if bytes.len() > maximum {
        return refuse(scope, "ValueLimit");
    }
    let array = v8::ArrayBuffer::new_backing_store_from_vec(bytes).make_shared();
    let array = v8::ArrayBuffer::with_backing_store(scope, &array);
    let length = array.byte_length();
    if let Some(array) = v8::Uint8Array::new(scope, array, 0, length) {
        rv.set(array.into());
    }
}

pub(super) fn decode(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue,
) {
    let maximum = limit(scope);
    let Ok(array) = v8::Local::<v8::Uint8Array>::try_from(args.get(0)) else {
        return refuse(scope, "ValidationError");
    };
    let length = array.byte_length();
    if length > maximum {
        return refuse(scope, "ValueLimit");
    }
    let mut bytes = vec![0; length];
    array.copy_contents(&mut bytes);
    // Reject external/host capabilities and malformed graphs before passing any
    // stored bytes to the engine's decoder. Both engines use the same native
    // persisted-clone codec; there is no Node-specific serializer delegate.
    if clone::decode_with_limits(
        &bytes,
        clone::Limits {
            bytes: maximum,
            objects: 100_000,
            values: 200_000,
            depth: 64,
        },
    )
    .is_err()
    {
        return refuse(scope, "DataCloneError");
    }
    let deserializer = v8::ValueDeserializer::new(scope, Box::new(Codec), &bytes);
    if deserializer.read_header(scope.get_current_context()) != Some(true) {
        return refuse(scope, "DataCloneError");
    }
    if let Some(value) = deserializer.read_value(scope.get_current_context()) {
        rv.set(value);
    }
}
