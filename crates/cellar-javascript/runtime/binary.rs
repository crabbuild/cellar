// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Copy exact views at the isolate boundary. No local handles cross an await.
use anyhow::{ensure, Context as _, Result};

pub(super) fn copy(
    scope: &v8::PinScope,
    value: v8::Local<v8::Value>,
    maximum: usize,
) -> Result<Option<Vec<u8>>> {
    if value.is_null_or_undefined() {
        return Ok(None);
    }
    let view = v8::Local::<v8::Uint8Array>::try_from(value)
        .context("JavaScript attachment must be Uint8Array")?;
    let backing = view
        .get_backing_store()
        .context("JavaScript attachment has no backing store")?;
    ensure!(
        !backing.is_shared() && !backing.is_resizable_by_user_javascript(),
        "JavaScript attachment requires fixed private storage"
    );
    let buffer = view
        .buffer(scope)
        .context("JavaScript attachment has no buffer")?;
    ensure!(
        !buffer.was_detached() && view.byte_length() <= maximum,
        "JavaScript attachment exceeds limit or is detached"
    );
    let mut bytes = vec![0; view.byte_length()];
    ensure!(
        view.copy_contents(&mut bytes) == bytes.len(),
        "JavaScript attachment copy failed"
    );
    Ok(Some(bytes))
}

pub(super) fn value<'s>(
    scope: &v8::PinScope<'s, '_>,
    body: Option<Vec<u8>>,
) -> Result<v8::Local<'s, v8::Value>> {
    let Some(body) = body else {
        return Ok(v8::undefined(scope).into());
    };
    let length = body.len();
    let backing = v8::ArrayBuffer::new_backing_store_from_vec(body).make_shared();
    let buffer = v8::ArrayBuffer::with_backing_store(scope, &backing);
    Ok(v8::Uint8Array::new(scope, buffer, 0, length)
        .context("JavaScript attachment allocation failed")?
        .into())
}
