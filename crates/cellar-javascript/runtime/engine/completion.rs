// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A terminal result cannot depend on capacity held by other admitted work.
//! Inspect first, extend the invocation's reserved allowance, then copy bytes.
use super::*;

const EXHAUSTED: &str = r#"{"rpc_error":"ResourceExhausted"}"#;
const INVALID: &str = r#"{"rpc_error":"InvalidResponse"}"#;

pub(super) enum Outcome<'s> {
    Resolved(v8::Local<'s, v8::Value>),
    Rejected(v8::Local<'s, v8::Value>),
    Failed,
}

enum Content<'s> {
    Fixed(&'static str),
    Text(v8::Local<'s, v8::String>, usize),
    Body(v8::Local<'s, v8::Uint8Array>),
}

impl Content<'_> {
    fn bytes(&self) -> usize {
        match self {
            Self::Fixed(value) => value.len(),
            Self::Text(_, bytes) => *bytes,
            // Inspection caps the body at the value ceiling minus these bytes.
            Self::Body(value) => 4 + value.byte_length(),
        }
    }
}

fn inspect<'s>(scope: &v8::PinScope, maximum: usize, outcome: Outcome<'s>) -> (Content<'s>, bool) {
    let result = match outcome {
        Outcome::Failed => return (Content::Fixed("null"), true),
        Outcome::Rejected(value) => {
            // Only canonical categories leave entered handlers. Even category
            // inspection stays inside the already reserved control allowance.
            let payload = v8::Local::<v8::String>::try_from(value)
                .ok()
                .filter(|value| value.utf8_length(scope) <= 64)
                .and_then(|value| {
                    cellar_guest::rpc_failure::payload(&value.to_rust_string_lossy(scope))
                })
                .unwrap_or("null");
            return (Content::Fixed(payload), true);
        }
        Outcome::Resolved(value) => value,
    };
    if result.is_uint8_array() {
        match super::super::binary::view(scope, result, maximum.saturating_sub(4)) {
            Ok(Some(view)) => (Content::Body(view), false),
            _ => (Content::Fixed(INVALID), true),
        }
    } else if let Ok(value) = v8::Local::<v8::String>::try_from(result) {
        let bytes = value.utf8_length(scope);
        if bytes <= maximum {
            (Content::Text(value, bytes), false)
        } else {
            (Content::Fixed(INVALID), true)
        }
    } else {
        (Content::Fixed(INVALID), true)
    }
}

pub(super) fn event(
    scope: &v8::PinScope,
    runtime: &Runtime,
    outcome: Outcome<'_>,
    mut reservation: OwnedSemaphorePermit,
) -> Event {
    let (mut content, mut failed) = inspect(scope, runtime.options.max_value_bytes, outcome);
    if content.bytes() > reservation.num_permits() {
        match runtime.reserve(content.bytes() - reservation.num_permits()) {
            Ok(extra) => reservation.merge(extra),
            Err(_) => {
                content = Content::Fixed(EXHAUSTED);
                failed = true;
            }
        }
    }
    let (payload, body) = match content {
        Content::Fixed(value) => (value.to_owned(), None),
        Content::Text(value, _) => (value.to_rust_string_lossy(scope), None),
        Content::Body(view) => match super::super::binary::copy_view(Some(view)) {
            Ok(body) => {
                #[cfg(test)]
                runtime
                    .completion_copied
                    .fetch_add(body.as_ref().map_or(0, Vec::len) as u64, Ordering::Relaxed);
                ("null".to_owned(), body)
            }
            Err(_) => {
                failed = true;
                (INVALID.to_owned(), None)
            }
        },
    };
    Event::Complete {
        payload,
        body,
        failed,
        _bytes: reservation,
    }
}
