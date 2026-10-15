//! Long polling has a declared wait in addition to the host transport budget.
use crate::engine_api::RpcData;

pub(super) fn duration(method: &str, args: &RpcData, transport_ms: u64, max_poll_ms: i64) -> u64 {
    if method != "__streamNext" {
        return transport_ms;
    }
    let wait =
        poll_wait(args).filter(|wait| i64::try_from(*wait).is_ok_and(|wait| wait <= max_poll_ms));
    transport_ms.saturating_add(wait.unwrap_or(0))
}

fn canonical_wait(text: &str) -> Option<u64> {
    let wait = text.parse::<i64>().ok()?;
    (wait >= 0 && wait.to_string() == text).then_some(wait as u64)
}

fn poll_wait(args: &RpcData) -> Option<u64> {
    // Budget inspection never materializes a large request or expands aliases.
    const MAX_BYTES: usize = 16 * 1024;
    match args {
        RpcData::Json(text) if text.len() <= MAX_BYTES => {
            let value: serde_json::Value = serde_json::from_str(text).ok()?;
            let args = value.as_array()?;
            if args.len() != 1 {
                return None;
            }
            canonical_wait(args[0].get("wait_ms")?.as_str()?)
        }
        RpcData::Structured(bytes) if bytes.len() <= MAX_BYTES => {
            use cellar_native::clone::{Object, Value};
            let value = cellar_native::clone::decode_with_limits(
                bytes,
                cellar_native::clone::Limits {
                    bytes: MAX_BYTES,
                    objects: 256,
                    values: 1024,
                    depth: 8,
                },
            )
            .ok()?;
            let Value::Reference(root) = value.root else {
                return None;
            };
            let Object::DenseArray { elements, .. } = value.objects.get(root as usize)? else {
                return None;
            };
            let [Value::Reference(body)] = elements.as_slice() else {
                return None;
            };
            let Object::Object(properties) = value.objects.get(*body as usize)? else {
                return None;
            };
            let mut wait = None;
            for (key, field) in properties {
                if !matches!(key, Value::String(key) if key.iter().copied().eq("wait_ms".encode_utf16()))
                {
                    continue;
                }
                if wait.is_some() {
                    return None;
                }
                let Value::String(text) = field else {
                    return None;
                };
                if text.len() > 19 {
                    return None;
                }
                wait = Some(canonical_wait(&String::from_utf16(text).ok()?)?);
            }
            wait
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validated_poll_wait_is_added_once_to_transport_budget() -> anyhow::Result<()> {
        let value = serde_json::json!([{"wait_ms":"30000"}]);
        let structured = crate::application::encode_rpc(
            &cellar_native::clone::StructuredValue::from_json(value.clone())?,
        )?;
        for args in [RpcData::Json(value.to_string()), structured] {
            assert_eq!(duration("__streamNext", &args, 15_000, 30_000), 45_000);
            assert_eq!(duration("__streamAck", &args, 15_000, 30_000), 15_000);
            assert_eq!(duration("__streamNext", &args, 15_000, 29_999), 15_000);
            assert_eq!(duration("__streamNext", &args, u64::MAX, 30_000), u64::MAX);
        }
        Ok(())
    }

    #[test]
    fn malformed_or_oversized_requests_cannot_extend_the_budget() -> anyhow::Result<()> {
        for text in ["-1", "01", "+1", "1.0", "9223372036854775808"] {
            let value = serde_json::json!([{"wait_ms":text}]);
            let structured = crate::application::encode_rpc(
                &cellar_native::clone::StructuredValue::from_json(value.clone())?,
            )?;
            for args in [RpcData::Json(value.to_string()), structured] {
                assert_eq!(duration("__streamNext", &args, 15_000, 30_000), 15_000);
            }
        }
        let args = RpcData::Json(
            serde_json::json!([{"wait_ms":"30000","padding":"x".repeat(16*1024)}]).to_string(),
        );
        assert_eq!(duration("__streamNext", &args, 15_000, 30_000), 15_000);
        assert_eq!(
            duration(
                "__streamNext",
                &RpcData::Structured(bytes::Bytes::from_static(b"bad")),
                15_000,
                30_000
            ),
            15_000
        );
        Ok(())
    }
}
