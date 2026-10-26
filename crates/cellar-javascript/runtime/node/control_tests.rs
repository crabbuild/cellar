// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use serde_json::json;

#[test]
fn string_sizes_match_decoded_utf8_and_scratch_is_separate() -> Result<()> {
    for encoded in [
        r#""""#,
        r#""plain""#,
        "\"😀\u{feff}\"",
        r#""\u0000\u007f\u0080\u07ff\u0800\uffff""#,
        r#""\uD800\uDC00\uDBFF\uDFFF""#,
        r#""\"\\\/\b\f\n\r\t""#,
    ] {
        let decoded: String = serde_json::from_str(encoded)?;
        let wire = format!(r#"{{"payload":{encoded}}}"#);
        let bounds = preflight(wire.as_bytes(), 3)?;
        let strings = "payload".len() + decoded.len();
        assert_eq!(bounds.retained, strings + 3);
        assert_eq!(bounds.scratch, 2 * strings + 8);
    }
    Ok(())
}

#[test]
fn malformed_strings_and_composite_shapes_refuse_without_decoding() {
    for value in [
        r#"{"payload":"\uD800"}"#,
        r#"{"payload":"\uDC00"}"#,
        r#"{"payload":"\uD800\u0041"}"#,
        r#"{"payload":"\u000g"}"#,
        r#"{"payload":"\q"}"#,
        r#"{"payload":"unfinished}"#,
        r#"{"payload":[0]}"#,
        r#"{"payload":{}}"#,
        r#"{"payload":0,}"#,
        r#"{} false"#,
    ] {
        assert!(preflight(value.as_bytes(), 0).is_err(), "admitted {value}");
    }
    assert!(preflight(b"{\"payload\":\"\xff\"}", 0).is_err());
}

#[test]
fn literal_keys_fixed_fields_and_bounded_numbers_cover_protocol_controls() -> Result<()> {
    let mut fields = serde_json::Map::new();
    for index in 0..MAX_FIELDS {
        fields.insert(format!("field_{index}"), json!(false));
    }
    preflight(&serde_json::to_vec(&fields)?, 0)?;
    fields.insert("extra".into(), json!(false));
    assert!(preflight(&serde_json::to_vec(&fields)?, 0).is_err());
    for wire in [
        r#"{"id":9007199254740991,"has_body":false}"#,
        r#"{"budget_ms":4294967295,"failed":true}"#,
        r#"{"id":-1,"value":null}"#,
    ] {
        preflight(wire.as_bytes(), 0)?;
    }
    assert!(preflight(br#"{"id":12345678901234567890123456789012}"#, 0).is_ok());
    assert!(preflight(br#"{"id":123456789012345678901234567890123}"#, 0).is_err());
    assert!(preflight(br#"{"id":1,"id":2}"#, 0).is_err());
    assert!(preflight(br#"{"\u0069d":1}"#, 0).is_err());
    Ok(())
}

#[test]
fn string_accounting_matches_serialized_unicode_corpus() -> Result<()> {
    let mut state = 7_u32;
    for _ in 0..1024 {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        let Some(character) = char::from_u32(state % 0x11_0000) else {
            continue;
        };
        let value = format!("prefix\"\\\0{character}");
        let wire = serde_json::to_vec(&json!({"has_body":false,"payload":value}))?;
        let bounds = preflight(&wire, 0)?;
        assert_eq!(
            bounds.retained,
            "has_body".len() + "payload".len() + value.len()
        );
    }
    Ok(())
}
