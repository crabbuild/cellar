// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use serde_json::json;

#[test]
fn ndjson_preserves_split_utf8_and_refuses_incomplete_or_oversized_records() {
    let bytes = b"{\"stream\":\"stdout\",\"data\":\"\xc3\xa9\"}\n{\"end\":7}\n";
    for split in 0..=bytes.len() {
        let mut decoder = Decoder::new(1024);
        let mut values = decoder.push(&bytes[..split]).unwrap();
        values.extend(decoder.push(&bytes[split..]).unwrap());
        decoder.finish().unwrap();
        assert_eq!(
            values,
            vec![json!({"stream":"stdout","data":"é"}), json!({"end":7})]
        );
    }
    let mut decoder = Decoder::new(1024);
    decoder.push(&bytes[..bytes.len() - 1]).unwrap();
    assert!(decoder.finish().is_err());
    assert!(Decoder::new(2).push(bytes).is_err());
    assert!(Decoder::new(1024).push(b"null\n").is_err());
    assert!(Decoder::new(1024).push(b"{bad}\n").is_err());
}
