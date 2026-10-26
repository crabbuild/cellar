// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use serde_json::json;

#[test]
fn connect_frames_survive_every_header_and_payload_split_and_require_terminal() {
    let event = json!({"event":{"data":{"stdout":"AP8q"}}});
    let mut bytes = frame(&event).unwrap();
    let mut terminal = frame(&json!({})).unwrap();
    terminal[0] = 2;
    bytes.extend(terminal);
    for split in 0..=bytes.len() {
        let mut decoder = Decoder::new(1024);
        let mut values = decoder.push(&bytes[..split]).unwrap();
        values.extend(decoder.push(&bytes[split..]).unwrap());
        assert_eq!(values.as_slice(), std::slice::from_ref(&event));
        decoder.finish().unwrap();
    }
    let mut decoder = Decoder::new(1024);
    for byte in &bytes {
        decoder.push(&[*byte]).unwrap();
    }
    decoder.finish().unwrap();
    assert!(decoder.push(&[0]).is_err());
    let mut decoder = Decoder::new(2);
    assert!(decoder.push(&bytes[..5]).is_err());
    let mut decoder = Decoder::new(1024);
    decoder.push(&bytes[..bytes.len() - 1]).unwrap();
    assert!(decoder.finish().is_err());
    let mut error = frame(&json!({"error":{"code":"unavailable"}})).unwrap();
    error[0] = 2;
    assert!(Decoder::new(1024).push(&error).is_err());
    assert!(Decoder::new(1024).push(&[1, 0, 0, 0, 0]).is_err());
    let mut invalid_terminal = frame(&Value::Null).unwrap();
    invalid_terminal[0] = 2;
    assert!(Decoder::new(1024).push(&invalid_terminal).is_err());
}
