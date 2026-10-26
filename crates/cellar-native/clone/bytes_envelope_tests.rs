// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn graph(label: &str, body: Vec<u8>) -> StructuredValue {
    StructuredValue {
        root: Value::Reference(0),
        objects: vec![
            Object::DenseArray {
                elements: vec![
                    "test.bytes".into(),
                    Value::Number(1.0),
                    label.into(),
                    Value::Reference(1),
                ],
                properties: vec![],
            },
            Object::ArrayBuffer {
                bytes: body,
                max_length: None,
                immutable: false,
            },
        ],
    }
}

#[test]
fn binary_envelope_borrows_its_exact_body_across_supported_versions() {
    for body in [vec![], vec![0, 255, 128], vec![42; 1024 * 1024]] {
        let mut encoded = encode(&graph("标签", body.clone())).unwrap();
        for version in 10..=16 {
            encoded[1] = version;
            let (label, borrowed) = decode_tagged_bytes(&encoded, "test.bytes", 1, 64).unwrap();
            assert_eq!(label, "标签");
            assert_eq!(borrowed, body);
            // The returned slice is backed by the original encoded allocation.
            let start = encoded.as_ptr() as usize;
            let at = borrowed.as_ptr() as usize;
            assert!(at >= start && at + borrowed.len() <= start + encoded.len());
        }
    }
}

#[test]
fn binary_envelope_accepts_existing_primitive_and_wrapper_encodings() {
    let original = encode(&graph("label", vec![0, 255])).unwrap();
    let marker = original.iter().position(|byte| *byte == b'N').unwrap();
    for number in [
        vec![b'N', 0, 0, 0, 0, 0, 0, 0xf0, 0x3f],
        vec![b'I', 2],
        vec![b'U', 1],
    ] {
        for utf8 in [false, true] {
            let mut encoded = original.clone();
            encoded.splice(marker..marker + 9, number.clone());
            if utf8 {
                // Both labels are ASCII, so Latin-1 and UTF-8 have identical
                // bytes. The binary body contains no quote tag.
                for byte in &mut encoded {
                    if *byte == b'"' {
                        *byte = b'S';
                    }
                }
            }
            for wrapped in [false, true] {
                let mut encoded = encoded.clone();
                if wrapped {
                    encoded.splice(2..2, [b'?', 2]);
                }
                let owned = decode_with_limits(
                    &encoded,
                    Limits {
                        objects: 2,
                        values: 8,
                        depth: 4,
                        ..Limits::default()
                    },
                )
                .unwrap();
                assert_eq!(owned, graph("label", vec![0, 255]));
                assert_eq!(
                    decode_tagged_bytes(&encoded, "test.bytes", 1, 64).unwrap(),
                    ("label".into(), &[0, 255][..])
                );
            }
        }
    }
}

#[test]
fn malformed_envelopes_cannot_allocate_or_hide_unbounded_metadata() {
    let encoded = encode(&graph("label", vec![0, 255])).unwrap();
    for end in 0..encoded.len() {
        assert!(decode_tagged_bytes(&encoded[..end], "test.bytes", 1, 64).is_err());
    }
    assert!(decode_tagged_bytes(&encoded, "wrong", 1, 64).is_err());
    assert!(decode_tagged_bytes(&encoded, "test.bytes", 2, 64).is_err());
    assert!(decode_tagged_bytes(&encoded, "test.bytes", 1, 4).is_err());
    let mut padded = encoded.clone();
    padded.extend(vec![0; METADATA_BYTES]);
    assert!(decode_tagged_bytes(&padded, "test.bytes", 1, 64).is_err());
    for mutate in 0..4 {
        let mut value = graph("label", vec![0, 255]);
        if let Object::DenseArray {
            elements,
            properties,
        } = &mut value.objects[0]
        {
            match mutate {
                0 => properties.push(("hidden".into(), "value".into())),
                1 => elements.push(Value::Null),
                _ => {}
            }
        }
        if let Object::ArrayBuffer {
            max_length,
            immutable,
            ..
        } = &mut value.objects[1]
        {
            match mutate {
                2 => *max_length = Some(1024),
                3 => *immutable = true,
                _ => {}
            }
        }
        let encoded = encode(&value).unwrap();
        assert!(decode_tagged_bytes(&encoded, "test.bytes", 1, 64).is_err());
    }
}
