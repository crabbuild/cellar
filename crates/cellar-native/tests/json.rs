// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use cellar_native::clone::{Limits, Object, StructuredValue, Value, ViewKind};
use cellar_native::json::{parse, parse_utf8, parse_with_limits, stringify, stringify_with_limits};

#[test]
fn json_retains_utf16_binary64_duplicate_keys_and_property_order() {
    let value=parse_utf8(r#"{"10":1,"2":2,"x":1,"x":3,"__proto__":4,"s":"\ud800","z":-0,"n":1e400,"big":9007199254740993}"#).unwrap();
    let Value::Reference(root) = value.root else {
        panic!("object")
    };
    let Object::Object(properties) = &value.objects[root as usize] else {
        panic!("object")
    };
    assert_eq!(properties[0].0, Value::from("2"));
    assert_eq!(properties[1].0, Value::from("10"));
    assert_eq!(properties[2], (Value::from("x"), Value::Number(3.0)));
    assert_eq!(properties[4].1, Value::String(vec![0xd800]));
    assert!(
        matches!(properties[5].1,Value::Number(number) if number==0.0 && number.is_sign_negative())
    );
    assert!(matches!(properties[6].1,Value::Number(number) if number==f64::INFINITY));
    assert_eq!(properties[7].1, Value::Number(9007199254740992.0));
    assert_eq!(
        stringify(&value).unwrap().unwrap(),
        r#"{"2":2,"10":1,"x":3,"__proto__":4,"s":"\ud800","z":0,"n":null,"big":9007199254740992}"#
    );
    let mut text = vec![34, 0xd800, 0xdc00, 0xd800, 34];
    let value = parse(&text).unwrap();
    assert_eq!(value.root, Value::String(vec![0xd800, 0xdc00, 0xd800]));
    assert_eq!(stringify(&value).unwrap().unwrap(), "\"𐀀\\ud800\"");
    text[0] = 0xfeff;
    assert!(
        parse(&text).is_err(),
        "JSON.parse does not consume a text BOM"
    );
}

#[test]
fn json_matches_number_and_builtin_data_serialization() {
    let graph = StructuredValue {
        root: Value::Reference(0),
        objects: vec![
            Object::DenseArray {
                elements: (1..=10).map(Value::Reference).collect(),
                properties: Vec::new(),
            },
            Object::Date(-1.0),
            Object::Date(-8_640_000_000_000_000.0),
            Object::Date(8_640_000_000_000_000.0),
            Object::Date(f64::NAN),
            Object::Boxed(Value::String(vec![0xd800])),
            Object::Boxed(Value::Number(-0.0)),
            Object::Map(vec![(
                Value::Reference(7),
                Value::BigInt {
                    negative: false,
                    magnitude: vec![1],
                },
            )]),
            Object::Set(vec![Value::Reference(8)]),
            Object::RegExp {
                source: vec![120],
                flags: 0,
            },
            Object::Error {
                prototype: None,
                message: Some(vec![120]),
                stack: None,
                cause: Some(Value::Reference(10)),
            },
        ],
    };
    assert_eq!(
        stringify(&graph).unwrap().unwrap(),
        r#"["1969-12-31T23:59:59.999Z","-271821-04-20T00:00:00.000Z","+275760-09-13T00:00:00.000Z",null,"\ud800",0,{},{},{},{}]"#
    );
    let graph = StructuredValue {
        root: Value::Reference(0),
        objects: vec![Object::DenseArray {
            elements: vec![
                Value::Hole,
                Value::Undefined,
                Value::Number(f64::NAN),
                Value::Number(-0.0),
                Value::Number(1e-7),
                Value::Number(1e-6),
                Value::Number(1e20),
                Value::Number(1e21),
                Value::Number(f64::from_bits(1)),
            ],
            properties: Vec::new(),
        }],
    };
    assert_eq!(
        stringify(&graph).unwrap().unwrap(),
        "[null,null,null,0,1e-7,0.000001,100000000000000000000,1e+21,5e-324]"
    );
}

#[test]
fn json_typed_views_use_their_element_values_and_offsets() {
    let graph = StructuredValue {
        root: Value::Reference(0),
        objects: vec![
            Object::View {
                buffer: 1,
                kind: ViewKind::Uint16,
                offset: 2,
                length: 4,
                flags: 0,
            },
            Object::ArrayBuffer {
                bytes: [9u16, 65535, 256, 9]
                    .into_iter()
                    .flat_map(u16::to_ne_bytes)
                    .collect(),
                max_length: None,
                immutable: false,
            },
        ],
    };
    assert_eq!(
        stringify(&graph).unwrap().unwrap(),
        r#"{"0":65535,"1":256}"#
    );
    for (kind, length, result) in [
        (ViewKind::DataView, 4, Some("{}")),
        (ViewKind::BigInt64, 0, Some("{}")),
        (ViewKind::BigInt64, 8, None),
    ] {
        let mut graph = graph.clone();
        graph.objects[0] = Object::View {
            buffer: 1,
            kind,
            offset: 0,
            length,
            flags: 0,
        };
        match result {
            Some(expected) => assert_eq!(stringify(&graph).unwrap().unwrap(), expected),
            None => assert!(stringify(&graph).is_err()),
        }
    }
    let mut bad = graph;
    bad.objects[0] = Object::View {
        buffer: 1,
        kind: ViewKind::Float64,
        offset: 2,
        length: 8,
        flags: 0,
    };
    assert!(stringify(&bad).is_err());
}

#[test]
fn json_aliases_expand_but_cycles_bigints_and_resource_expansion_fail() {
    let graph = StructuredValue {
        root: Value::Reference(0),
        objects: vec![
            Object::Object(vec![
                ("a".into(), Value::Reference(1)),
                ("b".into(), Value::Reference(1)),
            ]),
            Object::SparseArray {
                length: 3,
                properties: vec![
                    ("1".into(), Value::Number(2.0)),
                    ("extra".into(), Value::Undefined),
                ],
            },
        ],
    };
    assert_eq!(
        stringify(&graph).unwrap().unwrap(),
        r#"{"a":[null,2,null],"b":[null,2,null]}"#
    );
    assert!(stringify_with_limits(
        &graph,
        Limits {
            bytes: 8,
            ..Limits::default()
        }
    )
    .is_err());
    assert!(stringify_with_limits(
        &graph,
        Limits {
            values: 3,
            ..Limits::default()
        }
    )
    .is_err());
    assert!(stringify_with_limits(
        &graph,
        Limits {
            depth: 0,
            ..Limits::default()
        }
    )
    .is_err());
    let mut cycle = graph.clone();
    cycle.objects[1] = Object::Boxed(Value::Reference(0));
    assert!(stringify(&cycle).is_err());
    cycle.objects[1] = Object::Object(vec![("self".into(), Value::Reference(1))]);
    assert!(stringify(&cycle).is_err());
    cycle.root = Value::BigInt {
        negative: false,
        magnitude: vec![1],
    };
    assert!(stringify(&cycle).is_err());
    cycle.root = Value::Undefined;
    assert_eq!(stringify(&cycle).unwrap(), None);
    cycle.root = Value::Reference(u32::MAX);
    assert!(stringify(&cycle).is_err());
}

#[test]
fn json_parser_rejects_invalid_grammar_and_limits_expansion() {
    for text in [
        "",
        "undefined",
        "NaN",
        "Infinity",
        "01",
        "-",
        "1.",
        "1e",
        "1e+",
        "[1,]",
        "{\"x\":1,}",
        "{x:1}",
        "true false",
        "\"\\x\"",
        "\"\\u123\"",
        "\"\n\"",
        "\u{a0}null",
        "[",
        "{\"x\" 1}",
    ] {
        assert!(parse_utf8(text).is_err(), "accepted {text:?}");
    }
    let units = "[[null]]".encode_utf16().collect::<Vec<_>>();
    for limits in [
        Limits {
            bytes: 4,
            ..Limits::default()
        },
        Limits {
            objects: 1,
            ..Limits::default()
        },
        Limits {
            values: 2,
            ..Limits::default()
        },
        Limits {
            depth: 1,
            ..Limits::default()
        },
    ] {
        assert!(parse_with_limits(&units, limits).is_err());
    }
    assert_eq!(parse_utf8(" \t\r\nnull ").unwrap().root, Value::Null);
    let depth = "[".repeat(257) + "0" + &"]".repeat(257);
    assert!(parse_utf8(&depth).is_err());
}
