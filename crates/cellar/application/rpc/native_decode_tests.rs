// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn child(name: &str, repeated: bool) -> Field {
    Field {
        kind: Type::Message,
        type_name: name.into(),
        repeated,
    }
}

#[test]
fn inline_layouts_include_absent_children_but_stop_at_containers_and_cycles() {
    let messages = BTreeMap::from([
        (
            "Wide".into(),
            (1..=512)
                .map(|number| {
                    (
                        number,
                        Field {
                            kind: Type::String,
                            type_name: String::new(),
                            repeated: false,
                        },
                    )
                })
                .collect(),
        ),
        ("Outer".into(), BTreeMap::from([(1, child("Wide", false))])),
        ("Batch".into(), BTreeMap::from([(1, child("Outer", true))])),
        (
            "Recursive".into(),
            BTreeMap::from([(1, child("Recursive", false))]),
        ),
    ]);
    let sizes = layouts(&messages);
    assert_eq!(sizes["Wide"], 513);
    assert_eq!(sizes["Outer"], 515);
    assert_eq!(sizes["Batch"], 2);
    assert_eq!(sizes["Recursive"], 2);
}

#[test]
fn long_reference_fanout_saturates_without_recursing_or_wrapping() {
    let mut messages = BTreeMap::new();
    for index in 0..1024 {
        let fields = if index == 1023 {
            BTreeMap::new()
        } else {
            let next = format!("Message{}", index + 1);
            BTreeMap::from([(1, child(&next, false)), (2, child(&next, false))])
        };
        messages.insert(format!("Message{index}"), fields);
    }
    let sizes = layouts(&messages);
    assert_eq!(sizes.len(), 1024);
    assert_eq!(sizes["Message0"], usize::MAX);
    assert_eq!(sizes["Message1023"], 1);
}
