// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One ordinary KV API contract shared by Fleet and managed Quorum applications.
use anyhow::Result;
use cellar::sdk::{
    kv::{ListOptions, Namespace, PutOptions, ReadType, ReadValue},
    Context,
};
use futures_util::{StreamExt, TryStreamExt};
use serde_json::json;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

fn text(value: Option<ReadValue>) -> String {
    match value {
        Some(ReadValue::Text(value)) => value,
        _ => panic!("expected text"),
    }
}

fn bytes(value: Option<ReadValue>) -> Vec<u8> {
    match value {
        Some(ReadValue::Bytes(value)) => value,
        _ => panic!("expected bytes"),
    }
}

pub const PARTS: [&str; 5] = ["types", "bulk", "list", "unicode", "streams"];

pub async fn basic(context: &Context, namespace: &Namespace) -> Result<()> {
    for part in PARTS {
        phase(context, namespace, part).await?;
    }
    Ok(())
}

/// Identical assertions can run as independent admitted application requests.
/// This does not grant a long chain a fresh parent deadline between calls.
pub async fn phase(context: &Context, namespace: &Namespace, part: &str) -> Result<()> {
    match part {
        "types" => {
            assert!(namespace
                .get(context, "missing", ReadType::Text)
                .await?
                .is_none());
            namespace
                .put_text(
                    context,
                    "A/one",
                    "first",
                    PutOptions {
                        metadata: Some(json!({"n":1,"emoji":"🦀"})),
                        ..Default::default()
                    },
                )
                .await?;
            namespace
                .put_text(context, "A/two", "{\"answer\":42}", Default::default())
                .await?;
            namespace
                .put_text(context, "a/three", "lower", Default::default())
                .await?;
            namespace
                .put_bytes(context, "__proto__", vec![0, 255, 1], Default::default())
                .await?;
            namespace
                .put_bytes(
                    context,
                    "bom",
                    vec![239, 187, 191, 65, 255],
                    Default::default(),
                )
                .await?;
            assert_eq!(
                text(namespace.get(context, "bom", ReadType::Text).await?),
                "A�"
            );
            let value = namespace
                .get_with_metadata(context, "A/one", ReadType::Text)
                .await?;
            assert_eq!(text(value.value), "first");
            assert_eq!(value.metadata, Some(json!({"n":1,"emoji":"🦀"})));
            assert_eq!(value.cache_status, None);
        }
        "bulk" => {
            match namespace.get(context, "A/two", ReadType::Json).await? {
                Some(ReadValue::Json(value)) => assert_eq!(value, json!({"answer":42})),
                _ => panic!("expected JSON"),
            }
            assert!(namespace
                .get(context, "A/one", ReadType::Json)
                .await
                .is_err());
            let keys = vec!["__proto__".into(), "missing".into(), "__proto__".into()];
            let mut bulk = namespace.get_many(context, &keys, ReadType::Bytes).await?;
            assert_eq!(bulk.len(), 2);
            assert_eq!(
                bulk.keys().map(String::as_str).collect::<Vec<_>>(),
                ["__proto__", "missing"]
            );
            assert!(bulk.shift_remove("missing").unwrap().is_none());
            assert_eq!(bytes(bulk.shift_remove("__proto__").unwrap()), [0, 255, 1]);
            let metadata = namespace
                .get_many_with_metadata(
                    context,
                    &["A/one".into(), "missing".into()],
                    ReadType::Text,
                )
                .await?;
            assert_eq!(
                metadata["A/one"].metadata,
                Some(json!({"n":1,"emoji":"🦀"}))
            );
            assert!(metadata["missing"].value.is_none());
            for keys in [Vec::new(), vec!["x".into(); 101], vec![String::new()]] {
                assert!(namespace
                    .get_many(context, &keys, ReadType::Text)
                    .await
                    .is_err());
                assert!(namespace.delete_many(context, &keys).await.is_err());
            }
        }
        "list" => {
            let page = namespace
                .list(
                    context,
                    ListOptions {
                        prefix: "A/".into(),
                        limit: Some(1),
                        ..Default::default()
                    },
                )
                .await?;
            assert_eq!(page.keys[0].name, "A/one");
            assert!(!page.list_complete);
            assert_eq!(page.cursor.as_deref(), Some("412f6f6e65"));
            assert_eq!(page.cache_status, None);
            namespace
                .put_text(context, "A/zero", "later insertion", Default::default())
                .await?;
            let next = namespace
                .list(
                    context,
                    ListOptions {
                        prefix: "A/".into(),
                        limit: Some(1),
                        cursor: page.cursor,
                    },
                )
                .await?;
            assert_eq!(next.keys[0].name, "A/two");
            assert!(!next.list_complete);
            let final_page = namespace
                .list(
                    context,
                    ListOptions {
                        prefix: "A/".into(),
                        limit: Some(1),
                        cursor: next.cursor,
                    },
                )
                .await?;
            assert_eq!(final_page.keys[0].name, "A/zero");
            assert!(final_page.list_complete);
            assert_eq!(final_page.cursor, None);
            for cursor in ["0", "GG", "AB"] {
                assert!(namespace
                    .list(
                        context,
                        ListOptions {
                            cursor: Some(cursor.into()),
                            ..Default::default()
                        }
                    )
                    .await
                    .is_err());
            }
            // A forged cursor below the prefix must still filter and respect case.
            assert_eq!(
                namespace
                    .list(
                        context,
                        ListOptions {
                            prefix: "A/".into(),
                            cursor: Some("30".into()),
                            limit: Some(0)
                        }
                    )
                    .await?
                    .keys
                    .len(),
                3
            );
        }
        "unicode" => {
            for key in ["🦀/1", "🦀/2", "\u{d7ff}/x", "\u{e000}/x", "\u{10ffff}/1"] {
                namespace
                    .put_text(context, key, key, Default::default())
                    .await?;
            }
            for prefix in ["🦀/", "\u{d7ff}", "\u{10ffff}"] {
                let page = namespace
                    .list(
                        context,
                        ListOptions {
                            prefix: prefix.into(),
                            ..Default::default()
                        },
                    )
                    .await?;
                assert!(page.keys.iter().all(|key| key.name.starts_with(prefix)));
                assert!(!page.keys.is_empty());
            }
        }
        "streams" => {
            let stream = Box::pin(futures_util::stream::iter([
                Ok(vec![1, 2]),
                Ok(vec![255, 0]),
            ]));
            namespace
                .put_stream(context, "stream", stream, Default::default())
                .await?;
            let Some(ReadValue::Stream(mut stream)) =
                namespace.get(context, "stream", ReadType::Stream).await?
            else {
                panic!("expected stream")
            };
            assert_eq!(
                stream.try_next().await.map_err(anyhow::Error::msg)?,
                Some(vec![1, 2, 255, 0])
            );
            assert!(stream
                .try_next()
                .await
                .map_err(anyhow::Error::msg)?
                .is_none());
            let observed = Arc::new(AtomicBool::new(false));
            let last = observed.clone();
            let stream = Box::pin(
                futures_util::stream::iter([
                    Ok(vec![0; cellar_logic::kv::MAX_VALUE_BYTES]),
                    Ok(vec![1]),
                ])
                .chain(futures_util::stream::once(async move {
                    last.store(true, Ordering::SeqCst);
                    Ok(vec![2])
                })),
            );
            assert!(namespace
                .put_stream(context, "oversize-stream", stream, Default::default())
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("stream is larger"));
            assert!(!observed.load(Ordering::SeqCst));
            assert!(namespace
                .get(context, "oversize-stream", ReadType::Bytes)
                .await?
                .is_none());
            for (key, value, options, expected) in [
                (
                    String::new(),
                    Vec::new(),
                    PutOptions::default(),
                    "must not be empty",
                ),
                (
                    "🦀".repeat(129),
                    Vec::new(),
                    PutOptions::default(),
                    "512 bytes",
                ),
                (
                    "oversize".into(),
                    vec![0; cellar_logic::kv::MAX_VALUE_BYTES + 1],
                    PutOptions::default(),
                    "26214400 bytes",
                ),
                (
                    "metadata".into(),
                    Vec::new(),
                    PutOptions {
                        metadata: Some(json!("x".repeat(1023))),
                        ..Default::default()
                    },
                    "1024 bytes",
                ),
                (
                    "ttl".into(),
                    Vec::new(),
                    PutOptions {
                        expiration_ttl: Some(0.5),
                        ..Default::default()
                    },
                    "expirationTtl",
                ),
                (
                    "past".into(),
                    Vec::new(),
                    PutOptions {
                        expiration: Some(1.0),
                        ..Default::default()
                    },
                    "in the past",
                ),
            ] {
                assert!(namespace
                    .put_bytes(context, &key, value, options)
                    .await
                    .err()
                    .unwrap()
                    .to_string()
                    .contains(expected));
            }
        }
        _ => anyhow::bail!("unknown KV contract phase"),
    }
    Ok(())
}

pub async fn large(context: &Context, namespace: &Namespace) -> Result<()> {
    let value = (0..cellar_logic::kv::MAX_VALUE_BYTES)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    namespace
        .put_bytes(
            context,
            "large",
            value.clone(),
            PutOptions {
                metadata: Some(json!({"large":true})),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(
        bytes(namespace.get(context, "large", ReadType::Bytes).await?),
        value
    );
    Ok(())
}
