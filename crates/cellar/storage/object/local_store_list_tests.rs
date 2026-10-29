// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use anyhow::Result;

#[tokio::test]
async fn listing_decodes_only_the_requested_page_before_later_corruption() -> Result<()> {
    let domain = crate::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain.run(async {
        let directory = tempfile::tempdir()?;
        let store = LocalStore::open(directory.path().join("objects.sqlite"))?;
        for index in 0..512 {
            store.put_sync(format!("queue/{index:04}"), Bytes::new(), PutOptions::default())?;
        }
        store.connect()?.execute(
            "INSERT INTO __sys_local_objects(key, etag, modified_ms, attributes, body) VALUES ('queue/9999', 'invalid-etag', 0, '[]', X'')", [],
        )?;
        let mut listed = store.list(Some(&Path::from("queue")));
        for index in 0..128 {
            let object = listed.try_next().await?.expect("missing page entry");
            assert_eq!(object.location.as_ref(), format!("queue/{index:04}"));
        }
        assert!(listed.try_collect::<Vec<_>>().await.is_err());
        Ok(())
    }).await;
    domain.shutdown().await;
    result?
}

#[tokio::test]
async fn paged_listing_preserves_staged_body_sizes() -> Result<()> {
    let domain = crate::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let directory = tempfile::tempdir()?;
            let store = LocalStore::open(directory.path().join("objects.sqlite"))?;
            for index in 0..=LIST_PAGE_OBJECTS {
                let size = if index % LIST_PAGE_OBJECTS == 0 {
                    READ_CHUNK_BYTES + 17
                } else {
                    0
                };
                store.put_sync(
                    format!("selected/{index:04}"),
                    Bytes::from(vec![0x5a; size]),
                    PutOptions::default(),
                )?;
            }
            let objects = store
                .list(Some(&Path::from("selected")))
                .try_collect::<Vec<_>>()
                .await?;
            assert_eq!(objects.len(), LIST_PAGE_OBJECTS + 1);
            for (index, object) in objects.iter().enumerate() {
                assert_eq!(object.location.as_ref(), format!("selected/{index:04}"));
                assert_eq!(
                    object.size,
                    if index % LIST_PAGE_OBJECTS == 0 {
                        (READ_CHUNK_BYTES + 17) as u64
                    } else {
                        0
                    }
                );
            }
            Ok(())
        })
        .await;
    domain.shutdown().await;
    result?
}

#[test]
fn prefix_ranges_cover_unicode_boundaries_without_wildcards() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalStore::open(directory.path().join("objects.sqlite"))?;
    let keys = [
        "",
        "a",
        "a%_",
        "a%_/one",
        "ab",
        "a\u{10ffff}",
        "a\u{10ffff}/one",
        "\u{d7ff}",
        "\u{d7ff}/one",
        "\u{e000}",
        "\u{10ffff}",
        "\u{10ffff}/one",
    ];
    for key in keys {
        store.put_sync(key.into(), Bytes::new(), PutOptions::default())?;
    }
    for prefix in keys {
        let got: Vec<_> = store
            .metadata(prefix)?
            .into_iter()
            .map(|object| object.key)
            .collect();
        let mut expected: Vec<_> = keys
            .into_iter()
            .filter(|key| key.starts_with(prefix))
            .collect();
        expected.sort_unstable();
        assert_eq!(got, expected, "prefix {prefix:?}");
    }
    Ok(())
}

#[tokio::test]
async fn prefix_listing_does_not_decode_unrelated_metadata() -> Result<()> {
    let domain = crate::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let directory = tempfile::tempdir()?;
            let store = LocalStore::open(directory.path().join("objects.sqlite"))?;
            store.put_sync(
                "selected/one".into(),
                Bytes::from_static(b"one"),
                PutOptions::default(),
            )?;
            // A damaged row outside the requested prefix must not be read/decoded by
            // that listing. The unscoped listing still reports its corruption.
            store.connect()?.execute(
        "INSERT INTO __sys_local_objects(key, etag, modified_ms, attributes, body) VALUES ('unrelated/broken', 'invalid-etag', 0, '[]', X'')",
        [],
    )?;
            let prefix = Path::from("selected");
            let objects = store.list(Some(&prefix)).try_collect::<Vec<_>>().await?;
            assert_eq!(objects.len(), 1);
            assert_eq!(objects[0].location, Path::from("selected/one"));
            let delimited = store.list_with_delimiter(Some(&prefix)).await?;
            assert_eq!(delimited.objects, objects);
            let page = store
                .list_paginated(Some("selected/"), PaginatedListOptions::default())
                .await?;
            assert_eq!(page.result.objects, objects);
            assert!(store.list(None).try_collect::<Vec<_>>().await.is_err());
            Ok(())
        })
        .await;
    domain.shutdown().await;
    result?
}

#[tokio::test]
async fn path_prefixes_match_memory_store_with_literal_and_encoded_names() -> Result<()> {
    let domain = crate::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let directory = tempfile::tempdir()?;
            let store = LocalStore::open(directory.path().join("objects.sqlite"))?;
            let memory = object_store::memory::InMemory::new();
            for key in [
                "a",
                "a/one",
                "a/two/child",
                "ab/other",
                "a%_/literal",
                "axb/wildcard",
                "přehled.html/one",
                "雪/one",
            ] {
                let path = Path::from(key);
                store.put_sync(
                    path.to_string(),
                    Bytes::from_static(b"body"),
                    PutOptions::default(),
                )?;
                memory
                    .put(&path, Bytes::from_static(b"body").into())
                    .await?;
            }
            for prefix in [
                None,
                Some(""),
                Some("a"),
                Some("a%_"),
                Some("přehled.html"),
                Some("雪"),
                Some("missing"),
            ] {
                let prefix = prefix.map(Path::from);
                let locations = |objects: Vec<ObjectMeta>| {
                    objects.into_iter().map(|m| m.location).collect::<Vec<_>>()
                };
                assert_eq!(
                    locations(store.list(prefix.as_ref()).try_collect().await?),
                    locations(memory.list(prefix.as_ref()).try_collect().await?)
                );
                let got = store.list_with_delimiter(prefix.as_ref()).await?;
                let expected = memory.list_with_delimiter(prefix.as_ref()).await?;
                assert_eq!(got.common_prefixes, expected.common_prefixes);
                assert_eq!(locations(got.objects), locations(expected.objects));
            }
            Ok(())
        })
        .await;
    domain.shutdown().await;
    result?
}

#[tokio::test]
async fn raw_prefix_pages_preserve_grouping_and_resume_boundaries() -> Result<()> {
    let domain = crate::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let directory = tempfile::tempdir()?;
            let store = LocalStore::open(directory.path().join("objects.sqlite"))?;
            for key in [
                "raw%/a/one",
                "raw%/a/two",
                "raw%/b",
                "raw%/c/one",
                "rawx/other",
            ] {
                store.put_sync(
                    key.into(),
                    Bytes::from_static(b"body"),
                    PutOptions::default(),
                )?;
            }
            let mut token = None;
            let mut found = Vec::new();
            loop {
                let page = store
                    .list_paginated(
                        Some("raw%/"),
                        PaginatedListOptions {
                            max_keys: Some(1),
                            delimiter: Some("/".into()),
                            page_token: token,
                            ..Default::default()
                        },
                    )
                    .await?;
                found.extend(
                    page.result
                        .common_prefixes
                        .into_iter()
                        .map(|p| format!("group:{p}")),
                );
                found.extend(
                    page.result
                        .objects
                        .into_iter()
                        .map(|m| format!("object:{}", m.location)),
                );
                token = page.page_token;
                if token.is_none() {
                    break;
                }
                assert!(found.len() <= 3, "pagination did not advance");
            }
            assert_eq!(found, ["group:raw%/a", "object:raw%/b", "group:raw%/c"]);
            let page = store
                .list_paginated(
                    Some("raw%/"),
                    PaginatedListOptions {
                        offset: Some("raw%/b".into()),
                        ..Default::default()
                    },
                )
                .await?;
            assert_eq!(
                page.result
                    .objects
                    .iter()
                    .map(|m| m.location.as_ref())
                    .collect::<Vec<_>>(),
                ["raw%/c/one"]
            );
            Ok(())
        })
        .await;
    domain.shutdown().await;
    result?
}
