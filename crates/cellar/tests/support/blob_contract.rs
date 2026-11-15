// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared native Blob behavior through Fleet and shared-group execution.

use anyhow::{anyhow, Context as _, Result};
use cellar::application::blob::{
    Algorithm, Checksum, Conditional, GetOptions, GetResult, HttpMetadata, ListOptions, Multipart,
    Range, StorageClass, UploadedPart, WriteOptions,
};
use cellar::application::{Context, Output};
use cellar::engine_api::HttpResponse;
use futures_util::Stream;
use serde_json::json;
use sha2::Digest;
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context as TaskContext, Poll},
};
use tokio::sync::Notify;

#[derive(Default)]
pub(crate) struct Signals {
    pub(crate) gates: Mutex<Vec<cellar::actor::GateTicket>>,
    pub(crate) refuse: AtomicBool,
    pub(crate) upload: Mutex<Option<Multipart>>,
    pub(crate) staged_part: Mutex<Option<cellar::sdk::blob::ResumablePart>>,
    pub(crate) stream_started: Notify,
    pub(crate) stream_dropped: AtomicBool,
}

struct NeverStream {
    signals: Arc<Signals>,
    first: bool,
}

impl Stream for NeverStream {
    type Item = Result<Vec<u8>, String>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        if self.first {
            self.first = false;
            Poll::Ready(Some(Ok(vec![7; (8 << 20) + 1])))
        } else {
            self.signals.stream_started.notify_one();
            Poll::Pending
        }
    }
}

impl Drop for NeverStream {
    fn drop(&mut self) {
        self.signals.stream_dropped.store(true, Ordering::SeqCst);
    }
}

fn response() -> Output {
    Output::Http(HttpResponse {
        status: 200,
        body: b"checked".to_vec(),
        stream: None,
        headers: Vec::new(),
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}

async fn body(context: &Context, key: &str) -> Result<cellar::application::blob::ObjectBody> {
    let GetResult::Body(body) = context
        .blob("DATA")?
        .get(context, key, GetOptions::default())
        .await?
    else {
        return Err(anyhow!("missing body {key}"));
    };
    Ok(body)
}

pub(crate) async fn run_path(
    context: Context,
    path: &str,
    signals: Arc<Signals>,
    http_body: bool,
) -> Result<Output> {
    let bucket = context.blob("DATA")?;
    assert_eq!(bucket.name(), "native-objects");
    assert!(context.blob("missing").is_err());
    match path {
        "/no-bucket" => {
            assert!(bucket
                .head(&context, "record")
                .await
                .unwrap_err()
                .to_string()
                .contains("need a fleet bucket"));
        }
        "/basic" => {
            assert!(bucket.head(&context, "record").await?.is_none());
            assert!(matches!(
                bucket
                    .get(&context, "record", GetOptions::default())
                    .await?,
                GetResult::Missing
            ));
            let options = WriteOptions {
                http_metadata: HttpMetadata {
                    content_type: Some("text/plain".into()),
                    cache_control: Some("max-age=1".into()),
                    cache_expiry: Some(0),
                    ..Default::default()
                },
                custom_metadata: std::collections::BTreeMap::from([(
                    "Case".into(),
                    "雪 🐟".into(),
                )]),
                storage_class: Some(StorageClass::InfrequentAccess),
                checksums: std::collections::BTreeMap::from([(
                    Algorithm::Sha256,
                    Checksum::Bytes(sha2::Sha256::digest(b"alpha").to_vec()),
                )]),
                ..Default::default()
            };
            let object = bucket
                .put(&context, "record", b"alpha".to_vec(), options)
                .await?
                .unwrap();
            assert_eq!(object.size, 5);
            assert_eq!(object.custom_metadata["Case"], "雪 🐟");
            assert_eq!(object.storage_class, "InfrequentAccess");
            assert_eq!(
                object.checksum(Algorithm::Sha256).unwrap(),
                sha2::Sha256::digest(b"alpha").to_vec()
            );
            assert!(object.checksum(Algorithm::Md5).is_some());
            assert_eq!(object.http_etag, format!("\"{}\"", object.etag));
            let mut headers = vec![("Content-Type".into(), "wrong".into())];
            object.write_http_metadata(&mut headers);
            assert!(headers.contains(&("expires".into(), "Thu, 01 Jan 1970 00:00:00 GMT".into())));
            assert!(headers.contains(&("content-type".into(), "text/plain".into())));
            let mut value = body(&context, "record").await?;
            assert!(!value.body_used());
            assert_eq!(value.bytes(&context).await?, b"alpha");
            assert!(value.body_used());
            assert!(value.text(&context).await.is_err());
            let GetResult::Body(mut ranged) = bucket
                .get(
                    &context,
                    "record",
                    GetOptions {
                        range: Some(Range {
                            offset: Some(1),
                            length: Some(3),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )
                .await?
            else {
                panic!("range");
            };
            assert_eq!(ranged.object.range.as_ref().unwrap().length, 3);
            assert_eq!(ranged.text(&context).await?, "lph");
            let GetResult::Body(mut suffix) = bucket
                .get(
                    &context,
                    "record",
                    GetOptions {
                        range: Some(Range {
                            suffix: Some(2),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )
                .await?
            else {
                panic!("suffix");
            };
            assert_eq!(suffix.bytes(&context).await?, b"ha");
            let conditional = Conditional {
                etag_matches: Some("wrong".into()),
                ..Default::default()
            };
            assert!(matches!(
                bucket
                    .get(
                        &context,
                        "record",
                        GetOptions {
                            only_if: Some(conditional.clone()),
                            ..Default::default()
                        }
                    )
                    .await?,
                GetResult::Unmet(_)
            ));
            assert!(bucket
                .put(
                    &context,
                    "record",
                    b"bad".to_vec(),
                    WriteOptions {
                        only_if: Some(conditional),
                        ..Default::default()
                    }
                )
                .await?
                .is_none());
            assert_eq!(
                body(&context, "record").await?.text(&context).await?,
                "alpha"
            );
            bucket
                .put(
                    &context,
                    "json",
                    [b"\xef\xbb\xbf".as_slice(), br#"{"lone":"\ud800","n":-0}"#].concat(),
                    WriteOptions::default(),
                )
                .await?;
            let value = body(&context, "json").await?.json(&context).await?;
            assert_eq!(
                value
                    .property(&"lone".encode_utf16().collect::<Vec<_>>())?
                    .unwrap()
                    .root,
                cellar::native::clone::Value::String(vec![0xd800])
            );
            bucket
                .put(
                    &context,
                    "invalid-text",
                    vec![0xef, 0xbb, 0xbf, 0xff, 0xef, 0xbb, 0xbf],
                    WriteOptions::default(),
                )
                .await?;
            assert_eq!(
                body(&context, "invalid-text").await?.text(&context).await?,
                "�\u{feff}"
            );
        }
        "/preflight" => {
            let conditional = Conditional::from_headers(&[
                ("IF-MATCH".into(), "etag".into()),
                (
                    "If-Unmodified-Since".into(),
                    "Thu, 01 Jan 1970 00:00:00 GMT".into(),
                ),
            ])
            .unwrap();
            assert_eq!(conditional.etag_matches.as_deref(), Some("etag"));
            assert_eq!(conditional.uploaded_before, Some(0));
            for date in [
                "Thursday, 01-Jan-70 00:00:00 GMT",
                "Thu Jan  1 00:00:00 1970",
            ] {
                assert_eq!(
                    Conditional::from_headers(&[("If-Modified-Since".into(), date.into())])
                        .unwrap()
                        .uploaded_after,
                    Some(0)
                );
            }
            assert_eq!(Range::from_header("bytes=1-3")?.length, Some(3));
            assert_eq!(Range::from_header("bytes=-2")?.suffix, Some(2));
            assert_eq!(Range::from_header("bytes=3-")?.offset, Some(3));
            assert!(Range::from_header("bytes=1-2,5-6").is_err());
            let before = signals.gates.lock().unwrap().len();
            for key in ["".to_string(), "🐟".repeat(257)] {
                assert!(bucket
                    .put(&context, &key, Vec::new(), WriteOptions::default())
                    .await
                    .is_err());
            }
            assert!(bucket
                .delete(&context, &vec!["ok".into(); 1001])
                .await
                .is_err());
            assert!(bucket
                .create_multipart(
                    &context,
                    "invalid",
                    WriteOptions {
                        only_if: Some(Conditional {
                            etag_matches: Some("*".into()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }
                )
                .await
                .is_err());
            assert!(bucket
                .create_multipart(
                    &context,
                    "invalid",
                    WriteOptions {
                        checksums: std::collections::BTreeMap::from([(
                            Algorithm::Md5,
                            Checksum::Hex("bad".into())
                        )]),
                        ..Default::default()
                    }
                )
                .await
                .is_err());
            assert_eq!(signals.gates.lock().unwrap().len(), before);
        }
        "/rejected" => {
            assert!(bucket
                .put(&context, "rejected", vec![1], WriteOptions::default())
                .await
                .is_err());
        }
        "/list" => {
            for key in [
                "a//",
                "a/one",
                "a/two",
                "a/sub/one",
                "přehled.html",
                "a~",
                "aa",
            ] {
                bucket
                    .put(&context, key, vec![9], WriteOptions::default())
                    .await?;
            }
            let page = bucket
                .list(
                    &context,
                    ListOptions {
                        prefix: "a/".into(),
                        delimiter: Some("/".into()),
                        limit: Some(100),
                        ..Default::default()
                    },
                )
                .await?;
            assert!(page.delimited_prefixes.contains(&"a/sub/".into()));
            assert!(page
                .objects
                .iter()
                .all(|object| object.key.starts_with("a/")));
            let mut cursor = None;
            let mut found = Vec::new();
            loop {
                let page = bucket
                    .list(
                        &context,
                        ListOptions {
                            limit: Some(2),
                            cursor,
                            ..Default::default()
                        },
                    )
                    .await?;
                found.extend(page.objects.iter().map(|object| object.key.clone()));
                if !page.truncated {
                    assert!(page.cursor.is_none());
                    break;
                }
                cursor = page.cursor;
            }
            assert!(found.contains(&"přehled.html".into()));
            assert!(found.contains(&"a//".into()));
            let page = bucket
                .list(
                    &context,
                    ListOptions {
                        prefix: "record".into(),
                        include_http_metadata: true,
                        ..Default::default()
                    },
                )
                .await?;
            assert_eq!(
                page.objects[0].http_metadata.content_type.as_deref(),
                Some("text/plain")
            );
            assert!(page.objects[0].custom_metadata.is_empty());
            let page = bucket
                .list(
                    &context,
                    ListOptions {
                        prefix: "record".into(),
                        include_custom_metadata: true,
                        ..Default::default()
                    },
                )
                .await?;
            assert!(page.objects[0].http_metadata.content_type.is_none());
            assert_eq!(page.objects[0].custom_metadata["Case"], "雪 🐟");
            assert!(context
                .blob("OTHER")?
                .head(&context, "record")
                .await?
                .is_none());
            bucket
                .delete(&context, &["a/one".into(), "absent".into()])
                .await?;
            assert!(bucket.head(&context, "a/one").await?.is_none());
        }
        "/multipart" => {
            let empty_condition = bucket
                .create_multipart(
                    &context,
                    "empty-condition",
                    WriteOptions {
                        only_if: Some(Conditional::default()),
                        ..Default::default()
                    },
                )
                .await?;
            empty_condition.abort(&context).await?;
            let upload = bucket
                .create_multipart(&context, "multipart", WriteOptions::default())
                .await?;
            let two = upload.upload_part(&context, 2, b"old".to_vec()).await?;
            assert_eq!(two.etag, "");
            let two = upload.upload_part(&context, 2, b"beta".to_vec()).await?;
            let one = upload.upload_part(&context, 1, b"alpha".to_vec()).await?;
            assert!(upload
                .upload_part(&context, 1, b"bad".to_vec())
                .await
                .is_err());
            let object = upload.complete(&context, &[one, two]).await?;
            assert_eq!(object.size, 9);
            assert!(object.checksums.is_empty());
            assert_eq!(
                body(&context, "multipart").await?.text(&context).await?,
                "alphabeta"
            );
            upload.abort(&context).await?;
            let invalid = bucket
                .create_multipart(&context, "invalid-order", WriteOptions::default())
                .await?;
            let one = invalid.upload_part(&context, 1, b"one".to_vec()).await?;
            assert!(invalid
                .complete(
                    &context,
                    &[
                        UploadedPart {
                            part_number: 2,
                            etag: String::new()
                        },
                        one
                    ]
                )
                .await
                .is_err());
            invalid.abort(&context).await?;
            assert!(bucket.head(&context, "invalid-order").await?.is_none());
            let retained = bucket
                .create_multipart(&context, "retained", WriteOptions::default())
                .await?;
            retained.upload_part(&context, 2, b"beta".to_vec()).await?;
            *signals.upload.lock().unwrap() = Some(retained);
        }
        "/stage-owned" => {
            let upload = bucket
                .create_resumable_multipart(&context, "staged", WriteOptions::default())
                .await?;
            assert!(upload.supports_streamed_parts()?);
            let refused = upload.begin_part(&context, 1, 256 * 1024 * 1024 + 1).await;
            assert!(refused
                .err()
                .unwrap()
                .is::<cellar::sdk::blob::PartRefused>());
            let mut part = upload.begin_part(&context, 1, 2).await?;
            part.write(&context, bytes::Bytes::from_static(&[1]))
                .await?;
            context
                .storage()?
                .put_json("staged", &json!(upload.upload_id()))?;
            *signals.staged_part.lock().unwrap() = Some(part);
        }
        "/stage-foreign" => {
            let mut part = signals.staged_part.lock().unwrap().take().unwrap();
            let before = signals.gates.lock().unwrap().len();
            assert!(part
                .write(&context, bytes::Bytes::from_static(&[2]))
                .await
                .is_err());
            assert!(part.complete(&context).await.is_err());
            assert_eq!(
                signals.gates.lock().unwrap().len(),
                before,
                "foreign stage entered a new effect gate"
            );
            let saved = context.storage()?.get_json("staged")?.unwrap();
            let upload = bucket.resume_resumable_multipart("staged", saved.as_str().unwrap())?;
            let mut fresh = upload.begin_part(&context, 1, 2).await?;
            fresh
                .write(&context, bytes::Bytes::from_static(&[7, 8]))
                .await?;
            let receipt = fresh.complete(&context).await?;
            assert_eq!(upload.complete(&context, &[receipt]).await?.size, 2);
            assert_eq!(
                body(&context, "staged").await?.bytes(&context).await?,
                vec![7, 8]
            );
            upload.abort(&context).await?;
        }
        "/resumable-create" => {
            assert!(bucket.supports_resumable_multipart()?);
            bucket
                .put(
                    &context,
                    "resumable",
                    b"previous".to_vec(),
                    WriteOptions::default(),
                )
                .await?;
            let upload = bucket
                .create_resumable_multipart(
                    &context,
                    "resumable",
                    WriteOptions {
                        http_metadata: HttpMetadata {
                            content_type: Some("application/zstd".into()),
                            ..Default::default()
                        },
                        custom_metadata: std::collections::BTreeMap::from([(
                            "name".into(),
                            "backup 雪".into(),
                        )]),
                        ..Default::default()
                    },
                )
                .await?;
            let two = upload.upload_part(&context, 2, b"beta".to_vec()).await?;
            let obsolete = upload
                .upload_part(&context, 1, b"obsolete".to_vec())
                .await?;
            let one = upload.upload_part(&context, 1, b"alpha".to_vec()).await?;
            assert!(!one.etag.is_empty() && one.etag != obsolete.etag);
            assert!(upload
                .complete(&context, &[obsolete, two.clone()])
                .await
                .is_err());
            assert_eq!(
                body(&context, "resumable").await?.text(&context).await?,
                "previous"
            );
            context.storage()?.put_json(
                "resumable",
                &json!({"id":upload.upload_id(), "one":one.etag, "two":two.etag}),
            )?;
        }
        "/resumable-finish" => {
            let saved = context
                .storage()?
                .get_json("resumable")?
                .context("saved resumable upload missing")?;
            let id = saved["id"].as_str().context("upload ID missing")?;
            let upload = bucket.resume_resumable_multipart("resumable", id)?;
            let parts = [
                UploadedPart {
                    part_number: 1,
                    etag: saved["one"]
                        .as_str()
                        .context("first receipt missing")?
                        .into(),
                },
                UploadedPart {
                    part_number: 2,
                    etag: saved["two"]
                        .as_str()
                        .context("second receipt missing")?
                        .into(),
                },
            ];
            let foreign = context
                .blob("OTHER")?
                .resume_resumable_multipart("resumable", id)?;
            assert!(foreign
                .upload_part(&context, 1, b"foreign".to_vec())
                .await
                .is_err());
            assert!(bucket
                .resume_resumable_multipart("resumable", "invalid\0id")
                .is_err());
            signals.refuse.store(true, Ordering::SeqCst);
            assert!(upload.complete(&context, &parts).await.is_err());
            signals.refuse.store(false, Ordering::SeqCst);
            assert_eq!(
                body(&context, "resumable").await?.text(&context).await?,
                "previous"
            );
            let object = upload.complete(&context, &parts).await?;
            assert_eq!(object.size, 9);
            assert_eq!(
                object.http_metadata.content_type.as_deref(),
                Some("application/zstd")
            );
            assert_eq!(object.custom_metadata["name"], "backup 雪");
            assert_eq!(
                body(&context, "resumable").await?.text(&context).await?,
                "alphabeta"
            );
            upload.abort(&context).await?;
        }
        "/resume" => {
            let retained = signals.upload.lock().unwrap().clone().unwrap();
            let wrong = context
                .blob("OTHER")?
                .resume_multipart("retained", retained.upload_id())?;
            assert!(wrong.upload_part(&context, 1, Vec::new()).await.is_err());
            let resumed = bucket.resume_multipart("retained", retained.upload_id())?;
            let one = resumed.upload_part(&context, 1, b"alpha".to_vec()).await?;
            signals.refuse.store(true, Ordering::SeqCst);
            let two = UploadedPart {
                part_number: 2,
                etag: String::new(),
            };
            assert!(resumed
                .complete(&context, &[one.clone(), two.clone()])
                .await
                .is_err());
            signals.refuse.store(false, Ordering::SeqCst);
        }
        "/finish-resume" => {
            let retained = signals.upload.lock().unwrap().clone().unwrap();
            let resumed = bucket.resume_multipart("retained", retained.upload_id())?;
            let object = resumed
                .complete(
                    &context,
                    &[
                        UploadedPart {
                            part_number: 1,
                            etag: String::new(),
                        },
                        UploadedPart {
                            part_number: 2,
                            etag: String::new(),
                        },
                    ],
                )
                .await?;
            assert_eq!(object.size, 9);
            assert_eq!(
                body(&context, "retained").await?.text(&context).await?,
                "alphabeta"
            );
        }
        "/stream" => {
            let bytes = vec![6; (8 << 20) + 1];
            let digest = sha2::Sha256::digest(&bytes).to_vec();
            let result = bucket
                .put_stream(
                    &context,
                    "stream",
                    futures_util::stream::iter([Ok(bytes)]),
                    WriteOptions {
                        only_if: Some(Conditional::default()),
                        checksums: std::collections::BTreeMap::from([(
                            Algorithm::Sha256,
                            Checksum::Bytes(digest.clone()),
                        )]),
                        ..Default::default()
                    },
                )
                .await?
                .unwrap();
            assert_eq!(result.size, (8 << 20) + 1);
            assert_eq!(result.checksum(Algorithm::Sha256).unwrap(), digest);
            assert!(result.checksum(Algorithm::Md5).is_none());
            assert!(bucket
                .put_stream(
                    &context,
                    "conditional-stream",
                    futures_util::stream::iter([Ok(vec![6; (8 << 20) + 1])]),
                    WriteOptions {
                        only_if: Some(Conditional {
                            etag_matches: Some("*".into()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }
                )
                .await
                .is_err());
            assert!(bucket.head(&context, "conditional-stream").await?.is_none());
            assert!(bucket
                .put_stream(
                    &context,
                    "checksum-stream",
                    futures_util::stream::iter([Ok(vec![6; (8 << 20) + 1])]),
                    WriteOptions {
                        checksums: std::collections::BTreeMap::from([(
                            Algorithm::Sha256,
                            Checksum::Hex("wrong".into())
                        )]),
                        ..Default::default()
                    }
                )
                .await
                .is_err());
            assert!(bucket.head(&context, "checksum-stream").await?.is_none());
            if !http_body {
                let bytes = body(&context, "stream").await?.bytes(&context).await?;
                assert_eq!(bytes.len(), (8 << 20) + 1);
                assert!(bytes.iter().all(|byte| *byte == 6));
                return Ok(response());
            }
            return Ok(Output::Http(
                body(&context, "stream")
                    .await?
                    .into_response(&context, 200, Vec::new())?,
            ));
        }
        "/cancel-stream" => {
            bucket
                .put_stream(
                    &context,
                    "cancelled",
                    NeverStream {
                        signals,
                        first: true,
                    },
                    WriteOptions::default(),
                )
                .await?;
            return Err(anyhow!("cancelled stream must remain pending"));
        }
        _ => return Err(anyhow!("unknown path")),
    }
    Ok(response())
}
