//! Native Blob calls cross real cell turns and output gates, without engine initialization.

use anyhow::{anyhow, Result};
use cellar::application::blob::{
    Algorithm, Checksum, Conditional, GetOptions, GetResult, HttpMetadata, ListOptions, Multipart,
    Range, StorageClass, UploadedPart, WriteOptions,
};
use cellar::application::{Cell, Context, HandlerFuture, Invocation, Output, Registry, Storage};
use cellar::engine_api::{Compat, HttpResponse, RuntimeFetch, RuntimeOptions, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::runtime::RuntimeManager;
use futures_util::{Stream, TryStreamExt};
use serde_json::json;
use sha2::Digest;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;
use tokio::sync::Notify;

const SCRIPT: &str = "native-blob";
const SCOPE: &str = "Objects:one";

#[derive(Default)]
struct Signals {
    gates: Mutex<Vec<cellar::actor::GateTicket>>,
    entered: Notify,
    release: Notify,
    held: AtomicBool,
    refuse: AtomicBool,
    upload: Mutex<Option<Multipart>>,
    stream_started: Notify,
    stream_dropped: AtomicBool,
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

struct Objects(Storage, Arc<Signals>);

impl Cell for Objects {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let calls = self
                .0
                .get_json("calls")?
                .and_then(|value| value.as_u64())
                .unwrap_or(0)
                + 1;
            self.0.put_json("calls", &json!(calls))?;
            worker(context, invocation, self.1.clone()).await
        })
    }
}

fn graph() -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        native: None,
        options: WorkerConfigOptions {
            src: String::new(),
            script_name: SCRIPT.into(),
            do_classes: vec!["Objects".into()],
            bindings: Vec::new(),
            blob_bindings: vec![
                ("DATA".into(), "native-objects".into()),
                ("OTHER".into(), "other-objects".into()),
            ],
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: "test".into(),
            modules: Vec::new(),
            compat: Compat::default(),
        },
        script_name: SCRIPT.into(),
        version: "test".into(),
        prefix: "test/".into(),
        asset_binding: None,
        loader_bindings: Vec::new(),
        assets: None,
        services: Vec::new(),
        crons: Vec::new(),
        cron_revision: None,
        containers: Vec::new(),
        fence_image: None,
        container_images: Vec::new(),
    })
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

async fn worker(context: Context, invocation: Invocation, signals: Arc<Signals>) -> Result<Output> {
    let Invocation::Fetch(request) = invocation else {
        return Err(anyhow!("expected fetch"));
    };
    let bucket = context.blob("DATA")?;
    assert_eq!(bucket.name(), "native-objects");
    assert!(context.blob("missing").is_err());
    let url = url::Url::parse(&request.url)?;
    match url.path() {
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

async fn open(runtime: &RuntimeManager) -> Result<()> {
    runtime
        .restore_cell(
            SCOPE,
            &cellar_logic::RestoreSpec {
                epoch: 1,
                fresh: true,
                took_over: false,
                resume_local: false,
                prior: None,
            },
        )
        .await?;
    runtime.start_cell(SCOPE.into(), 1, true).await?;
    runtime.publish_cell(SCOPE, 1)
}

async fn call(runtime: &RuntimeManager, path: &str) -> Result<HttpResponse> {
    runtime
        .fetch_cell(
            SCOPE.into(),
            None,
            RuntimeFetch {
                url: format!("http://objects/{path}"),
                method: "GET".into(),
                body: Vec::new().into(),
                headers: Vec::new(),
                request_id: Some(cellar::engine_api::next_request_id()),
                order: None,
                parent: None,
            },
            None,
        )
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_blob_preserves_records_streams_multipart_and_durability_gates() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(30), run()).await??;
    Ok(())
}

async fn run() -> Result<()> {
    let signals = Arc::new(Signals::default());
    let worker_signals = signals.clone();
    let cell_signals = signals.clone();
    let registry = Registry::new(move |context, invocation| {
        Box::pin(worker(context, invocation, worker_signals.clone()))
    })
    .cell("Objects", move |context, _| {
        Ok(Arc::new(Objects(context.storage()?, cell_signals.clone())))
    })?;
    let generation = Generation::build_native(
        1,
        graph(),
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([(
            SCRIPT.into(),
            Arc::new(registry) as Arc<dyn cellar::application::Application>,
        )]),
    )?;
    let directory = tempfile::tempdir()?;
    let runtime = RuntimeManager::start(
        generation,
        RuntimeOptions {
            data_dir: directory.path().join("cells"),
            replication: None,
            wake: None,
            alarm_observer: Arc::new(|_, _| {}),
            node: "test".into(),
            region: "test".into(),
            bucket: None,
        },
    )?;
    open(&runtime).await?;
    call(&runtime, "no-bucket").await?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("bucket.sqlite"))?;
    cellar::host_channels::set_blob_store(bucket.clone());
    let (tx, mut gates) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_gate_tx(tx);
    let (shutdown, mut stop) = tokio::sync::watch::channel(false);
    let captured = signals.clone();
    let adapter = cellar::asyncrt::spawn(async move {
        while let Some(gate) = cellar::asyncrt::select_biased! {"native Blob proof adapter stops";_=stop.changed()=>None,gate=gates.recv()=>gate,}
        {
            captured.gates.lock().unwrap().push(gate.ticket);
            if !captured.held.swap(true, Ordering::SeqCst) {
                captured.entered.notify_one();
                captured.release.notified().await;
            }
            let result = if captured.refuse.load(Ordering::SeqCst) {
                Err(cellar_logic::RequestError::DurabilityUnproven)
            } else {
                Ok(())
            };
            let _ = gate.reply.send(result);
        }
    });
    let caller = runtime.clone();
    let basic = cellar::asyncrt::spawn(async move { call(&caller, "basic").await });
    signals.entered.notified().await;
    assert!(
        bucket
            .head_blob("blob/native-objects/record")
            .await?
            .is_none(),
        "no Blob write may precede its cell proof"
    );
    signals.release.notify_one();
    let reply = basic.await??;
    assert!(reply.write_position.is_some());
    assert_eq!(reply.body, b"checked");
    call(&runtime, "preflight").await?;
    signals.refuse.store(true, Ordering::SeqCst);
    call(&runtime, "rejected").await?;
    assert!(bucket
        .head_blob("blob/native-objects/rejected")
        .await?
        .is_none());
    signals.refuse.store(false, Ordering::SeqCst);
    call(&runtime, "list").await?;
    call(&runtime, "multipart").await?;
    runtime.swap_out_cell(SCOPE, 1).await?;
    runtime.start_cell(SCOPE.into(), 1, false).await?;
    runtime.publish_cell(SCOPE, 1)?;
    call(&runtime, "resume").await?;
    call(&runtime, "finish-resume").await?;
    let reply = call(&runtime, "stream").await?;
    assert_eq!(reply.status, 200);
    assert!(reply.body.is_empty());
    let size = reply
        .stream
        .unwrap()
        .try_fold(0usize, |size, chunk| async move {
            assert!(chunk.iter().all(|byte| *byte == 6));
            Ok(size + chunk.len())
        })
        .await
        .map_err(anyhow::Error::msg)?;
    assert_eq!(size, (8 << 20) + 1);
    let caller = runtime.clone();
    let suspended = cellar::asyncrt::spawn(async move { call(&caller, "cancel-stream").await });
    signals.stream_started.notified().await;
    runtime.swap_out_cell(SCOPE, 1).await?;
    assert!(suspended.await?.is_err());
    assert!(signals.stream_dropped.load(Ordering::SeqCst));
    assert!(bucket
        .head_blob("blob/native-objects/cancelled")
        .await?
        .is_none());
    assert!(signals
        .gates
        .lock()
        .unwrap()
        .iter()
        .any(|ticket| ticket.channel == cellar_logic::Channel::Blob
            && ticket.epoch == Some(1)
            && ticket.position.is_some()));
    shutdown.send(true)?;
    adapter.await?;
    Ok(())
}
