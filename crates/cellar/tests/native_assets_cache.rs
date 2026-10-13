//! Assets and the retained Cache API cross native turns without initializing V8.

#[path = "support/assets.rs"]
mod fixture;
use anyhow::{anyhow, Result};
use cellar::application::{
    assets::Fetcher,
    cache::{Error as CacheError, MatchOptions},
    http::Response,
    Cell, Context, HandlerFuture, Invocation, Output, Registry, Request, Storage,
};
use cellar::engine_api::{HttpResponse, RequestBody, RuntimeFetch, RuntimeOptions};
use cellar::generation::{Generation, GenerationOptions};
use cellar::runtime::RuntimeManager;
use futures_util::{Stream, TryStreamExt};
use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tokio::sync::Notify;

const SCRIPT: &str = "native-assets-cache";
const PEER: &str = "native-assets-peer";
const SCOPE: &str = "Source:one";

#[derive(Default)]
struct Signals {
    calls: Mutex<Vec<(u64, String, String, String)>>,
    held: Notify,
    release: Notify,
    pending: Notify,
    cancelled: AtomicBool,
    descriptor: Mutex<Option<Fetcher>>,
    old_context: Mutex<Option<Context>>,
}

struct Producer {
    chunks: VecDeque<std::result::Result<Vec<u8>, String>>,
    pulls: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
    pending: Option<Arc<Signals>>,
}

impl Stream for Producer {
    type Item = std::result::Result<Vec<u8>, String>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        self.pulls.fetch_add(1, Ordering::SeqCst);
        if let Some(chunk) = self.chunks.pop_front() {
            return Poll::Ready(Some(chunk));
        }
        if let Some(signals) = &self.pending {
            signals.pending.notify_one();
            Poll::Pending
        } else {
            Poll::Ready(None)
        }
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
        if let Some(signals) = &self.pending {
            signals.cancelled.store(true, Ordering::SeqCst);
        }
    }
}

fn producer(
    chunks: VecDeque<std::result::Result<Vec<u8>, String>>,
    pending: Option<Arc<Signals>>,
) -> (Producer, Arc<AtomicUsize>, Arc<AtomicBool>) {
    let pulls = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    (
        Producer {
            chunks,
            pulls: pulls.clone(),
            dropped: dropped.clone(),
            pending,
        },
        pulls,
        dropped,
    )
}

struct Source(Storage, Arc<Signals>);

impl Cell for Source {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let calls = self
                .0
                .get_json("calls")?
                .and_then(|value| value.as_u64())
                .unwrap_or(0)
                + 1;
            self.0.put_json("calls", &calls.into())?;
            worker(context, invocation, self.1.clone()).await
        })
    }
}

fn request(url: &str, method: &str, headers: Vec<(String, String)>) -> Request {
    Request {
        url: url.into(),
        method: method.into(),
        headers,
        body: RequestBody::Bytes(Vec::new().into()),
        entrypoint: None,
    }
}

async fn worker(context: Context, invocation: Invocation, signals: Arc<Signals>) -> Result<Output> {
    let Invocation::Fetch(mut incoming) = invocation else {
        return Err(anyhow!("expected fetch"));
    };
    let asset = context.assets("ASSETS")?;
    assert!(context.assets("missing").is_err());
    let path = url::Url::parse(&incoming.url)?.path().to_string();
    match path.as_str() {
        "/cache-check" => {
            cache_checks(&context).await?;
        }
        "/cache-cancel" => {
            *signals.old_context.lock().unwrap() = Some(context.clone());
            let (stream, _, _) = producer(
                VecDeque::from([Ok(vec![3; 1 << 20])]),
                Some(signals.clone()),
            );
            let mut response = Response::streaming(200, Box::pin(stream), Vec::new());
            context
                .caches()
                .default()
                .put(&context, "https://cache/key", &mut response)
                .await?;
            unreachable!();
        }
        "/stale" => {
            let old = signals.old_context.lock().unwrap().clone().unwrap();
            let mut response = Response::new(200, b"safe".to_vec(), Vec::new());
            assert!(context
                .caches()
                .default()
                .put(&old, "https://cache/key", &mut response)
                .await
                .is_err());
            assert!(!response.body_used());
            assert!(asset
                .fetch_url(&old, "https://assets/plain.txt")
                .await
                .is_err());
        }
        "/validation" => {
            assert!(asset.fetch_url(&context, "/relative").await.is_err());
            assert!(asset
                .fetch(
                    &context,
                    request("https://assets/plain.txt", "TRACE", Vec::new())
                )
                .await
                .is_err());
        }
        "/held" => {
            return Ok(Output::Http(
                asset
                    .fetch_url(&context, "https://assets/plain.txt?hold=1")
                    .await?,
            ))
        }
        "/retained" => {
            let retained = signals.descriptor.lock().unwrap().clone().unwrap();
            return Ok(Output::Http(
                retained
                    .fetch_url(&context, "https://assets/plain.txt")
                    .await?,
            ));
        }
        "/discard-request" => {
            incoming.url = "https://assets/plain.txt".into();
            return Ok(Output::Http(asset.fetch(&context, incoming).await?));
        }
        _ => {
            signals
                .descriptor
                .lock()
                .unwrap()
                .get_or_insert_with(|| asset.clone());
            incoming.url = incoming.url.replace("http://worker", "https://assets");
            return Ok(Output::Http(asset.fetch(&context, incoming).await?));
        }
    }
    Ok(Output::Http(
        Response::new(200, b"checked".to_vec(), Vec::new()).into_http(&context)?,
    ))
}

async fn cache_checks(context: &Context) -> Result<()> {
    let storage = context.caches();
    assert!(storage.open("").is_ok());
    assert!(storage.open(&"🐟".repeat(511)).is_ok());
    assert_eq!(
        storage
            .open(&"🐟".repeat(512))
            .unwrap_err()
            .downcast_ref::<CacheError>(),
        Some(&CacheError::NameTooLong)
    );
    assert!(storage.open_utf16(&[0xd800]).is_ok());
    for cache in [storage.default(), storage.open("named")?] {
        for key in ["relative", "file:///asset", "ftp://cache/key"] {
            assert_eq!(
                cache
                    .match_request(context, key, MatchOptions::default())
                    .await
                    .err()
                    .unwrap()
                    .downcast_ref::<CacheError>(),
                Some(&CacheError::InvalidUrl)
            );
            assert!(cache
                .delete(context, key, MatchOptions::default())
                .await
                .is_err());
        }
        for (key, status, headers, error) in [
            (
                request("https://cache/key", "POST", Vec::new()),
                200,
                Vec::new(),
                CacheError::NonGet,
            ),
            (
                request("https://cache/key", "GET", Vec::new()),
                206,
                Vec::new(),
                CacheError::PartialResponse,
            ),
            (
                request("https://cache/key", "GET", Vec::new()),
                200,
                vec![("Vary".into(), "accept, *".into())],
                CacheError::VaryStar,
            ),
        ] {
            let (stream, pulls, dropped) =
                producer(VecDeque::from([Ok(b"unpulled".to_vec())]), None);
            let mut response = Response::streaming(status, Box::pin(stream), headers);
            assert_eq!(
                cache
                    .put(context, &key, &mut response)
                    .await
                    .unwrap_err()
                    .downcast_ref::<CacheError>(),
                Some(&error)
            );
            assert!(!response.body_used());
            assert_eq!(pulls.load(Ordering::SeqCst), 0);
            drop(response);
            assert!(dropped.load(Ordering::SeqCst));
        }
        let mut null = Response::empty(204, Vec::new());
        cache.put(context, "https://cache/key", &mut null).await?;
        assert!(!null.body_used());
        let mut bytes = Response::new(200, vec![239, 187, 191, 255], Vec::new());
        cache.put(context, "https://cache/key", &mut bytes).await?;
        assert!(bytes.body_used());
        assert_eq!(bytes.text(context).await?, "�");
        assert_eq!(
            cache
                .put(context, "https://cache/key", &mut bytes)
                .await
                .unwrap_err()
                .downcast_ref::<CacheError>(),
            Some(&CacheError::UsedBody)
        );
        assert_eq!(bytes.into_http(context)?.body, vec![239, 187, 191, 255]);
        let (stream, pulls, dropped) = producer(
            VecDeque::from([Ok(vec![4; 1 << 20]), Ok(vec![5; 1 << 20])]),
            None,
        );
        let mut response = Response::streaming(200, Box::pin(stream), Vec::new());
        cache
            .put(context, "https://cache/key", &mut response)
            .await?;
        assert!(response.body_used());
        assert_eq!(pulls.load(Ordering::SeqCst), 3);
        assert!(dropped.load(Ordering::SeqCst));
        let materialized = response.into_http(context)?;
        assert_eq!(
            materialized.body,
            [vec![4; 1 << 20], vec![5; 1 << 20]].concat()
        );
        assert!(materialized.stream.is_none());
        let (stream, _, dropped) = producer(
            VecDeque::from([Ok(vec![1]), Err("producer failure".into())]),
            None,
        );
        let mut failed = Response::streaming(200, Box::pin(stream), Vec::new());
        assert_eq!(
            cache
                .put(context, "https://cache/key", &mut failed)
                .await
                .unwrap_err()
                .to_string(),
            "producer failure"
        );
        assert!(failed.body_used());
        assert!(dropped.load(Ordering::SeqCst));
        let post = request("https://cache/key", "POST", Vec::new());
        assert!(cache
            .match_request(
                context,
                &post,
                MatchOptions {
                    ignore_method: true
                }
            )
            .await?
            .is_none());
        assert!(
            !cache
                .delete(context, "https://cache/key", MatchOptions::default())
                .await?
        );
        let mut json = Response::new(
            200,
            [b"\xef\xbb\xbf".as_slice(), br#"{"lone":"\ud800"}"#].concat(),
            Vec::new(),
        );
        let value = json.json(context).await?;
        assert_eq!(
            value
                .property(&"lone".encode_utf16().collect::<Vec<_>>())?
                .unwrap()
                .root,
            cellar::native::clone::Value::String(vec![0xd800])
        );
    }
    Ok(())
}

fn generation(
    id: u64,
    resolver: cellar::assets::AssetResolver,
    peer: cellar::assets::AssetResolver,
    signals: Arc<Signals>,
) -> Result<Generation> {
    let stateless = signals.clone();
    let cells = signals.clone();
    let registry = Registry::new(move |context, invocation| {
        Box::pin(worker(context, invocation, stateless.clone()))
    })
    .cell("Source", move |context, _| {
        Ok(Arc::new(Source(context.storage()?, cells.clone())))
    })?;
    let mut graph = fixture::graph(SCRIPT, "", Some(resolver), vec!["Source".into()]);
    graph
        .cohosted
        .push(fixture::graph(PEER, "", Some(peer), Vec::new()).primary);
    graph
        .cohosted
        .push(fixture::graph("missing-resolver", "", None, Vec::new()).primary);
    let peer_signals = signals.clone();
    let peer_registry = Registry::new(move |context, invocation| {
        Box::pin(worker(context, invocation, peer_signals.clone()))
    });
    let missing = Registry::new(move |context, invocation| {
        Box::pin(worker(context, invocation, signals.clone()))
    });
    Generation::build_native(
        id,
        graph,
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        HashMap::from([
            (
                SCRIPT.into(),
                Arc::new(registry) as Arc<dyn cellar::application::Application>,
            ),
            (
                PEER.into(),
                Arc::new(peer_registry) as Arc<dyn cellar::application::Application>,
            ),
            (
                "missing-resolver".into(),
                Arc::new(missing) as Arc<dyn cellar::application::Application>,
            ),
        ]),
    )
}

fn fetch(path: &str, method: &str, headers: Vec<(String, String)>) -> RuntimeFetch {
    RuntimeFetch {
        url: format!("http://worker{path}"),
        method: method.into(),
        headers,
        body: RequestBody::Bytes(Vec::new().into()),
        request_id: None,
        order: None,
        parent: None,
    }
}

async fn call(
    runtime: &RuntimeManager,
    path: &str,
    method: &str,
    headers: Vec<(String, String)>,
) -> Result<HttpResponse> {
    runtime
        .fetch_cell(SCOPE.into(), None, fetch(path, method, headers), None)
        .await
}

async fn bytes(mut response: HttpResponse) -> Result<(u16, Vec<(String, String)>, Vec<u8>)> {
    if let Some(stream) = response.stream.take() {
        response.body = stream
            .try_fold(Vec::new(), |mut bytes, chunk| async move {
                bytes.extend(chunk);
                Ok(bytes)
            })
            .await
            .map_err(anyhow::Error::msg)?;
    }
    Ok((response.status, response.headers, response.body))
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
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

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn native_assets_and_cache_preserve_routing_generations_bodies_and_retirement() -> Result<()>
{
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(30), run()).await??;
    Ok(())
}

async fn run() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let asset_cache = cellar::sdk::AssetCache::new(cellar::sdk::AssetCacheOptions::new(
        directory.path().join("asset-cache"),
    ))?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("bucket.sqlite"))?;
    let first =
        fixture::resolver(&bucket, "first", b"version-one", "404-page", &asset_cache).await?;
    assert!(first.should_run_worker_first("/api/read"));
    assert!(!first.should_run_worker_first("/api/static/one"));
    let ordinary = first
        .ingress_response("/unknown", None, false, &axum::http::HeaderMap::new())
        .await?;
    assert!(ordinary.is_none());
    let second =
        fixture::resolver(&bucket, "second", b"version-two", "404-page", &asset_cache).await?;
    let peer = fixture::resolver(&bucket, "peer", b"peer-assets", "404-page", &asset_cache).await?;
    let signals = Arc::new(Signals::default());
    let runtime = RuntimeManager::start(
        generation(1, first, peer.clone(), signals.clone())?,
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
    assert!(call(&runtime, "/plain.txt", "GET", Vec::new())
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("no asset resolver channel"));
    let (tx, mut calls) =
        tokio::sync::mpsc::unbounded_channel::<cellar::host_channels::AssetCallReq>();
    cellar::host_channels::set_asset_call_tx(tx);
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let adapter_runtime = runtime.clone();
    let captured = signals.clone();
    let adapter = cellar::asyncrt::spawn(async move {
        let mut tasks = Vec::new();
        while let Some(call) = cellar::asyncrt::select_biased! {"asset adapter stops";_=stopped.changed()=>None,call=calls.recv()=>call,}
        {
            let runtime = adapter_runtime.clone();
            let signals = captured.clone();
            tasks.push(cellar::asyncrt::spawn(async move {
                signals.calls.lock().unwrap().push((
                    call.generation,
                    call.script.clone(),
                    call.url.clone(),
                    call.method.clone(),
                ));
                if call.url.contains("hold=1") {
                    signals.held.notify_one();
                    signals.release.notified().await;
                }
                let generation = runtime.generation_by_id(call.generation);
                let result = match generation.assets(&call.script) {
                    Some(resolver) => {
                        resolver
                            .binding_response(&call.url, &call.method, &call.headers)
                            .await
                    }
                    None => Err(anyhow!("no asset resolver for script {}", call.script)),
                };
                let _ = call.reply.send(result);
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
    });
    let service = |script: &str| cellar::engine_api::ServiceFetch {
        generation: 1,
        script: script.into(),
        entrypoint: None,
        url: "http://worker/plain.txt".into(),
        method: "GET".into(),
        body: RequestBody::Bytes(Vec::new().into()),
        headers: Vec::new(),
        cancel: None,
    };
    assert_eq!(
        bytes(runtime.fetch_service(service(PEER)).await?).await?.2,
        b"peer-assets"
    );
    assert!(runtime
        .fetch_service(service("missing-resolver"))
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("no asset resolver for script missing-resolver"));
    let reply = call(&runtime, "/plain.txt", "get", Vec::new()).await?;
    assert!(reply.write_position.is_some());
    let (status, headers, body) = bytes(reply).await?;
    assert_eq!(status, 200);
    assert_eq!(body, b"version-one");
    assert_eq!(header(&headers, "x-asset"), Some("shared"));
    assert!(header(&headers, "x-remove").is_none());
    let etag = header(&headers, "etag").unwrap().to_string();
    let (status, headers, body) =
        bytes(call(&runtime, "/plain.txt", "HEAD", Vec::new()).await?).await?;
    assert_eq!(status, 200);
    assert_eq!(header(&headers, "content-length"), Some("11"));
    assert!(body.is_empty());
    for (path, method, headers, want_status, want_body) in [
        (
            "/plain.txt",
            "GET",
            vec![("if-none-match".into(), etag.clone())],
            304,
            b"".as_slice(),
        ),
        (
            "/plain.txt",
            "GET",
            vec![("range".into(), "bytes=1-3".into())],
            206,
            b"ers".as_slice(),
        ),
        (
            "/plain.txt",
            "GET",
            vec![("range".into(), "bytes=99-".into())],
            416,
            b"".as_slice(),
        ),
        (
            "/plain.txt",
            "GET",
            vec![
                ("range".into(), "bytes=1-3".into()),
                ("if-range".into(), "wrong".into()),
            ],
            200,
            b"version-one".as_slice(),
        ),
        ("/unknown", "GET", Vec::new(), 404, b"absent".as_slice()),
        ("/proxy", "GET", Vec::new(), 200, b"version-one".as_slice()),
        ("/%E9%9B%AA.txt", "GET", Vec::new(), 200, b"snow".as_slice()),
        (
            "/plain.txt",
            "POST",
            Vec::new(),
            405,
            b"Method not allowed".as_slice(),
        ),
    ] {
        let (status, _, body) = bytes(call(&runtime, path, method, headers).await?).await?;
        assert_eq!(status, want_status, "{path}");
        assert_eq!(body, want_body, "{path}");
    }
    let (status, headers, _) =
        bytes(call(&runtime, "/docs?q=snow", "GET", Vec::new()).await?).await?;
    assert_eq!(status, 307);
    assert_eq!(header(&headers, "location"), Some("/docs/?q=snow"));
    let (status, headers, _) = bytes(call(&runtime, "/old", "GET", Vec::new()).await?).await?;
    assert_eq!(status, 302);
    assert_eq!(header(&headers, "location"), Some("/docs"));
    let (_, headers, body) = bytes(call(&runtime, "/opaque", "GET", Vec::new()).await?).await?;
    assert!(header(&headers, "content-type").is_none());
    assert_eq!(body, b"raw");
    for path in ["/broken.txt", "/gone.txt"] {
        assert!(call(&runtime, path, "GET", Vec::new())
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("asset fetch:"));
    }
    let before = signals.calls.lock().unwrap().len();
    call(&runtime, "/validation", "GET", Vec::new()).await?;
    assert_eq!(signals.calls.lock().unwrap().len(), before);
    let (stream, pulls, dropped) = producer(VecDeque::from([Ok(b"ignored".to_vec())]), None);
    let id =
        cellar::http_streams::register_body_stream(Box::pin(stream)).map_err(anyhow::Error::msg)?;
    let mut ignored = fetch("/discard-request", "POST", Vec::new());
    ignored.body = RequestBody::Stream(id);
    assert_eq!(
        runtime
            .fetch_cell(SCOPE.into(), None, ignored, None)
            .await?
            .status,
        405
    );
    assert_eq!(pulls.load(Ordering::SeqCst), 0);
    assert!(dropped.load(Ordering::SeqCst));
    call(&runtime, "/cache-check", "GET", Vec::new()).await?;
    let pending_runtime = runtime.clone();
    let held =
        cellar::asyncrt::spawn(
            async move { call(&pending_runtime, "/held", "GET", Vec::new()).await },
        );
    signals.held.notified().await;
    runtime.adopt(generation(2, second, peer, signals.clone())?);
    let reply = runtime
        .fetch_worker(
            "http://worker/plain.txt".into(),
            "GET".into(),
            Vec::new(),
            Vec::new(),
        )
        .await?;
    assert_eq!(bytes(reply).await?.2, b"version-two");
    signals.release.notify_one();
    assert_eq!(bytes(held.await??).await?.2, b"version-one");
    let pending_runtime = runtime.clone();
    let pending = cellar::asyncrt::spawn(async move {
        call(&pending_runtime, "/cache-cancel", "GET", Vec::new()).await
    });
    signals.pending.notified().await;
    runtime.swap_out_cell(SCOPE, 1).await?;
    assert!(pending.await?.is_err());
    assert!(signals.cancelled.load(Ordering::SeqCst));
    open(&runtime).await?;
    call(&runtime, "/stale", "GET", Vec::new()).await?;
    assert_eq!(
        bytes(call(&runtime, "/retained", "GET", Vec::new()).await?)
            .await?
            .2,
        b"version-two"
    );
    assert!(signals
        .calls
        .lock()
        .unwrap()
        .iter()
        .all(|(_, script, _, _)| [SCRIPT, PEER, "missing-resolver"].contains(&script.as_str())));
    assert!(signals
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|(generation, _, _, _)| *generation == 2));
    stop.send(true)?;
    adapter.await?;
    Ok(())
}
