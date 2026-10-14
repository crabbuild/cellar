//! Bounded technician diagnostics. These exercise the less frequent SDK
//! capabilities separately from the ordinary field-service traffic mix.
use crate::{application::response, config::*};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{
    self, blob,
    clone::{Object, StructuredValue, Value as CloneValue},
    http::Response,
    kv,
    loader::WorkerCode,
    storage::ListOptions,
    Application, Cell, Context, HandlerFuture, Invocation, Output, Registry, Request,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};

pub struct Diagnostic;

impl Cell for Diagnostic {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let storage = context.storage()?;
            let runs = storage
                .get_json("runs")?
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
                + 1;
            storage.put_json("runs", &json!(runs))?;
            let value = json!({"runs": runs, "props": context.facet().context("diagnostic requires a facet")?.props.to_json()?});
            match invocation {
                Invocation::Fetch(_) => response(&context, value),
                Invocation::CellRpc { .. } => Ok(Output::Rpc(sdk::encode_rpc(
                    &StructuredValue::from_json(value)?,
                )?)),
                _ => anyhow::bail!("unsupported diagnostic event"),
            }
        })
    }
}

pub async fn cell(context: &Context, tenant: usize, case: &str) -> Result<Output> {
    match case {
        "container" => {
            let storage = context.storage()?;
            storage.put_json(
                "diagnostic/container",
                &json!({"tenant":tenant,"requested":true}),
            )?;
            let child = context.container()?;
            child
                .start(context, sdk::containers::StartOptions::default())
                .await?;
            let result = async {
                child
                    .port(context, 8080)?
                    .wait_ready(context, Duration::from_secs(20))
                    .await?;
                let reply = child
                    .port(context, 8080)?
                    .fetch(
                        context,
                        sdk::http::OutboundRequest {
                            url: "http://sandbox/".into(),
                            method: "GET".into(),
                            headers: Vec::new(),
                            body: None,
                            redirect: Default::default(),
                        },
                    )
                    .await?;
                ensure!(reply.status == 200, "container port fetch failed");
                ensure!(
                    !Response::from_http(reply).bytes(context).await?.is_empty(),
                    "container response absent"
                );
                let process = child
                    .exec(
                        context,
                        sdk::containers::ExecOptions {
                            cmd: vec![
                                "python3".into(),
                                "-c".into(),
                                "print('technician diagnostic')".into(),
                            ],
                            ..Default::default()
                        },
                    )
                    .await?;
                let output = process.output(context, 65536).await?;
                ensure!(
                    output.stdout == b"technician diagnostic\n" && output.exit_code == 0,
                    "container exec failed"
                );
                Ok::<_, anyhow::Error>(())
            }
            .await;
            child.destroy(context).await?;
            result?;
            storage.delete("diagnostic/container")?;
            response(
                context,
                json!({"passed":["container_gated_start_port_fetch_exec_exit_destroy"],
                "child_cpus":0.0625,"child_memory_bytes":268435456}),
            )
        }
        "storage" => {
            let storage = context.storage()?;
            storage.transaction_sync(|tx| {
                tx.put_many_json(&[
                    ("diagnostic/a".into(), json!({"threshold":80})),
                    ("diagnostic/b".into(), json!([1, 2, 3])),
                ])?;
                let rolled_back: Result<()> = tx.transaction_sync(|nested| {
                    nested.put_json("diagnostic/rollback", &json!(true))?;
                    anyhow::bail!("injected diagnostic rollback")
                });
                ensure!(
                    rolled_back.is_err() && tx.get_json("diagnostic/rollback")?.is_none(),
                    "nested rollback leaked state"
                );
                Ok(())
            })?;
            storage
                .transaction(context, |tx| async move {
                    tx.put_json("diagnostic/async", &json!(1))?;
                    cellar::asyncrt::sleep(Duration::from_millis(1)).await;
                    ensure!(
                        tx.get_json("diagnostic/async")? == Some(json!(1)),
                        "async transaction lost authority"
                    );
                    Ok(())
                })
                .await?;
            let graph = StructuredValue {
                root: CloneValue::Reference(0),
                objects: vec![
                    Object::Object(vec![
                        ("self".into(), CloneValue::Reference(0)),
                        ("bytes".into(), CloneValue::Reference(1)),
                    ]),
                    Object::ArrayBuffer {
                        bytes: vec![0, 1, 2, 255],
                        max_length: None,
                        immutable: false,
                    },
                ],
            };
            storage.put_clone("diagnostic/graph", &graph)?;
            let restored = storage
                .get_clone("diagnostic/graph")?
                .context("clone missing")?;
            ensure!(
                sdk::clone::encode(&restored.value)? == sdk::clone::encode(&graph)?,
                "clone graph changed"
            );
            let keys: Vec<_> = ["diagnostic/a", "diagnostic/b", "diagnostic/graph"]
                .into_iter()
                .map(str::to_owned)
                .collect();
            ensure!(
                storage.get_many_clone(&keys)?.len() == 3,
                "bulk clone lookup failed"
            );
            let options = ListOptions {
                prefix: Some("diagnostic/".into()),
                limit: Some(100),
                ..Default::default()
            };
            ensure!(
                storage.list_clone(&options)?.len() == 4,
                "bounded storage list failed"
            );
            let mut cursor = storage.sql_cursor(
                "SELECT ordinal FROM command_receipts ORDER BY ordinal LIMIT 1",
                &[],
            )?;
            cursor.next_raw()?;
            cursor.close()?;
            let mut cursor = storage.list_cursor(&options)?;
            cursor.next().transpose()?;
            drop(cursor);
            ensure!(storage.delete_many(&keys)? == 3, "bulk delete failed");
            storage.delete("diagnostic/async")?;
            storage.sync().await?;
            response(
                context,
                json!({"passed": ["storage_json_clone_bulk_list_delete", "sync_async_nested_transactions", "sql_kv_cursors", "explicit_sync"]}),
            )
        }
        "facet" => {
            let facets = context.facets()?;
            let props = StructuredValue::from_json(json!({"tenant":tenant, "schema":1}))?;
            let class = context
                .exported_cell_class("Diagnostic")?
                .with_props(&props)?;
            let child = facets.get(context, "technician", move |_| {
                Box::pin(async move { Ok(sdk::facets::Startup::new(class)) })
            })?;
            let reply = child
                .rpc(
                    context,
                    "inspect",
                    sdk::encode_rpc(&StructuredValue::from_json(json!({}))?)?,
                )
                .await?;
            let value = sdk::decode_rpc_result(&reply)?.to_json()?;
            ensure!(
                value["props"]["tenant"] == tenant,
                "facet props crossed tenant"
            );
            facets.delete(context, "technician").await?;
            response(
                context,
                json!({"passed": ["facet_startup_props_rpc_delete"], "child":value}),
            )
        }
        _ => anyhow::bail!("unknown cell diagnostic"),
    }
}

pub async fn root(context: &Context, config: &AppConfig, case: &str) -> Result<Output> {
    match case {
        "outbound" => {
            let simulator = config
                .simulator
                .as_deref()
                .context("technician simulator is required")?;
            let reply = context
                .fetch(sdk::http::OutboundRequest {
                    url: format!("{simulator}/health"),
                    method: "GET".into(),
                    headers: Vec::new(),
                    body: None,
                    redirect: Default::default(),
                })
                .await?;
            let state = Response::from_http(reply).json(context).await?.to_json()?;
            let socket = context
                .connect_websocket("ws://technician:8080/ws", Vec::new())
                .await?;
            socket.send_text(context, "diagnostic")?;
            loop {
                match socket.next(context).await? {
                    sdk::websocket::SocketEvent::Open(_) => {}
                    sdk::websocket::SocketEvent::Text(text) => {
                        ensure!(text == "diagnostic", "outbound echo changed");
                        break;
                    }
                    _ => anyhow::bail!("outbound websocket returned wrong frame"),
                }
            }
            socket.close(context, 1000, "diagnostic done")?;
            response(
                context,
                json!({"passed":["outbound_http_websocket_text_close"],"technician":state}),
            )
        }
        "tls" => {
            use sdk::sockets::{SecureTransport, SocketOptions};
            let direct = context.connect(
                "technician:8443",
                SocketOptions {
                    secure_transport: SecureTransport::On,
                    ..Default::default()
                },
            )?;
            ensure!(
                direct.opened().await.is_err(),
                "private self-signed TLS peer was trusted"
            );
            let plain = context.connect(
                "technician:8443",
                SocketOptions {
                    secure_transport: SecureTransport::StartTls,
                    ..Default::default()
                },
            )?;
            plain.opened().await?;
            let secure = plain.start_tls(context, Some("technician".into()))?;
            ensure!(
                secure.opened().await.is_err(),
                "STARTTLS bypassed server authentication"
            );
            response(
                context,
                json!({"passed":["tls_starttls_private_certificate_rejection"],
                "limitation":"The production TCP service uses public WebPKI roots; private CA throughput is not measured."}),
            )
        }
        "assets" => {
            let reply = context
                .assets("ASSETS")?
                .fetch_url(context, "http://manual/assets/manual.txt")
                .await?;
            ensure!(
                reply.status == 200
                    && Response::from_http(reply).bytes(context).await? == crate::assets::MANUAL,
                "asset binding changed technician documentation"
            );
            response(
                context,
                json!({"passed":["content_addressed_asset_manifest_generation_binding_read"]}),
            )
        }
        "service" => {
            let service = context
                .service("REPORTS")?
                .with_props(&StructuredValue::from_json(json!({"tenant":0}))?)?;
            let reply = service
                .fetch(
                    context,
                    Request {
                        url: "http://reports/summary".into(),
                        method: "GET".into(),
                        headers: Vec::new(),
                        body: cellar::engine_api::RequestBody::Bytes(Vec::new().into()),
                        entrypoint: None,
                    },
                )
                .await?;
            let value = Response::from_http(reply).json(context).await?.to_json()?;
            ensure!(
                value["service"] == "Reports" && value["props"]["tenant"] == 0,
                "service routing/props failed"
            );
            let reply = service
                .rpc(
                    context,
                    cellar::WorkerRpcOperation::Get {
                        path: vec!["status".into()],
                    },
                )
                .await?;
            ensure!(
                sdk::clone::decode(&reply)?.to_json()?["service"] == "Reports",
                "named service RPC failed"
            );
            response(
                context,
                json!({"passed":["service_binding_named_entrypoint_props_fetch_rpc"]}),
            )
        }
        "bindings" => {
            let kv = context.kv(&binding("KV", 0, 0))?;
            kv.put_text(context, "diagnostic/text", "technician", Default::default())
                .await?;
            kv.put_bytes(
                context,
                "diagnostic/bytes",
                vec![0, 1, 255],
                kv::PutOptions {
                    expiration_ttl: Some(60.0),
                    metadata: Some(json!({"purpose":"diagnostic"})),
                    ..Default::default()
                },
            )
            .await?;
            kv.put_stream(
                context,
                "diagnostic/stream",
                Box::pin(futures_util::stream::iter([Ok::<_, String>(
                    b"stream".to_vec(),
                )])),
                Default::default(),
            )
            .await?;
            let keys: Vec<_> = ["diagnostic/text", "diagnostic/bytes", "diagnostic/stream"]
                .into_iter()
                .map(str::to_owned)
                .collect();
            let values = kv
                .get_many_with_metadata(context, &keys, kv::ReadType::Bytes)
                .await?;
            ensure!(values.len() == 3, "KV bulk lookup failed");
            let page = kv
                .list(
                    context,
                    kv::ListOptions {
                        prefix: "diagnostic/".into(),
                        limit: Some(2),
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(
                page.keys.len() == 2 && !page.list_complete,
                "KV cursor pagination failed"
            );
            let tail = kv
                .list(
                    context,
                    kv::ListOptions {
                        prefix: "diagnostic/".into(),
                        cursor: page.cursor,
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(tail.keys.len() == 1, "KV continuation failed");
            kv.delete_many(context, &keys).await?;
            let db = context.sql(&binding("SQL", 0, 0))?;
            let session = db.with_session(Some("first-primary"))?;
            let row = session
                .prepare("SELECT count(*) AS n FROM projections")
                .first(context, Some("n"))
                .await?;
            ensure!(
                row.is_some() && session.get_bookmark().is_some(),
                "SQL session bookmark absent"
            );
            ensure!(
                db.migrations(context, "__sys_sql_migrations_fleetcare")
                    .await?
                    .len()
                    == 1,
                "migration history mismatch"
            );
            ensure!(
                db.dump().await.is_err(),
                "SQL dump capability changed; update coverage expectations"
            );
            response(
                context,
                json!({"passed": ["kv_text_bytes_stream_metadata_ttl_bulk_cursor_delete", "sql_prepared_session_migrations"],
                "unsupported": ["sql_dump"]}),
            )
        }
        "blob" => {
            let bucket = context.blob("ATTACHMENTS")?;
            let key = "diagnostic/object";
            let object = bucket
                .put(context, key, b"diagnostic".to_vec(), Default::default())
                .await?
                .context("blob not stored")?;
            ensure!(
                bucket
                    .head(context, key)
                    .await?
                    .is_some_and(|h| h.size == 10),
                "Blob head mismatch"
            );
            let blob::GetResult::Body(mut body) = bucket
                .get(
                    context,
                    key,
                    blob::GetOptions {
                        range: Some(blob::Range {
                            offset: Some(0),
                            length: Some(4),
                            suffix: None,
                        }),
                        ..Default::default()
                    },
                )
                .await?
            else {
                anyhow::bail!("range absent");
            };
            ensure!(body.bytes(context).await? == b"diag", "Blob range mismatch");
            let unmet = bucket
                .put(
                    context,
                    key,
                    b"wrong".to_vec(),
                    blob::WriteOptions {
                        only_if: Some(blob::Conditional {
                            etag_matches: Some("impossible-etag".into()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(unmet.is_none(), "Blob conditional write ignored");
            ensure!(
                bucket
                    .list(
                        context,
                        blob::ListOptions {
                            prefix: "diagnostic/".into(),
                            limit: Some(10),
                            ..Default::default()
                        }
                    )
                    .await?
                    .objects
                    .iter()
                    .any(|o| o.etag == object.etag),
                "Blob list missing object"
            );
            let multipart = bucket
                .create_multipart(context, "diagnostic/multipart", Default::default())
                .await?;
            let part = multipart
                .upload_part(context, 1, b"multipart".to_vec())
                .await?;
            let resumed = bucket.resume_multipart(multipart.key(), multipart.upload_id())?;
            ensure!(
                resumed.complete(context, &[part]).await?.size == 9,
                "multipart completion mismatch"
            );
            bucket
                .create_multipart(context, "diagnostic/abort", Default::default())
                .await?
                .abort(context)
                .await?;
            bucket
                .delete(context, &[key.into(), "diagnostic/multipart".into()])
                .await?;
            response(
                context,
                json!({"passed":["blob_put_head_range_conditionals_list_delete", "blob_multipart_resume_complete_abort"]}),
            )
        }
        "loader" => {
            let app: Arc<dyn Application> = Arc::new(Registry::new(|ctx, _| {
                Box::pin(async move {
                    response(
                        &ctx,
                        json!({"diagnostic":"compiled", "env":ctx.loaded_environment()?.map(StructuredValue::to_json).transpose()?}),
                    )
                })
            }));
            let mut code = WorkerCode::new(app);
            code.environment = Some(StructuredValue::from_json(json!({"tenant":0}))?);
            let worker = context.loader("DIAGNOSTICS")?.load(context, code)?;
            let reply = worker
                .entrypoint("default")
                .fetch(
                    context,
                    Request {
                        url: "http://diagnostic/".into(),
                        method: "GET".into(),
                        headers: Vec::new(),
                        body: Vec::new().into(),
                        entrypoint: None,
                    },
                )
                .await?;
            let value = Response::from_http(reply).json(context).await?.to_json()?;
            ensure!(
                value["diagnostic"] == "compiled" && value["env"]["tenant"] == 0,
                "compiled loader returned wrong environment"
            );
            worker.dispose(context)?;
            response(
                context,
                json!({"passed":["compiled_loader_environment_entrypoint_dispose"]}),
            )
        }
        "web" => {
            let cache = context.caches().open("technicians")?;
            let mut console = Response::new(
                200,
                b"<main data-tenant=\"unset\">FleetCare</main>".to_vec(),
                vec![("content-type".into(), "text/html".into())],
            );
            cache
                .put(context, "https://fleetcare.example/console", &mut console)
                .await?;
            ensure!(
                cache
                    .match_request(
                        context,
                        "https://fleetcare.example/console",
                        Default::default()
                    )
                    .await?
                    .is_none(),
                "Cache semantics changed"
            );
            ensure!(
                !cache
                    .delete(
                        context,
                        "https://fleetcare.example/console",
                        Default::default()
                    )
                    .await?,
                "Cache delete semantics changed"
            );
            let rewriter = sdk::html_rewriter::HtmlRewriter::new().on(
                "main",
                sdk::html_rewriter::ElementHandlers::new().element(|_, element| {
                    Box::pin(async move { element.set_attribute("data-tenant", "0") })
                }),
            );
            let rewritten = rewriter
                .transform(
                    context,
                    Response::new(200, b"<main>FleetCare</main>".to_vec(), vec![]),
                )?
                .text(context)
                .await?;
            ensure!(
                rewritten.contains("data-tenant=\"0\""),
                "HTML rewriter failed"
            );
            let digest = sdk::crypto::digest(
                sdk::crypto::DigestAlgorithm::Hash(sdk::crypto::HashAlgorithm::Sha256),
                b"FleetCare",
            );
            ensure!(digest.len() == 32, "native digest failed");
            let compressed = sdk::compression::transform("gzip", b"FleetCare")?;
            ensure!(
                sdk::compression::transform("gunzip", &compressed)? == b"FleetCare",
                "native compression round trip failed"
            );
            response(
                context,
                json!({"passed":["cache_compatibility_always_miss", "html_rewriter", "native_crypto_compression_clone"], "cache_hits_supported":false}),
            )
        }
        "tcp" => {
            let socket = context.connect(
                sdk::sockets::SocketAddress::new("rustfs", 9000)?,
                Default::default(),
            )?;
            socket.opened().await?;
            let writer = socket.writable.get_writer()?;
            writer
                .write(
                    context,
                    b"GET /health HTTP/1.1\r\nHost: rustfs\r\nConnection: close\r\n\r\n".to_vec(),
                )
                .await?;
            let reader = socket.readable.get_reader()?;
            let mut bytes = Vec::new();
            loop {
                let chunk = reader.read(context).await?;
                if chunk.is_empty() {
                    break;
                }
                ensure!(
                    bytes.len() + chunk.len() <= 65536,
                    "diagnostic response too large"
                );
                bytes.extend(chunk);
            }
            ensure!(
                bytes.starts_with(b"HTTP/1.1 200"),
                "TCP diagnostic dependency failed"
            );
            reader.release_lock();
            writer.release_lock();
            socket.close(context).await?;
            response(
                context,
                json!({"passed":["tcp_open_read_write_close"], "dependency":"private RustFS health endpoint", "profile":config.profile}),
            )
        }
        "queue" => {
            use sdk::queue::{BatchEntry, Body};
            let producer = context.queue(&binding("Q", 0, 0))?;
            let mut entries = Vec::new();
            for encoding in 0..4 {
                let effect = crate::contracts::Effect {
                    effect_id: format!("diagnostic-queue-{encoding}"),
                    tenant: 0,
                    source_scope: format!("diagnostic-queue-{encoding}"),
                    kind: "projection".into(),
                    version: 1,
                    snapshot: json!({"encoding":encoding}),
                };
                let value = serde_json::to_value(effect)?;
                let body = match encoding {
                    0 => Body::Text(serde_json::to_string(&value)?),
                    1 => Body::Bytes(serde_json::to_vec(&value)?),
                    2 => Body::Structured(StructuredValue::from_json(value)?),
                    _ => Body::Json(value),
                };
                entries.push(BatchEntry {
                    body,
                    delay_seconds: (encoding == 1).then_some(1),
                });
            }
            producer
                .send_batch(context, entries, Default::default())
                .await?;
            let poison = crate::contracts::Effect {
                effect_id: "diagnostic-poison".into(),
                tenant: 0,
                source_scope: "diagnostic-poison".into(),
                kind: "poison".into(),
                version: 1,
                snapshot: json!({"injected":true}),
            };
            producer
                .send(
                    context,
                    Body::Json(serde_json::to_value(poison)?),
                    Default::default(),
                )
                .await?;
            response(
                context,
                json!({"passed":["queue_batch_text_bytes_structured_json_delay_send"],
                "poison_effect_id":"diagnostic-poison"}),
            )
        }
        _ => anyhow::bail!("unknown application diagnostic"),
    }
}
