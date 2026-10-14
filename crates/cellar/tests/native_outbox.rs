//! Source SDK transactions through native application turns. The fixture
//! checks persisted emission; fleet dispatch/proof qualification is separate.

use anyhow::{anyhow, Context as _, Result};
use cellar::application::event_stream::{
    Binding, DeliveryError, Envelope, ErrorCode, Event, Resource,
};
use cellar::application::{
    Cell, Context, HandlerFuture, Invocation, OutboxOptions, Output, Registry,
};
use cellar::engine_api::{Compat, HttpResponse, RuntimeFetch, RuntimeOptions, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::runtime::RuntimeManager;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const SCRIPT: &str = "native-outbox";
const SCOPE: &str = "Order:source";

#[derive(Serialize, Deserialize)]
struct OrderEvent {
    order: String,
}

impl Event for OrderEvent {
    const FAMILY: &'static str = "orders";
    const VERSION: u32 = 1;

    fn key(&self) -> &str {
        &self.order
    }
}

fn event() -> OrderEvent {
    OrderEvent {
        order: "123".into(),
    }
}

struct Oversize<'a>(&'a AtomicUsize);

impl Serialize for Oversize<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(None)?;
        for _ in 0..10_000_000 {
            self.0.fetch_add(1, Ordering::Relaxed);
            sequence.serialize_element(&"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz")?;
        }
        sequence.end()
    }
}

impl Event for Oversize<'_> {
    const FAMILY: &'static str = "orders";
    const VERSION: u32 = 1;

    fn key(&self) -> &str {
        "123"
    }
}

#[derive(Serialize)]
struct ZeroVersion;

#[derive(Serialize)]
struct WideInteger {
    number: u128,
}

impl Event for WideInteger {
    const FAMILY: &'static str = "orders";
    const VERSION: u32 = 1;

    fn key(&self) -> &str {
        "123"
    }
}

impl Event for ZeroVersion {
    const FAMILY: &'static str = "orders";
    const VERSION: u32 = 0;

    fn key(&self) -> &str {
        "123"
    }
}

fn code(failure: &anyhow::Error) -> ErrorCode {
    failure
        .downcast_ref::<DeliveryError>()
        .expect("typed emission failure")
        .code
}

fn response() -> Output {
    Output::Http(HttpResponse {
        status: 200,
        body: b"ok".to_vec(),
        stream: None,
        headers: vec![],
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}

struct Order;

impl Cell for Order {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Err(anyhow!("expected fetch"));
            };
            let db = context.storage()?;
            let orders = context.stream::<OrderEvent>("ORDERS")?;
            assert!(context.stream::<OrderEvent>("missing").is_err());
            assert_eq!(
                code(&context.stream::<OrderEvent>("WRONG_FAMILY").err().unwrap()),
                ErrorCode::EventFamilyMismatch
            );
            if request.url.ends_with("/checks") {
                let empty = db.outbox(&OutboxOptions::default())?;
                assert!(empty.incarnation.is_none() && empty.lanes.is_empty());
                assert_eq!((empty.pending_rows, empty.pending_bytes), (0, 0));
                assert!(empty.observation.oldest_pending_age_ms.is_none());
                assert_eq!(empty.observation.blocked_lanes.len(), 7);
                assert!(empty
                    .observation
                    .blocked_lanes
                    .iter()
                    .all(|count| count.lanes == 0));
                assert_eq!(
                    code(&db.emit(&orders, event()).unwrap_err()),
                    ErrorCode::TransactionRequired
                );
                let readonly = context.stream::<OrderEvent>("READ_ONLY")?;
                let rejected: Result<()> = db.tx(|tx| {
                    tx.put_json("forbidden", &true.into())?;
                    tx.emit(&readonly, event())
                });
                assert_eq!(code(&rejected.unwrap_err()), ErrorCode::Forbidden);
                assert!(db.get_json("forbidden")?.is_none());
            } else if request.url.ends_with("/encoding") {
                let produced = AtomicUsize::new(0);
                let huge = context.stream::<Oversize<'_>>("ORDERS")?;
                let invalid = context.stream::<ZeroVersion>("ORDERS")?;
                let wide = context.stream::<WideInteger>("ORDERS")?;
                db.tx(|tx| {
                    assert_eq!(
                        code(&tx.emit(&huge, Oversize(&produced)).unwrap_err()),
                        ErrorCode::PayloadTooLarge
                    );
                    assert!(
                        produced.load(Ordering::Relaxed) < 4096,
                        "encoder must stop before traversing the unbounded input"
                    );
                    assert_eq!(
                        code(
                            &tx.emit(
                                &orders,
                                OrderEvent {
                                    order: "x".repeat(1025)
                                }
                            )
                            .unwrap_err()
                        ),
                        ErrorCode::PayloadTooLarge
                    );
                    assert_eq!(
                        code(&tx.emit(&invalid, ZeroVersion).unwrap_err()),
                        ErrorCode::InvalidRequest
                    );
                    assert_eq!(
                        code(
                            &tx.emit(&wide, WideInteger { number: u128::MAX })
                                .unwrap_err()
                        ),
                        ErrorCode::InvalidRequest
                    );
                    tx.put_json("caught_encoding_error", &true.into())
                })?;
                assert_eq!(db.get_json("caught_encoding_error")?, Some(true.into()));
            } else if request.url.ends_with("/rollback") {
                let result: Result<()> = db.tx(|tx| {
                    tx.put_json("rolled_back", &true.into())?;
                    tx.emit(&orders, event())?;
                    Err(anyhow!("business validation failed"))
                });
                assert!(result.is_err());
                assert!(db.get_json("rolled_back")?.is_none());
            } else if request.url.ends_with("/nested") {
                db.tx(|outer| {
                    let rejected: Result<()> = outer.tx(|inner| {
                        inner.emit(&orders, event())?;
                        Err(anyhow!("nested validation failed"))
                    });
                    assert!(rejected.is_err());
                    outer.emit(&orders, event())
                })?;
            } else if request.url.ends_with("/commit") {
                db.tx(|tx| {
                    tx.put_json("confirmed", &true.into())?;
                    tx.emit(&orders, event())
                })?;
                assert_eq!(db.get_json("confirmed")?, Some(true.into()));
                assert!(db.delete_all().is_err());
                assert_eq!(db.get_json("confirmed")?, Some(true.into()));
            } else if request.url.ends_with("/inspect") {
                let before = db.outbox(&OutboxOptions::default())?;
                assert_eq!(before.pending_rows, 2);
                assert_eq!(before.lanes.len(), 1);
                assert_eq!(before.lanes[0].pending_rows, 2);
                assert!(before.observation.oldest_pending_age_ms.is_some());
                assert!(before.lanes[0].oldest_pending_age_ms.is_some());
                assert!(before.lanes[0].blocked_reason.is_none());
                assert!(before
                    .observation
                    .blocked_lanes
                    .iter()
                    .all(|count| count.lanes == 0));
                assert!(before.pending_bytes > 0 && before.next.is_none());
                assert_eq!(before.lanes[0].revision.next_sequence, 3);
                let wire = serde_json::to_value(&before)?;
                assert_eq!(wire["pending_rows"], "2");
                assert!(wire["observation"]["oldest_pending_age_ms"].is_string());
                assert!(wire["observation"]["observed_ms"].is_string());
                assert!(wire["observation"]["blocked_lanes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|count| count["lanes"] == "0"));
                assert!(wire["lanes"][0].get("envelope").is_none());
                assert_eq!(
                    code(
                        &db.outbox(&OutboxOptions {
                            limit: 0,
                            cursor: None
                        })
                        .unwrap_err()
                    ),
                    ErrorCode::InvalidRequest
                );
                assert_eq!(
                    code(
                        &db.outbox(&OutboxOptions {
                            limit: 101,
                            cursor: None
                        })
                        .unwrap_err()
                    ),
                    ErrorCode::InvalidRequest
                );
                assert_eq!(
                    code(
                        &db.outbox(&OutboxOptions {
                            limit: 1,
                            cursor: Some("x".repeat(1025))
                        })
                        .unwrap_err()
                    ),
                    ErrorCode::InvalidCursor
                );
                let other = context.stream::<OrderEvent>("OTHER")?;
                let rolled_back: Result<()> = db.tx(|tx| {
                    tx.emit(&other, event())?;
                    let first = tx.outbox(&OutboxOptions {
                        limit: 1,
                        cursor: None,
                    })?;
                    assert_eq!(first.pending_rows, 3);
                    assert_eq!(first.lanes.len(), 1);
                    let next = first.next.context("second lane continuation")?;
                    let second = tx.outbox(&OutboxOptions {
                        limit: 1,
                        cursor: Some(next.clone()),
                    })?;
                    assert_eq!(second.lanes.len(), 1);
                    assert!(second.next.is_none());
                    assert_ne!(first.lanes[0].revision.lane, second.lanes[0].revision.lane);
                    assert_eq!(
                        first.lanes[0].pending_rows + second.lanes[0].pending_rows,
                        3
                    );
                    let bytes = base64::Engine::decode(
                        &base64::engine::general_purpose::URL_SAFE_NO_PAD,
                        next.strip_prefix("v1.").unwrap(),
                    )?;
                    let mut cursor: (String, String, String) = serde_json::from_slice(&bytes)?;
                    cursor.1 = "ffffffffffffffffffffffffffffffff".into();
                    let forged = format!(
                        "v1.{}",
                        base64::Engine::encode(
                            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
                            serde_json::to_vec(&cursor)?
                        )
                    );
                    assert_eq!(
                        code(
                            &tx.outbox(&OutboxOptions {
                                limit: 1,
                                cursor: Some(forged)
                            })
                            .unwrap_err()
                        ),
                        ErrorCode::InvalidCursor
                    );
                    assert!(tx.retry_outbox_lane(&before.lanes[0].revision)?);
                    assert_eq!(
                        code(&tx.retry_outbox_lane(&before.lanes[0].revision).unwrap_err()),
                        ErrorCode::ConfigConflict
                    );
                    Err(anyhow!("rollback inspected retry"))
                });
                assert!(rolled_back.is_err());
                let after = db.outbox(&OutboxOptions::default())?;
                assert_eq!(after.pending_rows, before.pending_rows);
                assert_eq!(after.pending_bytes, before.pending_bytes);
                assert_eq!(
                    after.lanes[0].revision.dispatch_generation,
                    before.lanes[0].revision.dispatch_generation
                );
                let mut stale = before.lanes[0].revision.clone();
                stale.incarnation = "ffffffffffffffffffffffffffffffff".into();
                assert_eq!(
                    code(&db.retry_outbox_lane(&stale).unwrap_err()),
                    ErrorCode::ConfigConflict
                );
                stale = before.lanes[0].revision.clone();
                stale.source = "Order:another".into();
                assert_eq!(
                    code(&db.retry_outbox_lane(&stale).unwrap_err()),
                    ErrorCode::InvalidRequest
                );
                assert!(db.retry_outbox_lane(&before.lanes[0].revision)?);
                assert_eq!(
                    code(&db.retry_outbox_lane(&before.lanes[0].revision).unwrap_err()),
                    ErrorCode::ConfigConflict
                );
                let after = db.outbox(&OutboxOptions::default())?;
                assert_eq!(
                    (after.pending_rows, after.pending_bytes),
                    (before.pending_rows, before.pending_bytes)
                );
                assert_eq!(
                    after.lanes[0].revision.dispatch_generation,
                    before.lanes[0].revision.dispatch_generation + 1
                );
                db.retry_outbox()?;
                assert_eq!(
                    code(&db.retry_outbox_lane(&after.lanes[0].revision).unwrap_err()),
                    ErrorCode::ConfigConflict
                );
                let retried = db.outbox(&OutboxOptions::default())?;
                assert_eq!(
                    retried.lanes[0].revision.dispatch_generation,
                    after.lanes[0].revision.dispatch_generation + 1
                );
                assert_eq!(
                    (retried.pending_rows, retried.pending_bytes),
                    (before.pending_rows, before.pending_bytes)
                );
            } else {
                return Err(anyhow!("unknown source operation"));
            }
            Ok(response())
        })
    }
}

fn bindings() -> Vec<Binding> {
    let orders = Binding {
        environment: "ORDERS".into(),
        resource: Resource {
            tenant: "tenant".into(),
            application: "shop".into(),
            id: "00000000000000000000000000000001".into(),
            name: "orders".into(),
            family: "orders".into(),
        },
        principal: SCRIPT.into(),
        grant: "publish-orders".into(),
        publish: true,
        read: false,
        subscriptions: vec![],
        administer: false,
    };
    let mut readonly = orders.clone();
    readonly.environment = "READ_ONLY".into();
    readonly.publish = false;
    let mut wrong = orders.clone();
    wrong.environment = "WRONG_FAMILY".into();
    wrong.resource.family = "shipments".into();
    let mut other = orders.clone();
    other.environment = "OTHER".into();
    other.resource.id = "00000000000000000000000000000002".into();
    other.resource.name = "other-orders".into();
    vec![orders, readonly, wrong, other]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sdk_emission_obeys_transaction_authority_and_bound_publication_grants() -> Result<()> {
    let domain = cellar::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut limits = cellar::sdk::CellLimits::default();
    limits.events.max_operation_receipts = 2;
    domain.services().configure_cell_limits(limits)?;
    let result = domain.run(source_scenario()).await;
    domain.shutdown().await;
    result?
}

async fn source_scenario() -> Result<()> {
    let registry = Registry::new(|_, _| Box::pin(async { Ok(response()) }))
        .cell("Order", |_, _| Ok(Arc::new(Order)))?;
    let graph = DeploymentGraph::single(LoadedDeployment {
        event_streams: Vec::new(),
        native: None,
        options: WorkerConfigOptions {
            src: String::new(),
            script_name: SCRIPT.into(),
            do_classes: vec!["Order".into()],
            bindings: vec![],
            blob_bindings: vec![],
            sql_bindings: vec![],
            kv_bindings: vec![],
            queue_bindings: vec![],
            stream_bindings: bindings(),
            queue_consumers: vec![],
            workflow_bindings: vec![],
            vars: vec![],
            node: "test".into(),
            modules: vec![],
            compat: Compat::default(),
        },
        script_name: SCRIPT.into(),
        version: "test".into(),
        prefix: "test/".into(),
        asset_binding: None,
        loader_bindings: vec![],
        assets: None,
        services: vec![],
        crons: vec![],
        containers: vec![],
        fence_image: None,
        container_images: vec![],
    });
    let generation = Generation::build_native(
        1,
        graph,
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
    runtime.publish_cell(SCOPE, 1)?;
    for path in [
        "checks", "commit", "encoding", "rollback", "nested", "inspect",
    ] {
        let result = runtime
            .fetch_cell(
                SCOPE.into(),
                None,
                RuntimeFetch {
                    url: format!("http://cell/{path}"),
                    method: "GET".into(),
                    body: Vec::new().into(),
                    headers: vec![],
                    request_id: Some(cellar::engine_api::next_request_id()),
                    order: None,
                    parent: None,
                },
                None,
            )
            .await?;
        assert_eq!(result.body, b"ok");
    }
    let initial = source_operation(&runtime, serde_json::json!({"op":"inspect"})).await?;
    let first = serde_json::json!({"op":"retry","change":{"operation":"00000000000000000000000000000001","reason":"test retained retry receipt","expected":initial["result"]["lanes"][0]["revision"]}});
    let first_receipt = source_operation(&runtime, first.clone()).await?;
    assert_eq!(first_receipt["result"]["rearmed"], true);
    let page = source_operation(&runtime, serde_json::json!({"op":"inspect"})).await?;
    let second = serde_json::json!({"op":"retry","change":{"operation":"00000000000000000000000000000002","reason":"test second retained receipt","expected":page["result"]["lanes"][0]["revision"]}});
    assert_eq!(
        source_operation(&runtime, second).await?["result"]["rearmed"],
        true
    );
    let before = source_operation(&runtime, serde_json::json!({"op":"inspect"})).await?;
    let third = serde_json::json!({"op":"retry","change":{"operation":"00000000000000000000000000000003","reason":"test exhausted operation budget","expected":before["result"]["lanes"][0]["revision"]}});
    assert_eq!(
        source_operation(&runtime, third).await?["error"]["code"],
        "OperationLimit"
    );
    let stable = |mut reply: serde_json::Value| -> Result<serde_json::Value> {
        let observed = reply["result"]["observation"]["observed_ms"]
            .as_str()
            .context("sample time missing")?
            .parse::<i64>()?;
        // Compare sampled ages as creation timestamps. Every stored counter,
        // identity, reason and wake deadline still participates in equality.
        for lane in reply["result"]["lanes"]
            .as_array_mut()
            .context("lanes missing")?
        {
            if let Some(age) = lane["oldest_pending_age_ms"].as_str() {
                lane["oldest_pending_age_ms"] = (observed - age.parse::<i64>()?).to_string().into();
            }
        }
        if let Some(age) = reply["result"]["observation"]["oldest_pending_age_ms"].as_str() {
            reply["result"]["observation"]["oldest_pending_age_ms"] =
                (observed - age.parse::<i64>()?).to_string().into();
        }
        reply["result"]["observation"]
            .as_object_mut()
            .context("observation missing")?
            .remove("observed_ms");
        Ok(reply)
    };
    assert_eq!(
        stable(source_operation(&runtime, serde_json::json!({"op":"inspect"})).await?)?,
        stable(before)?,
        "capacity failure changed source metadata or wake deadline"
    );
    assert_eq!(
        source_operation(&runtime, first).await?,
        first_receipt,
        "receipt replay consumed capacity or retried again"
    );
    let audit = source_operation(&runtime, serde_json::json!({"op":"audit","limit":100})).await?;
    assert_eq!(audit["result"]["records"].as_array().unwrap().len(), 2);
    runtime.swap_out_cell(SCOPE, 1).await?;
    let connection = rusqlite::Connection::open(
        directory
            .path()
            .join("cells")
            .join(SCOPE)
            .join("ltx/e1/db.sqlite"),
    )?;
    let mut statement =
        connection.prepare("SELECT envelope FROM __sys_outbox ORDER BY sequence")?;
    let bytes = statement
        .query_map([], |row| row.get::<_, Vec<u8>>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let events = bytes
        .iter()
        .map(|bytes| serde_json::from_slice::<Envelope>(bytes))
        .collect::<serde_json::Result<Vec<_>>>()?;
    assert_eq!(
        events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(events[0].lane, events[1].lane);
    assert_eq!(events[0].decode::<OrderEvent>()?.order, "123");
    Ok(())
}

// This fixture exercises local storage admission, not ownership or fleet proof.
// The public Node fixture separately covers the authenticated production route.
async fn source_operation(
    runtime: &RuntimeManager,
    body: serde_json::Value,
) -> Result<serde_json::Value> {
    let authority = serde_json::from_value::<cellar::runtime::RpcAuthority>(
        serde_json::json!({"kind":"outbox-operator-v1","scope":SCOPE,"operator":"outbox"}),
    )?;
    let (reply, _receive) = tokio::sync::oneshot::channel();
    let mut call = cellar::host_channels::RpcCallReq {
        authority: Some(authority),
        request_id: None,
        cancel: None,
        scope: SCOPE.into(),
        name: None,
        method: "__outboxControl".into(),
        args: cellar::engine_api::RpcData::Json(body.to_string()),
        reply,
    };
    let result = runtime.rpc_from_host(&mut call).await?;
    cellar::application::decode_rpc_result(&result.data)?.to_json()
}
