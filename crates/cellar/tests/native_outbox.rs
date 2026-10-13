//! Source SDK transactions through native application turns. The fixture
//! checks persisted emission; fleet dispatch/proof qualification is separate.

use anyhow::{anyhow, Result};
use cellar::application::event_stream::{
    Binding, DeliveryError, Envelope, ErrorCode, Event, Resource,
};
use cellar::application::{Cell, Context, HandlerFuture, Invocation, Output, Registry};
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
    vec![orders, readonly, wrong]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sdk_emission_obeys_transaction_authority_and_bound_publication_grants() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
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
    for path in ["checks", "commit", "encoding", "rollback", "nested"] {
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
