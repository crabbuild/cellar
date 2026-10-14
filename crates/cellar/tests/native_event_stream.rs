//! Native source/catalog/stream turns with real persisted system cells. The
//! fixture owns routing and uses an explicit proof adapter; fleet qualification
//! exercises the production node driver separately.

use anyhow::{anyhow, Result};
use cellar::application::{
    decode_rpc_result, Cell, Context, HandlerFuture, Invocation, Output, Registry,
};
use cellar::engine_api::{
    Compat, HttpResponse, RpcData, RuntimeFetch, RuntimeOptions, WorkerConfigOptions,
};
use cellar::fleet::LoadedDeployment;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::host_channels::RpcCallReq;
use cellar::runtime::RuntimeManager;
use cellar::sdk::event_stream::{
    BatchOptions, Binding, Change, Definition, DeliveryError, Envelope, Event, Grant, ReadOptions,
    Receipt, Resource, Retention, Retirement, RetirementMode, Start, SubscriptionDefinition,
};
use futures_util::{stream::FuturesUnordered, FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const SCRIPT: &str = "native-events";
const SOURCE: &str = "Producer:source";

#[derive(Serialize, Deserialize)]
struct Order {
    id: String,
    business_id: i64,
}

impl Event for Order {
    const FAMILY: &'static str = "orders";
    const VERSION: u32 = 1;

    fn key(&self) -> &str {
        &self.id
    }
}

fn binding() -> Binding {
    Binding {
        environment: "ORDERS".into(),
        resource: Resource {
            tenant: "tenant".into(),
            application: "shop".into(),
            id: "00000000000000000000000000000001".into(),
            name: "orders".into(),
            family: "orders".into(),
        },
        principal: SCRIPT.into(),
        grant: "orders-owner".into(),
        publish: true,
        read: true,
        subscriptions: vec!["fulfillment".into(), "analytics".into()],
        administer: true,
    }
}

fn definition(revision: i64, publish: bool) -> Definition {
    let mut grant = Grant::from_binding(&binding());
    grant.publish = publish;
    Definition {
        resource: binding().resource,
        revision,
        retention: Retention::Bounded,
        max_bytes: 1 << 20,
        max_records: 1000,
        max_age_ms: 1000,
        grants: vec![grant, view_grant()],
        subscriptions: vec![
            SubscriptionDefinition {
                name: "fulfillment".into(),
                start: Start::Earliest,
                required: false,
            },
            SubscriptionDefinition {
                name: "analytics".into(),
                start: Start::Earliest,
                required: false,
            },
        ],
    }
}

fn view_binding() -> Binding {
    let mut binding = binding();
    binding.environment = "VIEW".into();
    binding.grant = "orders-view".into();
    binding.publish = false;
    // A deployment's stale administrative/subscription permissions must not
    // override the narrower grants persisted at the resource owner.
    binding
}

fn view_grant() -> Grant {
    let mut grant = Grant::from_binding(&view_binding());
    grant.administer = false;
    grant.subscriptions = vec!["fulfillment".into()];
    grant
}

fn new_binding() -> Binding {
    let mut binding = binding();
    binding.environment = "ORDERS_NEW".into();
    binding.resource.id = "00000000000000000000000000000002".into();
    binding
}

fn new_admin_binding() -> Binding {
    let mut binding = new_binding();
    binding.environment = "ORDERS_NEW_ADMIN".into();
    binding.grant = "orders-replacement-admin".into();
    binding.publish = false;
    binding.read = false;
    binding.subscriptions.clear();
    binding
}

fn response(value: Value) -> Output {
    Output::Http(HttpResponse {
        status: 200,
        body: serde_json::to_vec(&value).unwrap(),
        stream: None,
        headers: vec![],
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}

struct Producer;

impl Cell for Producer {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Err(anyhow!("unexpected source event"));
            };
            let orders = context.stream::<Order>("ORDERS")?;
            let url = url::Url::parse(&request.url)?;
            let query = url
                .query_pairs()
                .collect::<std::collections::HashMap<_, _>>();
            let orders = if query.contains_key("admin") {
                context.stream::<Order>("ORDERS_NEW_ADMIN")?
            } else if query.contains_key("new") {
                context.stream::<Order>("ORDERS_NEW")?
            } else if query.contains_key("view") {
                context.stream::<Order>("VIEW")?
            } else {
                orders
            };
            if matches!(
                url.path(),
                "/next" | "/ack" | "/retry-batch" | "/extend" | "/status" | "/change" | "/audit"
            ) {
                let name = query
                    .get("subscription")
                    .map(|name| name.as_ref())
                    .unwrap_or("fulfillment");
                let subscription = orders.subscription(name)?;
                let result:Result<Value>=async {
                    if url.path()=="/status" {return Ok(serde_json::to_value(subscription.status(&context).await?)?);}
                    if url.path()=="/change" {
                        use base64::Engine;
                        let encoded=query.get("change").ok_or_else(||anyhow!("change missing"))?;
                        let change:Change=serde_json::from_slice(&base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(encoded.as_bytes())?)?;
                        return Ok(serde_json::to_value(subscription.change(&context,&change).await?)?);
                    }
                    if url.path()=="/audit" {
                        let after=query.get("after").map(|value|value.parse()).transpose()?.unwrap_or(0);
                        return Ok(serde_json::to_value(orders.audit(&context,after,2).await?)?);
                    }
                    if url.path()=="/next" {
                        let fetch_id=query.get("fetch_id").ok_or_else(||anyhow!("fetch ID missing"))?;
                        let options=BatchOptions { wait_ms:query.get("wait_ms").map(|value|value.parse()).transpose()?.unwrap_or(0),
                            max_bytes:query.get("max_bytes").map(|value|value.parse()).transpose()?.unwrap_or(1<<20),..BatchOptions::default() };
                        let Some(batch)=subscription.next_with_id(&context,fetch_id,options).await? else {return Ok(Value::Null);};
                        return Ok(json!({"receipt":batch.receipt().encode()?,"issued_ms":batch.issued_at_ms.to_string(),
                            "expires_ms":batch.expires_at_ms.to_string(),"records":batch.records.iter().map(|record|json!({"offset":record.offset.to_string(),"event":record.event})).collect::<Vec<_>>()}));
                    }
                    let receipt=Receipt::decode(query.get("receipt").ok_or_else(||anyhow!("receipt missing"))?)?;
                    if url.path()=="/ack" {return Ok(json!({"status":format!("{:?}",subscription.ack(&context,&receipt).await?)}));}
                    if url.path()=="/extend" {
                        let target=query.get("target").ok_or_else(||anyhow!("target missing"))?.parse()?;
                        return Ok(json!({"expires_ms":subscription.extend_until(&context,&receipt,target).await?.to_string()}));
                    }
                    subscription.retry(&context,&receipt,0).await?;
                    Ok(json!({"retried":true}))
                }.await;
                let value = match result {
                    Ok(value) => value,
                    Err(failure) => {
                        json!({"error":failure.downcast_ref::<DeliveryError>().map(|failure|format!("{:?}",failure.code)).unwrap_or_else(||failure.to_string())})
                    }
                };
                return Ok(response(value));
            }
            if url.path() == "/stats" {
                return Ok(response(serde_json::to_value(
                    orders.stats(&context).await?,
                )?));
            }
            let value = if matches!(
                url.path(),
                "/retire"
                    | "/retirement-status"
                    | "/new-configure"
                    | "/new-revoke-admin"
                    | "/configuration-status"
            ) {
                let result: Result<Value> = async {
                    if url.path() == "/configuration-status" {
                        return Ok(serde_json::to_value(
                            orders.configuration_status(&context).await?,
                        )?);
                    }
                    if url.path() == "/new-revoke-admin" {
                        let mut desired = definition(2, true);
                        desired.resource = new_binding().resource;
                        desired.grants[0].administer = false;
                        desired
                            .grants
                            .push(Grant::from_binding(&new_admin_binding()));
                        return Ok(serde_json::to_value(
                            orders
                                .configure(&context, &desired, "00000000000000000000000000000074")
                                .await?,
                        )?);
                    }
                    if url.path() == "/new-configure" {
                        let mut desired = definition(1, true);
                        desired.resource = new_binding().resource;
                        return Ok(serde_json::to_value(
                            orders
                                .configure(&context, &desired, "00000000000000000000000000000071")
                                .await?,
                        )?);
                    }
                    let operation = query
                        .get("operation")
                        .map(|value| value.as_ref())
                        .unwrap_or("00000000000000000000000000000070");
                    if url.path() == "/retirement-status" {
                        return Ok(serde_json::to_value(
                            orders.retirement_status(&context, operation).await?,
                        )?);
                    }
                    let mode = if query.contains_key("force") {
                        RetirementMode::Force {
                            data_loss_acknowledgment:
                                "accept retained-history loss and blocked pending producers".into(),
                        }
                    } else {
                        RetirementMode::Coordinated {
                            procedure:
                                "test procedure quiesced all sources and handled all subscriptions"
                                    .into(),
                        }
                    };
                    let request = Retirement {
                        operation: operation.into(),
                        revision: query
                            .get("revision")
                            .map(|value| value.parse())
                            .transpose()?
                            .unwrap_or(5),
                        reason: "retire this resource incarnation".into(),
                        mode,
                    };
                    Ok(serde_json::to_value(
                        orders.retire(&context, &request).await?,
                    )?)
                }
                .await;
                match result {
                    Ok(value) => value,
                    Err(failure) => {
                        json!({"error":failure.downcast_ref::<DeliveryError>().map(|failure|format!("{:?}",failure.code)).unwrap_or_else(||failure.to_string())})
                    }
                }
            } else if request.url.ends_with("/configure") {
                serde_json::to_value(
                    orders
                        .configure(
                            &context,
                            &definition(1, true),
                            "00000000000000000000000000000011",
                        )
                        .await?,
                )?
            } else if request.url.ends_with("/revoke") {
                serde_json::to_value(
                    orders
                        .configure(
                            &context,
                            &definition(2, false),
                            "00000000000000000000000000000012",
                        )
                        .await?,
                )?
            } else if request.url.ends_with("/restore-grant") {
                serde_json::to_value(
                    orders
                        .configure(
                            &context,
                            &definition(3, true),
                            "00000000000000000000000000000013",
                        )
                        .await?,
                )?
            } else if request.url.ends_with("/required") {
                let mut desired = definition(4, true);
                desired.retention = Retention::Required;
                desired.max_records = 1;
                desired.subscriptions[0].required = true;
                serde_json::to_value(
                    orders
                        .configure(&context, &desired, "00000000000000000000000000000014")
                        .await?,
                )?
            } else if request.url.ends_with("/recreate") {
                let mut desired = definition(5, true);
                desired.retention = Retention::Required;
                desired.max_records = 1;
                desired.subscriptions[0].required = true;
                serde_json::to_value(
                    orders
                        .configure(&context, &desired, "00000000000000000000000000000015")
                        .await?,
                )?
            } else if request.url.ends_with("/inspect-settled") {
                let db = context.storage()?;
                let page = db.outbox(&cellar::sdk::OutboxOptions::default())?;
                let revision = page.lanes[0].revision.clone();
                assert!(!db.retry_outbox_lane(&revision)?);
                assert_eq!(
                    db.outbox(&cellar::sdk::OutboxOptions::default())?.lanes[0]
                        .revision
                        .dispatch_generation,
                    revision.dispatch_generation
                );
                serde_json::to_value(page)?
            } else if request.url.ends_with("/seal") {
                let result = context
                    .storage()?
                    .tx(|tx| tx.seal(&orders, "00000000000000000000000000000050"));
                match result {
                    Ok(seal) => serde_json::to_value(seal)?,
                    Err(failure) => {
                        json!({"error":failure.downcast_ref::<DeliveryError>().map(|failure|format!("{:?}",failure.code)).unwrap_or_else(||failure.to_string())})
                    }
                }
            } else if url.path() == "/emit" {
                context.storage()?.tx(|tx| {
                    tx.emit(
                        &orders,
                        Order {
                            id: "123".into(),
                            business_id: i64::MAX,
                        },
                    )
                })?;
                json!({"emitted":true})
            } else if request.url.ends_with("/retry") {
                context.storage()?.retry_outbox()?;
                json!({"retry":true})
            } else if url.path() == "/read" {
                let page = orders.read(&context, None, ReadOptions::default()).await?;
                json!({"events":page.records.iter().map(|record|record.event.clone()).collect::<Vec<_>>(),
                    "earliest":page.earliest.to_string(),"end":page.end.to_string()})
            } else {
                return Err(anyhow!("unknown source operation"));
            };
            Ok(response(value))
        })
    }
}

#[derive(Clone)]
struct Captured {
    scope: String,
    name: Option<String>,
    method: String,
    args: RpcData,
    authority: Option<cellar::runtime::RpcAuthority>,
}

async fn resident(
    runtime: &RuntimeManager,
    scope: &str,
    owned: &tokio::sync::Mutex<BTreeSet<String>>,
) -> Result<()> {
    let mut owned = owned.lock().await;
    if owned.contains(scope) {
        return Ok(());
    }
    runtime
        .restore_cell(
            scope,
            &cellar_logic::RestoreSpec {
                epoch: 1,
                fresh: true,
                took_over: false,
                resume_local: false,
                prior: None,
            },
        )
        .await?;
    runtime.start_cell(scope.into(), 1, true).await?;
    runtime.publish_cell(scope, 1)?;
    owned.insert(scope.into());
    Ok(())
}

async fn fetch(runtime: &RuntimeManager, path: &str) -> Result<Value> {
    let result = runtime
        .fetch_cell(
            SOURCE.into(),
            None,
            RuntimeFetch {
                url: format!("http://source/{path}"),
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
    Ok(serde_json::from_slice(&result.body)?)
}

async fn fire(runtime: &RuntimeManager, scope: &str, id: u64) -> Result<()> {
    // A raw runtime response may beat its driver's final alarm-cache report.
    // Wait for that report, rather than assuming synchronous node observation.
    let deadline = cellar::asyncrt::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(deadline) = runtime
                .alarm(scope)
                .filter(|deadline| *deadline <= cellar::asyncrt::wall_ms().saturating_add(2000))
            {
                break deadline;
            }
            cellar::asyncrt::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("no work deadline for {scope} at step {id}"))?;
    let now = cellar::asyncrt::wall_ms();
    if deadline > now {
        if deadline - now > 2000 {
            return Err(anyhow!(
                "unexpected long deadline for {scope} at step {id}: {}ms",
                deadline - now
            ));
        }
        cellar::asyncrt::sleep(Duration::from_millis((deadline - now + 5) as u64)).await;
    }
    let (snapshot, _, _) = runtime.fire_alarm(id, scope.into(), deadline).await?;
    // The response carries the committed projection; wait for its asynchronous
    // observer report before using the cache to schedule another firing.
    cellar::asyncrt::timeout(Duration::from_secs(2), async {
        while runtime.alarm(scope) != snapshot.at_ms() {
            cellar::asyncrt::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("alarm-cache report did not reach {scope}"))?;
    Ok(())
}

async fn replay(runtime: &RuntimeManager, captured: &Captured) -> Result<Value> {
    let (send, _receive) = tokio::sync::oneshot::channel();
    let mut request = RpcCallReq {
        authority: captured.authority.clone(),
        request_id: Some(cellar::engine_api::next_request_id()),
        cancel: None,
        scope: captured.scope.clone(),
        name: captured.name.clone(),
        method: captured.method.clone(),
        args: captured.args.clone(),
        reply: send,
    };
    let result = runtime.rpc_from_host(&mut request).await?;
    decode_rpc_result(&result.data)?.to_json()
}

async fn provision(
    runtime: &RuntimeManager,
    generation: &Generation,
    resource: &Resource,
    operation: &str,
    owned: &Arc<tokio::sync::Mutex<BTreeSet<String>>>,
) -> Result<Value> {
    let (reply, _receiver) = tokio::sync::oneshot::channel();
    let mut request = generation.event_provisioning_request(resource, operation, reply)?;
    resident(runtime, &request.scope, owned).await?;
    let result = runtime.rpc_from_host(&mut request).await?;
    decode_rpc_result(&result.data)?.to_json()
}

fn event_graph(script: &str, bindings: Vec<Binding>) -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        event_streams: Vec::new(),
        native: None,
        options: WorkerConfigOptions {
            src: String::new(),
            script_name: script.into(),
            do_classes: vec![
                "Producer".into(),
                cellar::deploy::EVENT_STREAM_CLASS.into(),
                cellar::deploy::EVENT_CATALOG_CLASS.into(),
            ],
            bindings: vec![],
            blob_bindings: vec![],
            sql_bindings: vec![],
            kv_bindings: vec![],
            queue_bindings: vec![],
            queue_consumers: vec![],
            stream_bindings: bindings.into_iter().map(Into::into).collect(),
            workflow_bindings: vec![],
            vars: vec![],
            node: "test".into(),
            modules: vec![],
            compat: Compat::default(),
        },
        script_name: script.into(),
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
    })
}

async fn reopen_source(runtime: &RuntimeManager) -> Result<()> {
    runtime.swap_out_cell(SOURCE, 1).await?;
    runtime
        .restore_cell(
            SOURCE,
            &cellar_logic::RestoreSpec {
                epoch: 1,
                fresh: false,
                took_over: false,
                resume_local: true,
                prior: None,
            },
        )
        .await?;
    runtime.start_cell(SOURCE.into(), 1, false).await?;
    runtime.publish_cell(SOURCE, 1)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_catalog_delivery_retention_and_revocation_preserve_durable_identity() -> Result<()>
{
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::services().configure_cell_limits(cellar::sdk::CellLimits {
        events: cellar::sdk::EventLimits {
            lease_ms: 2000,
            max_lease_lifetime_ms: 5000,
            max_operation_receipts: 5,
            sweep_rows: 2,
            ..Default::default()
        },
        ..Default::default()
    })?;
    let registry: Arc<dyn cellar::application::Application> = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(response(json!({}))) }))
            .cell("Producer", |_, _| Ok(Arc::new(Producer)))?,
    );
    let graph = event_graph(
        SCRIPT,
        vec![
            binding(),
            view_binding(),
            new_binding(),
            new_admin_binding(),
        ],
    );
    let generation = Generation::build_native(
        1,
        graph,
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([(SCRIPT.into(), registry.clone())]),
    )?;
    // Serving bindings intentionally contain stale permissions to exercise
    // receiver-side grants. Trusted provisioning uses a separate valid host
    // declaration, without admitting those stale bindings as declarations.
    let mut provisioning_graph = event_graph(SCRIPT, vec![binding()]);
    provisioning_graph.primary.event_streams = vec![definition(1, true)];
    let provisioning_generation = Generation::build_native(
        1,
        provisioning_graph,
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([(SCRIPT.into(), registry.clone())]),
    )?;
    let directory = tempfile::tempdir()?;
    let resource = binding().resource;
    let catalog_name = serde_json::to_string(&(&resource.tenant, &resource.application))?;
    let catalog_id =
        cellar_native::cell_id::for_name("cells:v1:event-catalog:__EventCatalog", &catalog_name);
    let catalog_scope = format!(
        "{}:{}",
        cellar::deploy::EVENT_CATALOG_CLASS,
        cellar_native::cell_id::encode_hex(&catalog_id)
    );
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
    let owned = Arc::new(tokio::sync::Mutex::new(BTreeSet::new()));
    resident(&runtime, SOURCE, &owned).await?;
    let (gate_tx, mut gates) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_gate_tx(gate_tx);
    let (rpc_tx, mut calls) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_rpc_call_tx(rpc_tx);
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let mut gate_stop = stop.subscribe();
    let gate_task = cellar::asyncrt::spawn(async move {
        loop {
            let request = cellar::asyncrt::select! {request=gates.recv()=>request,_=gate_stop.changed()=>None};
            let Some(request) = request else {
                break;
            };
            let _ = request.reply.send(Ok(()));
        }
    });
    let captured = Arc::new(Mutex::new(None));
    let recordings = captured.clone();
    let lose_retirement_reply = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let reply_fault = lose_retirement_reply.clone();
    let retirement_request = Arc::new(Mutex::new(None));
    let retirement_recordings = retirement_request.clone();
    let sealing_request = Arc::new(Mutex::new(None));
    let seal_recordings = sealing_request.clone();
    let fetched_request = Arc::new(Mutex::new(None));
    let fetch_recordings = fetched_request.clone();
    let (next_observed, mut next_signal) = tokio::sync::watch::channel(0u64);
    let host = runtime.clone();
    let locations = owned.clone();
    let router = cellar::asyncrt::spawn(async move {
        let mut running = FuturesUnordered::new();
        loop {
            cellar::asyncrt::select! {
                call=calls.recv()=>{
                    let Some(mut call)=call else { break; };
                    if call.method=="__streamAppend" { *recordings.lock().unwrap()=Some(Captured{
                        scope:call.scope.clone(),name:call.name.clone(),method:call.method.clone(),args:call.args.clone(),authority:call.authority.clone(),
                    }); }
                    if call.method=="__streamNext" {
                        // The fixture separately captures terminal producer requests.
                        *fetch_recordings.lock().unwrap()=Some(Captured{scope:call.scope.clone(),name:call.name.clone(),method:call.method.clone(),args:call.args.clone(),authority:call.authority.clone()});
                        next_observed.send_modify(|revision|*revision+=1);
                    }
                    if call.method=="__streamSeal" {*seal_recordings.lock().unwrap()=Some(Captured{scope:call.scope.clone(),name:call.name.clone(),method:call.method.clone(),args:call.args.clone(),authority:call.authority.clone()});}
                    if call.method=="__streamRetire" {*retirement_recordings.lock().unwrap()=Some(Captured{scope:call.scope.clone(),name:call.name.clone(),method:call.method.clone(),args:call.args.clone(),authority:call.authority.clone()});}
                    let host=host.clone();let locations=locations.clone();
                    let reply_fault=reply_fault.clone();
                    running.push(async move {
                        let result=async {resident(&host,&call.scope,&locations).await?;let reply=host.rpc_from_host(&mut call).await?.data;
                            if call.method=="__streamRetire" && reply_fault.swap(false,std::sync::atomic::Ordering::AcqRel) {return Err(anyhow!("injected lost retirement response"));}
                            Ok::<_,anyhow::Error>(reply)}.await;
                        let _=call.reply.send(result);
                    }.boxed());
                },
                _=running.next(),if !running.is_empty()=>{},
                _=stopped.changed()=>break,
            }
        }
        while running.next().await.is_some() {}
    });
    assert!(fetch(&runtime, "configure")
        .await
        .unwrap_err()
        .to_string()
        .contains("Forbidden"));
    assert_eq!(
        provision(
            &runtime,
            &provisioning_generation,
            &binding().resource,
            "00000000000000000000000000000011",
            &owned
        )
        .await?["ok"]["state"],
        "active"
    );
    assert_eq!(fetch(&runtime, "configure").await?["state"], "active");
    assert_eq!(fetch(&runtime, "configure").await?["state"], "active");
    let initial = fetch(&runtime, "status").await?;
    assert_eq!(initial["observation"]["lag_records"], "0");
    assert_eq!(
        initial["observation"]["oldest_retained_age_ms"],
        Value::Null
    );
    let mut repeated = fetch(&runtime, "status").await?;
    let mut stable_initial = initial.clone();
    // Observation time advances; all durable status fields must remain equal.
    repeated["observation"]
        .as_object_mut()
        .unwrap()
        .remove("observed_ms");
    stable_initial["observation"]
        .as_object_mut()
        .unwrap()
        .remove("observed_ms");
    assert_eq!(stable_initial, repeated);
    let empty_stats = fetch(&runtime, "stats").await?;
    assert_eq!(empty_stats["rows"], "0");
    assert_eq!(empty_stats["observation"]["protected_bytes"], "0");
    assert_eq!(
        empty_stats["observation"]["oldest_retained_age_ms"],
        Value::Null
    );
    fetch(&runtime, "emit").await?;
    assert_eq!(fetch(&runtime, "seal").await?["error"], "PendingDelivery");
    fire(&runtime, SOURCE, 1).await?;
    let page = fetch(&runtime, "read").await?;
    let event: Envelope = serde_json::from_value(page["events"][0].clone())?;
    assert_eq!(event.decode::<Order>()?.business_id, i64::MAX);
    let recorded = captured
        .lock()
        .unwrap()
        .take()
        .ok_or_else(|| anyhow!("append was not captured"))?;
    assert_eq!(
        replay(&runtime, &recorded).await?["ok"]["accepted_through"],
        "1"
    );
    assert_eq!(
        fetch(&runtime, "read").await?["events"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let next =
        |id: &str, subscription: &str| format!("next?fetch_id={id}&subscription={subscription}");
    let first_id = "00000000000000000000000000000021";
    let second_id = "00000000000000000000000000000022";
    let first_batch = fetch(&runtime, &next(first_id, "fulfillment")).await?;
    let receipt = first_batch["receipt"].as_str().unwrap();
    let stream_stats = fetch(&runtime, "stats").await?;
    assert_eq!(stream_stats["observation"]["active_leases"], "1");
    assert_eq!(stream_stats["observation"]["protected_records"], "1");
    assert_eq!(
        stream_stats["observation"]["protected_bytes"],
        stream_stats["bytes"]
    );
    assert!(
        stream_stats["observation"]["sqlite_allocated_bytes"]
            .as_str()
            .unwrap()
            .parse::<i64>()?
            > 0
    );
    let lag = fetch(&runtime, "status").await?;
    assert_eq!(lag["observation"]["lag_records"], "1");
    assert_eq!(lag["observation"]["retained_lag_records"], "1");
    assert_eq!(lag["observation"]["history_expired"], false);
    assert_eq!(lag["observation"]["lease_active"], true);
    assert!(lag["observation"]["oldest_retained_age_ms"].is_string());
    assert_eq!(
        fetch(&runtime, &next(first_id, "fulfillment")).await?["receipt"],
        receipt
    );
    assert_eq!(
        fetch(&runtime, &next(second_id, "fulfillment")).await?["error"],
        "LeaseBusy"
    );
    let target = first_batch["issued_ms"].as_str().unwrap().parse::<i64>()? + 3000;
    let extension = format!("extend?receipt={receipt}&target={target}");
    assert_eq!(
        fetch(&runtime, &extension).await?["expires_ms"],
        target.to_string()
    );
    assert_eq!(
        fetch(&runtime, &extension).await?["expires_ms"],
        target.to_string()
    );
    assert_eq!(
        fetch(&runtime, &format!("retry-batch?receipt={receipt}")).await?["retried"],
        true
    );
    let second_batch = fetch(&runtime, &next(second_id, "fulfillment")).await?;
    assert_ne!(second_batch["receipt"], receipt);
    assert_eq!(
        fetch(&runtime, &format!("ack?receipt={receipt}")).await?["error"],
        "StaleReceipt"
    );
    let second_receipt = second_batch["receipt"].as_str().unwrap();
    // Eviction drops the broker and its notifications. The exact lease is
    // reopened from SQLite, including its token and fetch request identity.
    runtime.swap_out_cell(&recorded.scope, 1).await?;
    runtime
        .restore_cell(
            &recorded.scope,
            &cellar_logic::RestoreSpec {
                epoch: 1,
                fresh: false,
                took_over: false,
                resume_local: true,
                prior: None,
            },
        )
        .await?;
    runtime.start_cell(recorded.scope.clone(), 1, false).await?;
    runtime.publish_cell(&recorded.scope, 1)?;
    assert_eq!(
        fetch(&runtime, &next(second_id, "fulfillment")).await?["receipt"],
        second_receipt
    );
    let mut forged = serde_json::to_value(Receipt::decode(second_receipt)?)?;
    forged["token"] = json!("0".repeat(64));
    use base64::Engine;
    let forged = format!(
        "v1.{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(&forged)?)
    );
    assert_eq!(
        fetch(&runtime, &format!("ack?receipt={forged}")).await?["error"],
        "StaleReceipt"
    );
    assert_eq!(
        fetch(&runtime, &format!("ack?receipt={second_receipt}")).await?["status"],
        "Acknowledged"
    );
    assert_eq!(
        fetch(&runtime, &format!("ack?receipt={second_receipt}")).await?["status"],
        "AlreadyAcknowledged"
    );
    let caught_up = fetch(&runtime, "status").await?;
    assert_eq!(caught_up["observation"]["lag_records"], "0");
    assert_eq!(caught_up["observation"]["retained_lag_records"], "0");
    assert_eq!(
        caught_up["observation"]["oldest_retained_age_ms"],
        Value::Null
    );
    assert_eq!(caught_up["observation"]["lease_active"], false);
    let analytics = fetch(&runtime, &next(first_id, "analytics")).await?;
    let analytics_receipt = analytics["receipt"].as_str().unwrap();
    assert!(runtime
        .rpc(
            recorded.scope.clone(),
            recorded.name.clone(),
            recorded.method.clone(),
            recorded.args.clone(),
            None
        )
        .await
        .is_err());
    // A live lease protects retained bytes even past their age target.
    fire(&runtime, &recorded.scope, 2).await?;
    let age_blocked = fetch(&runtime, "stats").await?;
    assert_eq!(
        age_blocked["observation"]["retention_blocked_bytes"],
        age_blocked["bytes"]
    );
    assert_eq!(
        age_blocked["observation"]["protected_bytes"],
        age_blocked["bytes"]
    );
    assert_eq!(
        fetch(&runtime, "read").await?["events"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    fetch(
        &runtime,
        &format!("retry-batch?receipt={analytics_receipt}&subscription=analytics"),
    )
    .await?;
    // Releasing the lease permits age reclamation, but never resets producers.
    assert!(fetch(&runtime, "read").await?["events"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        fetch(&runtime, &next(second_id, "analytics")).await?["error"],
        "CursorExpired"
    );
    assert_eq!(
        replay(&runtime, &recorded).await?["ok"]["accepted_through"],
        "1"
    );
    assert!(fetch(&runtime, "read").await?["events"]
        .as_array()
        .unwrap()
        .is_empty());
    let lost_history = fetch(&runtime, "status?subscription=analytics").await?;
    assert_eq!(lost_history["observation"]["lag_records"], "1");
    assert_eq!(lost_history["observation"]["retained_lag_records"], "0");
    assert_eq!(lost_history["observation"]["history_expired"], true);
    assert_eq!(
        lost_history["observation"]["oldest_retained_age_ms"],
        Value::Null
    );
    let reclaimed = fetch(&runtime, "stats").await?;
    assert_eq!(reclaimed["rows"], "0");
    assert_eq!(reclaimed["bytes"], "0");
    assert_eq!(reclaimed["observation"]["protected_bytes"], "0");
    assert_eq!(reclaimed["observation"]["expired_subscriptions"], "1");
    fetch(&runtime, "revoke").await?;
    assert_eq!(
        replay(&runtime, &recorded).await?["error"]["code"],
        "Forbidden"
    );
    fetch(&runtime, "emit").await?;
    fire(&runtime, SOURCE, 3).await?;
    fetch(&runtime, "restore-grant").await?;
    fetch(&runtime, "retry").await?;
    next_signal.borrow_and_update();
    let poll_runtime = runtime.clone();
    let long_poll = cellar::asyncrt::spawn(async move {
        fetch(
            &poll_runtime,
            "next?fetch_id=00000000000000000000000000000023&wait_ms=2000",
        )
        .await
    });
    cellar::asyncrt::timeout(Duration::from_secs(2), next_signal.changed()).await??;
    fire(&runtime, SOURCE, 4).await?;
    let resumed = long_poll.await??;
    assert_eq!(resumed["records"][0]["offset"], "2");
    let resumed_receipt = resumed["receipt"].as_str().unwrap();
    assert_eq!(
        fetch(&runtime, &format!("ack?receipt={resumed_receipt}")).await?["status"],
        "Acknowledged"
    );
    assert_eq!(
        fetch(&runtime, "read").await?["events"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    // A required checkpoint pins capacity. Adapt an oversized atomic append
    // to the resource's batch limit; never drop a prefix to relieve pressure.
    assert_eq!(fetch(&runtime, "required").await?["state"], "active");
    fetch(&runtime, "emit").await?;
    fetch(&runtime, "emit").await?;
    fire(&runtime, SOURCE, 5).await?;
    fetch(&runtime, "retry").await?;
    fire(&runtime, SOURCE, 6).await?;
    let pressured = fetch(&runtime, "read").await?;
    assert_eq!(pressured["events"].as_array().unwrap().len(), 1);
    assert_eq!(pressured["events"][0]["sequence"], "3");
    let pinned = fetch(&runtime, "stats").await?;
    assert_eq!(pinned["observation"]["protected_records"], "1");
    assert_eq!(pinned["observation"]["protected_bytes"], pinned["bytes"]);
    fetch(&runtime, "retry").await?;
    fire(&runtime, SOURCE, 7).await?;
    assert_eq!(fetch(&runtime, "read").await?["events"][0]["sequence"], "3");
    let batch = fetch(
        &runtime,
        &next("00000000000000000000000000000024", "fulfillment"),
    )
    .await?;
    assert_eq!(batch["records"][0]["offset"], "3");
    let receipt = batch["receipt"].as_str().unwrap();
    fetch(&runtime, &format!("ack?receipt={receipt}")).await?;
    fetch(&runtime, "retry").await?;
    fire(&runtime, SOURCE, 8).await?;
    assert_eq!(fetch(&runtime, "read").await?["events"][0]["sequence"], "4");
    let expired = fetch(
        &runtime,
        &next("00000000000000000000000000000025", "fulfillment"),
    )
    .await?;
    let receipt = expired["receipt"].as_str().unwrap();
    let expiry = expired["expires_ms"].as_str().unwrap().parse::<i64>()?;
    let remaining = expiry.saturating_sub(cellar::asyncrt::wall_ms()).max(0);
    cellar::asyncrt::sleep(Duration::from_millis((remaining + 5) as u64)).await;
    assert_eq!(
        fetch(&runtime, &format!("ack?receipt={receipt}")).await?["error"],
        "LeaseExpired"
    );
    assert_eq!(
        fetch(
            &runtime,
            &format!("extend?receipt={receipt}&target={}", expiry + 1000)
        )
        .await?["error"],
        "LeaseExpired"
    );
    let expired_observation = fetch(&runtime, "status").await?;
    assert_eq!(expired_observation["observation"]["lease_active"], false);
    assert!(expired_observation["lease"].is_object());
    let redelivery = fetch(
        &runtime,
        &next("00000000000000000000000000000026", "fulfillment"),
    )
    .await?;
    assert_eq!(redelivery["records"][0]["offset"], "4");
    assert_ne!(redelivery["receipt"], receipt);
    let fresh_receipt = redelivery["receipt"].as_str().unwrap();
    assert_eq!(
        fetch(&runtime, &format!("ack?receipt={fresh_receipt}")).await?["status"],
        "Acknowledged"
    );
    let command = |operation: &str,
                   status: &Value,
                   action: Value,
                   reason: &str,
                   subscription: &str|
     -> Result<String> {
        let change = json!({"operation":operation,"expected":status["identity"],"action":action,"reason":reason});
        let encoded =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(&change)?);
        Ok(format!(
            "change?subscription={subscription}&change={encoded}"
        ))
    };
    let status = fetch(&runtime, "status").await?;
    assert_eq!(
        fetch(&runtime, "status?view=true").await?["identity"],
        status["identity"]
    );
    assert_eq!(
        fetch(&runtime, "status?view=true&subscription=analytics").await?["error"],
        "Forbidden"
    );
    let pause = command(
        "00000000000000000000000000000031",
        &status,
        json!({"kind":"pause"}),
        "pause processing for repair",
        "fulfillment",
    )?;
    let paused = fetch(&runtime, &pause).await?;
    assert_eq!(
        fetch(&runtime, &format!("{pause}&view=true")).await?["error"],
        "Forbidden"
    );
    assert_eq!(paused["state"], "paused");
    assert_eq!(paused["current"], status["identity"]);
    fetch(&runtime, "emit").await?;
    fire(&runtime, SOURCE, 9).await?;
    fire(&runtime, &recorded.scope, 10).await?;
    assert_eq!(fetch(&runtime, "read").await?["events"][0]["sequence"], "5");
    assert_eq!(
        fetch(
            &runtime,
            &next("00000000000000000000000000000032", "fulfillment")
        )
        .await?["error"],
        "SubscriptionPaused"
    );
    let resume = command(
        "00000000000000000000000000000033",
        &status,
        json!({"kind":"resume"}),
        "repair complete",
        "fulfillment",
    )?;
    assert_eq!(fetch(&runtime, &resume).await?["state"], "active");
    assert_eq!(
        fetch(&runtime, &resume).await?["current"],
        status["identity"]
    );
    let poison = fetch(
        &runtime,
        &next("00000000000000000000000000000034", "fulfillment"),
    )
    .await?;
    let old_fetch = fetched_request.lock().unwrap().take().unwrap();
    let poison_receipt = poison["receipt"].as_str().unwrap();
    let reset = command(
        "00000000000000000000000000000035",
        &status,
        json!({"kind":"reset","cursor":{"resource":binding().resource.id,"partition":0,"next":"6"}}),
        "quarantined event five before explicitly skipping",
        "fulfillment",
    )?;
    let reset_result = fetch(&runtime, &reset).await?;
    assert_eq!(reset_result["checkpoint"], "5");
    assert_eq!(reset_result["skipped"], json!({"first":"5","last":"5"}));
    assert_ne!(
        reset_result["current"]["generation"],
        status["identity"]["generation"]
    );
    assert_eq!(fetch(&runtime, &reset).await?, reset_result);
    assert_eq!(
        fetch(&runtime, &format!("ack?receipt={poison_receipt}")).await?["error"],
        "SubscriptionReset"
    );
    assert_eq!(
        replay(&runtime, &old_fetch).await?["error"]["code"],
        "SubscriptionReset"
    );
    let conflicting = command(
        "00000000000000000000000000000035",
        &status,
        json!({"kind":"pause"}),
        "different request with reused ID",
        "fulfillment",
    )?;
    assert_eq!(
        fetch(&runtime, &conflicting).await?["error"],
        "ConfigConflict"
    );
    // Even an old successful pause retry cannot pause a newer generation.
    assert_eq!(fetch(&runtime, &pause).await?, paused);
    let current = fetch(&runtime, "status").await?;
    assert_eq!(current["state"], "active");
    assert_eq!(current["identity"], reset_result["current"]);
    let old_resume = command(
        "00000000000000000000000000000036",
        &status,
        json!({"kind":"resume"}),
        "delayed old command",
        "fulfillment",
    )?;
    assert_eq!(
        fetch(&runtime, &old_resume).await?["error"],
        "SubscriptionReset"
    );
    let required_delete = command(
        "00000000000000000000000000000037",
        &current,
        json!({"kind":"delete","force":true}),
        "cannot remove last required protection",
        "fulfillment",
    )?;
    assert_eq!(
        fetch(&runtime, &required_delete).await?["error"],
        "ConfigConflict"
    );
    let analytics = fetch(&runtime, "status?subscription=analytics").await?;
    let analytics_reset = command(
        "00000000000000000000000000000038",
        &analytics,
        json!({"kind":"reset","cursor":{"resource":binding().resource.id,"partition":0,"next":"6"}}),
        "explicitly resume analytics after lost history",
        "analytics",
    )?;
    assert_eq!(
        fetch(&runtime, &analytics_reset).await?["skipped"],
        json!({"first":"1","last":"5"})
    );
    let analytics_current = fetch(&runtime, "status?subscription=analytics").await?;
    let analytics_fetch = fetch(
        &runtime,
        &next("00000000000000000000000000000039", "analytics"),
    )
    .await?;
    assert!(analytics_fetch.is_null());
    let analytics_packet = fetched_request.lock().unwrap().take().unwrap();
    let delete = command(
        "00000000000000000000000000000040",
        &analytics_current,
        json!({"kind":"delete","force":false}),
        "retire analytics subscription",
        "analytics",
    )?;
    let deleted = fetch(&runtime, &delete).await?;
    assert_eq!(deleted["state"], "deleted");
    assert_eq!(
        fetch(&runtime, "status?subscription=analytics").await?["error"],
        "SubscriptionReset"
    );
    assert_eq!(fetch(&runtime, "recreate").await?["state"], "active");
    let recreated = fetch(&runtime, "status?subscription=analytics").await?;
    assert_ne!(
        recreated["identity"]["id"],
        analytics_current["identity"]["id"]
    );
    assert_eq!(fetch(&runtime, &delete).await?, deleted);
    assert_eq!(
        replay(&runtime, &analytics_packet).await?["error"]["code"],
        "SubscriptionReset"
    );
    assert_eq!(
        fetch(&runtime, "status?subscription=analytics").await?["identity"],
        recreated["identity"]
    );
    let first_page = fetch(&runtime, "audit").await?;
    assert_eq!(first_page["records"].as_array().unwrap().len(), 2);
    assert_eq!(first_page["has_more"], true);
    let second_page = fetch(
        &runtime,
        &format!("audit?after={}", first_page["next"].as_str().unwrap()),
    )
    .await?;
    assert_eq!(second_page["records"][0]["receipt"], reset_result);
    let last_page = fetch(
        &runtime,
        &format!("audit?after={}", second_page["next"].as_str().unwrap()),
    )
    .await?;
    assert_eq!(last_page["records"].as_array().unwrap().len(), 1);
    assert_eq!(last_page["records"][0]["receipt"], deleted);
    assert_eq!(last_page["has_more"], false);
    let overflow = command(
        "00000000000000000000000000000041",
        &recreated,
        json!({"kind":"pause"}),
        "cannot exceed audit capacity",
        "analytics",
    )?;
    assert_eq!(fetch(&runtime, &overflow).await?["error"], "OperationLimit");
    assert_eq!(
        fetch(&runtime, "status?subscription=analytics").await?["state"],
        "active"
    );
    let seal = fetch(&runtime, "seal").await?;
    assert_eq!(seal["state"], "pending");
    assert_eq!(seal["terminal"], "5");
    assert_eq!(fetch(&runtime, "retire").await?["error"], "PendingDelivery");
    assert!(fetch(&runtime, "emit")
        .await
        .unwrap_err()
        .to_string()
        .contains("ProducerSealed"));
    // Reopening the source must retain disabled emission and the seal wake.
    runtime.swap_out_cell(SOURCE, 1).await?;
    runtime
        .restore_cell(
            SOURCE,
            &cellar_logic::RestoreSpec {
                epoch: 1,
                fresh: false,
                took_over: false,
                resume_local: true,
                prior: None,
            },
        )
        .await?;
    runtime.start_cell(SOURCE.into(), 1, false).await?;
    runtime.publish_cell(SOURCE, 1)?;
    assert_eq!(fetch(&runtime, "seal").await?["state"], "pending");
    fire(&runtime, SOURCE, 11).await?;
    assert_eq!(fetch(&runtime, "seal").await?["state"], "accepted");
    let settled = fetch(&runtime, "inspect-settled").await?;
    assert_eq!(settled["pending_rows"], "0");
    assert_eq!(settled["lanes"][0]["status"], "sealed");
    assert_eq!(settled["lanes"][0]["seal_state"], "accepted");
    let seal_packet = sealing_request.lock().unwrap().take().unwrap();
    assert_eq!(replay(&runtime, &seal_packet).await?["ok"]["terminal"], "5");
    assert_eq!(
        replay(&runtime, &recorded).await?["ok"]["accepted_through"],
        "5"
    );
    assert!(fetch(&runtime, "emit")
        .await
        .unwrap_err()
        .to_string()
        .contains("ProducerSealed"));
    assert_eq!(
        fetch(&runtime, "retire?revision=4").await?["error"],
        "ConfigConflict"
    );
    assert_eq!(
        fetch(&runtime, "retire?view=true").await?["error"],
        "Forbidden"
    );
    let retirement = fetch(&runtime, "retire").await?;
    assert_eq!(retirement["state"], "retiring");
    assert!(retirement["last_error"]
        .as_str()
        .unwrap()
        .contains("injected lost retirement response"));
    assert_eq!(fetch(&runtime, "retire").await?["state"], "retiring");
    assert_eq!(
        fetch(&runtime, "new-configure?new=true").await?["error"],
        "ConfigConflict"
    );
    assert_eq!(
        replay(&runtime, &recorded).await?["error"]["code"],
        "DestinationDeleted"
    );
    assert!(fetch(&runtime, "read")
        .await
        .unwrap_err()
        .to_string()
        .contains("DestinationDeleted"));
    let retirement_packet = retirement_request.lock().unwrap().take().unwrap();
    for id in 20..32 {
        if replay(&runtime, &retirement_packet).await?["ok"]["state"] == "deleted" {
            break;
        }
        fire(&runtime, &recorded.scope, id).await?;
        if id == 20 {
            assert_eq!(
                replay(&runtime, &retirement_packet).await?["ok"]["state"],
                "retiring"
            );
            runtime.swap_out_cell(&recorded.scope, 1).await?;
            runtime
                .restore_cell(
                    &recorded.scope,
                    &cellar_logic::RestoreSpec {
                        epoch: 1,
                        fresh: false,
                        took_over: false,
                        resume_local: true,
                        prior: None,
                    },
                )
                .await?;
            runtime.start_cell(recorded.scope.clone(), 1, false).await?;
            runtime.publish_cell(&recorded.scope, 1)?;
        }
    }
    let partition_result = replay(&runtime, &retirement_packet).await?;
    assert_eq!(
        partition_result["ok"]["state"], "deleted",
        "partition retirement: {partition_result}"
    );
    fire(&runtime, &catalog_scope, 13).await?;
    let terminal = fetch(&runtime, "retirement-status").await?;
    assert_eq!(
        terminal["state"], "deleted",
        "catalog retirement: {terminal}"
    );
    assert_eq!(fetch(&runtime, "retire").await?, terminal);
    assert_eq!(
        fetch(&runtime, "new-configure?new=true").await?["error"],
        "Forbidden"
    );
    let mut replacement_graph = event_graph(SCRIPT, vec![new_binding()]);
    let mut replacement_definition = definition(1, true);
    replacement_definition.resource = new_binding().resource;
    replacement_graph.primary.event_streams = vec![replacement_definition];
    let replacement_generation = Generation::build_native(
        2,
        replacement_graph,
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([(SCRIPT.into(), registry.clone())]),
    )?;
    assert_eq!(
        provision(
            &runtime,
            &replacement_generation,
            &new_binding().resource,
            "00000000000000000000000000000071",
            &owned
        )
        .await?["ok"]["state"],
        "active"
    );
    assert_eq!(
        fetch(&runtime, "new-configure?new=true").await?["state"],
        "active"
    );
    assert_eq!(
        fetch(&runtime, "new-revoke-admin?new=true").await?["state"],
        "active"
    );
    // Historical receipt ownership cannot bypass a later grant revocation.
    assert_eq!(
        fetch(&runtime, "new-configure?new=true").await?["error"],
        "Forbidden"
    );
    assert_eq!(
        fetch(&runtime, "new-revoke-admin?new=true").await?["error"],
        "Forbidden"
    );
    assert_eq!(
        fetch(&runtime, "configuration-status?new=true").await?["error"],
        "Forbidden"
    );
    assert_eq!(
        fetch(&runtime, "configuration-status?admin=true").await?["state"],
        "active"
    );
    assert_eq!(fetch(&runtime, "retire").await?, terminal);
    assert_eq!(fetch(&runtime, "configure").await?["state"], "active");
    assert!(fetch(&runtime, "read")
        .await
        .unwrap_err()
        .to_string()
        .contains("DestinationDeleted"));
    assert!(fetch(&runtime, "read?new=true").await?["events"]
        .as_array()
        .unwrap()
        .is_empty());
    fetch(&runtime, "emit?new=true").await?;
    fire(&runtime, SOURCE, 14).await?;
    assert_eq!(
        fetch(&runtime, "read?new=true").await?["events"][0]["sequence"],
        "1"
    );
    assert_eq!(
        replay(&runtime, &recorded).await?["error"]["code"],
        "DestinationDeleted"
    );
    let live = fetch(
        &runtime,
        &format!(
            "{}&new=true",
            next("00000000000000000000000000000073", "fulfillment")
        ),
    )
    .await?;
    let live_receipt = live["receipt"].as_str().unwrap();
    fetch(&runtime, "emit?new=true").await?;
    // Pending delivery must use its stored principal after a script rename
    // removes every local binding. New application emission remains forbidden.
    let renamed = "native-events-renamed";
    let adopted = Generation::build_native(
        2,
        event_graph(renamed, Vec::new()),
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([(renamed.into(), registry.clone())]),
    )?;
    runtime.adopt(adopted);
    reopen_source(&runtime).await?;
    assert!(fetch(&runtime, "emit?new=true")
        .await
        .unwrap_err()
        .to_string()
        .contains("no event stream binding"));
    fire(&runtime, SOURCE, 17).await?;
    let delivered = captured.lock().unwrap().as_ref().unwrap().clone();
    assert_eq!(
        replay(&runtime, &delivered).await?["ok"]["accepted_through"],
        "2"
    );
    let restored = Generation::build_native(
        3,
        event_graph(
            SCRIPT,
            vec![
                binding(),
                view_binding(),
                new_binding(),
                new_admin_binding(),
            ],
        ),
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([(SCRIPT.into(), registry.clone())]),
    )?;
    runtime.adopt(restored);
    reopen_source(&runtime).await?;
    fetch(&runtime, "emit?new=true").await?;
    assert_eq!(
        fetch(
            &runtime,
            "retire?admin=true&revision=2&operation=00000000000000000000000000000072"
        )
        .await?["error"],
        "PendingDelivery"
    );
    let forced = fetch(
        &runtime,
        "retire?admin=true&revision=2&operation=00000000000000000000000000000072&force=true",
    )
    .await?;
    assert_eq!(forced["state"], "retiring");
    assert_eq!(
        fetch(
            &runtime,
            "retirement-status?new=true&operation=00000000000000000000000000000072"
        )
        .await?["error"],
        "Forbidden"
    );
    assert_eq!(
        fetch(
            &runtime,
            "retire?new=true&revision=2&operation=00000000000000000000000000000072&force=true"
        )
        .await?["error"],
        "Forbidden"
    );
    assert_eq!(
        fetch(&runtime, &format!("ack?receipt={live_receipt}&new=true")).await?["error"],
        "DestinationDeleted"
    );
    let forced_packet = retirement_request.lock().unwrap().take().unwrap();
    fire(&runtime, SOURCE, 15).await?;
    let second_scope = captured.lock().unwrap().as_ref().unwrap().scope.clone();
    for id in 32..40 {
        if replay(&runtime, &forced_packet).await?["ok"]["state"] == "deleted" {
            break;
        }
        fire(&runtime, &second_scope, id).await?;
    }
    assert_eq!(
        replay(&runtime, &forced_packet).await?["ok"]["state"],
        "deleted"
    );
    fire(&runtime, &catalog_scope, 16).await?;
    assert_eq!(
        fetch(
            &runtime,
            "retirement-status?admin=true&operation=00000000000000000000000000000072"
        )
        .await?["state"],
        "deleted"
    );
    let scopes = owned.lock().await.iter().cloned().collect::<Vec<_>>();
    for scope in &scopes {
        runtime.swap_out_cell(scope, 1).await?;
    }
    let source = rusqlite::Connection::open(
        directory
            .path()
            .join("cells")
            .join(SOURCE)
            .join("ltx/e1/db.sqlite"),
    )?;
    assert_eq!(
        source.query_row("SELECT count(*) FROM __sys_outbox", [], |row| row
            .get::<_, i64>(0))?,
        1
    );
    assert_eq!(
        source.query_row(
            "SELECT status FROM __sys_outbox_lanes WHERE lane IN (SELECT lane FROM __sys_outbox)",
            [],
            |row| row.get::<_, String>(0)
        )?,
        "blocked"
    );
    assert!(source.query_row("SELECT last_error FROM __sys_outbox_lanes WHERE lane IN (SELECT lane FROM __sys_outbox)", [], |row| row.get::<_, String>(0))?.contains("DestinationDeleted"));
    assert_eq!(
        source.query_row("SELECT state FROM __sys_outbox_seals", [], |row| row
            .get::<_, String>(0))?,
        "accepted"
    );
    assert_eq!(
        source.query_row("SELECT status FROM __sys_outbox_lanes WHERE lane IN (SELECT lane FROM __sys_outbox_seals)", [], |row| row
            .get::<_, String>(0))?,
        "sealed"
    );
    let destination = rusqlite::Connection::open(
        directory
            .path()
            .join("cells")
            .join(&recorded.scope)
            .join("ltx/e1/db.sqlite"),
    )?;
    assert_eq!(
        destination.query_row("SELECT version FROM __sys_stream_schema", [], |row| row
            .get::<_, i64>(0))?,
        1
    );
    assert_eq!(
        destination.query_row("SELECT count(*) FROM __sys_stream_operations", [], |row| {
            row.get::<_, i64>(0)
        })?,
        0
    );
    assert_eq!(
        destination.query_row(
            "SELECT count(*) FROM __sys_stream_subscriptions",
            [],
            |row| row.get::<_, i64>(0)
        )?,
        0
    );
    assert_eq!(
        destination.query_row("SELECT count(*) FROM __sys_stream_producers", [], |row| row
            .get::<_, i64>(0))?,
        0
    );
    assert!(destination
        .query_row(
            "SELECT finished_ms FROM __sys_stream_retirement",
            [],
            |row| row.get::<_, Option<i64>>(0)
        )?
        .is_some());
    assert_eq!(
        destination.query_row("SELECT lifecycle FROM __sys_stream", [], |row| row
            .get::<_, String>(0))?,
        "deleted"
    );
    let catalog = rusqlite::Connection::open(
        directory
            .path()
            .join("cells")
            .join(&catalog_scope)
            .join("ltx/e1/db.sqlite"),
    )?;
    assert_eq!(
        catalog.query_row(
            "SELECT version FROM __sys_event_catalog_schema",
            [],
            |row| row.get::<_, i64>(0)
        )?,
        1
    );
    assert_eq!(catalog.query_row("SELECT count(*) FROM __sys_event_catalog_receipts WHERE operation='00000000000000000000000000000011' AND result IS NOT NULL", [], |row| row.get::<_, i64>(0))?, 1);
    assert_eq!(
        catalog.query_row(
            "SELECT count(*) FROM __sys_event_catalog WHERE lifecycle='deleted'",
            [],
            |row| row.get::<_, i64>(0)
        )?,
        2
    );
    assert_eq!(
        catalog.query_row(
            "SELECT count(*) FROM __sys_event_catalog WHERE lifecycle='active'",
            [],
            |row| row.get::<_, i64>(0)
        )?,
        0
    );
    stop.send(true)?;
    router.await?;
    gate_task.await?;
    Ok(())
}
