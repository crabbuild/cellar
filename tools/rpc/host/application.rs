// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{rpc, *};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

/// Host-side evidence of the actual execution domain. This qualification-only
/// observer records names and method identity, never application message bytes.
#[derive(Clone, Debug)]
pub struct Visit {
    pub node: String,
    pub class: Option<String>,
    pub scope: Option<String>,
    pub epoch: Option<u64>,
    pub method: String,
}

/// Qualification-only observation after a validated typed handler result,
/// before the native host is allowed to release that result through proof.
#[derive(Clone, Debug)]
pub struct Returned {
    pub visit: Visit,
    pub count_matching_storage: Option<String>,
    pub transaction_open: Option<bool>,
    pub write_position: Option<u64>,
}

fn matching_count(
    context: &Context,
    output: &Output,
    contract: &rpc::Contract,
) -> Result<Option<String>> {
    use rpc::prost::Message;
    let Output::Rpc(reply) = output else {
        return Ok(None);
    };
    let Ok(bytes) = rpc::decode_reply(contract.digest(), reply, contract.limits()) else {
        return Ok(None);
    };
    let Ok(reply) = super::generated::IncrementResponse::decode(bytes.as_slice()) else {
        return Ok(None);
    };
    let Some(super::generated::increment_response::Result::Count(count)) = reply.result else {
        return Ok(None);
    };
    let count = count.value.to_string();
    let stored = context.storage()?.get_json("count")?;
    Ok(
        (stored.as_ref().and_then(serde_json::Value::as_str) == Some(count.as_str()))
            .then_some(count),
    )
}

#[derive(Default)]
pub struct Audit(Mutex<Vec<Visit>>, Mutex<Vec<Returned>>, Mutex<Vec<Visit>>);

impl Audit {
    /// Start a sequential case only after all previous native nodes have
    /// joined. No prior case may retain a cursor across this boundary. Keep the
    /// fixed admission bound within each case instead of accumulating every
    /// probe from the combined lifecycle matrices for the whole process.
    pub fn reset_after_node_drain(&self) {
        self.0.lock().unwrap().clear();
        self.1.lock().unwrap().clear();
        self.2.lock().unwrap().clear();
    }

    pub fn cursor(&self) -> usize {
        self.0.lock().unwrap().len()
    }

    pub fn since(&self, cursor: usize) -> Vec<Visit> {
        self.0.lock().unwrap()[cursor..].to_vec()
    }

    pub fn returned_cursor(&self) -> usize {
        self.1.lock().unwrap().len()
    }

    pub fn returned_since(&self, cursor: usize) -> Vec<Returned> {
        self.1.lock().unwrap()[cursor..].to_vec()
    }

    pub fn factories(&self) -> Vec<Visit> {
        self.2.lock().unwrap().clone()
    }

    fn factory(&self, context: &Context) -> Result<()> {
        let mut visits = self.2.lock().unwrap();
        ensure!(
            visits.len() < 4096,
            "qualification factory audit exceeds its bounded case"
        );
        visits.push(Visit {
            node: context
                .var("DOMAIN_MARKER")
                .context("factory has no domain marker")?
                .into(),
            class: context.identity().map(|identity| identity.class.clone()),
            scope: context.identity().map(|identity| identity.scope.clone()),
            epoch: context.identity().map(|identity| identity.epoch),
            method: "create_cell".into(),
        });
        Ok(())
    }

    fn returned(
        &self,
        context: &Context,
        method: &str,
        output: &Output,
        contract: &rpc::Contract,
    ) -> Result<()> {
        let count_matching_storage = if method.ends_with("/Increment") {
            matching_count(context, output, contract)?
        } else {
            None
        };
        let sample = context
            .identity()
            .map(|id| cellar::storage::sync_sample(&id.scope))
            .transpose()?
            .flatten();
        let mut visits = self.1.lock().unwrap();
        ensure!(
            visits.len() < 4096,
            "qualification returned audit exceeds its bounded run"
        );
        visits.push(Returned {
            visit: Visit {
                node: context
                    .var("DOMAIN_MARKER")
                    .context("missing domain marker")?
                    .into(),
                class: context.identity().map(|identity| identity.class.clone()),
                scope: context.identity().map(|identity| identity.scope.clone()),
                epoch: context.identity().map(|identity| identity.epoch),
                method: method.into(),
            },
            count_matching_storage,
            transaction_open: sample.as_ref().map(|sample| sample.in_transaction),
            write_position: sample.map(|sample| sample.position),
        });
        Ok(())
    }

    fn record(&self, context: &Context, method: &str) -> Result<()> {
        let mut visits = self.0.lock().unwrap();
        ensure!(
            visits.len() < 4096,
            "qualification audit exceeds its bounded run"
        );
        visits.push(Visit {
            node: context
                .var("DOMAIN_MARKER")
                .context("missing domain marker")?
                .into(),
            class: context.identity().map(|identity| identity.class.clone()),
            scope: context.identity().map(|identity| identity.scope.clone()),
            epoch: context.identity().map(|identity| identity.epoch),
            method: method.into(),
        });
        Ok(())
    }
}

struct ObservedCell {
    inner: Arc<dyn Cell>,
    audit: Arc<Audit>,
    crash: Arc<super::crash::Gate>,
    contract: Arc<rpc::Contract>,
    cycles: Arc<super::cycles::Tracker>,
}

impl Cell for ObservedCell {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let cycle = self.cycles.enter(&context, &invocation)?;
            if cycle.as_ref().is_some_and(|frame| frame.gate) {
                std::future::pending::<()>().await;
            }
            let returned_method = match &invocation {
                Invocation::CellRpc { method, .. } => Some(method.clone()),
                _ => None,
            };
            if let Invocation::CellRpc { method, .. } = &invocation {
                self.audit.record(&context, method)?;
            }
            let checkpoint = match &invocation {
                Invocation::CellRpc { method, args } if self.crash.armed() => {
                    Some(Invocation::CellRpc {
                        method: method.clone(),
                        args: args.clone(),
                    })
                }
                _ => None,
            };
            if let Some(checkpoint) = &checkpoint {
                if self.crash.boundary(
                    &context,
                    checkpoint,
                    super::crash::Point::BeforeHandler,
                    None,
                )? {
                    std::future::pending::<()>().await;
                }
            }
            let sqlite = self.crash.sqlite_watch(&context, &invocation)?;
            let mut invocation = self.inner.clone().invoke(context.clone(), invocation);
            // Guest SDK operations execute inside this original native poll.
            // The observer follows it across awaits and worker migration; no
            // thread-local authority is left installed between turns.
            let output = std::future::poll_fn(|cx| {
                let _turn = super::crash::sqlite::Turn::enter(sqlite.clone());
                invocation.as_mut().poll(cx)
            })
            .await?;
            if let Some(frame) = cycle.as_ref().filter(|frame| frame.native) {
                frame.wait_native(&context).await?;
            }
            if let Some(method) = returned_method {
                self.audit
                    .returned(&context, &method, &output, &self.contract)?;
            }
            if let Some(checkpoint) = &checkpoint {
                if self.crash.boundary(
                    &context,
                    checkpoint,
                    super::crash::Point::AfterSyncedHandler,
                    Some(&output),
                )? {
                    std::future::pending::<()>().await;
                }
            }
            Ok(output)
        })
    }
}

pub struct Composite {
    pub contract: Arc<rpc::Contract>,
    pub audit: Arc<Audit>,
    pub(super) crash: Arc<super::crash::Gate>,
    pub(super) durability: super::proof::Controls,
    live: Arc<super::proof::Live>,
    cancel_calls: Arc<super::cancel_calls::Calls>,
    pub(super) cycles: Arc<super::cycles::Tracker>,
    provenance: Arc<super::provenance::Native>,
    catalog: rpc::Catalog,
    providers: BTreeMap<String, Arc<dyn Application>>,
    callers: BTreeMap<String, Arc<dyn Application>>,
}

impl Composite {
    pub fn new(
        applications: Vec<(&str, &str, Arc<dyn Application>)>,
        contract: Arc<rpc::Contract>,
        crash: Arc<super::crash::Gate>,
        live: Arc<super::proof::Live>,
        provenance: Arc<super::provenance::Native>,
    ) -> Result<Self> {
        let mut providers = BTreeMap::new();
        let mut callers = BTreeMap::new();
        let mut catalog = rpc::Catalog::new();
        for (caller, class, app) in applications {
            catalog.register(class, "example.counter.v1.Counter", contract.clone())?;
            ensure!(
                providers.insert(class.into(), app.clone()).is_none(),
                "duplicate provider"
            );
            ensure!(
                callers.insert(caller.into(), app).is_none(),
                "duplicate caller"
            );
        }
        Ok(Self {
            contract,
            audit: Arc::default(),
            crash,
            durability: Default::default(),
            live,
            cancel_calls: Default::default(),
            cycles: Default::default(),
            provenance,
            catalog,
            providers,
            callers,
        })
    }
}

impl Application for Composite {
    fn rpc_catalog(&self) -> Option<&rpc::Catalog> {
        Some(&self.catalog)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.providers.contains_key(class)
    }

    fn validate_configuration(
        &self,
        options: &cellar::engine_api::WorkerConfigOptions,
    ) -> Result<()> {
        for (class, app) in &self.providers {
            let mut selected = options.clone();
            selected.do_classes.retain(|candidate| candidate == class);
            app.validate_configuration(&selected)?;
        }
        ensure!(
            options
                .do_classes
                .iter()
                .all(|class| self.supports_cell(class)),
            "unknown RPC class"
        );
        Ok(())
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        self.audit.factory(&context)?;
        let inner = self
            .providers
            .get(&identity.class)
            .context("unregistered RPC class")?
            .create_cell(context, identity)?;
        Ok(Arc::new(ObservedCell {
            inner,
            audit: self.audit.clone(),
            crash: self.crash.clone(),
            contract: self.contract.clone(),
            cycles: self.cycles.clone(),
        }))
    }

    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        let caller = match &event {
            Invocation::Fetch(request) => reqwest::Url::parse(&request.url)
                .ok()
                .and_then(|url| url.path().split('/').nth(1).map(String::from)),
            _ => None,
        };
        if let Err(error) = self.audit.record(
            &context,
            &format!("ingress:{}", caller.as_deref().unwrap_or("unknown")),
        ) {
            return Box::pin(async move { Err(error) });
        }
        if let Some(caller) = caller.filter(|caller| caller != "rust") {
            if let Some(app) = self.callers.get(&caller) {
                return app.clone().invoke(context, event);
            }
        }
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                anyhow::bail!("expected HTTP ingress")
            };
            let url = reqwest::Url::parse(&request.url)?;
            let query: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
            if url.path() == "/rust/probe"
                || url.path() == "/rust/probe-release"
                || url.path() == "/rust/probe-clear"
            {
                let value = match url.path() {
                    "/rust/probe" => self.live.snapshot(),
                    "/rust/probe-release" => self.live.release().await?,
                    _ => self.live.clear()?,
                };
                return Ok(Output::Http(
                    http::Response::from_json(&value)?.into_http(&context)?,
                ));
            }
            if url.path().starts_with("/control/") {
                let value = match url.path() {
                    "/control/arm" => {
                        self.crash.arm(
                            super::crash::Point::parse(
                                query.get("point").context("missing crash point")?,
                            )?,
                            query.get("class").context("missing crash class")?,
                            query.get("id").context("missing crash ID")?,
                        )?;
                        serde_json::json!({"armed":true})
                    }
                    "/control/state" => self.crash.snapshot(),
                    "/control/durability" => self.durability.snapshot(),
                    "/control/data-hold" => {
                        self.durability.hold_data().await;
                        serde_json::json!({"held":true})
                    }
                    "/control/data-open" => {
                        self.durability.open_data();
                        serde_json::json!({"opened":true})
                    }
                    "/control/cell" => {
                        let class = query.get("class").context("missing observed class")?;
                        let visit = self
                            .audit
                            .since(0)
                            .into_iter()
                            .rfind(|visit| visit.class.as_deref() == Some(class.as_str()))
                            .context("class has no native invocation observation")?;
                        serde_json::json!({"node":visit.node,"class":visit.class,
                            "scope":visit.scope,"epoch":visit.epoch})
                    }
                    _ => anyhow::bail!("unknown crash control path"),
                };
                return Ok(Output::Http(
                    http::Response::from_json(&value)?.into_http(&context)?,
                ));
            }
            let target = query.get("target").map(String::as_str).unwrap_or("rust");
            let binding = match target {
                "python" => "PYTHON",
                "v8" => "V8",
                "node" => "NODE",
                _ => "RUST",
            };
            let value = if url.path().ends_with("/provenance-control") {
                self.provenance
                    .control(query.get("action").is_some_and(|action| action == "clear"))
            } else if url.path().ends_with("/cancel-state") {
                self.cancel_calls
                    .snapshot(query.get("name").context("missing pending proof call")?)?
            } else if url.path().ends_with("/cancel-release") {
                self.cancel_calls.release(
                    query.get("name").context("missing pending proof call")?,
                    query.get("phase").is_some_and(|phase| phase == "finish"),
                )?
            } else if url.path().ends_with("/proof-followup") {
                let client =
                    super::generated::CounterClient::bind(&context, "RUST", "cancel-followup")?;
                serde_json::json!({"value":client.get_count(super::generated::GetCountRequest {}).await?.count.context("missing followup count")?.value.to_string()})
            } else if url.path().ends_with("/proof-cancel") {
                self.cancel_calls
                    .increment(
                        &context,
                        (
                            binding,
                            query.get("cell").map(String::as_str).unwrap_or("alice"),
                        ),
                        query.get("id").context("missing proof operation ID")?,
                        query.get("delta").context("missing proof delta")?.parse()?,
                        query
                            .get("trigger")
                            .is_some_and(|trigger| trigger == "deadline"),
                    )
                    .await?
            } else if url.path().ends_with("/preflight") {
                let stub = context.namespace(binding)?.get_by_name("never-construct");
                let args = rpc::encode_envelope(
                    self.contract.digest(),
                    vec![],
                    self.contract.limits().max_message_bytes,
                )?;
                let reply = stub
                    .rpc(&context, "example.counter.v1.Counter/PrivateHelper", args)
                    .await?;
                let error =
                    rpc::decode_reply(self.contract.digest(), &reply, self.contract.limits())
                        .unwrap_err();
                ensure!(
                    error.code() == rpc::Code::MethodNotFound
                        && error.outcome() == rpc::EffectOutcome::NotCommitted
                );
                serde_json::json!({"refused":true})
            } else {
                let counter = super::generated::CounterClient::bind(
                    &context,
                    binding,
                    query.get("cell").map(String::as_str).unwrap_or("alice"),
                )?;
                if url.path().ends_with("/read") {
                    serde_json::json!({"value":counter.get_count(super::generated::GetCountRequest {}).await?.count.context("missing count")?.value.to_string()})
                } else {
                    let reply = counter
                        .increment(super::generated::IncrementRequest {
                            delta: query.get("delta").map(|value| value.parse()).transpose()?,
                            operation_id: query.get("id").cloned().unwrap_or_default(),
                        })
                        .await;
                    match reply {
                        Ok(reply) => match reply.result {
                            Some(super::generated::increment_response::Result::Count(count)) => {
                                serde_json::json!({"value":count.value.to_string()})
                            }
                            Some(super::generated::increment_response::Result::OutOfRange(_)) => {
                                serde_json::json!({"error":"out_of_range"})
                            }
                            Some(
                                super::generated::increment_response::Result::OperationConflict(_),
                            ) => serde_json::json!({"error":"operation_conflict"}),
                            None => anyhow::bail!("missing business result"),
                        },
                        Err(error) => {
                            serde_json::json!({"code":error.code(), "outcome":error.outcome()})
                        }
                    }
                }
            };
            Ok(Output::Http(
                http::Response::from_json(&value)?.into_http(&context)?,
            ))
        })
    }
}
