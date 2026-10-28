// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Observe the real native owner around independently generated guest handlers.
use super::*;
use std::{collections::BTreeMap, sync::Mutex};

#[derive(Clone, serde::Serialize)]
pub(super) struct Visit {
    pub node: String,
    pub class: Option<String>,
    pub scope: Option<String>,
    pub epoch: Option<u64>,
    pub method: String,
    pub vector: String,
}

#[derive(Default)]
pub(super) struct Audit {
    visits: Mutex<Vec<Visit>>,
    services: Mutex<BTreeMap<String, Arc<cellar::host_services::HostServices>>>,
}

impl Audit {
    fn record(&self, context: &Context, method: &str, vector: &str) -> Result<()> {
        let node = context
            .var("DOMAIN_MARKER")
            .context("missing profile node marker")?
            .to_owned();
        self.services
            .lock()
            .unwrap()
            .entry(node.clone())
            .or_insert_with(cellar::asyncrt::services);
        let mut visits = self.visits.lock().unwrap();
        ensure!(visits.len() < 4096, "profile audit exceeded bounded corpus");
        visits.push(Visit {
            node,
            class: context.identity().map(|identity| identity.class.clone()),
            scope: context.identity().map(|identity| identity.scope.clone()),
            epoch: context.identity().map(|identity| identity.epoch),
            method: method.to_owned(),
            vector: vector.to_owned(),
        });
        Ok(())
    }

    pub fn cursor(&self) -> usize {
        self.visits.lock().unwrap().len()
    }

    pub fn since(&self, cursor: usize) -> Vec<Visit> {
        self.visits.lock().unwrap()[cursor..].to_vec()
    }

    pub fn drained(&self) -> Result<()> {
        for services in self.services.lock().unwrap().values() {
            ensure!(
                services.inbound_rpc_usage() == Default::default()
                    && services.outbound_rpc_usage() == Default::default()
                    && services.native_rpc_decode_usage() == Default::default()
                    && services.rpc_response_usage() == Default::default(),
                "profile retained native admission after node join"
            );
        }
        Ok(())
    }
}

struct Observed {
    inner: Arc<dyn Cell>,
    contract: Arc<rpc::Contract>,
    vectors: Arc<Vec<Vector>>,
    audit: Arc<Audit>,
}

impl Cell for Observed {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { method, args } = &invocation else {
                anyhow::bail!("unexpected profile event")
            };
            let identity = context
                .identity()
                .context("profile business call lacks identity")?;
            let id = identity
                .name
                .as_deref()
                .and_then(|name| name.strip_prefix("profile-"))
                .context("unexpected profile cell name")?;
            let vector = self
                .vectors
                .iter()
                .find(|vector| vector.id == id)
                .context("unknown profile vector")?;
            ensure!(
                method == &format!("{SERVICE}/{}", vector.method),
                "profile call used a different method"
            );
            let (digest, request) =
                rpc::decode_envelope(args, self.contract.limits().max_message_bytes)?;
            ensure!(
                digest == self.contract.digest(),
                "profile input digest changed"
            );
            self.contract.validate_message(&vector.r#type, &request)?;
            native::equivalent(vector, &request)?;
            let method = method.clone();
            let output = self
                .inner
                .clone()
                .invoke(context.clone(), invocation)
                .await?;
            let Output::Rpc(reply) = &output else {
                anyhow::bail!("profile handler did not return RPC")
            };
            let bytes = rpc::decode_reply(self.contract.digest(), reply, self.contract.limits())?;
            self.contract.validate_message(&vector.r#type, &bytes)?;
            native::equivalent(vector, &bytes)?;
            self.audit.record(&context, &method, &vector.id)?;
            Ok(output)
        })
    }
}

pub(super) struct Composite {
    pub audit: Arc<Audit>,
    contract: Arc<rpc::Contract>,
    vectors: Arc<Vec<Vector>>,
    catalog: rpc::Catalog,
    providers: BTreeMap<String, Arc<dyn Application>>,
    callers: BTreeMap<String, Arc<dyn Application>>,
}

impl Composite {
    pub fn contract_digest(&self) -> &str {
        self.contract.digest()
    }

    pub fn new(
        applications: Vec<(&str, &str, Arc<dyn Application>)>,
        contract: Arc<rpc::Contract>,
        vectors: Arc<Vec<Vector>>,
    ) -> Result<Self> {
        let mut providers = BTreeMap::new();
        let mut callers = BTreeMap::new();
        let mut catalog = rpc::Catalog::new();
        for (caller, class, application) in applications {
            catalog.register(class, SERVICE, contract.clone())?;
            ensure!(
                providers
                    .insert(class.to_owned(), application.clone())
                    .is_none(),
                "duplicate profile provider"
            );
            ensure!(
                callers.insert(caller.to_owned(), application).is_none(),
                "duplicate profile caller"
            );
        }
        Ok(Self {
            audit: Arc::default(),
            contract,
            vectors,
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
        for (class, application) in &self.providers {
            let mut selected = options.clone();
            selected.do_classes.retain(|candidate| candidate == class);
            application.validate_configuration(&selected)?;
        }
        ensure!(
            options
                .do_classes
                .iter()
                .all(|class| self.supports_cell(class)),
            "unknown profile class"
        );
        Ok(())
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        let inner = self
            .providers
            .get(&identity.class)
            .context("unknown profile provider")?
            .create_cell(context, identity)?;
        Ok(Arc::new(Observed {
            inner,
            contract: self.contract.clone(),
            vectors: self.vectors.clone(),
            audit: self.audit.clone(),
        }))
    }

    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = &invocation else {
                anyhow::bail!("profile ingress requires HTTP")
            };
            let url = reqwest::Url::parse(&request.url)?;
            let caller = url
                .path()
                .split('/')
                .nth(1)
                .context("missing profile caller")?;
            let query: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
            let vector = self
                .vectors
                .iter()
                .find(|vector| Some(&vector.id) == query.get("vector"))
                .context("unknown profile vector")?;
            self.audit
                .record(&context, &format!("ingress:{caller}"), &vector.id)?;
            if caller != "rust" {
                return self
                    .callers
                    .get(caller)
                    .context("unknown profile caller")?
                    .clone()
                    .invoke(context, invocation)
                    .await;
            }
            let target = query.get("target").context("missing profile target")?;
            let (_, binding, _) = crate::fleet::CLASSES
                .iter()
                .find(|(language, _, _)| language == target)
                .context("unknown profile target")?;
            native::call(&context, binding, vector).await?;
            Ok(Output::Http(
                http::Response::from_json(
                    &serde_json::json!({"id":vector.id,"method":vector.method,"verified":true}),
                )?
                .into_http(&context)?,
            ))
        })
    }
}
