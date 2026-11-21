// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ordinary SDK bindings and Axum application handlers; no consensus dispatch here.
use super::*;
use cellar::sdk::{Cell, CellIdentity, Context, HandlerFuture, Invocation, Output, Registry};
use futures_util::TryStreamExt;
use serde_json::json;
use std::sync::Mutex;
use subtle::ConstantTimeEq;

#[derive(Default)]
pub(super) struct Api {
    pub(super) cells: Mutex<Option<RaftCells>>,
}

impl Api {
    async fn inspect(&self) -> Result<Value> {
        let cells = self
            .cells
            .lock()
            .map_err(|_| anyhow::anyhow!("primitive binding poisoned"))?
            .clone()
            .context("primitive service not installed")?;
        let mut groups = Vec::new();
        for index in 0..2 {
            let history = group(index)?;
            let observed = cells.group(history)?.observe(TIMEOUT).await?;
            groups.push(json!({
                "namespace":hex::encode(history.namespace.bytes()),
                "group":hex::encode(history.group.bytes()),
                "incarnation":hex::encode(history.incarnation.bytes()),
                "member":observed.replica.member.get(),"term":observed.status.term.get(),
                "leader":observed.status.leader,"ready":observed.status.ready,
                "is_leader":observed.role==RaftReplicaRole::Leader,"cells":observed.cells,
            }));
        }
        Ok(json!({"groups":groups,"hosted_groups":cells.resource_usage().host.controller.groups}))
    }
}

pub(super) fn deployment() -> cellar::generation::DeploymentGraph {
    let mut graph = graph::graph("raft-cli", "", vec![]);
    graph.primary.options.do_classes = vec![
        cellar::deploy::SQL_CLASS.into(),
        cellar::deploy::KV_CLASS.into(),
    ];
    graph.primary.options.sql_bindings = vec![
        ("DB_ONE".into(), "daemon/one".into()),
        ("DB_TWO".into(), "daemon/two".into()),
    ];
    graph.primary.options.kv_bindings = vec![
        ("KV_ONE".into(), "daemon/one".into()),
        ("KV_TWO".into(), "daemon/two".into()),
    ];
    graph
}

pub(super) fn configuration() -> Value {
    let options = deployment().primary.options;
    json!({"do_classes":options.do_classes,"sql_bindings":options.sql_bindings,"kv_bindings":options.kv_bindings})
}

struct App {
    native: Arc<Registry>,
    axum: Arc<cellar_axum::AxumApplication>,
}

impl Application for App {
    fn validate_configuration(
        &self,
        options: &cellar::engine_api::WorkerConfigOptions,
    ) -> Result<()> {
        self.native.validate_configuration(options)
    }

    fn validate_deployment(&self, deployment: &cellar::fleet::LoadedDeployment) -> Result<()> {
        self.native.validate_deployment(deployment)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.native.supports_cell(class)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        self.native.create_cell(context, identity)
    }

    fn resolve_raft_class(
        &self,
        class: &str,
    ) -> std::result::Result<Arc<RaftClass>, RegistrationError> {
        self.native.resolve_raft_class(class)
    }

    fn resolve_raft_namespace(
        &self,
        namespace: &NamespaceConfig,
        class: &str,
    ) -> std::result::Result<Arc<RaftClass>, RegistrationError> {
        self.native.resolve_raft_namespace(namespace, class)
    }

    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        match &invocation {
            Invocation::Fetch(request)
                if request
                    .url
                    .parse::<axum::http::Uri>()
                    .is_ok_and(|uri| uri.path().starts_with("/axum/")) =>
            {
                self.axum.clone().invoke(context, invocation)
            }
            _ => self.native.clone().invoke(context, invocation),
        }
    }
}

pub(super) fn application() -> Result<Arc<dyn Application>> {
    application_with_api(Arc::new(Api::default()))
}

pub(super) fn application_with_api(api: Arc<Api>) -> Result<Arc<dyn Application>> {
    let native = Arc::new(
        Registry::new(move |context, invocation| {
            let api = api.clone();
            Box::pin(native_request(context, invocation, api))
        })
        .raft_primitives()?,
    );
    let router = axum::Router::new().fallback(axum_request);
    let axum = Arc::new(cellar_axum::AxumApplication::with_application(
        router,
        native.clone(),
    ));
    Ok(Arc::new(App { native, axum }))
}

fn authorized(headers: &[(String, String)]) -> bool {
    let mut values = headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("authorization"));
    let Some((_, value)) = values.next() else {
        return false;
    };
    values.next().is_none()
        && bool::from(
            value
                .as_bytes()
                .ct_eq(format!("Bearer {SECRET}").as_bytes()),
        )
}

async fn native_request(context: Context, invocation: Invocation, api: Arc<Api>) -> Result<Output> {
    let Invocation::Fetch(request) = invocation else {
        anyhow::bail!("primitive HTTP invocation required")
    };
    let result = if !authorized(&request.headers) {
        (401, json!({"error":"unauthorized"}))
    } else if request.method == "GET"
        && request.url.parse::<axum::http::Uri>()?.path() == "/inspect"
    {
        reply(api.inspect().await)
    } else {
        let input = context
            .take_body(request.body)?
            .try_concat()
            .await
            .map_err(anyhow::Error::msg)?;
        reply(execute(&context, &request.method, &request.url, &input).await)
    };
    Ok(Output::Http(cellar::engine_api::HttpResponse {
        status: result.0,
        headers: vec![("content-type".into(), "application/json".into())],
        body: serde_json::to_vec(&result.1)?,
        stream: None,
        websocket: None,
        write_position: None,
        observed_position: None,
    }))
}

async fn axum_request(
    cellar_axum::CellContext(context): cellar_axum::CellContext,
    method: axum::http::Method,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> (axum::http::StatusCode, axum::Json<Value>) {
    let headers = headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.to_string(), value.to_owned()))
        })
        .collect::<Vec<_>>();
    let (status, body) = if authorized(&headers) {
        reply(execute(&context, method.as_str(), uri.path(), &body).await)
    } else {
        (401, json!({"error":"unauthorized"}))
    };
    (
        axum::http::StatusCode::from_u16(status).expect("fixture status"),
        axum::Json(body),
    )
}

fn reply(result: Result<Value>) -> (u16, Value) {
    match result {
        Ok(value) => (200, value),
        Err(error) => (503, json!({"error":format!("{error:#}")})),
    }
}

async fn execute(context: &Context, method: &str, url: &str, bytes: &[u8]) -> Result<Value> {
    ensure!(
        context.identity().is_none(),
        "bindings require a stateless HTTP entrypoint"
    );
    ensure!(bytes.len() <= 16 * 1024, "primitive request body limit");
    let uri = url.parse::<axum::http::Uri>()?;
    let parts = uri
        .path()
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    let [adapter, kind, name, operation] = parts.as_slice() else {
        anyhow::bail!("primitive route")
    };
    ensure!(
        matches!(*adapter, "native" | "axum") && matches!(*name, "one" | "two"),
        "primitive route scope"
    );
    ensure!(
        method == if *operation == "read" { "GET" } else { "POST" },
        "primitive HTTP method"
    );
    let input: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(bytes)?
    };
    match *kind {
        "sql" => {
            let database = context.sql(if *name == "one" { "DB_ONE" } else { "DB_TWO" })?;
            match *operation {
                "setup" => {
                    database
                        .migrate(
                            context,
                            "initial",
                            "CREATE TABLE entries(id INTEGER PRIMARY KEY,v TEXT NOT NULL);",
                            "__sys_sql_migrations",
                        )
                        .await?;
                    Ok(json!(true))
                }
                "write" => {
                    let id = input["id"].as_i64().context("SQL row ID")?;
                    let value = input["value"].as_str().context("SQL value")?;
                    let result = database.prepare("INSERT INTO entries VALUES(?,?) ON CONFLICT(id) DO UPDATE SET v=excluded.v")
                        .bind_json([json!(id),json!(value)])?.run(context).await?;
                    ensure!(
                        result.success && result.meta.changes == 1,
                        "SQL write result"
                    );
                    Ok(json!(true))
                }
                "read" => {
                    let session = database.with_session(Some("first-primary"))?;
                    let rows = session
                        .prepare("SELECT id,v FROM entries ORDER BY id")
                        .raw(context, false)
                        .await?;
                    ensure!(
                        session.get_bookmark().as_deref() == Some("cellar:primary"),
                        "SQL primary session bookmark"
                    );
                    Ok(json!(rows))
                }
                "rollback" => {
                    let failure = database
                        .batch(
                            context,
                            &[
                                database.prepare("INSERT INTO entries VALUES(99,'must-roll-back')"),
                                database.prepare("INSERT INTO entries VALUES(1,'duplicate')"),
                            ],
                        )
                        .await
                        .err()
                        .context("SQL batch unexpectedly committed")?;
                    ensure!(
                        failure.to_string().contains("UNIQUE constraint"),
                        "SQL rollback failure: {failure}"
                    );
                    Ok(json!(true))
                }
                _ => anyhow::bail!("unknown SQL operation"),
            }
        }
        "kv" => {
            let namespace = context.kv(if *name == "one" { "KV_ONE" } else { "KV_TWO" })?;
            match *operation {
                "write" => {
                    namespace
                        .put_text(
                            context,
                            "entry",
                            input["value"].as_str().context("KV value")?,
                            Default::default(),
                        )
                        .await?;
                    Ok(json!(true))
                }
                "read" => {
                    let value = namespace
                        .get(context, "entry", cellar::sdk::kv::ReadType::Text)
                        .await?;
                    match value {
                        Some(cellar::sdk::kv::ReadValue::Text(value)) => Ok(json!(value)),
                        None => Ok(Value::Null),
                        _ => anyhow::bail!("KV reply type"),
                    }
                }
                _ => anyhow::bail!("unknown KV operation"),
            }
        }
        _ => anyhow::bail!("unknown primitive family"),
    }
}
