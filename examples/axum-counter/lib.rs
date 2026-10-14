//! The same application declaration is compiled into both host binaries.
use anyhow::Result;
use axum::{routing::get, Json, Router};
#[cfg(feature = "openapi")]
use cellar_axum::{
    utoipa,
    utoipa_axum::{router::OpenApiRouter, routes},
};
use cellar_axum::{ApplicationBuilder, ApplicationDefinition, CellContext};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Count {
    pub count: u64,
}

type Error = (axum::http::StatusCode, String);

fn error(error: impl std::fmt::Display) -> Error {
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        error.to_string(),
    )
}

#[cfg_attr(feature = "openapi", utoipa::path(get, path = "/counters/{name}", operation_id = "readCounter", responses((status = 200, body = Count), (status = 500, body = String))))]
async fn read(CellContext(context): CellContext) -> Result<Json<Count>, Error> {
    let count = context
        .storage()
        .map_err(error)?
        .get_json("count")
        .map_err(error)?
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    Ok(Json(Count { count }))
}

#[cfg_attr(feature = "openapi", utoipa::path(post, path = "/counters/{name}", operation_id = "incrementCounter", responses((status = 200, body = Count), (status = 500, body = String))))]
async fn increment(context: CellContext) -> Result<Json<Count>, Error> {
    let storage = context.0.storage().map_err(error)?;
    let count = storage
        .get_json("count")
        .map_err(error)?
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    let count = count
        .checked_add(1)
        .ok_or_else(|| error("counter overflow"))?;
    storage
        .put_json("count", &serde_json::json!(count))
        .map_err(error)?;
    read(context).await
}

pub fn definition() -> Result<ApplicationDefinition> {
    let router = Router::new().route("/health", get(|| async { "ready" }));
    #[cfg(feature = "openapi")]
    let router = router
        .merge(
            utoipa_swagger_ui::SwaggerUi::new("/docs")
                .config(utoipa_swagger_ui::Config::new(["/openapi.json"]).validator_url("none")),
        )
        .route(
            "/api",
            get(|| async { axum::response::Redirect::to("/docs/") }),
        );
    let builder = ApplicationBuilder::new(router);
    #[cfg(feature = "openapi")]
    let builder = builder
        .info(utoipa::openapi::Info::new("Cellar Axum Counter", "v1"))
        .server("/")
        .cell_api(
            "Counter",
            "COUNTERS",
            "name",
            OpenApiRouter::new().routes(routes!(read, increment)),
        )?;
    #[cfg(not(feature = "openapi"))]
    let builder = builder
        .cell("Counter", |_, _| {
            Ok(std::sync::Arc::new(cellar_axum::AxumCell::new(
                Router::new().route("/counters/{name}", get(read).post(increment)),
            )))
        })?
        .cell_route("/counters/{name}", "COUNTERS", "Counter", "name")?;
    builder.build()
}

pub fn graph() -> cellar::sdk::DeploymentGraph {
    use cellar::{
        engine_api::{Compat, WorkerConfigOptions},
        fleet::LoadedDeployment,
        sdk::DeploymentGraph,
    };
    DeploymentGraph::single(LoadedDeployment {
        event_streams: Vec::new(),
        native: None,
        options: WorkerConfigOptions {
            stream_bindings: Vec::new(),
            src: String::new(),
            script_name: "axum-counter".into(),
            do_classes: vec!["Counter".into()],
            bindings: vec![("COUNTERS".into(), "Counter".into())],
            blob_bindings: vec![],
            sql_bindings: vec![],
            kv_bindings: vec![],
            queue_bindings: vec![],
            queue_consumers: vec![],
            workflow_bindings: vec![],
            vars: vec![],
            node: String::new(),
            modules: vec![],
            compat: Compat::default(),
        },
        script_name: "axum-counter".into(),
        version: "v1".into(),
        prefix: "axum-counter/v1".into(),
        asset_binding: None,
        loader_bindings: vec![],
        assets: None,
        services: vec![],
        crons: vec![],
        cron_revision: None,
        containers: vec![],
        fence_image: None,
        container_images: vec![],
    })
}
