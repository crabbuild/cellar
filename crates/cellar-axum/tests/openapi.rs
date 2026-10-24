// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![cfg(feature = "openapi")]
mod support;
use anyhow::{ensure, Result};
use axum::{Json, Router};
use cellar_axum::{utoipa, utoipa_axum, ApplicationBuilder, ApplicationDefinition, CellContext};
use serde::{Deserialize, Serialize};
use utoipa_axum::{router::OpenApiRouter, routes};

#[derive(Serialize, Deserialize, utoipa::ToSchema)]
struct Count {
    count: u64,
}

#[utoipa::path(get, path = "/counters/{name}", operation_id = "readCounter",
    responses((status = 200, body = Count), (status = 500, body = String)))]
async fn read(
    CellContext(context): CellContext,
) -> Result<Json<Count>, (axum::http::StatusCode, String)> {
    let result = context
        .storage()
        .and_then(|storage| storage.get_json("count"));
    result
        .map(|count| {
            Json(Count {
                count: count.and_then(|value| value.as_u64()).unwrap_or(0),
            })
        })
        .map_err(|error| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                error.to_string(),
            )
        })
}

fn definition() -> Result<ApplicationDefinition> {
    ApplicationBuilder::new(Router::new())
        .cell_api(
            "Counter",
            "COUNTERS",
            "name",
            OpenApiRouter::new().routes(routes!(read)),
        )?
        .server("/")
        .build()
}

#[test]
fn offline_export_describes_the_same_public_cell_registration() -> Result<()> {
    let definition = definition()?;
    let value: serde_json::Value = serde_json::from_str(&definition.export_openapi()?)?;
    ensure!(value["openapi"] == "3.1.0");
    ensure!(value["paths"]
        .as_object()
        .is_some_and(|paths| paths.len() == 1));
    ensure!(
        value["paths"]["/counters/{name}"]["get"]["operationId"] == "Counter.COUNTERS.readCounter"
    );
    let params = &value["paths"]["/counters/{name}"]["parameters"];
    ensure!(
        params[0]["name"] == "name" && params[0]["in"] == "path" && params[0]["required"] == true
    );
    ensure!(value["components"]["schemas"]["Count"]["properties"]["count"].is_object());
    ensure!(value["paths"]["/peer/probe"].is_null());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn served_document_and_export_agree_under_both_hosts() -> Result<()> {
    let expected: serde_json::Value = serde_json::from_str(&definition()?.export_openapi()?)?;
    let client = support::client()?;
    for external in [false, true] {
        let directory = tempfile::tempdir()?;
        let host = support::start(directory.path(), external, "", definition).await?;
        let response = client
            .get(format!("{}/openapi.json", host.url))
            .send()
            .await?;
        ensure!(response.status() == 200);
        ensure!(response.json::<serde_json::Value>().await? == expected);
        let response = client
            .get(format!("{}/counters/alpha", host.url))
            .send()
            .await?;
        ensure!(response.status() == 200 && response.json::<Count>().await?.count == 0);
        host.stop().await?;
    }
    Ok(())
}

#[test]
fn duplicate_operation_ids_and_conflicting_schemas_are_errors() -> Result<()> {
    let (_, mut api) = OpenApiRouter::<()>::new()
        .routes(routes!(read))
        .split_for_parts();
    let item = api.paths.paths["/counters/{name}"].clone();
    api.paths.paths.insert("/duplicate/{name}".into(), item);
    ensure!(ApplicationBuilder::api(OpenApiRouter::with_openapi(api)).is_err());
    let (_, mut api) = OpenApiRouter::<()>::new()
        .routes(routes!(read))
        .split_for_parts();
    api.paths.paths.clear();
    let count = api
        .components
        .as_mut()
        .and_then(|components| components.schemas.get_mut("Count"));
    ensure!(count.is_some());
    let schema = utoipa::openapi::schema::ObjectBuilder::new()
        .schema_type(utoipa::openapi::schema::Type::String)
        .build();
    api.components
        .as_mut()
        .unwrap()
        .schemas
        .insert("Count".into(), schema.into());
    let builder = ApplicationBuilder::api(OpenApiRouter::with_openapi(api))?;
    ensure!(builder
        .cell_api(
            "Counter",
            "COUNTERS",
            "name",
            OpenApiRouter::new().routes(routes!(read))
        )
        .is_err());
    Ok(())
}

#[test]
fn unresolved_schemas_ambiguous_routes_and_server_declaration_order_are_checked() -> Result<()> {
    let (_, mut api) = OpenApiRouter::<()>::new()
        .routes(routes!(read))
        .split_for_parts();
    api.components.as_mut().unwrap().schemas.clear();
    ensure!(ApplicationBuilder::api(OpenApiRouter::with_openapi(api)).is_err());
    let (_, mut api) = OpenApiRouter::<()>::new()
        .routes(routes!(read))
        .split_for_parts();
    let mut item = api.paths.paths["/counters/{name}"].clone();
    item.get.as_mut().unwrap().operation_id = Some("other".into());
    api.paths.paths.insert("/counters/{id}".into(), item);
    ensure!(ApplicationBuilder::api(OpenApiRouter::with_openapi(api)).is_err());
    let definition = ApplicationBuilder::default()
        .server("/api")
        .cell_api(
            "Counter",
            "COUNTERS",
            "name",
            OpenApiRouter::new().routes(routes!(read)),
        )?
        .build()?;
    ensure!(definition.openapi().unwrap().servers.as_ref().unwrap()[0].url == "/api");
    Ok(())
}

fn versioned_definition(version: &'static str) -> Result<ApplicationDefinition> {
    let router = OpenApiRouter::new().routes(routes!(read));
    ApplicationBuilder::new(Router::new().route(
        "/generation",
        axum::routing::get(move || async move { version }),
    ))
    .info(utoipa::openapi::Info::new("Versioned", version))
    .cell_api("Counter", "COUNTERS", "name", router)?
    .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reload_adopts_handlers_and_the_document_as_one_generation() -> Result<()> {
    for external in [false, true] {
        let directory = tempfile::tempdir()?;
        let mut host = support::start(directory.path(), external, "", || {
            versioned_definition("v1")
        })
        .await?;
        let client = support::client()?;
        let before: serde_json::Value = client
            .get(format!("{}/openapi.json", host.url))
            .send()
            .await?
            .json()
            .await?;
        ensure!(before["info"]["version"] == "v1");
        ensure!(
            client
                .get(format!("{}/generation", host.url))
                .send()
                .await?
                .text()
                .await?
                == "v1"
        );
        let mut graph = support::graph();
        graph.primary.version = "v2".into();
        let applications =
            cellar::sdk::CompiledApplications::default().register("adapter", "v2", || {
                versioned_definition("v2")?.instantiate()
            })?;
        host.node.adopt(graph, applications).await?;
        let after: serde_json::Value = client
            .get(format!("{}/openapi.json", host.url))
            .send()
            .await?
            .json()
            .await?;
        ensure!(after["info"]["version"] == "v2");
        ensure!(
            client
                .get(format!("{}/generation", host.url))
                .send()
                .await?
                .text()
                .await?
                == "v2"
        );
        ensure!(
            client
                .get(format!("{}/counters/alpha", host.url))
                .send()
                .await?
                .status()
                == 200
        );
        host.stop().await?;
    }
    Ok(())
}

fn secured_definition() -> Result<ApplicationDefinition> {
    use axum::middleware::{self, Next};
    let (_, api) = OpenApiRouter::<()>::new()
        .routes(routes!(read))
        .split_for_parts();
    let mut value = serde_json::to_value(api)?;
    value["components"]["securitySchemes"] =
        serde_json::json!({"bearer":{"type":"http","scheme":"bearer"}});
    value["paths"]["/counters/{name}"]["get"]["security"] = serde_json::json!([{"bearer":[]}]);
    value["paths"]["/counters/{name}"]["get"]["responses"]["401"] =
        serde_json::json!({"description":"Bearer token required"});
    let api = serde_json::from_value(value)?;
    Ok(ApplicationBuilder::default()
        .cell_api("Counter", "COUNTERS", "name", {
            let mut router = OpenApiRouter::new().routes(routes!(read));
            *router.get_openapi_mut() = api;
            router
        })?
        .build()?
        .layer(middleware::from_fn(
            |request: axum::extract::Request, next: Next| async move {
                if request
                    .headers()
                    .get("authorization")
                    .is_some_and(|value| value == "Bearer test")
                {
                    next.run(request).await
                } else {
                    axum::response::IntoResponse::into_response((
                        axum::http::StatusCode::UNAUTHORIZED,
                        "Bearer token required",
                    ))
                }
            },
        )))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn security_metadata_matches_enforced_middleware_and_protects_document_routes() -> Result<()>
{
    let directory = tempfile::tempdir()?;
    let host = support::start(directory.path(), true, "", secured_definition).await?;
    let client = support::client()?;
    ensure!(
        client
            .get(format!("{}/counters/alpha", host.url))
            .send()
            .await?
            .status()
            == 401
    );
    ensure!(
        client
            .get(format!("{}/openapi.json", host.url))
            .send()
            .await?
            .status()
            == 401
    );
    let value: serde_json::Value = client
        .get(format!("{}/openapi.json", host.url))
        .header("authorization", "Bearer test")
        .send()
        .await?
        .json()
        .await?;
    ensure!(value["paths"]["/counters/{name}"]["get"]["security"][0]["bearer"].is_array());
    ensure!(value["paths"]["/counters/{name}"]["get"]["responses"]["401"].is_object());
    ensure!(
        client
            .get(format!("{}/counters/alpha", host.url))
            .header("authorization", "Bearer test")
            .send()
            .await?
            .status()
            == 200
    );
    host.stop().await?;
    Ok(())
}
