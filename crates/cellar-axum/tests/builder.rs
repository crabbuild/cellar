use anyhow::{ensure, Result};
use axum::Router;
use cellar_axum::{ApplicationBuilder, AxumCell};
use std::sync::Arc;

#[test]
fn builder_rejects_missing_factories_duplicate_classes_and_ambiguous_paths() -> Result<()> {
    ensure!(ApplicationBuilder::default()
        .cell_route("/counters/{name}", "COUNTERS", "Counter", "name")?
        .build()
        .is_err());
    let builder = ApplicationBuilder::default()
        .cell("Counter", |_, _| Ok(Arc::new(AxumCell::new(Router::new()))))?;
    ensure!(builder
        .cell("Counter", |_, _| Ok(Arc::new(AxumCell::new(Router::new()))))
        .is_err());
    let builder = ApplicationBuilder::default().cell_route(
        "/counters/{name}",
        "COUNTERS",
        "Counter",
        "name",
    )?;
    ensure!(builder
        .cell_route("/counters/{id}", "OTHER", "Other", "id")
        .is_err());
    for path in [
        "no-leading-slash",
        "/counters/:name",
        "/{*name}",
        "/x{name}",
        "/{name}/{name}",
    ] {
        ensure!(ApplicationBuilder::default()
            .cell_route(path, "COUNTERS", "Counter", "name")
            .is_err());
    }
    ensure!(ApplicationBuilder::default()
        .entrypoint("default", Router::new())
        .is_err());
    Ok(())
}
