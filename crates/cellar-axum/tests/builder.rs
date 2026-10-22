use anyhow::{ensure, Result};
use axum::Router;
use cellar_axum::{ApplicationBuilder, AxumCell};
use std::sync::Arc;

#[test]
fn raft_definitions_delegate_without_a_mutable_factory() -> Result<()> {
    use cellar::sdk::RaftClass;
    let definition = ApplicationBuilder::default()
        .raft_class(RaftClass::new("Counter", 1, [7; 32])?.command(
            "Increment",
            [8; 32],
            |_, _| Ok(vec![]),
        )?)?
        .build()?;
    let application = definition.instantiate()?;
    ensure!(!application.supports_cell("Counter"));
    ensure!(application
        .resolve_raft_class("Counter")?
        .contract("Increment")
        .is_some());
    ensure!(ApplicationBuilder::default()
        .raft_class(RaftClass::new("Counter", 1, [7; 32])?)?
        .cell("Counter", |_, _| Ok(Arc::new(AxumCell::new(Router::new()))))
        .is_err());
    ensure!(ApplicationBuilder::default()
        .cell("Counter", |_, _| Ok(Arc::new(AxumCell::new(Router::new()))))?
        .raft_class(RaftClass::new("Counter", 1, [7; 32])?)
        .is_err());
    Ok(())
}

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
