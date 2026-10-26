// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![cfg(any(feature = "node", feature = "embed"))]
#![allow(clippy::disallowed_methods)]
use anyhow::Result;
use cellar_javascript::{EventKind, Options, Source};
use std::time::Duration;
#[path = "../../cellar-container/tests/support/provider_language.rs"]
mod acceptance;
#[path = "../../cellar-container/tests/support/vercel.rs"]
mod peer;

fn source(peer: &peer::Peer) -> Result<Source> {
    let code = include_str!("support/provider.mjs").replace("export ", "");
    Ok(Source::new(
        format!("var CellarApp = (() => {{{code}; return {{cells, fetch}}; }})();"),
        "vercel.mjs",
    )
    .events([EventKind::Fetch])
    .cell("Counter")
    .container_provider(peer.provider()?))
}

#[cfg(feature = "node")]
#[test]
fn node_vercel_provider() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let peer = runtime.block_on(peer::Peer::start())?;
    let source = source(&peer)?;
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("vercel.mjs");
    std::fs::write(&entry, include_str!("support/provider.mjs"))?;
    cellar_javascript::NodeRuntime::run(
        Options {
            turn_timeout: Duration::from_secs(2),
            ..Default::default()
        },
        cellar_javascript::NodeOptions::new(entry),
        source,
        move |application| runtime.block_on(acceptance::exercise(application, peer, "vercel", 4)),
    )
}

#[cfg(feature = "embed")]
#[test]
fn embedded_vercel_provider() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let peer = runtime.block_on(peer::Peer::start())?;
    let source = source(&peer)?;
    cellar_javascript::JavaScriptRuntime::run(
        Options {
            turn_timeout: Duration::from_secs(2),
            ..Default::default()
        },
        source,
        move |application| runtime.block_on(acceptance::exercise(application, peer, "vercel", 4)),
    )
}
