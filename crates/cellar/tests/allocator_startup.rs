// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Check compiled allocator defaults before runtime tuning can mask them.
#![allow(clippy::disallowed_methods)] // Fresh process and threads are the test boundary.

use anyhow::{ensure, Context as _, Result};

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> Result<()> {
    if std::env::args().nth(1).as_deref() == Some("--probe") {
        return probe();
    }
    // Allocator configuration is consumed before main. Remove overrides in a
    // fresh child instead of changing environment after allocation has begun.
    let output = std::process::Command::new(std::env::current_exe()?)
        .arg("--probe")
        .env_remove("_RJEM_MALLOC_CONF")
        .env_remove("MALLOC_CONF")
        .output()?;
    ensure!(
        output.status.success(),
        "allocator startup failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

fn probe() -> Result<()> {
    let configured = tikv_jemalloc_ctl::opt::background_thread::read()
        .map_err(|error| anyhow::anyhow!("read allocator default: {error}"))?;
    ensure!(
        configured == cfg!(target_os = "linux"),
        "compiled allocator background-thread default is {configured} on {}",
        std::env::consts::OS
    );
    if cfg!(target_os = "linux") {
        ensure!(
            tikv_jemalloc_ctl::background_thread::read()
                .map_err(|error| anyhow::anyhow!("read allocator thread state: {error}"))?,
            "Linux allocator background purging did not start"
        );
    }
    // Exercise allocation on another thread and deallocation on the caller,
    // then verify statistics still work after allocator startup.
    let values = std::thread::spawn(|| {
        (0..1024)
            .map(|index| vec![index as u8; 257])
            .collect::<Vec<_>>()
    })
    .join()
    .map_err(|_| anyhow::anyhow!("allocator worker panicked"))?;
    ensure!(values.len() == 1024 && values[17] == vec![17; 257]);
    drop(values);
    let stats = cellar::memory::allocator_stats().context("allocator statistics unavailable")?;
    ensure!(stats.allocated_bytes > 0 && stats.resident_bytes >= stats.allocated_bytes);
    Ok(())
}
