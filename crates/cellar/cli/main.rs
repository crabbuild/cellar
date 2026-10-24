// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> anyhow::Result<()> {
    cellar::native_deploy::cli::main()
}
