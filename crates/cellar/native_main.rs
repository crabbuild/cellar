// Copyright 2026 Deno Land Inc. Apache-2.0 license.
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> anyhow::Result<()> {
    cellar::native_deploy::cli::main()
}
