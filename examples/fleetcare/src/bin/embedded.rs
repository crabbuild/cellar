use anyhow::Result;
use fleetcare::config::AppConfig;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> Result<()> {
    fleetcare::embedded::run(AppConfig::from_env()?)
}
