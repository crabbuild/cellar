use anyhow::Result;
use cellar_fleetcare::config::AppConfig;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> Result<()> {
    cellar_fleetcare::embedded::run(AppConfig::from_env()?)
}
