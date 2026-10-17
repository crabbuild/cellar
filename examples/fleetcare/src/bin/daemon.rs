use anyhow::Result;
use cellar::sdk::Host;
use fleetcare::config::{AppConfig, SCRIPT, VERSION};
use std::sync::Arc;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> Result<()> {
    let config = Arc::new(AppConfig::from_env()?);
    let mut graph = fleetcare::deployment::graph(&config);
    fleetcare::assets::daemon_bootstrap(&mut graph)?;
    Host::local(graph)
        .register(SCRIPT, VERSION, move || {
            fleetcare::application(config.clone())
        })?
        .run()
}
