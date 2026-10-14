use anyhow::Result;
use cellar::sdk::Host;
use cellar_fleetcare::config::{AppConfig, SCRIPT, VERSION};
use std::sync::Arc;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> Result<()> {
    let config = Arc::new(AppConfig::from_env()?);
    let mut graph = cellar_fleetcare::deployment::graph(&config);
    cellar_fleetcare::assets::daemon_bootstrap(&mut graph)?;
    Host::local(graph)
        .register(SCRIPT, VERSION, move || {
            cellar_fleetcare::application(config.clone())
        })?
        .run()
}
