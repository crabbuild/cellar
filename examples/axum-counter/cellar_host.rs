// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

fn main() -> anyhow::Result<()> {
    cellar::sdk::Host::local(cellar_axum_counter::graph())
        .register("axum-counter", "v1", || {
            cellar_axum_counter::definition()?.instantiate()
        })?
        .run()
}
