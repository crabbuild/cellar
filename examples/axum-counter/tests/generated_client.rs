// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#[path = "../generated_client.rs"]
mod client;
#[path = "../../../crates/cellar-axum/tests/support/mod.rs"]
mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_client_uses_the_exported_counter_contract_on_both_hosts() -> anyhow::Result<()> {
    for external in [false, true] {
        let directory = tempfile::tempdir()?;
        let host = support::start(
            directory.path(),
            external,
            "",
            cellar_axum_counter::definition,
        )
        .await?;
        let http = support::client()?;
        assert_eq!(
            client::increment_counter(&http, &host.url, "snow day")
                .await?
                .count,
            1
        );
        assert_eq!(
            client::read_counter(&http, &host.url, "snow day")
                .await?
                .count,
            1
        );
        host.stop().await?;
    }
    Ok(())
}
