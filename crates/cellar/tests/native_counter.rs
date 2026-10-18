//! The counter recipe keeps reads out of SQLite's writer lane.
#![allow(clippy::disallowed_methods)]
#[allow(dead_code)]
#[path = "../../../examples/counter/main.rs"]
mod counter;
#[allow(dead_code)]
#[path = "support/html.rs"]
mod fixture;
#[allow(dead_code)]
#[path = "../../../examples/support/mod.rs"]
mod support;

use anyhow::Result;
use cellar::engine_api::RuntimeFetch;
use cellar::generation::Generation;
use cellar::runtime::RuntimeManager;
use cellar::sdk::{ExecutionDomain, Registry};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn counter_read_does_not_wait_for_the_capture_writer() -> Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let graph = fixture::graph("counter", "", vec!["Counter".into()]);
            let application = Registry::new(|_, _| Box::pin(async { unreachable!() }))
                .cell("Counter", |_, _| Ok(Arc::new(counter::Counter)))?;
            let generation = Generation::build_native(
                1,
                graph,
                fixture::generation_options(),
                std::collections::HashMap::from([(
                    "counter".into(),
                    Arc::new(application) as Arc<dyn cellar::sdk::Application>,
                )]),
            )?;
            let directory = tempfile::tempdir()?;
            let runtime = RuntimeManager::start(
                generation,
                fixture::runtime_options(directory.path().into()),
            )?;
            let scope = "Counter:read";
            runtime
                .restore_cell(
                    scope,
                    &cellar_logic::RestoreSpec {
                        epoch: 1,
                        fresh: true,
                        took_over: false,
                        resume_local: false,
                        prior: None,
                    },
                )
                .await?;
            runtime.start_cell(scope.into(), 1, true).await?;
            runtime.publish_cell(scope, 1)?;
            let request = |method: &str| RuntimeFetch {
                url: "http://counter/counters/read".into(),
                method: method.into(),
                body: Vec::new().into(),
                headers: Vec::new(),
                request_id: None,
                order: None,
                parent: None,
            };
            let written = runtime
                .fetch_cell(scope.into(), Some("read".into()), request("POST"), None)
                .await?;
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&written.body)?["count"],
                1
            );
            let capture =
                rusqlite::Connection::open(directory.path().join(scope).join("ltx/e1/db.sqlite"))?;
            capture.execute_batch("BEGIN IMMEDIATE")?;
            let read = runtime
                .fetch_cell(scope.into(), Some("read".into()), request("GET"), None)
                .await;
            capture.execute_batch("ROLLBACK")?;
            let read = read?;
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&read.body)?["count"],
                1
            );
            assert!(read.write_position.is_none());
            assert!(
                read.observed_position.is_some(),
                "reads still carry their observed durability position"
            );
            Ok(())
        })
        .await;
    domain.shutdown().await;
    result?
}
