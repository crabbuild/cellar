// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Recovery audits hash actual stored bytes without entering the write lane.
#![allow(clippy::disallowed_methods)]
#[allow(dead_code)]
#[path = "../../crates/cellar/tests/support/html.rs"]
mod fixture;

use super::*;
use cellar::engine_api::RuntimeFetch;
use cellar::generation::Generation;
use cellar::runtime::RuntimeManager;
use cellar::sdk::ExecutionDomain;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn payload_audit_distinguishes_same_length_values_without_mutating_state() -> Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let generation = Generation::build_native(
                1,
                deployment::graph(),
                fixture::generation_options(),
                std::collections::HashMap::from([(
                    "perf-cells".into(),
                    application("node".into())?,
                )]),
            )?;
            let directory = tempfile::tempdir()?;
            let runtime = RuntimeManager::start(
                generation,
                fixture::runtime_options(directory.path().into()),
            )?;
            let scope = "Entity:audit";
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
            let request = |method: &str, body: &str, audit: bool| RuntimeFetch {
                url: "http://counter/cells/audit".into(),
                method: method.into(),
                body: body.as_bytes().to_vec().into(),
                headers: if audit {
                    vec![("X-Cellar-Verify-Payload".into(), "sha256".into())]
                } else {
                    Vec::new()
                },
                request_id: None,
                order: None,
                parent: None,
            };
            // Known SHA-256 vectors, including equal-length values and UTF-8 bytes.
            for (writes, (payload, digest)) in [
                (
                    "",
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                ),
                (
                    "hello",
                    "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                ),
                (
                    "jello",
                    "187c9bceeb919e1b3e6d20fa50ecabf7d9d50b5343e8f9a3d912abb13929102e",
                ),
                (
                    "café ☕",
                    "a7e46d54289812af2aa5b08c2fbab5d24bccfc6586df55b187272c8a2a31c85f",
                ),
            ]
            .into_iter()
            .enumerate()
            {
                if writes != 0 {
                    let written = runtime
                        .fetch_cell(
                            scope.into(),
                            Some("audit".into()),
                            request("POST", payload, false),
                            None,
                        )
                        .await?;
                    let written: serde_json::Value = serde_json::from_slice(&written.body)?;
                    assert_eq!(written["writes"], writes);
                    assert!(written.get("payload_sha256").is_none());
                }
                let ordinary = runtime
                    .fetch_cell(
                        scope.into(),
                        Some("audit".into()),
                        request("GET", "", false),
                        None,
                    )
                    .await?;
                let ordinary: serde_json::Value = serde_json::from_slice(&ordinary.body)?;
                assert!(ordinary.get("payload_sha256").is_none());
                // Initialize lazy storage before taking the external lock. A reader
                // must still work while an external capture writer owns
                // SQLite's writer lane, and its response must carry no new write.
                let capture = rusqlite::Connection::open(
                    directory.path().join(scope).join("ltx/e1/db.sqlite"),
                )?;
                capture.execute_batch("BEGIN IMMEDIATE")?;
                let read = runtime
                    .fetch_cell(
                        scope.into(),
                        Some("audit".into()),
                        request("GET", "", true),
                        None,
                    )
                    .await;
                capture.execute_batch("ROLLBACK")?;
                let read = read?;
                assert!(read.write_position.is_none());
                let value: serde_json::Value = serde_json::from_slice(&read.body)?;
                assert_eq!(value["writes"], writes);
                assert_eq!(value["payload_bytes"], payload.len());
                assert_eq!(value["payload_sha256"], digest);
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result?
}
