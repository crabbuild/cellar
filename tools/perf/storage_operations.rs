// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Local SQLite operation costs; excludes routing, replication and output proofs.
#![allow(clippy::disallowed_methods)]

use anyhow::Result;
use cellar::storage;
use serde_json::json;
use std::hint::black_box;
use std::time::Instant;

fn main() -> Result<()> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    anyhow::ensure!(
        arguments.len() <= 2,
        "usage: cellar-perf-storage [iterations] [repetitions]"
    );
    let iterations = arguments
        .first()
        .map(|n| n.parse::<u64>())
        .transpose()?
        .unwrap_or(5_000);
    let repetitions = arguments
        .get(1)
        .map(|n| n.parse::<u64>())
        .transpose()?
        .unwrap_or(5);
    anyhow::ensure!(iterations > 0 && repetitions > 0, "counts must be positive");
    let cells = storage::Cells::default();
    let _installed = cells.install();
    let directory = tempfile::tempdir()?;
    let scope = "Storage:benchmark";
    storage::open(
        scope,
        directory.path().join("cell.sqlite").to_str().unwrap(),
    )?;
    storage::set_alarm(scope, 1)?;
    let sql = storage::SqlRequest::Prepared {
        sql: "SELECT ?1 AS value".into(),
        params: vec![json!(42)],
        first: true,
    };
    for repetition in 0..repetitions {
        let mut cases = vec![
            "bulk-root",
            "bulk-nested",
            "clone-delete",
            "sql-read",
            "alarm-read",
        ];
        if repetition % 2 != 0 {
            cases.reverse();
        }
        for case in cases {
            let nested = case == "bulk-nested";
            if nested {
                storage::transaction_control(scope, "start", false, "")
                    .map_err(anyhow::Error::msg)?;
            }
            let entries = (0..4)
                .map(|i| (format!("key-{i}"), json!(i)))
                .collect::<Vec<_>>();
            let clones = vec![
                ("clone-a".into(), vec![1; 64]),
                ("clone-b".into(), vec![2; 64]),
            ];
            let keys = vec!["clone-a".into(), "clone-b".into()];
            let operation = || -> Result<()> {
                match case {
                    "bulk-root" | "bulk-nested" => {
                        storage::put_many_json(scope, &entries)?;
                        anyhow::ensure!(
                            matches!(storage::get_stored(scope, "key-3")?, Some(storage::StoredValue::Json(value)) if value == "3")
                        );
                    }
                    "clone-delete" => {
                        storage::put_many_serialized(scope, &clones)?;
                        anyhow::ensure!(storage::delete_many(scope, &keys)? == 2);
                    }
                    "sql-read" => {
                        let result = storage::sql_run(scope, &sql)
                            .map_err(|e| anyhow::anyhow!(e.message))?;
                        let storage::SqlResponse::Prepared(result) = result else {
                            anyhow::bail!("wrong SQL response");
                        };
                        anyhow::ensure!(
                            result.rows == vec![vec![json!(42)]]
                                && !result.meta.changed_db
                                && result.meta.size_after > 0
                        );
                        black_box(result);
                    }
                    "alarm-read" => {
                        anyhow::ensure!(black_box(storage::get_alarm(scope)) == Some(1));
                    }
                    _ => unreachable!(),
                }
                Ok(())
            };
            for _ in 0..100 {
                operation()?;
            }
            let started = Instant::now();
            for _ in 0..iterations {
                operation()?;
            }
            let elapsed = started.elapsed();
            if nested {
                storage::transaction_control(scope, "rollback", false, "")
                    .map_err(anyhow::Error::msg)?;
            }
            cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(format_args!("{}", json!({"case":case,"repetition":repetition,"iterations":iterations,"elapsed_ns":elapsed.as_nanos(),"ns_per_operation":elapsed.as_nanos() as f64 / iterations as f64})))?;
        }
    }
    storage::close(scope);
    Ok(())
}
