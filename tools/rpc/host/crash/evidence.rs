// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fail closed when aggregating independently killed owner processes.
use anyhow::{ensure, Context, Result};
use serde_json::Value;
use std::collections::BTreeSet;

pub(super) fn validate(cases: &[Value]) -> Result<()> {
    let languages = ["rust", "python", "v8", "node"];
    let points = [
        "before_handler",
        "after_synced_handler",
        "after_count_before_receipt",
    ];
    let mut required = BTreeSet::new();
    for point in points {
        for caller in languages {
            for provider in languages {
                required.insert((point, caller, provider));
            }
        }
    }
    ensure!(
        cases.len() == required.len(),
        "crash case count is incomplete"
    );
    for case in cases {
        let point = case["point"].as_str().context("missing crash point")?;
        let caller = case["caller"].as_str().context("missing crash caller")?;
        let provider = case["provider"]
            .as_str()
            .context("missing crash provider")?;
        ensure!(
            required.remove(&(point, caller, provider)),
            "duplicate or unknown crash case"
        );
        ensure!(
            case["owner_signal"] == "SIGKILL" && case["owner_process_group_exited"] == true,
            "owner process loss was not verified"
        );
        ensure!(
            case["first_failure"]["outcome"] == "unknown"
                && matches!(
                    case["first_failure"]["code"].as_str(),
                    Some("Unavailable" | "DeadlineExceeded" | "OwnershipLost" | "ContextExpired")
                ),
            "crash failure is not conservatively classified"
        );
        ensure!(
            case["final_count"] == "9007199254740993"
                && case["duplicate_mutations"] == 0
                && case["scope_preserved"] == true
                && case["receipt_replayed"] == true,
            "crash receipt recovery was not verified"
        );
        let before = case["epoch_before"]
            .as_u64()
            .context("missing original epoch")?;
        let after = case["epoch_after"]
            .as_u64()
            .context("missing recovered epoch")?;
        ensure!(after > before, "crash recovery did not advance its epoch");
        let partial = point == "after_count_before_receipt";
        let committed = point == "after_synced_handler";
        ensure!(
            case["transaction_open"] == partial,
            "crash has no matching transaction observation"
        );
        ensure!(
            case["count_after_recovery"] == if committed { "9007199254740993" } else { "0" },
            "recovery exposed an uncommitted count or lost a committed one"
        );
        let receipt = case
            .get("receipt_at_crash")
            .context("crash has no receipt observation")?;
        ensure!(
            if committed {
                receipt
                    == &serde_json::json!({"delta":"9007199254740993","value":"9007199254740993"})
            } else {
                receipt.is_null()
            },
            "crash receipt observation differs from its boundary"
        );
        if point == "before_handler" {
            ensure!(
                case["count_at_crash"] == "0" && case["typed_reply_held"] == false,
                "pre-handler case has post-handler evidence"
            );
        } else if partial {
            ensure!(
                case["count_at_crash"] == "9007199254740993" && case["typed_reply_held"] == false,
                "partial transaction case lacks its first write or already has a typed reply"
            );
            if provider == "rust" {
                ensure!(
                    case["transaction_observer"] == "native_callback",
                    "native transaction checkpoint observer is missing"
                );
            } else {
                let sqlite = &case["sqlite_checkpoint"];
                let before = sqlite["total_changes_before"]
                    .as_i64()
                    .context("SQLite pre-write changes missing")?;
                ensure!(
                    case["transaction_observer"] == "sqlite_trace_profile"
                        && sqlite["statement_finished"] == true
                        && sqlite["receipt_read_rows"] == 0
                        && sqlite["count_changes"] == 1
                        && before >= 0
                        && sqlite["total_changes_after"].as_i64() == before.checked_add(1),
                    "guest transaction has no completed SQLite write/absent-receipt observation"
                );
            }
        } else {
            ensure!(
                case["count_at_crash"] == "9007199254740993" && case["typed_reply_held"] == true,
                "post-commit case has no synced state and held typed reply"
            );
        }
    }
    ensure!(
        required.is_empty(),
        "crash matrix did not cover every point and pair"
    );
    Ok(())
}

#[cfg(test)]
#[path = "evidence_tests.rs"]
mod tests;
