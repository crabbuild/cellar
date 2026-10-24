//! Every pair must refuse both metadata faults before provider construction.
use anyhow::{ensure, Context, Result};
use serde_json::Value;
use std::collections::BTreeSet;

pub(super) fn validate(cases: &[Value]) -> Result<()> {
    let mut required = BTreeSet::new();
    for point in ["missing_root", "replaced_root"] {
        for (caller, _, _) in super::fleet::CLASSES {
            for (provider, _, _) in super::fleet::CLASSES {
                required.insert((point, caller, provider));
            }
        }
    }
    ensure!(
        cases.len() == required.len(),
        "incomplete recovery admission matrix"
    );
    for case in cases {
        let point = case["point"].as_str().context("recovery point missing")?;
        let caller = case["caller"].as_str().context("recovery caller missing")?;
        let provider = case["provider"]
            .as_str()
            .context("recovery provider missing")?;
        ensure!(
            required.remove(&(point, caller, provider)),
            "duplicate or unknown recovery case"
        );
        let (_, _, class) = super::fleet::CLASSES
            .iter()
            .find(|(language, _, _)| *language == provider)
            .context("unknown recovery provider")?;
        ensure!(
            case["scope"]
                .as_str()
                .is_some_and(|scope| scope.starts_with(&format!("{class}:"))),
            "recovery scope belongs to another class"
        );
        let before = case["epoch_before"]
            .as_u64()
            .context("recovery original epoch missing")?;
        ensure!(
            before > 0
                && case["epoch_after"]
                    .as_u64()
                    .is_some_and(|after| after > before),
            "recovery did not advance ownership"
        );
        let number = |key: &str| -> Result<i64> {
            let text = case[key].as_str().context("recovery count missing")?;
            let value = text.parse::<i64>()?;
            ensure!(value.to_string() == text, "recovery count is not canonical");
            Ok(value)
        };
        let baseline = number("baseline_count")?;
        ensure!(
            (1..=32).contains(&baseline.saturating_sub(9_007_199_254_740_993))
                && baseline.checked_add(1) == Some(number("final_count")?),
            "recovery lost acknowledged state or duplicated a mutation"
        );
        ensure!(
            case["factories_before_restore"] == 0
                && case["handlers_before_restore"] == 0
                && case["metadata_fault_reads"]
                    .as_u64()
                    .is_some_and(|count| count > 0),
            "recovery fault was unobserved or reached business code"
        );
        for count in [
            "peer_append_acks",
            "bucket_publications_held",
            "retained_follower_batches",
        ] {
            ensure!(
                case[count].as_u64().is_some_and(|count| count > 0),
                "real retained log proof missing: {count}"
            );
        }
        let failures = case["failures"]
            .as_array()
            .context("recovery refusals missing")?;
        ensure!(
            failures.len() == 2 && case["failure_count"] == 2,
            "live recovery refusal was not repeated"
        );
        for failure in failures {
            ensure!(
                failure["outcome"] == "unknown"
                    && matches!(
                        failure["code"].as_str(),
                        Some(
                            "Unavailable" | "DeadlineExceeded" | "OwnershipLost" | "ContextExpired"
                        )
                    ),
                "recovery refusal claimed success or rollback"
            );
        }
        for flag in [
            "live_successor_preserved",
            "original_root_restored",
            "bucket_proof_preserved",
            "follower_proof_preserved",
            "log_only_acknowledgement_verified",
            "acknowledged_receipt_replayed",
            "pending_receipt_replayed",
            "receipt_conflict_verified",
            "owner_process_group_exited",
            "node_worker_group_exited",
        ] {
            ensure!(case[flag] == true, "recovery evidence missing: {flag}");
        }
        ensure!(
            case["automatic_replays"] == 0,
            "refused mutation automatically replayed"
        );
    }
    ensure!(required.is_empty(), "recovery matrix omitted a pair");
    Ok(())
}

#[cfg(test)]
#[path = "evidence_tests.rs"]
mod tests;
