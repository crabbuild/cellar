//! Reject skipped pairs, weak authority checks and optimistic proof outcomes.
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::collections::BTreeSet;

pub(super) fn validate(cases: &[Value]) -> Result<()> {
    let mut required = BTreeSet::new();
    for trigger in ["signal", "deadline"] {
        for topology in ["local", "remote"] {
            for phase in ["output", "sync"] {
                for (caller, _, _) in super::fleet::CLASSES {
                    for (provider, _, _) in super::fleet::CLASSES {
                        required.insert((trigger, topology, phase, caller, provider));
                    }
                }
            }
        }
    }
    ensure!(
        cases.len() == required.len(),
        "proof wait matrix is incomplete"
    );
    for case in cases {
        let text = |key: &str| case[key].as_str().context("proof wait field missing");
        let (trigger, topology, phase, caller, provider) = (
            text("trigger")?,
            text("topology")?,
            text("phase")?,
            text("caller")?,
            text("provider")?,
        );
        ensure!(
            required.remove(&(trigger, topology, phase, caller, provider)),
            "duplicate or unregistered proof wait case"
        );
        let class = super::fleet::CLASSES
            .iter()
            .find(|(language, _, _)| *language == provider)
            .context("unknown proof provider")?
            .2;
        let source = if topology == "local" {
            "proof-owner"
        } else {
            "proof-ingress"
        };
        ensure!(
            case["ingress"] == source
                && case["owner"] == "proof-owner"
                && case["class"] == class
                && text("scope")?.starts_with(&format!("{class}:"))
                && case["epoch"].as_u64().is_some_and(|epoch| epoch > 0),
            "proof wait has no stable native execution identity"
        );
        let code = if trigger == "deadline" {
            "DeadlineExceeded"
        } else {
            "Cancelled"
        };
        let expected = if caller == "python" && trigger == "signal" {
            json!({"cancelled":true})
        } else {
            json!({"code":code,"outcome":"unknown"})
        };
        ensure!(
            case["result"] == expected && case["settled_while_held"]["result"] == expected,
            "proof wait claimed success, rollback or a different cancellation cause"
        );
        ensure!(
            case["settled_while_held"]["settled"] == true
                && case["settled_while_held"]["followup_writes"] == 0
                && case["final_followup_writes"] == 0
                && case["parent_live_during_cleanup"] == true
                && case["proof_held_through_cleanup"] == true,
            "caller cleanup or continuing authority was not established while proof was held"
        );
        ensure!(
            case["peer_proof_held"]
                .as_u64()
                .is_some_and(|value| value > 0)
                && case["bucket_proof_held"]
                    .as_u64()
                    .is_some_and(|value| value > 0)
                && case["initial_replica_members"] == json!(["proof-ingress"])
                && case["replica_members"] == json!(["proof-ingress", "proof-replica"]),
            "real proof or replica establishment was skipped"
        );
        let number = |key| -> Result<i64> {
            let value = text(key)?;
            let number = value.parse::<i64>()?;
            ensure!(number.to_string() == value, "noncanonical proof wait count");
            Ok(number)
        };
        let baseline = number("baseline_count")?;
        let count = number("expected_count")?;
        ensure!(
            baseline > 0
                && baseline.checked_add(9_007_199_254_740_993) == Some(count)
                && number("local_count")? == count
                && number("final_count")? == count
                && case["receipt_replayed"] == true
                && case["automatic_replays"] == 0
                && case["owner_scope_epoch_verified"] == true,
            "proof wait lost the committed count, receipt or execution identity"
        );
        if phase == "sync" {
            let before = &case["live_before"];
            let after = &case["live_after"];
            ensure!(
                case["handler_returned"] == false
                    && before["ready"] == true
                    && before["committed"] == true
                    && before["active_before"] == true
                    && before["waiting"] == true
                    && before["cancelled"] == false
                    && before["late_done"] == false
                    && after["cancelled"] == true
                    && after["late_done"] == true
                    && after["storage_refused"] == true
                    && after["read_refused"] == true
                    && after["call_refused"] == true,
                "original live handler or its revoked SDK authority was not observed"
            );
            if provider == "rust" {
                ensure!(
                    after["authority_revoked"] == true,
                    "native control scope masked unrevoked authority"
                );
            }
            if provider == "python" {
                ensure!(
                    after["raw_refused"] == true && after["bridge_inactive"] == true,
                    "Python retained native sender authority"
                );
            }
        } else {
            ensure!(
                case["handler_returned"] == true
                    && case["transaction_open"] == false
                    && case["write_position"]
                        .as_u64()
                        .is_some_and(|position| position > 0),
                "output proof was not after the observed SQLite commit"
            );
        }
    }
    ensure!(required.is_empty(), "proof wait omitted a required pair");
    Ok(())
}

#[cfg(test)]
#[path = "wait_evidence_tests.rs"]
mod tests;
