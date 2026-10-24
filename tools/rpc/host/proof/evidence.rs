//! Fail closed if either proof path, any pair, or receipt recovery was skipped.
use anyhow::{ensure, Context, Result};
use serde_json::Value;
use std::collections::BTreeSet;

pub(super) fn validate(cases: &[Value]) -> Result<()> {
    validate_points(cases, &["release", "fail", "fence_output", "fence_sync"])
}

pub(super) fn validate_fences(cases: &[Value]) -> Result<()> {
    validate_points(cases, &["fence_output", "fence_sync"])
}

fn validate_points(cases: &[Value], points: &[&str]) -> Result<()> {
    let mut required = BTreeSet::new();
    for &point in points {
        for (caller, _, _) in super::fleet::CLASSES {
            for (provider, _, _) in super::fleet::CLASSES {
                required.insert((point, caller, provider));
            }
        }
    }
    ensure!(
        cases.len() == required.len(),
        "output proof matrix is incomplete"
    );
    for case in cases {
        let point = case["point"].as_str().context("proof point missing")?;
        let caller = case["caller"].as_str().context("proof caller missing")?;
        let provider = case["provider"]
            .as_str()
            .context("proof provider missing")?;
        ensure!(
            required.remove(&(point, caller, provider)),
            "duplicate or unregistered proof case"
        );
        let (_, _, class) = super::fleet::CLASSES
            .iter()
            .find(|(language, _, _)| *language == provider)
            .context("proof provider missing")?;
        ensure!(
            case["node"] == "proof-owner"
                && case["class"] == *class
                && case["scope"]
                    .as_str()
                    .is_some_and(|scope| scope.starts_with(&format!("{class}:")))
                && case["epoch"].as_u64().is_some_and(|epoch| epoch > 0),
            "proof came from an unproven execution domain"
        );
        if point == "fence_sync" {
            ensure!(
                case["handler_returned"] == false
                    && case["transaction_open"].is_null()
                    && case["write_position"].is_null(),
                "live sync evidence substituted a returned handler or an unobserved SQLite sample"
            );
        } else {
            ensure!(
                case["handler_returned"] == true
                    && case["transaction_open"] == false
                    && case["write_position"]
                        .as_u64()
                        .is_some_and(|position| position > 0),
                "typed handler did not return after a local commit"
            );
        }
        ensure!(
            case["peer_proof_held"]
                .as_u64()
                .is_some_and(|count| count > 0)
                && case["bucket_proof_held"]
                    .as_u64()
                    .is_some_and(|count| count > 0)
                && case["response_escaped_while_held"] == false,
            "real durability proof was not withheld"
        );
        ensure!(
            case["initial_replica_members"] == serde_json::json!(["proof-ingress"])
                && case["replica_members"] == serde_json::json!(["proof-ingress", "proof-replica"]),
            "late replica recruitment was not observed from real acknowledgements"
        );
        let number = |key: &str| -> Result<i64> {
            let text = case[key].as_str().context("proof count missing")?;
            let value = text.parse::<i64>()?;
            ensure!(value.to_string() == text, "proof count is not canonical");
            Ok(value)
        };
        let baseline = number("baseline_count")?;
        let expected = number("expected_count")?;
        ensure!(
            baseline > 0
                && baseline.checked_add(9_007_199_254_740_993) == Some(expected)
                && number("local_count")? == expected
                && number("final_count")? == expected,
            "proof count or receipt recovery is not exact"
        );
        if point == "fence_sync" {
            let before = &case["live_before"];
            let after = &case["live_after"];
            let epoch_text = case["epoch"].as_u64().unwrap().to_string();
            let expected_text = expected.to_string();
            ensure!(
                before["ready"] == true
                    && before["committed"] == true
                    && before["waiting"] == true
                    && before["active_before"] == true
                    && before["cancelled"] == false
                    && before["late_done"] == false,
                "original handler was not live inside its sync await"
            );
            let id = format!("proof-fence-sync-{caller}-{provider}");
            ensure!(
                before["id"] == id
                    && before["node"] == case["node"]
                    && before["class"] == case["class"]
                    && before["scope"] == case["scope"]
                    && before["epoch"].as_str() == Some(epoch_text.as_str())
                    && before["count"].as_str() == Some(expected_text.as_str())
                    && before["receipt"]
                        == serde_json::json!({"delta":"9007199254740993","value":expected.to_string()}),
                "live snapshot did not match the admitted native command and its atomic batch"
            );
            ensure!(
                after["id"] == before["id"]
                    && after["scope"] == before["scope"]
                    && after["epoch"] == before["epoch"]
                    && after["cancelled"] == true
                    && after["waiting"] == false
                    && after["late_done"] == true
                    && after["storage_refused"] == true
                    && after["read_refused"] == true
                    && after["call_refused"] == true,
                "fenced live handler retained SDK authority after controlled resume"
            );
            if provider == "python" {
                ensure!(
                    after["raw_refused"] == true && after["bridge_inactive"] == true,
                    "Python's original native sender was not revoked"
                );
            }
        }
        let recovered = number("count_after_recovery")?;
        ensure!(
            recovered == expected || point != "release" && recovered == baseline,
            "recovery exposed partial or duplicate work"
        );
        if point == "release" {
            ensure!(
                case["result"] == serde_json::json!({"value":expected.to_string()}),
                "released proof did not return the original typed result"
            );
        } else {
            ensure!(
                case["result"]["outcome"] == "unknown"
                    && matches!(
                        case["result"]["code"].as_str(),
                        Some(
                            "Unavailable" | "DeadlineExceeded" | "OwnershipLost" | "ContextExpired"
                        )
                    ),
                "failed proof claimed success or no effects"
            );
        }
        if matches!(point, "fence_output" | "fence_sync") {
            ensure!(
                case["fence"]
                    == serde_json::json!({"code":3,"reason":"NodeLeaseMismatch","domain_joined":true})
                    && case["fenced_new_request_refused"] == true
                    && case["acknowledged_receipt_replayed"] == true,
                "fence or acknowledged receipt evidence is missing"
            );
            let before = case["epoch"].as_u64().context("original epoch missing")?;
            ensure!(
                matches!(
                    case["recovered_node"].as_str(),
                    Some("proof-ingress" | "proof-replica")
                ) && case["recovered_epoch"]
                    .as_u64()
                    .is_some_and(|epoch| epoch > before),
                "fenced RPC did not recover on an advanced surviving owner"
            );
        }
        ensure!(
            case["automatic_replays"] == 0
                && case["receipt_replayed"] == true
                && case["scope_preserved"] == true,
            "proof command was replayed or lost its receipt/scope"
        );
    }
    ensure!(
        required.is_empty(),
        "output proof matrix omitted a required pair"
    );
    Ok(())
}

#[cfg(test)]
#[path = "evidence_tests.rs"]
mod tests;
