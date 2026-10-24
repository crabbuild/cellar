use anyhow::{ensure, Result};
use cellar::sdk::{NodeOutcome, ShutdownMode};
use serde_json::{json, Value};
use std::{collections::BTreeSet, sync::Arc};

async fn request(client: &reqwest::Client, base: &str, path: &str) -> Result<Value> {
    let response = client.get(format!("{base}{path}")).send().await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(
        status.is_success(),
        "RPC request failed at {path}: {status}: {body}"
    );
    Ok(serde_json::from_str(&body)?)
}

/// The same business/failure corpus runs locally and across authenticated peers.
pub(super) async fn matrix(
    client: &reqwest::Client,
    base: &str,
    round: usize,
) -> Result<BTreeSet<String>> {
    let languages = ["rust", "python", "v8", "node"];
    let large = 9_007_199_254_740_993_i64;
    let mut verified = BTreeSet::new();
    for target in languages {
        let constructors = if target == "rust" {
            None
        } else {
            Some(request(client, base, &format!("/{target}/constructors")).await?)
        };
        ensure!(
            request(client, base, &format!("/rust/preflight?target={target}")).await?
                == json!({"refused":true}),
            "private method was admitted"
        );
        if let Some(before) = constructors {
            ensure!(
                request(client, base, &format!("/{target}/constructors")).await? == before,
                "refusal executed a constructor"
            );
        }
        let current = if round == 0 {
            0
        } else {
            large * 4 + 2 + i64::from(target != "rust")
        };
        ensure!(
            request(client, base, &format!("/rust/read?target={target}")).await?
                == json!({"value":current.to_string()}),
            "restart audit failed for {target}"
        );
        for (index, caller) in languages.into_iter().enumerate() {
            let path =
                format!("/{caller}/increment?target={target}&delta={large}&id={caller}-{target}");
            let saved = (index as i64 + 1) * large;
            for _ in 0..2 {
                let actual = request(client, base, &path).await?;
                ensure!(
                    actual == json!({"value":saved.to_string()}),
                    "{caller} -> {target} round {round}: {actual}"
                );
            }
            verified.insert(format!("{caller}->{target}"));
            ensure!(
                request(
                    client,
                    base,
                    &format!("/{caller}/increment?target={target}&delta=1&id={caller}-{target}")
                )
                .await?
                    == json!({"error":"operation_conflict"}),
                "receipt conflict failed"
            );
        }
        for (caller, id, saved) in [
            ("python", "%EF%BB%BFunicode%F0%9F%99%82", large * 4 + 1),
            ("node", "unicode%F0%9F%99%82", large * 4 + 2),
            ("v8", "%EF%BB%BFunicode%F0%9F%99%82", large * 4 + 1),
        ] {
            ensure!(
                request(
                    client,
                    base,
                    &format!("/{caller}/increment?target={target}&delta=1&id={id}")
                )
                .await?
                    == json!({"value":saved.to_string()}),
                "Unicode receipt identity changed"
            );
        }
        ensure!(
            request(
                client,
                base,
                &format!(
                    "/node/increment?target={target}&delta={}&id=overflow",
                    i64::MAX
                )
            )
            .await?
                == json!({"error":"out_of_range"}),
            "overflow was admitted"
        );
        if target != "rust" {
            let actual = request(
                client,
                base,
                &format!("/rust/increment?target={target}&delta=1&id=invalid-reply"),
            )
            .await?;
            let expected = if round == 0 {
                json!({"code":"InvalidResponse", "outcome":"unknown"})
            } else {
                json!({"value":(large * 4 + 3).to_string()})
            };
            ensure!(
                actual == expected,
                "invalid reply / durable receipt failed for {target}: {actual}"
            );
            request(client, base, &format!("/{target}/capture")).await?;
            ensure!(
                request(client, base, &format!("/{target}/expired")).await?
                    == json!({"code":"ContextExpired", "outcome":"not_committed"}),
                "retained client was usable"
            );
        }
    }
    ensure!(verified.len() == 16, "incomplete backend matrix");
    Ok(verified)
}

pub async fn local(app: Arc<super::application::Composite>) -> Result<Value> {
    let directory = tempfile::tempdir()?;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .build()?;
    let mut verified = BTreeSet::new();
    for round in 0..2 {
        let (host, base) = super::native::start_with(
            directory.path(),
            app.clone(),
            vec![
                "RustCounter".into(),
                "PythonCounter".into(),
                "V8Counter".into(),
                "NodeCounter".into(),
            ],
            vec![
                ("RUST".into(), "RustCounter".into()),
                ("PYTHON".into(), "PythonCounter".into()),
                ("V8".into(), "V8Counter".into()),
                ("NODE".into(), "NodeCounter".into()),
            ],
        )
        .await?;
        verified.extend(matrix(&client, &base, round).await?);
        let NodeOutcome::Stopped(proof) = host.shutdown(ShutdownMode::Preserve).await? else {
            anyhow::bail!("RPC node failed")
        };
        ensure!(
            proof.ingress_drained
                && proof.application_work_drained
                && proof.cells_drained
                && !proof.drain_timed_out,
            "native node did not drain"
        );
    }
    ensure!(verified.len() == 16, "incomplete local backend matrix");
    Ok(
        json!({"format":1, "contract_digest":app.contract.digest(), "topology":"single-node", "pairs":verified, "pair_count":verified.len(),
        "native_node_starts":2, "acknowledged_state_and_receipts_recovered":true,
        "guest_runtime_restarts":0,
        "post_commit_invalid_reply_outcome":"unknown", "unicode_bom_identity_preserved":true,
        "remote_fleet_qualified":false, "abrupt_crash_qualified":false}),
    )
}
