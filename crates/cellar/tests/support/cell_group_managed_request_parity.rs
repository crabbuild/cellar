// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared-group SDK receipt and option parity through both HTTP adapters.
use super::*;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

pub(super) async fn check(
    web: &http::Web,
    client: &reqwest::Client,
    voter: usize,
    cell: &RaftCellHandle,
    retry: u64,
) -> Result<()> {
    let client_bytes = [199; 16];
    let options = CommandOptions::new(CommandId::new(ClientId::new(client_bytes)?, 1)?, retry);
    let increment = cell.contract("Increment")?;
    let original = cell
        .command(&increment, 0i64.to_be_bytes().to_vec(), options.clone())
        .await?;
    ensure!(original.result == 0i64.to_be_bytes() && original.durability == Durability::Quorum);
    let conflict = cell
        .command(&increment, 1i64.to_be_bytes().to_vec(), options)
        .await
        .err()
        .context("SDK accepted a conflicting payload for the original command")?;
    ensure!(conflict.command == original.id && conflict.ambiguous);
    ensure!(matches!(
        conflict.cause.refusal(),
        Some(CommandFailure::CommandConflict)
    ));
    let conflict_body = format!(
        "{:?}; cause={:?}",
        CommandFailure::Ambiguous {
            command: conflict.command
        },
        CommandFailure::CommandConflict,
    );
    let token = original.version_token.as_str();
    let mut forged = URL_SAFE_NO_PAD.decode(token)?;
    *forged.last_mut().context("version signature")? ^= 1;
    let forged = URL_SAFE_NO_PAD.encode(forged);
    CellVersionToken::from_encoded(&forged)?;
    let member = voter as u64 + 1;
    let command_id = format!("{}:1", hex::encode(client_bytes));
    let wrong_digest = hex::encode([99; 32]);
    let large_token = "a".repeat(1900);
    let invalid: &[&[(&str, &str)]] = &[
        &[("x-read-consistency", "at-least")],
        &[("x-min-version", token)],
        &[
            ("x-read-consistency", "linearizable"),
            ("x-min-version", token),
        ],
        &[("x-read-consistency", "unknown")],
        &[("x-read-policy", "unknown")],
        &[
            ("x-read-consistency", "linearizable"),
            ("x-read-policy", "recent-source"),
        ],
        &[("x-query-timeout-ms", "0")],
        &[("x-query-timeout-ms", "5001")],
        &[("x-query-members", "1,1")],
        &[("x-query-members", "0")],
        &[("x-query-members", "01")],
        &[("x-query-members", "1,2,3,4,5,6,7,8,9")],
        &[("x-query-attempt-ms", "10")],
        &[("x-leader-fallback", "true")],
        &[("x-query-members", "1"), ("x-query-attempt-ms", "0")],
        &[
            ("x-query-members", "1"),
            ("x-query-attempt-ms", "1001"),
            ("x-query-timeout-ms", "1000"),
        ],
        &[("x-query-members", "1"), ("x-leader-fallback", "yes")],
        &[
            ("x-read-consistency", "stale"),
            ("x-read-consistency", "linearizable"),
        ],
        &[
            ("x-read-consistency", "at-least"),
            ("x-min-version", &large_token),
        ],
        &[("x-write-durability", "quorum")],
    ];
    for axum in [false, true] {
        let url = web.url(voter, axum, "one");
        let command = |delta| {
            client
                .post(format!("{url}?delta={delta}"))
                .header("x-command-id", &command_id)
                .header("x-retry-until-ms", retry)
        };
        let receipt = http::Web::send(command(0), format!("SDK receipt replay axum={axum}"))
            .await?
            .success()?;
        http::value(&receipt, "one", 0)?;
        ensure!(receipt["served_by"].is_null() && receipt["durability"] == "quorum");
        ensure!(receipt["group"] == hex::encode(original.witness.group.group.bytes()));
        ensure!(
            receipt["incarnation"] == hex::encode(original.version.history.incarnation.bytes())
        );
        ensure!(receipt["index"].as_u64() == Some(original.witness.position.index.get()));
        ensure!(receipt["term"].as_u64() == Some(original.witness.position.term.get()));
        CellVersionToken::from_encoded(
            receipt["version_token"]
                .as_str()
                .context("HTTP receipt token")?,
        )?;

        for mode in ["stale", "at-least", "linearizable"] {
            for routed in [false, true] {
                let mut request = client.get(&url).header("x-read-consistency", mode);
                if mode == "at-least" {
                    request = request.header("x-min-version", token);
                }
                if routed {
                    request = request
                        .header("x-query-members", member)
                        .header("x-query-attempt-ms", "1000")
                        .header("x-query-timeout-ms", "2000")
                        .header("x-leader-fallback", "true");
                }
                let read = http::Web::send(request, format!("{mode} routed={routed} axum={axum}"))
                    .await?
                    .success()?;
                http::value(&read, "one", 0)?;
                ensure!(
                    read["group"] == receipt["group"]
                        && read["incarnation"] == receipt["incarnation"]
                );
                ensure!(read["served_by"].as_u64() == Some(member));
                ensure!(
                    read["index"].as_u64().context("HTTP query index")?
                        >= original.witness.position.index.get()
                );
                CellVersionToken::from_encoded(
                    read["version_token"].as_str().context("HTTP query token")?,
                )?;
            }
        }
        let stale = http::Web::send(
            client
                .get(&url)
                .header("x-read-policy", "disconnected-historical"),
            format!("historical stale query axum={axum}"),
        )
        .await?
        .success()?;
        http::value(&stale, "one", 0)?;
        for headers in invalid {
            let mut request = client.get(&url);
            for (key, value) in *headers {
                request = request.header(*key, *value);
            }
            http::Web::send(
                request,
                format!("invalid read options axum={axum}: {headers:?}"),
            )
            .await?
            .refused(400, "invalid request or unavailable cell")?;
        }
        http::Web::send(
            client
                .get(&url)
                .header("x-read-consistency", "at-least")
                .header("x-min-version", &forged),
            format!("forged token axum={axum}"),
        )
        .await?
        .refused(503, "VersionUnavailable")?;
        http::Web::send(
            client
                .get(&url)
                .header("x-read-consistency", "linearizable")
                .header("x-contract-digest", &wrong_digest),
            format!("wrong query contract axum={axum}"),
        )
        .await?
        .refused(422, "UnsupportedCapability")?;
        http::Web::send(
            command(0).header("x-contract-digest", &wrong_digest),
            format!("wrong command contract axum={axum}"),
        )
        .await?
        .refused(422, "UnsupportedCapability")?;
        http::Web::send(
            command(1),
            format!("conflicting SDK command replay axum={axum}"),
        )
        .await?
        .refused(409, &conflict_body)?;
        http::Web::send(
            command(0).header("x-read-consistency", "linearizable"),
            format!("read options on command axum={axum}"),
        )
        .await?
        .refused(400, "invalid request or unavailable cell")?;
        ensure!(client.get(&url).send().await?.status() == 401);
        ensure!(command(0).send().await?.status() == 401);
    }
    let sdk = cell
        .query(
            ProposalId::new([199; 16])?,
            &cell.contract("Get")?,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::AtLeast(original.version_token),
                ..Default::default()
            },
        )
        .await?;
    ensure!(
        sdk.result == 0i64.to_be_bytes(),
        "HTTP retries changed the SQL image"
    );
    Ok(())
}
