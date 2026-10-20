//! Real ingress failures retain neither malformed values nor source paths.
use super::*;

pub(super) fn child() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let report = run(
        Options::default(),
        Source::new(
            include_str!("privacy.py"),
            "PRIVATE-UNMANAGED-EXCEPTION-SENTINEL.py",
        )
        .events([EventKind::Fetch])
        .cell("Counter"),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let (node, url) = start(directory.path(), application).await?;
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(5))
                    .build()?;
                for path in ["headers", "header-value", "body", "payload", "json"] {
                    let response = client
                        .post(format!("{url}/{path}"))
                        .body("PRIVATE-UNMANAGED-EXCEPTION-SENTINEL")
                        .send()
                        .await?;
                    ensure!(
                        response.status().is_server_error(),
                        "invalid handler output succeeded: {path}"
                    );
                    ensure!(
                        !format!("{:?}", response.headers())
                            .contains("PRIVATE-UNMANAGED-EXCEPTION-SENTINEL"),
                        "handler output leaked into diagnostic headers: {path}"
                    );
                    let body = response.text().await?;
                    ensure!(
                        !body.contains("PRIVATE-UNMANAGED-EXCEPTION-SENTINEL"),
                        "handler output leaked into a public diagnostic: {path}"
                    );
                    ensure!(body.len() <= 512, "unbounded handler diagnostic: {path}");
                    ensure!(
                        client
                            .get(format!("{url}/healthy"))
                            .send()
                            .await?
                            .error_for_status()?
                            .json::<bool>()
                            .await?
                    );
                    let failure: Value = client
                        .post(format!("{url}/nested/{path}"))
                        .body("PRIVATE-UNMANAGED-EXCEPTION-SENTINEL")
                        .send()
                        .await?
                        .error_for_status()?
                        .json()
                        .await?;
                    let expected = if path == "header-value" {
                        json!({"status":200, "private_header":true})
                    } else {
                        json!({"code":"Ambiguous", "outcome":"unknown"})
                    };
                    ensure!(
                        failure == expected,
                        "cell response/error contract changed: {path}"
                    );
                }
                // Each nested call writes once, including calls with rejected
                // responses. Redaction cannot imply rollback or cause replay.
                ensure!(
                    client
                        .get(format!("{url}/state"))
                        .send()
                        .await?
                        .error_for_status()?
                        .json::<u64>()
                        .await?
                        == 5
                );
                stop(node, &monitor).await
            })
        },
    )?;
    ensure!(
        report.complete()
            && report.bridge.operations.settled()
            && report.bridge.pending_reservations == 0
    );
    Ok(())
}
