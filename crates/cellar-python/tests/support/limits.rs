//! Input, output and aggregate batch bytes at exact public boundaries.
use super::*;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

async fn header_count(url: &str) -> Result<usize> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let address = url.strip_prefix("http://").context("private HTTP origin")?;
        let mut stream = tokio::net::TcpStream::connect(address).await?;
        stream.write_all(format!("GET /header-count/exact HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n").as_bytes()).await?;
        let mut bytes = Vec::new();
        stream.take(65536).read_to_end(&mut bytes).await?;
        ensure!(bytes.len() < 65536, "header fixture exceeded its bounded reader");
        let response = std::str::from_utf8(&bytes)?;
        ensure!(response.starts_with("HTTP/1.1 200 "), "exact headers refused: {response}");
        let (head, _) = response.split_once("\r\n\r\n").context("HTTP header terminator")?;
        Ok(head.split("\r\n").filter(|line| line.split_once(':').is_some_and(|(name, _)| name.eq_ignore_ascii_case("x-boundary"))).count())
    }).await.context("bounded raw header reader timed out")?
}

pub(super) fn child() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let report = run(
        Options {
            max_body_bytes: 4096,
            max_value_bytes: 65536,
            ..Options::default()
        },
        Source::new(include_str!("limits.py"), "limits.py")
            .events([EventKind::Fetch])
            .cell("Counter"),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let (node, url) =
                    start_configured(directory.path(), application, "limits", None, |config| {
                        config.runtime.handler_budget = Duration::from_secs(5)
                    })
                    .await?;
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(10))
                    .build()?;
                let exact = client
                    .post(format!("{url}/echo"))
                    .body(vec![b'x'; 4096])
                    .send()
                    .await?
                    .error_for_status()?
                    .bytes()
                    .await?;
                ensure!(exact.as_ref() == vec![b'x'; 4096]);
                ensure!(client
                    .post(format!("{url}/echo"))
                    .body(vec![b'x'; 4097])
                    .send()
                    .await?
                    .status()
                    .is_server_error());
                for route in ["body", "header-count", "header-bytes"] {
                    if route == "header-count" {
                        ensure!(header_count(&url).await? == 128);
                    } else {
                        let response = client
                            .get(format!("{url}/{route}/exact"))
                            .send()
                            .await?
                            .error_for_status()?;
                        if route == "body" {
                            ensure!(response.bytes().await?.as_ref() == vec![b'x'; 4096]);
                        } else {
                            ensure!(
                                response
                                    .headers()
                                    .get("x")
                                    .context("exact byte header")?
                                    .as_bytes()
                                    .len()
                                    == 32767
                            );
                        }
                    }
                    ensure!(
                        client
                            .get(format!("{url}/{route}/over"))
                            .send()
                            .await?
                            .status()
                            .is_server_error(),
                        "over-limit {route} was accepted"
                    );
                }
                for route in ["nested-input", "batch-value"] {
                    ensure!(
                        client
                            .get(format!("{url}/{route}"))
                            .send()
                            .await?
                            .error_for_status()
                            .with_context(|| format!("{route} boundary"))?
                            .json::<bool>()
                            .await?
                    );
                }
                stop(node, &monitor).await
            })
        },
    )?;
    ensure!(
        report.complete() && report.bridge.operations.settled(),
        "limit rejection leaked work: {report:?}"
    );
    Ok(())
}
