//! Input, output and aggregate batch bytes at exact public boundaries.
use super::*;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

struct WithResults(Arc<PythonApplication>);

impl Application for WithResults {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        self.0.clone().invoke(context, event)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.0.supports_cell(class)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        Ok(Arc::new(ResultWriter(
            self.0.create_cell(context, identity)?,
        )))
    }
}

struct ResultWriter(Arc<dyn Cell>);

impl Cell for ResultWriter {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        if matches!(&event, Invocation::Fetch(request) if request.url.ends_with("/seed-results")) {
            return Box::pin(async move {
                let overhead =
                    serde_json::to_vec(&json!({"ok":true,"value":{"present":true,"value":""}}))?
                        .len();
                context.storage()?.transaction_sync(|tx| {
                    tx.put_json("result-exact", &json!("x".repeat(65536 - overhead)))?;
                    tx.put_json("result-over", &json!("x".repeat(65537 - overhead)))?;
                    let mut depth = Value::Null;
                    for _ in 0..63 {
                        depth = json!([depth]);
                    }
                    tx.put_json("result-depth-exact", &depth)?;
                    tx.put_json("result-depth-over", &json!([depth]))
                })?;
                Ok(Output::Http(
                    http::Response::new(200, Vec::new(), Vec::new()).into_http(&context)?,
                ))
            });
        }
        self.0.clone().invoke(context, event)
    }
}

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
                let observed = application.clone();
                let (node, url) = start_configured(
                    directory.path(),
                    Arc::new(WithResults(application)),
                    "limits",
                    None,
                    |config| config.runtime.handler_budget = Duration::from_secs(5),
                )
                .await?;
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(10))
                    .build()?;
                // Exercise binary request delivery through real ingress. The
                // bridge must copy the body as bytes, not expand it into a
                // base64 string in the invocation's JSON envelope.
                let binary: Vec<u8> = (0..4096).map(|byte| byte as u8).collect();
                for path in ["/echo", "/echo-buffer"] {
                    let before = observed.bridge_report();
                    let echoed = client
                        .post(format!("{url}{path}"))
                        .body(binary.clone())
                        .send()
                        .await?
                        .error_for_status()?
                        .bytes()
                        .await?;
                    ensure!(echoed.as_ref() == binary);
                    let after = observed.bridge_report();
                    let copied = after.output_copied_bytes - before.output_copied_bytes;
                    ensure!(
                        copied >= binary.len() && copied < binary.len() + 1024,
                        "binary invocation expanded its body: {copied} copied bytes"
                    );
                    let copied = after.input_copied_bytes - before.input_copied_bytes;
                    ensure!(
                        copied >= binary.len() && copied < binary.len() + 1024,
                        "binary completion expanded its body: {copied} copied bytes"
                    );
                }
                // Raw delivery must retain the previous encoded-envelope
                // bound, including escaped headers and base64-equivalent body
                // length. Observe the real ingress metadata instead of assuming
                // which transport headers the HTTP client adds.
                let input_overhead = client
                    .post(format!("{url}/input-envelope"))
                    .header("x-boundary", "")
                    .body(binary.clone())
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<usize>()
                    .await?;
                let remaining = 65536_usize
                    .checked_sub(input_overhead)
                    .context("input envelope fixture overhead")?;
                let exact_header = "\"".repeat(remaining / 2) + &"a".repeat(remaining % 2);
                for excess in [false, true, false] {
                    let header = if excess {
                        exact_header.clone() + "a"
                    } else {
                        exact_header.clone()
                    };
                    let response = client
                        .post(format!("{url}/input-envelope"))
                        .header("x-boundary", header)
                        .body(binary.clone())
                        .send()
                        .await?;
                    if excess {
                        ensure!(
                            response.status().is_server_error(),
                            "oversized invocation admitted"
                        );
                    } else {
                        ensure!(response.error_for_status()?.json::<usize>().await? == 65536);
                    }
                }
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
                for route in ["nested-input", "batch-value", "sdk-results"] {
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
                for suffix in ["exact", "over", "exact"] {
                    let response = client
                        .get(format!("{url}/completion/{suffix}"))
                        .send()
                        .await?;
                    if suffix == "over" {
                        ensure!(
                            response.status().is_server_error(),
                            "over-limit completion was accepted"
                        );
                    } else {
                        let response = response.error_for_status()?;
                        ensure!(
                            response
                                .headers()
                                .get("x")
                                .context("completion header missing")?
                                .as_bytes()
                                .len()
                                > 30_000
                        );
                        ensure!(response.bytes().await?.as_ref() == vec![b'x'; 4096]);
                    }
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
