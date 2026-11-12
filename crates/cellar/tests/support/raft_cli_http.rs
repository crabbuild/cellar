// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The actual operator CLI must not acquire TLS trust for an HTTP destination.
use anyhow::{ensure, Context as _, Result};
use base64::Engine as _;
use serde_json::{json, Value};
use std::{
    path::Path,
    process::Stdio,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const TOKEN: &str = "local-cli-transport-test-credential-000000";
const TIMEOUT: Duration = Duration::from_secs(5);

fn expected() -> Value {
    json!({"catalog_id": "01".repeat(32), "namespace_id": "02".repeat(16), "standby": "03".repeat(32)})
}

async fn respond<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    received: &AtomicUsize,
) -> Result<()> {
    let mut request = Vec::new();
    loop {
        let mut chunk = [0; 1024];
        let n = stream.read(&mut chunk).await?;
        ensure!(n != 0 && request.len() + n <= 16 * 1024);
        request.extend_from_slice(&chunk[..n]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    let request = std::str::from_utf8(&request)?;
    ensure!(request.starts_with(&format!(
        "GET /api/v1/fleets/cli-transport/raft/namespaces/{}/first-group/standby HTTP/1.1\r\n",
        "02".repeat(16)
    )));
    ensure!(request.lines().any(|line| line
        .split_once(':')
        .is_some_and(|(name, value)| name.eq_ignore_ascii_case("authorization")
            && value.trim() == format!("Bearer {TOKEN}"))));
    received.fetch_add(1, Ordering::Release);
    let body = serde_json::to_vec(&expected())?;
    stream.write_all(format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    ).as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}

struct Server {
    url: String,
    stop: Option<oneshot::Sender<()>>,
    joined: Option<JoinHandle<Result<()>>>,
    received: Arc<AtomicUsize>,
}

impl Server {
    async fn start(tls: bool) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (stop, stopped) = oneshot::channel();
        let acceptor = if tls {
            let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_no_client_auth()
            .with_single_cert(
                vec![include_bytes!("../../transport/tcp/fixtures/server.der")
                    .to_vec()
                    .into()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(
                    include_bytes!("../../transport/tcp/fixtures/key.der").to_vec(),
                )
                .into(),
            )?;
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            Some(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
        } else {
            None
        };
        let received = Arc::new(AtomicUsize::new(0));
        let observed = received.clone();
        let joined = tokio::spawn(async move {
            let request = async {
                let (stream, _) = listener.accept().await?;
                if let Some(acceptor) = acceptor {
                    match acceptor.accept(stream).await {
                        Ok(stream) => respond(stream, &observed).await,
                        // An untrusted server must not receive an HTTP request.
                        Err(_) => Ok(()),
                    }
                } else {
                    respond(stream, &observed).await
                }
            };
            tokio::select! {
                result = request => result,
                _ = stopped => Ok(()),
            }
        });
        Ok(Self {
            url: if tls {
                format!("https://localhost:{}", address.port())
            } else {
                format!("http://{address}")
            },
            stop: Some(stop),
            joined: Some(joined),
            received,
        })
    }

    async fn close(mut self) -> Result<usize> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.joined
            .take()
            .context("owned CLI server task")?
            .await??;
        Ok(self.received.load(Ordering::Acquire))
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

async fn output(reader: impl AsyncRead + Unpin) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.take(1024 * 1024 + 1).read_to_end(&mut bytes).await?;
    ensure!(bytes.len() <= 1024 * 1024);
    Ok(bytes)
}

async fn call(server: &Server, roots: &Path, directory: &Path) -> Result<(i32, Value)> {
    let executable = std::env::var_os("CELLAR_RAFT_OPERATOR_CLI")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_cellar").into());
    let mut command = tokio::process::Command::new(executable);
    command
        .args(["raft", "fleet", "first-group", "standby"])
        .arg("02".repeat(16))
        .args([
            "--fleet",
            "cli-transport",
            "--admin-url",
            &server.url,
            "--catalog-id",
        ])
        .arg("01".repeat(32))
        .arg("--json")
        .env("CELLAR_ADMIN_TOKEN", TOKEN)
        .env("SSL_CERT_FILE", roots)
        .env("SSL_CERT_DIR", directory)
        .env_remove("AWS_ACCESS_KEY_ID")
        .env_remove("AWS_SECRET_ACCESS_KEY")
        .env_remove("AWS_SESSION_TOKEN")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn()?;
    let stdout = child.stdout.take().context("CLI stdout")?;
    let stderr = child.stderr.take().context("CLI stderr")?;
    let stdout = tokio::spawn(output(stdout));
    let stderr = tokio::spawn(output(stderr));
    let status = match tokio::time::timeout(TIMEOUT, child.wait()).await {
        Ok(status) => status.map_err(anyhow::Error::from),
        Err(_) => match child.kill().await {
            Ok(()) => Err(anyhow::anyhow!(
                "operator CLI exceeded the original five-second deadline"
            )),
            Err(error) => Err(error.into()),
        },
    };
    // Join both drains even when the child exceeded its deadline.
    let (stdout, stderr) = tokio::join!(stdout, stderr);
    let stdout = stdout??;
    let stderr = stderr??;
    ensure!(!String::from_utf8_lossy(&stdout).contains(TOKEN));
    ensure!(!String::from_utf8_lossy(&stderr).contains(TOKEN));
    let status = status?;
    Ok((
        status.code().context("CLI exited by signal")?,
        serde_json::from_slice(&stdout)?,
    ))
}

#[cfg(unix)]
#[tokio::test]
async fn plain_http_does_not_open_native_certificate_source() -> Result<()> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};
    let root = tempfile::tempdir()?;
    let fifo = root.path().join("unavailable-certificates.pem");
    let name = CString::new(fifo.as_os_str().as_bytes())?;
    // SAFETY: name is a live NUL-terminated path in this test's owned directory.
    let created = unsafe { libc::mkfifo(name.as_ptr(), 0o600) };
    ensure!(
        created == 0,
        "create owned FIFO: {}",
        std::io::Error::last_os_error()
    );
    let directory = root.path().join("empty-roots");
    std::fs::create_dir(&directory)?;
    let server = Server::start(false).await?;
    let result = call(&server, &fifo, &directory).await;
    let received = server.close().await?;
    let (code, value) = result?;
    ensure!(received == 1 && code == 0 && value == expected());
    Ok(())
}

async fn https(trusted: bool) -> Result<()> {
    let root = tempfile::tempdir()?;
    let roots = root.path().join("roots.pem");
    let mut pem = String::new();
    if trusted {
        pem.push_str("-----BEGIN CERTIFICATE-----\n");
        let encoded = base64::engine::general_purpose::STANDARD
            .encode(include_bytes!("../../transport/tcp/fixtures/ca.der"));
        for line in encoded.as_bytes().chunks(64) {
            pem.push_str(std::str::from_utf8(line)?);
            pem.push('\n');
        }
        pem.push_str("-----END CERTIFICATE-----\n");
    }
    std::fs::write(&roots, pem)?;
    let directory = root.path().join("empty-roots");
    std::fs::create_dir(&directory)?;
    let server = Server::start(true).await?;
    let result = call(&server, &roots, &directory).await;
    let received = server.close().await?;
    let (code, value) = result?;
    if trusted {
        ensure!(received == 1 && code == 0 && value == expected());
    } else {
        ensure!(received == 0 && code == 5 && value["error"]["code"] == "unavailable");
    }
    Ok(())
}

#[tokio::test]
async fn https_preserves_explicit_native_root_trust() -> Result<()> {
    https(true).await
}

#[tokio::test]
async fn https_refuses_untrusted_server_before_http() -> Result<()> {
    https(false).await
}
