// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A recovery observation survives transport loss without replaying a mutation.
#[path = "../proof/recovery.rs"]
mod recovery;

use anyhow::{ensure, Result};
use std::time::Duration;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

#[tokio::test]
async fn recovery_reads_retry_disconnect_and_typed_refusal_inside_one_deadline() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        for index in 0..3 {
            let (mut socket, _) = listener.accept().await?;
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                ensure!(
                    request.len() < 4096,
                    "request headers exceeded fixture bound"
                );
                let byte = socket.read_u8().await?;
                request.push(byte);
            }
            ensure!(request.starts_with(b"GET /rust/read?target=python HTTP/1.1\r\n"));
            if index == 0 {
                socket.shutdown().await?;
                continue;
            }
            let (status, body) = if index == 1 {
                (
                    "503 Service Unavailable",
                    "Worker failed: Cellar RPC Unavailable (Unknown)",
                )
            } else {
                ("200 OK", r#"{"value":"7"}"#)
            };
            socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await?;
            socket.shutdown().await?;
        }
        Ok::<_, anyhow::Error>(())
    });
    let client = reqwest::Client::new();
    let observed = tokio::time::timeout(
        Duration::from_secs(1),
        recovery::read(&client, &url, "python"),
    )
    .await;
    if !matches!(&observed, Ok(Ok(_))) {
        server.abort();
    }
    let served = server.await;
    let observed = observed??;
    ensure!(observed == serde_json::json!({"value":"7"}));
    served??;
    Ok(())
}
