// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(clippy::disallowed_methods)] // Each finite peer is joined by its test.

use super::Docker;
use anyhow::{ensure, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

const ID: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

async fn head(socket: &mut tokio::net::UnixStream) -> Result<String> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        ensure!(bytes.len() < 4096, "unbounded Engine request");
        bytes.push(socket.read_u8().await?);
    }
    Ok(String::from_utf8(bytes)?)
}

#[tokio::test]
async fn attach_upgrades_exact_id_and_preserves_buffered_bytes_and_half_close() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("engine.sock");
    let listener = UnixListener::bind(&path)?;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let request = head(&mut socket).await?;
        ensure!(request.starts_with(&format!(
            "POST /containers/{ID}/attach?stream=1&stdin=1&stdout=1&stderr=1 HTTP/1.1\r\n"
        )));
        let lower = request.to_ascii_lowercase();
        ensure!(lower.contains("connection: upgrade\r\n"));
        ensure!(lower.contains("upgrade: tcp\r\n"));
        // Coalesce the first raw frame with the upgrade head. Hyper must
        // hand buffered protocol bytes to the owner instead of losing them.
        socket.write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n\x01\0\0\0\0\0\0\x03abc").await?;
        let mut input = Vec::new();
        socket.read_to_end(&mut input).await?;
        ensure!(input == b"raw stdin");
        socket.write_all(b"tail").await?;
        socket.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    });
    let operation = async {
        let mut stream = Docker::new(path).attach_io(ID).await?;
        let mut frame = [0; 11];
        stream.read_exact(&mut frame).await?;
        ensure!(&frame == b"\x01\0\0\0\0\0\0\x03abc");
        stream.write_all(b"raw stdin").await?;
        stream.shutdown().await?;
        let mut tail = Vec::new();
        stream.read_to_end(&mut tail).await?;
        ensure!(tail == b"tail");
        Ok::<_, anyhow::Error>(())
    };
    let (operation, peer) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::join!(operation, peer)
    })
    .await?;
    operation?;
    peer??;
    Ok(())
}

#[tokio::test]
async fn attach_refuses_names_partial_ids_and_path_injection_before_connect() -> Result<()> {
    let docker = Docker::new("/nonexistent-cellar-egress-engine.sock");
    for id in [
        "",
        "cellar-cell-name",
        &ID[..12],
        "../containers/name",
        &"z".repeat(64),
    ] {
        let error = match docker.attach_io(id).await {
            Ok(_) => anyhow::bail!("invalid private attach ID admitted"),
            Err(error) => error,
        };
        ensure!(error.to_string().contains("full Engine container ID"));
    }
    Ok(())
}

#[tokio::test]
async fn attach_preserves_bounded_daemon_refusal() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("engine.sock");
    let listener = UnixListener::bind(&path)?;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        head(&mut socket).await?;
        let body = b"{\"message\":\"container is gone\"}";
        socket
            .write_all(
                format!(
                    "HTTP/1.1 404 Not Found\r\nContent-Length: {}\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            )
            .await?;
        socket.write_all(body).await?;
        Ok::<_, anyhow::Error>(())
    });
    let result = Docker::new(path).attach_io(ID).await;
    peer.await??;
    let error = match result {
        Ok(_) => anyhow::bail!("daemon refusal admitted"),
        Err(error) => error,
    };
    ensure!(
        error
            .to_string()
            .contains("private container attach failed with [404] container is gone"),
        "unexpected Engine refusal: {error:#}"
    );
    Ok(())
}

#[tokio::test]
async fn upgrade_refuses_oversized_daemon_error_without_draining_it() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("engine.sock");
    let listener = UnixListener::bind(&path)?;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        head(&mut socket).await?;
        // No body is sent: the declared length alone must refuse, rather
        // than waiting indefinitely or allocating daemon-controlled bytes.
        socket
            .write_all(b"HTTP/1.1 500 Error\r\nContent-Length: 65537\r\n\r\n")
            .await?;
        let mut byte = [0];
        ensure!(socket.read(&mut byte).await? == 0);
        Ok::<_, anyhow::Error>(())
    });
    let docker = Docker::new(path);
    let (result, peer) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::join!(docker.attach_io(ID), peer)
    })
    .await?;
    peer??;
    let error = match result {
        Ok(_) => anyhow::bail!("oversized daemon error admitted"),
        Err(error) => error,
    };
    ensure!(error.to_string().contains("bounded Engine upgrade refusal"));
    Ok(())
}

#[tokio::test]
async fn connection_shutdown_cannot_turn_a_partial_reply_into_success() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("engine.sock");
    let listener = UnixListener::bind(&path)?;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        head(&mut socket).await?;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\na")
            .await?;
        Ok::<_, anyhow::Error>(())
    });
    let docker = Docker::new(path);
    let (result, peer) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::join!(docker.call("GET", "/containers/fixture/json", None), peer)
    })
    .await?;
    peer??;
    ensure!(result.is_err(), "truncated Engine reply reported success");
    Ok(())
}
