//! Real wire upgrade and credit/replay checks from the independent client.
use super::*;
use fastwebsockets::{handshake, FragmentCollector, Frame, OpCode};

struct Executor;

impl<F> hyper::rt::Executor<F> for Executor
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, future: F) {
        tokio::spawn(future);
    }
}

pub(super) async fn verify(
    client: &Client,
    config: &Config,
    writer: &Mutex<BufWriter<tokio::fs::File>>,
) -> Result<Vec<Value>> {
    let mut evidence = Vec::new();
    for mode in ["ordinary", "hibernating"] {
        let url = url::Url::parse(&format!(
            "{}/api/0/devices/0/ws?mode={mode}",
            config.urls[1 % config.urls.len()]
        ))?;
        let host = url.host_str().context("websocket URL missing host")?;
        let port = url
            .port_or_known_default()
            .context("websocket URL missing port")?;
        let stream = tokio::net::TcpStream::connect((host, port)).await?;
        let request = hyper::Request::builder()
            .uri(url.as_str())
            .header("Host", format!("{host}:{port}"))
            .header("Upgrade", "websocket")
            .header("Connection", "upgrade")
            .header("Sec-WebSocket-Key", handshake::generate_key())
            .header("Sec-WebSocket-Version", "13")
            .header("Authorization", format!("Bearer {}", config.token))
            .body(http_body_util::Empty::<bytes::Bytes>::new())?;
        let (socket, response) = handshake::client(&Executor, request, stream).await?;
        ensure!(response.status() == 101, "websocket upgrade failed");
        let mut socket = FragmentCollector::new(socket);
        let frame = tokio::time::timeout(Duration::from_secs(10), socket.read_frame())
            .await
            .with_context(|| format!("{mode} subscription initial state timed out"))??;
        ensure!(
            frame.opcode == OpCode::Text,
            "subscription did not send initial state"
        );
        let initial: Value = serde_json::from_slice(&frame.payload)?;
        let version = initial["version"]
            .as_u64()
            .context("subscription version absent")?;
        socket
            .write_frame(Frame::text(
                serde_json::to_vec(&json!({"ack":version}))?.into(),
            ))
            .await?;
        let started = Instant::now();
        let receipt = super::checks::command(
            client,
            config,
            writer,
            "/api/0/devices/0",
            Command {
                command_id: format!("probe-ws-{mode}"),
                action: "telemetry".into(),
                payload: json!({"temperature":25}),
                expected_version: None,
            },
        )
        .await?;
        ensure!(receipt.accepted, "subscription telemetry rejected");
        let frame = tokio::time::timeout(Duration::from_secs(10), socket.read_frame())
            .await
            .with_context(|| format!("{mode} subscription committed update timed out"))??;
        ensure!(
            frame.opcode == OpCode::Text,
            "subscription mutation produced wrong frame"
        );
        let value: Value = serde_json::from_slice(&frame.payload)?;
        ensure!(
            value["version"] == receipt.version && value["scope"] == initial["scope"],
            "subscription lost committed version"
        );
        let visible_ms = started.elapsed().as_secs_f64() * 1000.0;
        socket
            .write_frame(Frame::text(
                serde_json::to_vec(&json!({"ack":receipt.version}))?.into(),
            ))
            .await?;
        socket
            .write_frame(Frame::text(b"PING".to_vec().into()))
            .await?;
        let pong = tokio::time::timeout(Duration::from_secs(10), socket.read_frame())
            .await
            .with_context(|| format!("{mode} subscription auto response timed out"))??;
        ensure!(
            pong.payload.as_ref() == b"PONG",
            "automatic response failed"
        );
        socket
            .write_frame(Frame::binary(vec![0, 1, 255].into()))
            .await?;
        let binary = tokio::time::timeout(Duration::from_secs(10), socket.read_frame())
            .await
            .with_context(|| format!("{mode} subscription binary echo timed out"))??;
        ensure!(
            binary.opcode == OpCode::Binary && binary.payload.as_ref() == [0, 1, 255],
            "binary echo failed"
        );
        socket
            .write_frame(Frame::new(true, OpCode::Close, None, vec![3, 232].into()))
            .await?;
        let closed = tokio::time::timeout(Duration::from_secs(10), socket.read_frame())
            .await
            .with_context(|| format!("{mode} subscription close timed out"))??;
        ensure!(closed.opcode == OpCode::Close, "subscription did not close");
        evidence.push(
            json!({"case":"websocket_state_subscription","mode":mode,"visible_after_ms":visible_ms,
            "passed":["upgrade_pair_text_binary_attachment_ack_auto_response_close"]}),
        );
    }
    Ok(evidence)
}
