// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded Docker exec output: framed pipes or a raw pseudo-terminal.

use anyhow::{ensure, Context as _, Result};
use bytes::Bytes;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::mpsc,
};

pub(super) async fn pump(
    mut read: impl AsyncRead + Unpin,
    stdout: mpsc::Sender<Bytes>,
    stderr: mpsc::Sender<Bytes>,
    combined: bool,
    terminal: bool,
) -> Result<()> {
    let mut chunk = vec![0; 64 * 1024];
    if terminal {
        drop(stderr);
        loop {
            let length = read.read(&mut chunk).await.context("read exec terminal")?;
            if length == 0 {
                return Ok(());
            }
            // Continue draining if the consumer has gone away. The process
            // registry still owns and cancels this reader on resource release.
            let _ = stdout.send(Bytes::copy_from_slice(&chunk[..length])).await;
        }
    }
    loop {
        let mut header = [0; 8];
        if read.read(&mut header[..1]).await? == 0 {
            return Ok(());
        }
        read.read_exact(&mut header[1..])
            .await
            .context("truncated exec frame header")?;
        ensure!(
            matches!(header[0], 1 | 2) && header[1..4] == [0, 0, 0],
            "invalid exec output frame"
        );
        let (_, length) = super::frame_header(&header);
        let target = if header[0] == 2 && !combined {
            &stderr
        } else {
            &stdout
        };
        let mut remaining = length;
        while remaining > 0 {
            let length = remaining.min(chunk.len());
            read.read_exact(&mut chunk[..length])
                .await
                .context("truncated exec frame body")?;
            let _ = target.send(Bytes::copy_from_slice(&chunk[..length])).await;
            remaining -= length;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn terminal_output_is_raw_and_keeps_crlf_and_binary_bytes() -> Result<()> {
        let input = b"hello\r\n\0\xff";
        let (stdout, mut output) = mpsc::channel(2);
        let (stderr, mut errors) = mpsc::channel(2);
        pump(input.as_slice(), stdout, stderr, true, true).await?;
        assert_eq!(output.recv().await.unwrap().as_ref(), input);
        assert!(output.recv().await.is_none());
        assert!(errors.recv().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn truncated_frames_and_invalid_headers_are_failures() {
        for input in [
            vec![1],
            vec![1, 0, 0, 0, 0, 0, 0, 2, 42],
            vec![3, 0, 0, 0, 0, 0, 0, 0],
            vec![1, 1, 0, 0, 0, 0, 0, 0],
        ] {
            let (stdout, _output) = mpsc::channel(2);
            let (stderr, _errors) = mpsc::channel(2);
            assert!(pump(input.as_slice(), stdout, stderr, false, false)
                .await
                .is_err());
        }
    }

    #[tokio::test]
    async fn a_large_frame_is_delivered_in_bounded_chunks_with_backpressure() -> Result<()> {
        let length = 128 * 1024 + 17;
        let mut input = vec![1, 0, 0, 0];
        input.extend_from_slice(&(length as u32).to_be_bytes());
        input.resize(length + 8, 42);
        let (stdout, mut output) = mpsc::channel(1);
        let (stderr, _errors) = mpsc::channel(1);
        let (result, received) = tokio::join!(
            pump(input.as_slice(), stdout, stderr, false, false),
            async move {
                let mut received = 0;
                while let Some(chunk) = output.recv().await {
                    assert!(chunk.len() <= 64 * 1024);
                    assert!(chunk.iter().all(|byte| *byte == 42));
                    received += chunk.len();
                }
                received
            }
        );
        result?;
        assert_eq!(received, length);
        Ok(())
    }
}
