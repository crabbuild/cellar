// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn frame(kind: u8, bytes: &[u8]) -> Vec<u8> {
    let mut frame = vec![kind, 0, 0, 0];
    frame.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    frame.extend_from_slice(bytes);
    frame
}

#[tokio::test]
async fn fragmented_stdout_is_unframed_and_stdin_remains_raw() -> io::Result<()> {
    let (mut daemon, socket) = tokio::io::duplex(8);
    let mut attached = DockerIo::new(socket);
    let daemon = async {
        for byte in [
            frame(1, &[0, 255]),
            frame(1, &[]),
            frame(2, &[]),
            frame(1, b"tail"),
        ]
        .concat()
        {
            daemon.write_all(&[byte]).await?;
        }
        daemon.shutdown().await?;
        let mut request = Vec::new();
        daemon.read_to_end(&mut request).await?;
        assert_eq!(request, b"raw stdin");
        io::Result::Ok(())
    };
    let native = async {
        let mut output = Vec::new();
        attached.read_to_end(&mut output).await?;
        assert_eq!(output, [vec![0, 255], b"tail".to_vec()].concat());
        attached.write_all(b"raw stdin").await?;
        attached.shutdown().await?;
        io::Result::Ok(())
    };
    let (daemon, native) = tokio::join!(daemon, native);
    daemon?;
    native
}

#[tokio::test]
async fn invalid_stderr_and_truncated_frames_poison_both_directions() -> io::Result<()> {
    for bytes in [
        frame(2, b"private diagnostic"),
        frame(0, b"stdin"),
        vec![1, 1, 0, 0, 0, 0, 0, 0],
        vec![1, 0, 0],
        [
            vec![1, 0, 0, 0],
            (MAX_FRAME_BYTES + 1).to_be_bytes().to_vec(),
        ]
        .concat(),
        vec![1, 0, 0, 0, 0, 0, 0, 5, 1],
    ] {
        let mut io = DockerIo::new(std::io::Cursor::new(bytes));
        let mut output = Vec::new();
        assert!(io.read_to_end(&mut output).await.is_err());
        assert!(io.write_all(b"must not send").await.is_err());
        assert!(io.read_u8().await.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn payload_reads_are_limited_by_the_callers_buffer() -> io::Result<()> {
    let bytes = vec![7; MAX_FRAME_BYTES as usize];
    let framed = frame(1, &bytes);
    let mut io = DockerIo::new(framed.as_slice());
    let mut head = [0; 3];
    io.read_exact(&mut head).await?;
    assert_eq!(head, [7; 3]);
    assert_eq!(io.remaining, MAX_FRAME_BYTES - 3);
    Ok(())
}
