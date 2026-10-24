use super::*;
use serde_json::json;
use std::{
    net::TcpListener,
    time::{Duration, Instant},
};

fn connected(maximum: usize, budget: usize) -> Result<(TcpStream, Transport)> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let peer = TcpStream::connect(listener.local_addr()?)?;
    peer.set_read_timeout(Some(Duration::from_secs(2)))?;
    Ok((peer, Transport::new(listener.accept()?.0, maximum, budget)?))
}

fn frame(metadata: Value, body: &[u8]) -> Vec<u8> {
    let metadata = serde_json::to_vec(&metadata).unwrap();
    let mut bytes = (metadata.len() as u32).to_be_bytes().to_vec();
    bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
    bytes.extend(metadata);
    bytes.extend(body);
    bytes
}

fn receive(transport: &mut Transport) -> Result<Frame> {
    let until = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(value) = transport.receive()? {
            return Ok(value);
        }
        ensure!(Instant::now() < until, "Node transport stalled");
        std::thread::yield_now();
    }
}

#[test]
fn fragmented_binary_and_coalesced_empty_frames() -> Result<()> {
    let (mut peer, mut transport) = connected(128, 256)?;
    let bytes = frame(
        json!({"type":"operation","has_body":true}),
        &[0, 255, 128, 0],
    );
    // Every header, metadata and attachment boundary can split independently.
    for byte in &bytes[..bytes.len() - 1] {
        peer.write_all(&[*byte])?;
        assert!(transport.receive()?.is_none());
    }
    peer.write_all(&bytes[bytes.len() - 1..])?;
    let message = receive(&mut transport)?;
    assert_eq!(message.metadata["type"], "operation");
    assert_eq!(message.body, Some(vec![0, 255, 128, 0]));

    let mut bytes = frame(json!({"has_body":true}), &[]);
    bytes.extend(frame(json!({"has_body":false}), &[]));
    peer.write_all(&bytes)?;
    assert_eq!(receive(&mut transport)?.body, Some(vec![]));
    assert_eq!(receive(&mut transport)?.body, None);
    Ok(())
}

#[test]
fn raw_outbound_bytes_and_queue_reservations() -> Result<()> {
    let (mut peer, mut transport) = connected(128, 55)?;
    transport.send_parts(json!({"type":"x"}), Some(&[0, 255]))?;
    assert!(transport.send(json!({"type":"y"})).is_err());
    let expected = frame(json!({"type":"x","has_body":true}), &[0, 255]);
    transport.flush()?;
    let mut bytes = vec![0; expected.len()];
    peer.read_exact(&mut bytes)?;
    assert_eq!(bytes, expected);
    assert_eq!(transport.queued, 0);
    transport.send_parts(json!({}), Some(&[]))?;
    assert!(transport.send_parts(json!({}), Some(&[0; 129])).is_err());
    Ok(())
}

#[test]
fn announced_limits_and_invalid_presence_refuse_before_dispatch() -> Result<()> {
    for metadata in [
        json!({}),
        json!({"has_body":null}),
        json!({"has_body":false}),
    ] {
        let (mut peer, mut transport) = connected(128, 256)?;
        peer.write_all(&frame(metadata, &[7]))?;
        assert!(receive(&mut transport).is_err());
    }
    for (metadata, body) in [(0_u32, 0_u32), (129, 0), (1, 128), (u32::MAX, u32::MAX)] {
        let (mut peer, mut transport) = connected(128, 256)?;
        peer.write_all(&metadata.to_be_bytes())?;
        peer.write_all(&body.to_be_bytes())?;
        assert!(receive(&mut transport).is_err());
    }
    Ok(())
}

#[test]
fn disconnect_in_header_metadata_or_body_cannot_complete_a_frame() -> Result<()> {
    let bytes = frame(json!({"has_body":true}), &[0, 1, 255]);
    for length in [0, 3, 7, 9, bytes.len() - 1] {
        let (mut peer, mut transport) = connected(128, 256)?;
        peer.write_all(&bytes[..length])?;
        peer.shutdown(std::net::Shutdown::Write)?;
        assert!(receive(&mut transport).is_err());
    }
    Ok(())
}
