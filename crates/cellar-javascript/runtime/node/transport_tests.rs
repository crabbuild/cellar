// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

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
    Ok((
        peer,
        Transport::new(
            listener.accept()?.0,
            maximum,
            Arc::new(Semaphore::new(budget)),
        )?,
    ))
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

fn receive_partial(transport: &mut Transport, length: usize) -> Result<()> {
    let until = Instant::now() + Duration::from_secs(2);
    loop {
        assert!(transport.receive()?.is_none());
        if transport.input.len() == length {
            return Ok(());
        }
        ensure!(Instant::now() < until, "partial frame did not arrive");
        std::thread::yield_now();
    }
}

#[test]
fn fragmented_binary_and_coalesced_empty_frames() -> Result<()> {
    let (mut peer, mut transport) = connected(128, 256)?;
    assert!(!transport.input_ready()?);
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
    // Writes need not have reached the nonblocking socket yet. Once ready,
    // repeated diagnostic peeks must leave both frames untouched.
    let until = Instant::now() + Duration::from_secs(2);
    while !transport.input_ready()? {
        ensure!(Instant::now() < until, "Node diagnostic readiness stalled");
        std::thread::yield_now();
    }
    assert!(transport.input_ready()?);
    assert_eq!(receive(&mut transport)?.body, Some(vec![]));
    assert_eq!(receive(&mut transport)?.body, None);
    Ok(())
}

#[test]
fn raw_outbound_bytes_and_queue_reservations() -> Result<()> {
    let expected = frame(json!({"type":"x","has_body":true}), &[0, 255]);
    // Capacity includes the encoded frame and overlapping owned control Value.
    let metadata = expected.len() - HEADER_BYTES - 2;
    let (mut peer, mut transport) = connected(128, expected.len() + metadata)?;
    transport.send_parts(json!({"type":"x"}), Some(&[0, 255]))?;
    assert!(transport.send(json!({"type":"y"})).is_err());
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
        let budget = transport.budget.clone();
        peer.write_all(&bytes[..length])?;
        peer.shutdown(std::net::Shutdown::Write)?;
        assert!(receive(&mut transport).is_err());
        drop(transport);
        assert_eq!(budget.available_permits(), 256);
    }
    Ok(())
}

#[test]
fn incoming_frames_cannot_bypass_the_aggregate_bridge_budget() -> Result<()> {
    let bytes = frame(json!({"has_body":true}), &[7; 32]);
    let (mut peer, mut transport) = connected(256, bytes.len() - 1)?;
    peer.write_all(&bytes)?;
    assert!(
        receive(&mut transport).is_err(),
        "incoming frame bypassed the bridge byte budget"
    );
    Ok(())
}

#[test]
fn inbound_and_queued_outbound_frames_share_capacity() -> Result<()> {
    let outgoing = frame(json!({"type":"queued","has_body":true}), &[1; 16]);
    let incoming = frame(json!({"has_body":true}), &[2; 32]);
    let (mut peer, mut transport) = connected(256, outgoing.len() + incoming.len() - 1)?;
    transport.send_parts(json!({"type":"queued"}), Some(&[1; 16]))?;
    peer.write_all(&incoming)?;
    assert!(
        receive(&mut transport).is_err(),
        "inbound buffering ignored queued outbound allocations"
    );
    Ok(())
}

#[test]
fn incoming_frame_and_decoded_parts_are_charged_while_they_overlap() -> Result<()> {
    let bytes = frame(json!({"has_body":true}), &[7; 32]);
    let payload = bytes.len() - HEADER_BYTES;
    let (mut peer, mut transport) = connected(256, bytes.len() + payload - 1)?;
    peer.write_all(&bytes)?;
    assert!(
        receive(&mut transport).is_err(),
        "decoding copied metadata/body outside the aggregate byte budget"
    );
    Ok(())
}

#[test]
fn native_payload_owners_and_transport_frames_use_the_same_reservations() -> Result<()> {
    let (_peer, mut transport) = connected(128, 256)?;
    let budget = transport.budget.clone();
    let native = budget.clone().try_acquire_many_owned(240)?;
    assert!(transport.send(json!({"type":"x"})).is_err());
    assert!(transport.output.is_empty());
    assert_eq!(budget.available_permits(), 16);
    drop(native);
    transport.send(json!({"type":"x"}))?;
    assert_eq!(budget.available_permits(), 256 - transport.queued);
    drop(transport);
    assert_eq!(budget.available_permits(), 256);
    Ok(())
}

#[test]
fn partial_inputs_and_returned_frames_retain_only_their_owned_charges() -> Result<()> {
    let bytes = frame(json!({"has_body":true}), &[1, 2, 3]);
    let retained = "has_body".len() + 3;
    let (mut peer, mut transport) = connected(128, 256)?;
    let budget = transport.budget.clone();
    peer.write_all(&bytes[..3])?;
    receive_partial(&mut transport, 3)?;
    assert_eq!(budget.available_permits(), 256 - HEADER_BYTES);
    peer.write_all(&bytes[3..9])?;
    receive_partial(&mut transport, 9)?;
    assert_eq!(budget.available_permits(), 256 - bytes.len());
    peer.write_all(&bytes[9..])?;
    let decoded = receive(&mut transport)?;
    assert_eq!(decoded.body.as_deref(), Some(&[1, 2, 3][..]));
    assert_eq!(transport.input.capacity(), 0);
    assert_eq!(budget.available_permits(), 256 - retained);
    drop(transport);
    assert_eq!(budget.available_permits(), 256 - retained);
    drop(decoded);
    assert_eq!(budget.available_permits(), 256);
    Ok(())
}

#[test]
fn oversized_serialization_refuses_without_retaining_frame_bytes() -> Result<()> {
    let (_peer, mut transport) = connected(64, 256)?;
    let budget = transport.budget.clone();
    assert!(transport.send(json!({"payload":"\0".repeat(32)})).is_err());
    assert!(transport.output.is_empty());
    assert_eq!(budget.available_permits(), 256);
    assert_eq!(transport.queued, 0);
    Ok(())
}

#[test]
fn framed_value_capacity_checks_defaults_and_overflow() -> Result<()> {
    validate_budget(2 * 1024 * 1024, 32 * 1024 * 1024)?;
    assert!(validate_budget(128, 128).is_err());
    assert!(validate_budget(usize::MAX, usize::MAX).is_err());
    assert!(validate_budget(usize::MAX / 8, usize::MAX).is_err());
    Ok(())
}

#[test]
fn partial_socket_writes_keep_full_allocations_reserved_until_flush() -> Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()?;
    let socket = tokio::net::TcpSocket::new_v4()?;
    socket.set_send_buffer_size(4096)?;
    let socket = runtime
        .block_on(socket.connect(listener.local_addr()?))?
        .into_std()?;
    let (mut peer, _) = listener.accept()?;
    peer.set_nonblocking(true)?;
    let body = vec![7; 1024 * 1024];
    let expected = frame(json!({"type":"x","has_body":true}), &body);
    let capacity = 3 * expected.len();
    let budget = Arc::new(Semaphore::new(capacity));
    let mut transport = Transport::new(socket, body.len() + 128, budget.clone())?;
    transport.send_parts(json!({"type":"x"}), Some(&body))?;
    transport.flush()?;
    assert!(transport.offset > 0 && transport.offset < expected.len());
    assert_eq!(transport.queued, expected.len());
    assert_eq!(budget.available_permits(), capacity - expected.len());

    let until = Instant::now() + Duration::from_secs(2);
    let mut received = Vec::new();
    while received.len() != expected.len() {
        transport.flush()?;
        let mut bytes = [0; 65_536];
        match peer.read(&mut bytes) {
            Ok(0) => anyhow::bail!("test peer disconnected"),
            Ok(n) => received.extend_from_slice(&bytes[..n]),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error.into()),
        }
        ensure!(Instant::now() < until, "partial-write drain stalled");
        std::thread::yield_now();
    }
    assert_eq!(received, expected);
    assert_eq!(transport.queued, 0);
    assert_eq!(budget.available_permits(), capacity);
    Ok(())
}

#[test]
fn incoming_control_metadata_cannot_allocate_collection_graphs() -> Result<()> {
    for value in [json!(vec![0; 2048]), json!({"nested": {"value": 1}})] {
        let (mut peer, mut transport) = connected(8192, 128 * 1024)?;
        peer.write_all(&frame(json!({"has_body":false,"extra":value}), &[]))?;
        assert!(
            receive(&mut transport).is_err(),
            "structured control metadata reached the allocating JSON decoder"
        );
    }
    Ok(())
}

#[test]
fn incoming_controls_bound_keys_fields_and_duplicate_spellings() -> Result<()> {
    let mut many = serde_json::Map::new();
    many.insert("has_body".into(), Value::Bool(false));
    for index in 0..16 {
        many.insert(format!("field_{index}"), Value::Bool(false));
    }
    for metadata in [
        r#"{"has_body":false,"has_body":false}"#.to_string(),
        r#"{"has\u005fbody":false}"#.to_string(),
        json!({"has_body":false,"x".repeat(65):0}).to_string(),
        Value::Object(many).to_string(),
    ] {
        let (mut peer, mut transport) = connected(4096, 16 * 1024)?;
        let mut bytes = (metadata.len() as u32).to_be_bytes().to_vec();
        bytes.extend_from_slice(&0_u32.to_be_bytes());
        bytes.extend_from_slice(metadata.as_bytes());
        peer.write_all(&bytes)?;
        assert!(
            receive(&mut transport).is_err(),
            "unbounded or ambiguous control was admitted"
        );
    }
    Ok(())
}

#[test]
fn escaped_control_strings_reserve_parser_scratch_before_decoding() -> Result<()> {
    let bytes = frame(
        json!({"has_body":false,"payload":format!("{}\n", "x".repeat(1024))}),
        &[],
    );
    let capacity = bytes.len() + bytes.len() - HEADER_BYTES;
    let (mut peer, mut transport) = connected(4096, capacity)?;
    let budget = transport.budget.clone();
    peer.write_all(&bytes)?;
    assert!(
        receive(&mut transport).is_err(),
        "JSON parser scratch bypassed byte admission"
    );
    drop(transport);
    assert_eq!(budget.available_permits(), capacity);
    Ok(())
}

#[test]
fn decoded_control_strings_preserve_unicode_and_release_scratch() -> Result<()> {
    let metadata = r#"{"has_body":false,"payload":"\u0000\ufeff\uD83D\uDE00\"\\\n"}"#;
    let (mut peer, mut transport) = connected(1024, 4096)?;
    let budget = transport.budget.clone();
    let mut bytes = (metadata.len() as u32).to_be_bytes().to_vec();
    bytes.extend_from_slice(&0_u32.to_be_bytes());
    bytes.extend_from_slice(metadata.as_bytes());
    peer.write_all(&bytes)?;
    let decoded = receive(&mut transport)?;
    let expected = "\0\u{feff}😀\"\\\n";
    assert_eq!(decoded.metadata["payload"], expected);
    let retained = "has_body".len() + "payload".len() + expected.len();
    assert_eq!(budget.available_permits(), 4096 - retained);
    drop(decoded);
    drop(transport);
    assert_eq!(budget.available_permits(), 4096);
    Ok(())
}
