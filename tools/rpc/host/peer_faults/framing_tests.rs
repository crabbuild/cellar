// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[tokio::test]
async fn fragmented_messages_preserve_body_and_next_request() {
    let (mut sender, mut reader) = tokio::io::duplex(7);
    let writer = tokio::spawn(async move {
        for chunk in b"POST / HTTP/1.1\r\nContent-Length: 3\r\n\r\nabcPOST / HTTP/1.1\r\nContent-Length: 0\r\n\r\n".chunks(2) {
            sender.write_all(chunk).await.unwrap();
        }
    });
    let first = head(&mut reader).await.unwrap().unwrap();
    assert_eq!(body(&first, &mut reader).await.unwrap(), b"abc");
    let next = head(&mut reader).await.unwrap().unwrap();
    assert!(body(&next, &mut reader).await.unwrap().is_empty());
    assert!(head(&mut reader).await.unwrap().is_none());
    writer.await.unwrap();
}

#[tokio::test]
async fn malformed_and_unsupported_framing_cannot_certify_a_call() {
    for text in [
        "POST / HTTP/1.1\r\nContent-Length: 0\r\ncontent-length: 0\r\n\r\n",
        "POST / HTTP/1.1\r\nmissing-colon\r\n\r\n",
        "POST / HTTP/1.1\r\nContent-Length: 1\r\n",
    ] {
        assert!(head(&mut text.as_bytes()).await.is_err());
    }
    for text in [
        "POST / HTTP/1.1\r\nContent-Length: 65537\r\n\r\n",
        "POST / HTTP/1.1\r\nContent-Length: 3\r\n\r\na",
        "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
        "POST / HTTP/1.1\r\nContent-Length: -1\r\n\r\n",
    ] {
        let mut bytes = text.as_bytes();
        let parsed = head(&mut bytes).await.unwrap().unwrap();
        assert!(body(&parsed, &mut bytes).await.is_err());
    }
    let large = vec![b'a'; MAX_HEAD + 1];
    assert!(head(&mut large.as_slice()).await.is_err());
}

#[tokio::test]
async fn ordinary_outer_http_cannot_hide_a_later_pooled_upgrade() {
    for connection in ["", "Connection: keep-alive\r\n"] {
        let text = format!("GET /peer/probe HTTP/1.1\r\nX-Test: signed\r\n{connection}\r\n");
        let mut parsed = head(&mut text.as_bytes()).await.unwrap().unwrap();
        parsed.close_connection().unwrap();
        let reparsed = head(&mut parsed.raw.as_slice()).await.unwrap().unwrap();
        assert_eq!(reparsed.get("connection"), Some("close"));
        assert_eq!(reparsed.get("x-test"), Some("signed"));
        assert_eq!(reparsed.first, "GET /peer/probe HTTP/1.1");
    }
}
