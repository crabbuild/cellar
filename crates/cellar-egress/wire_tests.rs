// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[tokio::test]
async fn stream_prelude_preserves_trailing_http_bytes_and_ipv6_destination() -> Result<()> {
    let (mut write, mut read) = tokio::io::duplex(1024);
    let open = Open::Intercept {
        destination: "[2001:db8::1]:8080".parse()?,
    };
    let sending = async {
        write_open(&mut write, &open).await?;
        write
            .write_all(b"GET / HTTP/1.1\r\nHost: example\r\n\r\n")
            .await?;
        write.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    };
    let reading = async {
        ensure!(read_open(&mut read).await? == open);
        let mut body = Vec::new();
        read.read_to_end(&mut body).await?;
        ensure!(body == b"GET / HTTP/1.1\r\nHost: example\r\n\r\n");
        Ok::<_, anyhow::Error>(())
    };
    let (left, right) = tokio::join!(sending, reading);
    left?;
    right?;
    Ok(())
}

#[tokio::test]
async fn incompatible_oversized_and_unknown_control_messages_refuse() -> Result<()> {
    for bytes in [
        b"BADVER\0\x01".to_vec(),
        [
            PREFIX.as_slice(),
            &((MAX_CONTROL_BYTES + 1) as u32).to_be_bytes(),
        ]
        .concat(),
        [PREFIX.as_slice(), &0u32.to_be_bytes()].concat(),
    ] {
        assert!(read_open(&mut bytes.as_slice()).await.is_err());
    }
    for raw in [
        r#"{"kind":"unknown"}"#,
        r#"{"kind":"direct","host":"example","port":80,"unexpected":true}"#,
        r#"{"kind":"direct","host":"http://example/","port":80}"#,
        r#"{"kind":"intercept","destination":"127.0.0.1:0"}"#,
    ] {
        let bytes = [
            PREFIX.as_slice(),
            &(raw.len() as u32).to_be_bytes(),
            raw.as_bytes(),
        ]
        .concat();
        assert!(read_open(&mut bytes.as_slice()).await.is_err(), "{raw}");
    }
    let oversize = (MAX_CONTROL_BYTES as u32 + 1).to_be_bytes();
    assert!(read_control(&mut oversize.as_slice()).await.is_err());
    Ok(())
}

#[tokio::test]
async fn updates_validate_revision_duplicates_and_combined_entry_cost() -> Result<()> {
    let targets = (0..64)
        .map(|i| HttpTarget::parse(&format!("host-{i}.example")))
        .collect::<Result<Vec<_>>>()?;
    let value = Control::Install {
        revision: 7,
        targets,
    };
    let mut encoded = Vec::new();
    write_control(&mut encoded, &value).await?;
    assert_eq!(read_control(&mut encoded.as_slice()).await?, value);
    for value in [
        Control::Installed { revision: 0 },
        Control::Install {
            revision: 1,
            targets: vec![HttpTarget::all(); 2],
        },
        Control::Install {
            revision: 1,
            targets: (0..65)
                .map(|i| HttpTarget::parse(&format!("host-{i}.example")))
                .collect::<Result<_>>()?,
        },
    ] {
        let mut bytes = Vec::new();
        assert!(write_control(&mut bytes, &value).await.is_err());
        assert!(bytes.is_empty(), "invalid update wrote a prefix");
    }
    Ok(())
}
