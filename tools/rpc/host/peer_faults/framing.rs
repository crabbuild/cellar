// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The fixture uses bounded HTTP/1 full bodies inside the real signed upgrade.
//! Unsupported framing fails qualification instead of certifying unseen bytes.
use anyhow::{ensure, Context, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub(super) const MAX_BODY: usize = 64 * 1024;
const MAX_HEAD: usize = 16 * 1024;

pub(super) struct Head {
    pub raw: Vec<u8>,
    pub first: String,
    fields: Vec<(String, String)>,
}

impl Head {
    pub fn get(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub fn replace(&mut self, name: &str, value: &str) -> Result<()> {
        let field = self
            .fields
            .iter_mut()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .context("fault header missing")?;
        field.1 = value.into();
        self.rebuild()
    }

    pub fn close_connection(&mut self) -> Result<()> {
        if self.get("connection").is_some() {
            self.replace("connection", "close")
        } else {
            self.fields.push(("connection".into(), "close".into()));
            self.rebuild()
        }
    }

    pub fn chunked(&mut self) -> Result<()> {
        ensure!(
            self.get("transfer-encoding").is_none(),
            "reply is already chunked"
        );
        self.fields
            .retain(|(name, _)| !name.eq_ignore_ascii_case("content-length"));
        self.fields
            .push(("transfer-encoding".into(), "chunked".into()));
        self.rebuild()
    }

    fn rebuild(&mut self) -> Result<()> {
        self.raw = format!("{}\r\n", self.first).into_bytes();
        for (key, value) in &self.fields {
            self.raw
                .extend_from_slice(format!("{key}: {value}\r\n").as_bytes());
        }
        self.raw.extend_from_slice(b"\r\n");
        ensure!(
            self.raw.len() <= MAX_HEAD,
            "relay replacement header exceeds bound"
        );
        Ok(())
    }
}

pub(super) async fn head(reader: &mut (impl AsyncRead + Unpin)) -> Result<Option<Head>> {
    let mut raw = Vec::new();
    while !raw.ends_with(b"\r\n\r\n") {
        ensure!(raw.len() < MAX_HEAD, "relay header exceeds bound");
        match reader.read_u8().await {
            Ok(byte) => raw.push(byte),
            Err(error) if raw.is_empty() && error.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None)
            }
            Err(error) => return Err(error.into()),
        }
    }
    let text = std::str::from_utf8(&raw)?;
    let mut lines = text.split("\r\n");
    let first = lines
        .next()
        .context("relay first line missing")?
        .to_string();
    let mut fields: Vec<(String, String)> = Vec::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':').context("malformed relay header")?;
        ensure!(
            !fields.iter().any(|(key, _)| key.eq_ignore_ascii_case(name)),
            "duplicate relay header"
        );
        fields.push((name.into(), value.trim().into()));
    }
    Ok(Some(Head { raw, first, fields }))
}

pub(super) async fn body(head: &Head, reader: &mut (impl AsyncRead + Unpin)) -> Result<Vec<u8>> {
    ensure!(
        head.get("transfer-encoding").is_none(),
        "relay fixture requires full-body framing"
    );
    let size = head.get("content-length").unwrap_or("0").parse::<usize>()?;
    ensure!(size <= MAX_BODY, "relay body exceeds fixture bound");
    let mut body = vec![0; size];
    reader.read_exact(&mut body).await?;
    Ok(body)
}

pub(super) async fn write(
    writer: &mut (impl AsyncWrite + Unpin),
    head: &Head,
    body: &[u8],
) -> Result<()> {
    writer.write_all(&head.raw).await?;
    writer.write_all(body).await?;
    Ok(())
}

#[cfg(test)]
#[path = "framing_tests.rs"]
mod tests;
