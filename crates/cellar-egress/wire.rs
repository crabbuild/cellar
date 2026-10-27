// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Version-one stream preludes and control updates. HTTP bytes follow a data
//! prelude unchanged; this layer never buffers a complete HTTP body.

use crate::{HttpTarget, MAX_INTERCEPT_ENTRIES};
use anyhow::{ensure, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::HashSet,
    net::{IpAddr, SocketAddr},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const FORMAT: u8 = 1;
pub const MAX_CONTROL_BYTES: usize = 64 * 1024;
const PREFIX: &[u8; 8] = b"CELLEG\0\x01";

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Open {
    Control { session: [u8; 32] },
    Intercept { destination: SocketAddr },
    Direct { host: String, port: u16 },
}

impl Open {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Intercept { destination } => {
                ensure!(destination.port() != 0, "invalid intercepted port")
            }
            Self::Direct { host, port } => {
                ensure!(*port != 0, "invalid direct port");
                if host.parse::<IpAddr>().is_err() {
                    crate::Host::parse(host)?;
                }
            }
            Self::Control { .. } => {}
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Control {
    Install {
        revision: u64,
        targets: Vec<HttpTarget>,
    },
    Installed {
        revision: u64,
    },
}

impl Control {
    fn validate(&self) -> Result<()> {
        let revision = match self {
            Self::Install { revision, .. } | Self::Installed { revision } => *revision,
        };
        ensure!(revision != 0, "invalid control revision");
        if let Self::Install { targets, .. } = self {
            ensure!(
                targets.len() <= MAX_INTERCEPT_ENTRIES,
                "too many intercept targets"
            );
            ensure!(
                targets.iter().map(HttpTarget::entries).sum::<usize>() <= MAX_INTERCEPT_ENTRIES,
                "too many intercept entries"
            );
            let mut unique = HashSet::new();
            ensure!(
                targets.iter().all(|target| unique.insert(target.as_str())),
                "duplicate intercept target"
            );
        }
        Ok(())
    }
}

pub async fn write_open(io: &mut (impl AsyncWrite + Unpin), value: &Open) -> Result<()> {
    value.validate()?;
    let bytes = encode(value)?;
    io.write_all(PREFIX).await?;
    write_encoded(io, &bytes).await
}

pub async fn read_open(io: &mut (impl AsyncRead + Unpin)) -> Result<Open> {
    let mut prefix = [0; 8];
    io.read_exact(&mut prefix).await?;
    ensure!(prefix == *PREFIX, "incompatible egress stream prelude");
    let value: Open = read(io).await?;
    value.validate()?;
    Ok(value)
}

pub async fn write_control(io: &mut (impl AsyncWrite + Unpin), value: &Control) -> Result<()> {
    value.validate()?;
    write_encoded(io, &encode(value)?).await
}

pub async fn read_control(io: &mut (impl AsyncRead + Unpin)) -> Result<Control> {
    let value: Control = read(io).await?;
    value.validate()?;
    Ok(value)
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    struct Bounded(Vec<u8>);

    impl std::io::Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > MAX_CONTROL_BYTES.saturating_sub(self.0.len()) {
                return Err(std::io::Error::other("egress control frame exceeds limit"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut output = Bounded(Vec::new());
    serde_json::to_writer(&mut output, value)?;
    Ok(output.0)
}

async fn write_encoded(io: &mut (impl AsyncWrite + Unpin), bytes: &[u8]) -> Result<()> {
    io.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    io.write_all(bytes).await?;
    io.flush().await?;
    Ok(())
}

async fn read<T: DeserializeOwned>(io: &mut (impl AsyncRead + Unpin)) -> Result<T> {
    let length = io.read_u32().await? as usize;
    ensure!(
        length != 0 && length <= MAX_CONTROL_BYTES,
        "egress control frame exceeds limit"
    );
    let mut bytes = vec![0; length];
    io.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

#[cfg(test)]
#[path = "wire_tests.rs"]
mod tests;
