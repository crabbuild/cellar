// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::policy::Policy;
use crate::{
    dns::{self, Disposition, MAX_MESSAGE_BYTES},
    linux,
};
use anyhow::{ensure, Result};
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, UdpSocket},
};

pub(super) async fn udp(
    server: Arc<UdpSocket>,
    source: SocketAddr,
    packet: Vec<u8>,
    policy: Policy,
) -> Result<()> {
    let answer = match policy.resolve(&packet)? {
        Disposition::Answer(answer) => answer,
        Disposition::Drop => return Ok(()),
        Disposition::Forward => {
            let upstream = linux::dns_socket(policy.config.upstream).await?;
            upstream.send(&packet).await?;
            let mut answer = vec![0; MAX_MESSAGE_BYTES + 1];
            let length = upstream.recv(&mut answer).await?;
            answer.truncate(length);
            dns::validate_response(&packet, &answer)?;
            answer
        }
    };
    server.send_to(&answer, source).await?;
    Ok(())
}

pub(super) async fn tcp(mut client: TcpStream, policy: Policy) -> Result<()> {
    loop {
        let mut prefix = [0; 2];
        let first = client.read(&mut prefix[..1]).await?;
        if first == 0 {
            return Ok(());
        }
        client.read_exact(&mut prefix[1..]).await?;
        let length = u16::from_be_bytes(prefix) as usize;
        ensure!(
            length != 0 && length <= MAX_MESSAGE_BYTES,
            "DNS TCP message exceeds limit"
        );
        let mut packet = vec![0; length];
        client.read_exact(&mut packet).await?;
        let answer = match policy.resolve(&packet)? {
            Disposition::Answer(answer) => answer,
            Disposition::Drop => continue,
            Disposition::Forward => {
                let mut upstream = linux::connect(policy.config.upstream).await?;
                upstream.write_all(&prefix).await?;
                upstream.write_all(&packet).await?;
                let length = upstream.read_u16().await? as usize;
                ensure!(
                    length != 0 && length <= MAX_MESSAGE_BYTES,
                    "upstream DNS TCP message exceeds limit"
                );
                let mut answer = vec![0; length];
                upstream.read_exact(&mut answer).await?;
                dns::validate_response(&packet, &answer)?;
                answer
            }
        };
        client.write_u16(answer.len() as u16).await?;
        client.write_all(&answer).await?;
    }
}
