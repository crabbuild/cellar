// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::Config;
use anyhow::Result;
use futures_util::future::BoxFuture;
use std::{net::SocketAddr, sync::Arc};
use tokio::net::{TcpListener, TcpStream, UdpSocket};

#[derive(Clone)]
pub(super) enum Listener {
    Http(Arc<TcpListener>),
    DnsTcp(Arc<TcpListener>),
    DnsUdp(Arc<UdpSocket>),
}

pub(super) enum Packet {
    Http(TcpStream),
    DnsTcp(TcpStream),
    DnsUdp {
        server: Arc<UdpSocket>,
        source: SocketAddr,
        bytes: Vec<u8>,
    },
}

impl Listener {
    pub async fn bind(config: &Config) -> Result<Vec<Self>> {
        let mut listeners = Vec::new();
        for address in ["127.0.0.1", "::1"] {
            let ip = address.parse()?;
            listeners.push(Self::Http(Arc::new(
                TcpListener::bind(SocketAddr::new(ip, config.http_port)).await?,
            )));
            let dns = SocketAddr::new(ip, config.dns_port);
            listeners.push(Self::DnsTcp(Arc::new(TcpListener::bind(dns).await?)));
            listeners.push(Self::DnsUdp(Arc::new(UdpSocket::bind(dns).await?)));
        }
        Ok(listeners)
    }

    pub fn next(self, index: usize) -> BoxFuture<'static, (usize, Result<Packet>)> {
        Box::pin(async move {
            let packet = async {
                Ok(match self {
                    Self::Http(listener) => Packet::Http(listener.accept().await?.0),
                    Self::DnsTcp(listener) => Packet::DnsTcp(listener.accept().await?.0),
                    Self::DnsUdp(server) => {
                        let mut bytes = vec![0; crate::dns::MAX_MESSAGE_BYTES + 1];
                        let (length, source) = server.recv_from(&mut bytes).await?;
                        bytes.truncate(length);
                        Packet::DnsUdp {
                            server,
                            source,
                            bytes,
                        }
                    }
                })
            }
            .await;
            (index, packet)
        })
    }
}
