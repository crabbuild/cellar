// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Kernel socket plumbing for the Linux network helper. These calls run only
//! in its private network namespace and require the helper's NET_ADMIN grant.

use crate::firewall::SOCKET_MARK;
use anyhow::{ensure, Context, Result};
use nix::sys::socket::{getsockopt, setsockopt, sockopt};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
use tokio::net::{TcpSocket, TcpStream, UdpSocket};

/// Read the kernel's pre-redirect destination. Never infer IP/CIDR authority
/// from the untrusted HTTP Host header or the helper's listening address.
pub fn original_destination(stream: &TcpStream) -> Result<SocketAddr> {
    let address = if stream.local_addr()?.is_ipv4() {
        let raw =
            getsockopt(stream, sockopt::OriginalDst).context("read original IPv4 destination")?;
        SocketAddr::new(
            IpAddr::V4(Ipv4Addr::from(raw.sin_addr.s_addr.to_ne_bytes())),
            u16::from_be(raw.sin_port),
        )
    } else {
        let raw = getsockopt(stream, sockopt::Ip6tOriginalDst)
            .context("read original IPv6 destination")?;
        SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::from(raw.sin6_addr.s6_addr),
            u16::from_be(raw.sin6_port),
            u32::from_be(raw.sin6_flowinfo),
            raw.sin6_scope_id,
        ))
    };
    ensure!(address.port() != 0, "original destination has no port");
    Ok(address)
}

/// Connect only to a validated socket address supplied by the native router.
/// The caller enforces internet policy, reserved destinations and deadlines.
pub async fn connect(address: SocketAddr) -> Result<TcpStream> {
    let socket = if address.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    setsockopt(&socket, sockopt::Mark, &SOCKET_MARK).context("mark direct TCP socket")?;
    Ok(socket.connect(address).await?)
}

/// DNS forwarding uses the helper's explicitly configured upstream in the
/// same namespace. Mark before connecting so forwarding cannot re-enter DNS.
pub async fn dns_socket(upstream: SocketAddr) -> Result<UdpSocket> {
    let bind = if upstream.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(bind).await?;
    setsockopt(&socket, sockopt::Mark, &SOCKET_MARK).context("mark upstream DNS socket")?;
    socket.connect(upstream).await?;
    Ok(socket)
}
