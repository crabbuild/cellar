// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// This external qualification process owns real kernel I/O and its runtime.
// It never executes in Cellar's injected/simulated execution domain.
#![allow(clippy::disallowed_methods)]

#[cfg(target_os = "linux")]
mod qualification {
    use anyhow::{ensure, Context, Result};
    use cellar_egress::{dns, firewall::Program, linux, HttpTarget, Rules};
    use hickory_proto::{
        op::{Message, MessageType, OpCode, Query},
        rr::{Name, RecordType},
    };
    use std::{net::SocketAddr, process::Stdio, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream, UdpSocket},
        process::Command,
    };

    const HTTP: u16 = 39189;
    const DNS: u16 = 39190;

    async fn apply(internet: bool) -> Result<()> {
        let source = Program::new(
            &[
                HttpTarget::parse("backup.internal")?,
                HttpTarget::parse("127.0.0.2:8080")?,
                HttpTarget::parse("[::1]:8080")?,
            ],
            HTTP,
            DNS,
            internet,
        )?;
        let mut child = Command::new("nft")
            .args(["-f", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let mut input = child.stdin.take().context("nft stdin missing")?;
        input.write_all(source.as_str().as_bytes()).await?;
        drop(input);
        let output = child.wait_with_output().await?;
        ensure!(
            output.status.success(),
            "nft rejected program: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    async fn tcp_redirect(destination: SocketAddr, listen: SocketAddr) -> Result<()> {
        let proxy = TcpListener::bind(listen).await?;
        let upstream = TcpListener::bind(destination).await?;
        let mut client = TcpStream::connect(destination).await?;
        let (mut accepted, _) = proxy.accept().await?;
        ensure!(
            linux::original_destination(&accepted)? == destination,
            "kernel destination mismatch"
        );
        client.write_all(b"intercepted").await?;
        let mut bytes = [0; 11];
        accepted.read_exact(&mut bytes).await?;
        ensure!(&bytes == b"intercepted");
        accepted.write_all(b"response").await?;
        let mut bytes = [0; 8];
        client.read_exact(&mut bytes).await?;
        ensure!(&bytes == b"response");
        let mut direct = linux::connect(destination).await?;
        let (mut bypassed, _) = upstream.accept().await?;
        direct.write_all(b"marked").await?;
        let mut bytes = [0; 6];
        bypassed.read_exact(&mut bytes).await?;
        ensure!(&bytes == b"marked");
        Ok(())
    }

    async fn udp_dns(upstream: SocketAddr, listen: SocketAddr) -> Result<()> {
        let proxy = UdpSocket::bind(listen).await?;
        let resolver = UdpSocket::bind(upstream).await?;
        let client = UdpSocket::bind(if upstream.is_ipv4() {
            "127.0.0.1:0"
        } else {
            "[::1]:0"
        })
        .await?;
        let mut query = Message::new(71, MessageType::Query, OpCode::Query);
        query.queries.push(Query::query(
            Name::from_ascii("backup.internal.")?,
            if upstream.is_ipv4() {
                RecordType::A
            } else {
                RecordType::AAAA
            },
        ));
        let question = query.to_vec()?;
        client.send_to(&question, upstream).await?;
        let mut bytes = [0; dns::MAX_MESSAGE_BYTES + 1];
        let (length, source) = proxy.recv_from(&mut bytes).await?;
        ensure!(&bytes[..length] == question);
        let mut rules = Rules::default();
        let candidate = rules.prepare(HttpTarget::parse("backup.internal")?, ())?;
        rules.commit(candidate)?;
        let dns::Disposition::Answer(answer) = dns::resolve(&bytes[..length], &rules, false)?
        else {
            anyhow::bail!("granted DNS query had no answer");
        };
        proxy.send_to(&answer, source).await?;
        let (length, source) = client.recv_from(&mut bytes).await?;
        ensure!(
            source == upstream,
            "DNS reverse NAT changed source identity"
        );
        ensure!(&bytes[..length] == answer);

        let marked = linux::dns_socket(upstream).await?;
        marked.send(&question).await?;
        let (length, source) = resolver.recv_from(&mut bytes).await?;
        ensure!(&bytes[..length] == question);
        resolver.send_to(&answer, source).await?;
        let length = marked.recv(&mut bytes).await?;
        ensure!(&bytes[..length] == answer);
        Ok(())
    }

    async fn remote_redirect(peer: SocketAddr) -> Result<()> {
        let destination = SocketAddr::new(peer.ip(), 80);
        let proxy = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, HTTP)).await?;
        // A non-loopback destination selects the namespace's real source IP
        // before output NAT. Do not assume loopback-only probes cover this path.
        let mut client =
            tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(destination))
                .await
                .context("offline remote redirect did not connect")??;
        let (mut accepted, _) = tokio::time::timeout(Duration::from_secs(2), proxy.accept())
            .await
            .context("remote redirect did not reach proxy")??;
        ensure!(linux::original_destination(&accepted)? == destination);
        client.write_all(b"remote").await?;
        let mut bytes = [0; 6];
        accepted.read_exact(&mut bytes).await?;
        ensure!(&bytes == b"remote");
        accepted.write_all(b"captured").await?;
        let mut bytes = [0; 8];
        client.read_exact(&mut bytes).await?;
        ensure!(&bytes == b"captured");
        Ok(())
    }

    async fn docker_dns_bypass(peer: SocketAddr) -> Result<()> {
        let sockets = std::fs::read_to_string("/proc/net/udp")?;
        let mut ports = Vec::new();
        for line in sockets.lines().skip(1) {
            let Some(local) = line.split_whitespace().nth(1) else {
                continue;
            };
            let Some((address, port)) = local.split_once(':') else {
                continue;
            };
            let address = u32::from_str_radix(address, 16)?;
            if std::net::Ipv4Addr::from(address.to_ne_bytes())
                == std::net::Ipv4Addr::new(127, 0, 0, 11)
            {
                ports.push(u16::from_str_radix(port, 16)?);
            }
        }
        ensure!(
            ports.len() == 1,
            "Docker embedded DNS socket was not identified"
        );
        let destination = SocketAddr::new("127.0.0.11".parse()?, ports[0]);
        let mut query = Message::new(21169, MessageType::Query, OpCode::Query);
        query.queries.push(Query::query(
            Name::from_ascii("cellar-egress-peer.")?,
            RecordType::A,
        ));
        let question = query.to_vec()?;
        let marked = linux::dns_socket(destination).await?;
        marked.send(&question).await?;
        let mut bytes = [0; dns::MAX_MESSAGE_BYTES + 1];
        let length = tokio::time::timeout(Duration::from_secs(2), marked.recv(&mut bytes))
            .await
            .context("Docker DNS observer timed out")??;
        dns::validate_response(&question, &bytes[..length])?;
        let answer = Message::from_vec(&bytes[..length])?;
        ensure!(answer.answers.iter().any(|record| matches!(&record.data, hickory_proto::rr::RData::A(address) if std::net::IpAddr::V4(address.0) == peer.ip())), "Docker DNS observer did not resolve the owned peer");
        let client = UdpSocket::bind("127.0.0.1:0").await?;
        match client.send_to(&question, destination).await {
            Ok(length) => ensure!(length == question.len(), "short Docker DNS query"),
            Err(error) if error.raw_os_error() == Some(nix::libc::EPERM) => return Ok(()),
            Err(error) => return Err(error).context("unexpected unmarked Docker DNS send failure"),
        }
        ensure!(
            tokio::time::timeout(Duration::from_millis(150), client.recv(&mut bytes))
                .await
                .is_err(),
            "offline application bypassed DNS policy through Docker's embedded socket"
        );
        Ok(())
    }

    async fn external_fence(peer: SocketAddr) -> Result<()> {
        // Verify the peer is live before using an unmarked failure as evidence.
        let listener = TcpListener::bind("0.0.0.0:9091").await?;
        let mut marked = linux::connect(peer).await?;
        marked.write_all(b"{\"callbackPort\":9091}\n").await?;
        let (mut ingress, _) = listener.accept().await?;
        let mut incoming = [0; 7];
        ingress.read_exact(&mut incoming).await?;
        ensure!(&incoming == b"ingress");
        ingress.write_all(b"reply").await?;
        drop(ingress);
        let mut ack = [0; 11];
        marked.read_exact(&mut ack).await?;
        ensure!(&ack == b"callback-ok");
        ensure!(
            !matches!(
                tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(peer)).await,
                Ok(Ok(_))
            ),
            "offline unmarked connection escaped fence"
        );
        apply(true).await?;
        let mut online = TcpStream::connect(peer).await?;
        online.write_all(b"{}\n").await?;
        let mut bytes = [0; 6];
        online.read_exact(&mut bytes).await?;
        ensure!(&bytes == b"online");
        // Reapply offline policy in the same namespace. The transaction must
        // replace rather than append to the original accepting chain.
        apply(false).await?;
        ensure!(
            !matches!(
                tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(peer)).await,
                Ok(Ok(_))
            ),
            "replaced offline fence retained online policy"
        );
        Ok(())
    }

    pub async fn run(peer: SocketAddr) -> Result<()> {
        apply(false).await?;
        docker_dns_bypass(peer).await?;
        tcp_redirect(
            "127.0.0.2:80".parse()?,
            format!("127.0.0.1:{HTTP}").parse()?,
        )
        .await?;
        tcp_redirect("[::1]:80".parse()?, format!("[::1]:{HTTP}").parse()?).await?;
        tcp_redirect(
            "127.0.0.2:8080".parse()?,
            format!("127.0.0.1:{HTTP}").parse()?,
        )
        .await?;
        tcp_redirect("[::1]:8080".parse()?, format!("[::1]:{HTTP}").parse()?).await?;
        udp_dns("127.0.0.2:53".parse()?, format!("127.0.0.1:{DNS}").parse()?).await?;
        udp_dns("[::1]:53".parse()?, format!("[::1]:{DNS}").parse()?).await?;
        tcp_redirect("127.0.0.2:53".parse()?, format!("127.0.0.1:{DNS}").parse()?).await?;
        tcp_redirect("[::1]:53".parse()?, format!("[::1]:{DNS}").parse()?).await?;
        remote_redirect(peer).await?;
        external_fence(peer).await?;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn main() -> anyhow::Result<()> {
    use std::io::Write;
    let peer = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("expected owned peer socket address"))?
        .parse()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(30), qualification::run(peer)).await
    })??;
    std::io::stdout().write_all(b"{\"tcp_original_destination\":\"ipv4_ipv6\",\"remote_ipv4_redirect\":true,\"docker_dns_bypass_refused\":true,\"dns_udp_redirect\":\"ipv4_ipv6\",\"dns_tcp_redirect\":\"ipv4_ipv6\",\"marked_bypass\":true,\"offline_fence\":true,\"ingress_reply\":true,\"atomic_replacement\":true}\n")?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("network qualification requires an isolated Linux namespace with NET_ADMIN");
}
