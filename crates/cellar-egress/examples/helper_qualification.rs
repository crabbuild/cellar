// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// The external qualifier owns a real Linux namespace and runtime.
#![allow(clippy::disallowed_methods)]

#[cfg(target_os = "linux")]
mod qualification {
    use anyhow::{ensure, Context, Result};
    use cellar_egress::{
        control, dns,
        helper::Config,
        linux,
        transport::{self, Stream},
        wire::{self, DirectRefusal, DirectReply, Open},
        Destination, Host, HttpTarget,
    };
    use hickory_proto::{
        op::{Message, MessageType, OpCode, Query},
        rr::{
            rdata::{A, AAAA},
            Name, RData, Record, RecordType,
        },
    };
    use serde::Deserialize;
    use std::{
        net::{Ipv4Addr, Ipv6Addr, SocketAddr},
        process::Stdio,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpStream, UdpSocket},
        process::Command,
        sync::watch,
    };

    const REQUEST: &[u8] = b"GET /gateway HTTP/1.1\r\nHost: backup.internal\r\n\r\n";

    struct Retirement<T>(control::Routes<T>);

    impl<T> Drop for Retirement<T> {
        fn drop(&mut self) {
            self.0.retire();
        }
    }

    fn question(name: &str, kind: RecordType, id: u16) -> Result<Vec<u8>> {
        let mut query = Message::new(id, MessageType::Query, OpCode::Query);
        query
            .queries
            .push(Query::query(Name::from_ascii(format!("{name}."))?, kind));
        Ok(query.to_vec()?)
    }

    async fn udp(name: &str, kind: RecordType, expected: Option<RData>) -> Result<()> {
        let client = UdpSocket::bind("127.0.0.1:0").await?;
        let request = question(name, kind, 93)?;
        client.send_to(&request, "127.0.0.3:53").await?;
        let mut bytes = [0; dns::MAX_MESSAGE_BYTES + 1];
        let reply =
            tokio::time::timeout(Duration::from_millis(150), client.recv_from(&mut bytes)).await;
        let Some(expected) = expected else {
            ensure!(
                reply.is_err(),
                "ungranted or forged DNS answer escaped refusal"
            );
            return Ok(());
        };
        let (length, source) = reply??;
        ensure!(source == "127.0.0.3:53".parse()?);
        dns::validate_response(&request, &bytes[..length])?;
        let reply = Message::from_vec(&bytes[..length])?;
        ensure!(reply.answers.len() == 1 && reply.answers[0].data == expected);
        Ok(())
    }

    async fn tcp_query(socket: &mut TcpStream, name: &str, id: u16) -> Result<()> {
        let query = question(name, RecordType::A, id)?;
        socket.write_u16(query.len() as u16).await?;
        socket.write_all(&query).await?;
        let length = socket.read_u16().await? as usize;
        ensure!(length <= dns::MAX_MESSAGE_BYTES);
        let mut reply = vec![0; length];
        socket.read_exact(&mut reply).await?;
        dns::validate_response(&query, &reply)?;
        let reply = Message::from_vec(&reply)?;
        ensure!(
            reply.answers.len() == 1
                && reply.answers[0].data == RData::A(A(Ipv4Addr::new(127, 0, 0, 2)))
        );
        Ok(())
    }

    async fn upstream(server: UdpSocket, internet: bool, sent: Arc<AtomicUsize>) -> Result<()> {
        if !internet {
            return Ok(());
        }
        for forged in [false, true] {
            let mut bytes = [0; dns::MAX_MESSAGE_BYTES + 1];
            let (length, source) = server.recv_from(&mut bytes).await?;
            let request = Message::from_vec(&bytes[..length])?;
            let mut reply =
                Message::response(request.metadata.id + u16::from(forged), OpCode::Query);
            reply.queries = request.queries;
            reply.answers.push(Record::from_rdata(
                reply.queries[0].name().clone(),
                0,
                RData::A(A(Ipv4Addr::new(192, 0, 2, 7))),
            ));
            server.send_to(&reply.to_vec()?, source).await?;
            sent.fetch_add(1, Ordering::Release);
        }
        Ok(())
    }

    #[derive(Deserialize)]
    struct Metrics {
        connections: u64,
        requests: u64,
    }

    async fn metrics(peer: SocketAddr) -> Result<Metrics> {
        // Independent marked observer: detect a forbidden connection even if
        // no application bytes were sent and the caller only observed EOF.
        let mut socket = linux::connect(peer).await?;
        socket.write_all(b"{\"count\":true}\n").await?;
        let mut bytes = Vec::new();
        socket.take(1024).read_to_end(&mut bytes).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    async fn fallback(link: &transport::Link, peer: SocketAddr, internet: bool) -> Result<()> {
        let before = metrics(peer).await?;
        let mut stream = link.open().await?;
        wire::write_open(
            &mut stream,
            &Open::Direct {
                host: peer.ip().to_string(),
                port: peer.port(),
            },
        )
        .await?;
        let reply = wire::read_direct_reply(&mut stream).await?;
        if internet {
            ensure!(reply == DirectReply::Connected);
            stream.write_all(b"{}\n").await?;
            let mut bytes = [0; 6];
            stream.read_exact(&mut bytes).await?;
            ensure!(&bytes == b"online");
        } else {
            ensure!(
                reply
                    == DirectReply::Refused {
                        reason: DirectRefusal::InternetDisabled
                    }
            );
            let mut byte = [0];
            ensure!(
                stream.read(&mut byte).await? == 0,
                "offline fallback returned data"
            );
        }
        drop(stream);
        let after = metrics(peer).await?;
        ensure!(
            after.requests == before.requests + u64::from(internet),
            "denied fallback reached provider effects"
        );
        ensure!(
            after.connections == before.connections + 1 + u64::from(internet),
            "denied fallback opened an upstream connection"
        );
        Ok(())
    }

    async fn refused_destinations(link: &transport::Link, peer: SocketAddr) -> Result<()> {
        for (host, port, reason) in [
            (
                "unresolved.invalid",
                peer.port(),
                DirectRefusal::UnsupportedHost,
            ),
            ("127.0.0.1", 39189, DirectRefusal::ReservedDestination),
        ] {
            let before = metrics(peer).await?;
            let mut stream = link.open().await?;
            wire::write_open(
                &mut stream,
                &Open::Direct {
                    host: host.to_owned(),
                    port,
                },
            )
            .await?;
            ensure!(wire::read_direct_reply(&mut stream).await? == DirectReply::Refused { reason });
            drop(stream);
            let after = metrics(peer).await?;
            ensure!(
                after.requests == before.requests && after.connections == before.connections + 1,
                "refused destination opened an upstream connection"
            );
        }
        Ok(())
    }

    async fn gateway(
        mut stream: Stream,
        routes: &control::Routes<String>,
        destination: SocketAddr,
    ) -> Result<()> {
        ensure!(wire::read_open(&mut stream).await? == Open::Intercept { destination });
        for _ in 0..2 {
            let mut request = vec![0; REQUEST.len()];
            stream.read_exact(&mut request).await?;
            ensure!(request == REQUEST);
            let host = Host::parse("backup.internal")?;
            let handler = routes
                .resolve(&Destination {
                    protocol: cellar_egress::Protocol::Http,
                    socket: destination,
                    host: Some(&host),
                })
                .context("missing committed native route")?;
            let reply = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                handler.len(),
                handler
            );
            stream.write_all(reply.as_bytes()).await?;
        }
        stream.shutdown().await?;
        Ok(())
    }

    pub async fn run(peer: SocketAddr, internet: bool) -> Result<()> {
        let config = Config {
            https: false,
            session: [72; 32],
            internet,
            upstream: "127.0.0.3:53".parse()?,
            http_port: 39189,
            dns_port: 39190,
            operation_timeout: Duration::from_secs(2),
            install_timeout: Duration::from_secs(5),
            nft_path: "/usr/sbin/nft".into(),
        };
        let resolver = UdpSocket::bind(config.upstream).await?;
        let executable = std::env::current_exe()?
            .parent()
            .and_then(|path| path.parent())
            .context("missing helper binary directory")?
            .join("cellar-egress");
        let session = config
            .session
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let mut child = Command::new(executable)
            .args([
                "--session",
                &session,
                "--internet",
                if internet { "true" } else { "false" },
                "--dns",
                &config.upstream.to_string(),
                "--operation-timeout-ms",
                &config.operation_timeout.as_millis().to_string(),
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let host = tokio::io::join(
            child.stdout.take().context("missing helper stdout")?,
            child.stdin.take().context("missing helper stdin")?,
        );
        let helper = async {
            let output = child.wait_with_output().await?;
            ensure!(
                output.status.success(),
                "helper process failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            Ok::<_, anyhow::Error>(())
        };
        let (link, mut incoming, driver) = transport::session(host, transport::Role::Host);
        let (installer, routes, controller) = control::channel(link.clone(), config.session);
        let completed = Arc::new(AtomicBool::new(false));
        let finished = completed.clone();
        let sent = Arc::new(AtomicUsize::new(0));
        let (stop_dns, mut stop) = watch::channel(false);
        let destination: SocketAddr = if internet { "[::1]:80" } else { "127.0.0.2:80" }.parse()?;
        let native = async {
            let _retire = Retirement(routes.clone());
            let control = incoming
                .next()
                .await
                .context("helper omitted control stream")?;
            let data = async {
                let stream = incoming
                    .next()
                    .await
                    .context("helper omitted intercepted stream")?;
                gateway(stream, &routes, destination).await?;
                ensure!(
                    incoming.next().await.is_none(),
                    "unexpected later intercepted stream"
                );
                Ok::<_, anyhow::Error>(())
            };
            let (control, data) = tokio::join!(controller.run(control), data);
            control?;
            data?;
            Ok::<_, anyhow::Error>(())
        };
        let application = async {
            let _retire = Retirement(routes.clone());
            installer.ready().await?;
            installer
                .install(HttpTarget::parse("backup.internal")?, "old".to_owned())
                .await?;
            udp(
                "backup.internal",
                RecordType::A,
                Some(RData::A(A(Ipv4Addr::new(127, 0, 0, 2)))),
            )
            .await?;
            udp(
                "backup.internal",
                RecordType::AAAA,
                Some(RData::AAAA(AAAA(Ipv6Addr::LOCALHOST))),
            )
            .await?;
            let mut dns = TcpStream::connect("127.0.0.3:53").await?;
            tcp_query(&mut dns, "backup.internal", 11).await?;
            installer
                .install(HttpTarget::parse("later.internal")?, "later".to_owned())
                .await?;
            tcp_query(&mut dns, "later.internal", 12).await?;
            drop(dns);
            let mut socket = TcpStream::connect(destination).await?;
            for handler in ["old", "new"] {
                if handler == "new" {
                    installer
                        .install(HttpTarget::parse("backup.internal")?, handler.to_owned())
                        .await?;
                }
                socket.write_all(REQUEST).await?;
                let expected = format!("HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n{handler}");
                let mut reply = vec![0; expected.len()];
                socket.read_exact(&mut reply).await?;
                ensure!(
                    reply == expected.as_bytes(),
                    "live connection retained old handler"
                );
            }
            socket.shutdown().await?;
            drop(socket);
            fallback(&link, peer, internet).await?;
            if internet {
                refused_destinations(&link, peer).await?;
                udp(
                    "forwarded.internal",
                    RecordType::A,
                    Some(RData::A(A(Ipv4Addr::new(192, 0, 2, 7)))),
                )
                .await?;
                udp("forged.internal", RecordType::A, None).await?;
                ensure!(
                    sent.load(Ordering::Acquire) == 2,
                    "upstream forgery case was not exercised"
                );
            } else {
                udp("ungranted.internal", RecordType::A, None).await?;
            }
            // Malformed application queries cannot tear down the run.
            let bad = UdpSocket::bind("127.0.0.1:0").await?;
            bad.send_to(&[0; 12], "127.0.0.3:53").await?;
            udp(
                "backup.internal",
                RecordType::A,
                Some(RData::A(A(Ipv4Addr::new(127, 0, 0, 2)))),
            )
            .await?;
            installer
                .install(HttpTarget::parse("backup.internal")?, "new".to_owned())
                .await?;
            finished.store(true, Ordering::Release);
            routes.retire();
            Ok::<_, anyhow::Error>(())
        };
        let application = async {
            let result = application.await;
            stop_dns.send_replace(true);
            result
        };
        let resolver = async {
            let work = Box::pin(upstream(resolver, internet, sent.clone()));
            let closed = Box::pin(stop.changed());
            match futures_util::future::select(closed, work).await {
                futures_util::future::Either::Left(_) => Ok(()),
                futures_util::future::Either::Right((result, _)) => result,
            }
        };
        let (helper, driver, native, application, resolver) =
            tokio::join!(helper, driver.run(), native, application, resolver);
        application?;
        native?;
        driver?;
        resolver?;
        helper?;
        ensure!(completed.load(Ordering::Acquire));
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn main() -> anyhow::Result<()> {
    use std::io::Write;
    let peer = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("expected owned peer address"))?
        .parse()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            qualification::run(peer, false).await?;
            qualification::run(peer, true).await
        })
        .await?
    })?;
    std::io::stdout().write_all(b"{\"helper_control\":true,\"persistent_dns_tcp\":true,\"live_rule_replacement\":true,\"dns_forward_identity\":true,\"offline_no_effect\":true,\"joined_shutdown\":true}\n")?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("helper qualification requires Linux NET_ADMIN");
}
