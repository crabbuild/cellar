// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#[cfg(target_os = "linux")]
fn main() -> anyhow::Result<()> {
    use anyhow::{ensure, Context};
    use cellar_egress::helper::{self, Config};
    use std::{os::fd::AsFd, time::Duration};
    let mut args = std::env::args().skip(1);
    let mut session = None;
    let mut internet = false;
    let mut upstream = None;
    let mut http_port = 39189;
    let mut dns_port = 39190;
    let mut nft_path = std::path::PathBuf::from("/usr/sbin/nft");
    let mut operation_timeout = Duration::from_secs(300);
    let mut install_timeout = Duration::from_secs(5);
    while let Some(option) = args.next() {
        let value = args.next().context("missing helper option value")?;
        match option.as_str() {
            "--session" => {
                ensure!(
                    session.is_none() && value.len() == 64 && value.is_ascii(),
                    "invalid helper session"
                );
                let mut bytes = [0; 32];
                for (index, byte) in bytes.iter_mut().enumerate() {
                    *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
                        .context("invalid helper session")?;
                }
                session = Some(bytes);
            }
            "--internet" => internet = value.parse().context("invalid helper internet option")?,
            "--dns" => upstream = Some(value.parse().context("invalid helper DNS upstream")?),
            "--http-port" => http_port = value.parse().context("invalid helper HTTP port")?,
            "--dns-port" => dns_port = value.parse().context("invalid helper DNS port")?,
            "--nft-path" => nft_path = value.into(),
            "--operation-timeout-ms" => {
                operation_timeout = Duration::from_millis(
                    value.parse().context("invalid helper operation deadline")?,
                )
            }
            "--install-timeout-ms" => {
                install_timeout =
                    Duration::from_millis(value.parse().context("invalid helper install deadline")?)
            }
            _ => anyhow::bail!("unknown helper option"),
        }
    }
    let config = Config {
        session: session.context("missing helper session")?,
        internet,
        upstream: upstream.context("missing helper DNS upstream")?,
        http_port,
        dns_port,
        operation_timeout,
        install_timeout,
        nft_path,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        // Docker Tty=false supplies pipes. Duplicated owned descriptors become
        // nonblocking without a blocking stdin task that could outlive shutdown.
        let input = tokio::net::unix::pipe::Receiver::from_owned_fd(
            std::io::stdin().as_fd().try_clone_to_owned()?,
        )?;
        let output = tokio::net::unix::pipe::Sender::from_owned_fd(
            std::io::stdout().as_fd().try_clone_to_owned()?,
        )?;
        helper::run(tokio::io::join(input, output), config).await
    })
}

#[cfg(not(target_os = "linux"))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("the container network helper requires Linux");
}
