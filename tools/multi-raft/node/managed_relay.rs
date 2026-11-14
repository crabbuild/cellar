// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded transparent local-lab bridge to the retained RustFS service.
use super::*;
use serde::Deserialize;
use std::{
    io::Read as _,
    net::{IpAddr, SocketAddr},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{watch, Semaphore},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    upstream: SocketAddr,
}

pub(super) async fn run(domain: &ExecutionDomain, options: &Options) -> Result<()> {
    let path = options.config.clone();
    let config = domain
        .blocking(move || -> Result<Configuration> {
            let mut bytes = Vec::new();
            std::fs::File::open(path)?
                .take(4097)
                .read_to_end(&mut bytes)?;
            ensure!(bytes.len() <= 4096, "relay configuration too large");
            Ok(serde_json::from_slice(&bytes)?)
        })?
        .await??;
    ensure!(
        config.upstream.port() != 0
            && matches!(config.upstream.ip(),IpAddr::V4(ip) if ip.is_private() || ip.is_loopback()),
        "explicit private lab upstream required"
    );
    let listener = TcpListener::bind("0.0.0.0:9000").await?;
    let permits = Arc::new(Semaphore::new(32));
    let (stop, _) = watch::channel(false);
    let signal = terminate();
    tokio::pin!(signal);
    output(
        json!({"managed_relay":"ready","upstream":config.upstream.to_string(),"connections":32}),
    )?;
    loop {
        let (mut incoming, _) = cellar::asyncrt::select! {
            _=&mut signal=>break,
            accepted=listener.accept()=>accepted?,
        };
        let Ok(slot) = permits.clone().try_acquire_owned() else {
            continue;
        };
        let mut stopping = stop.subscribe();
        let upstream = config.upstream;
        domain
            .spawn(async move {
                let _slot = slot;
                let exchange = async {
                    let mut outgoing = TcpStream::connect(upstream).await?;
                    tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await?;
                    Ok::<_, std::io::Error>(())
                };
                cellar::asyncrt::select! { _=stopping.changed()=>{}, _=exchange=>{} }
            })?
            .detach();
    }
    drop(listener);
    stop.send_replace(true);
    let _drained = permits.acquire_many_owned(32).await?;
    // main joins the execution domain before process exit. No credential or
    // HTTP header/body is parsed, rewritten or logged by this TCP bridge.
    output(json!({"managed_relay":"stopped"}))
}

async fn terminate() {
    #[cfg(unix)]
    if let Ok(mut signal) =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    {
        signal.recv().await;
        return;
    }
    let _ = tokio::signal::ctrl_c().await;
}
