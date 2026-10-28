// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(clippy::disallowed_methods)]

use super::*;
use crate::{transport, Host};
use std::{sync::atomic::AtomicUsize, time::Duration};
use tokio::io::{AsyncReadExt, DuplexStream};

const SESSION: [u8; 32] = [43; 32];

fn setup<T>() -> (
    Installer<T>,
    Routes<T>,
    Controller<T>,
    transport::Driver<DuplexStream>,
) {
    let (io, _peer) = tokio::io::duplex(1024);
    let (link, _incoming, driver) = transport::session(io, transport::Role::Host);
    let (client, routes, controller) = channel(link, SESSION);
    (client, routes, controller, driver)
}

fn target() -> HttpTarget {
    HttpTarget::parse("backup.internal").unwrap()
}

fn route(routes: &Routes<String>) -> Option<Arc<String>> {
    routes.resolve(&Destination {
        socket: "127.0.0.2:80".parse().unwrap(),
        host: Some(&Host::parse("backup.internal").unwrap()),
    })
}

async fn read_install(io: &mut DuplexStream, expected: u64) -> Result<()> {
    ensure!(
        wire::read_control(io).await?
            == Control::Install {
                revision: expected,
                targets: vec![target()]
            },
        "unexpected installation"
    );
    Ok(())
}

async fn ack(io: &mut DuplexStream, revision: u64) -> Result<()> {
    wire::write_control(io, &Control::Installed { revision }).await
}

#[tokio::test]
async fn replacement_becomes_visible_only_after_ack_and_retirement_joins_waiters() -> Result<()> {
    let (client, routes, controller, _driver) = setup();
    let (host, mut peer) = tokio::io::duplex(1024);
    tokio::time::timeout(Duration::from_secs(3), async {
        let native = controller.run(host);
        let application = async {
            client.ready().await?;
            client.install(target(), "old".to_owned()).await?;
            let retained = route(&routes).unwrap();
            client.install(target(), "new".to_owned()).await?;
            ensure!(*route(&routes).unwrap() == "new");
            ensure!(*retained == "old");
            routes.retire();
            ensure!(route(&routes).is_none());
            ensure!(client.ready().await.is_err());
            ensure!(client.install(target(), "late".to_owned()).await.is_err());
            Ok::<_, anyhow::Error>(())
        };
        let helper = async {
            wire::write_open(&mut peer, &Open::Control { session: SESSION }).await?;
            read_install(&mut peer, 1).await?;
            ensure!(route(&routes).is_none());
            ack(&mut peer, 1).await?;
            read_install(&mut peer, 2).await?;
            ensure!(*route(&routes).unwrap() == "old");
            ack(&mut peer, 2).await?;
            ensure!(peer.read(&mut [0]).await? == 0);
            Ok::<_, anyhow::Error>(())
        };
        let (native, application, helper) = tokio::join!(native, application, helper);
        native?;
        application?;
        helper?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn cancelled_waiter_does_not_cancel_entered_installation() -> Result<()> {
    let (client, routes, controller, _driver) = setup();
    let (host, mut peer) = tokio::io::duplex(1024);
    let (entered, started) = oneshot::channel();
    let (release, released) = oneshot::channel();
    tokio::time::timeout(Duration::from_secs(3), async {
        let application = async {
            client.ready().await?;
            let mut waiting = Box::pin(client.install(target(), "cancelled waiter".to_owned()));
            ensure!(futures_util::poll!(waiting.as_mut()).is_pending());
            started.await?;
            drop(waiting);
            release
                .send(())
                .map_err(|_| anyhow!("helper stopped before release"))?;
            // This queued replacement is also a barrier for the first commit.
            client.install(target(), "replacement".to_owned()).await?;
            ensure!(*route(&routes).unwrap() == "replacement");
            routes.retire();
            Ok::<_, anyhow::Error>(())
        };
        let helper = async {
            wire::write_open(&mut peer, &Open::Control { session: SESSION }).await?;
            read_install(&mut peer, 1).await?;
            entered
                .send(())
                .map_err(|_| anyhow!("application stopped before entry"))?;
            released.await?;
            ack(&mut peer, 1).await?;
            read_install(&mut peer, 2).await?;
            ensure!(*route(&routes).unwrap() == "cancelled waiter");
            ack(&mut peer, 2).await?;
            ensure!(peer.read(&mut [0]).await? == 0);
            Ok::<_, anyhow::Error>(())
        };
        let (native, application, helper) = tokio::join!(controller.run(host), application, helper);
        native?;
        application?;
        helper?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn missing_or_wrong_ack_retires_rules_and_refuses_future_effects() -> Result<()> {
    for wrong_ack in [false, true] {
        let (client, routes, controller, _driver) = setup();
        let (host, mut peer) = tokio::io::duplex(1024);
        tokio::time::timeout(Duration::from_secs(3), async {
            let application = async {
                client.ready().await?;
                let error = client
                    .install(target(), "never committed".to_owned())
                    .await
                    .unwrap_err();
                ensure!(error.to_string().contains("outcome is unknown"));
                ensure!(route(&routes).is_none());
                ensure!(client.install(target(), "late".to_owned()).await.is_err());
                Ok::<_, anyhow::Error>(())
            };
            let helper = async {
                wire::write_open(&mut peer, &Open::Control { session: SESSION }).await?;
                read_install(&mut peer, 1).await?;
                if wrong_ack {
                    ack(&mut peer, 2).await?;
                }
                drop(peer);
                Ok::<_, anyhow::Error>(())
            };
            let (native, application, helper) =
                tokio::join!(controller.run(host), application, helper);
            ensure!(native.is_err());
            application?;
            helper?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    }
    Ok(())
}

#[tokio::test]
async fn queued_work_and_handshake_are_bounded_and_drop_retires_synchronously() -> Result<()> {
    let (client, routes, controller, _driver) = setup();
    let mut waiting = Vec::new();
    for _ in 0..PENDING_INSTALLS {
        let mut future = Box::pin(client.install(target(), "queued".to_owned()));
        ensure!(futures_util::poll!(future.as_mut()).is_pending());
        waiting.push(future);
    }
    ensure!(client
        .install(target(), "overflow".to_owned())
        .await
        .unwrap_err()
        .to_string()
        .contains("queue"));
    drop(controller);
    ensure!(route(&routes).is_none());
    ensure!(client.ready().await.is_err());
    for future in waiting {
        ensure!(future.await.is_err());
    }

    let (client, routes, controller, _driver) = setup::<String>();
    let (host, mut peer) = tokio::io::duplex(1024);
    let (native, helper) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(
            controller.run(host),
            wire::write_open(&mut peer, &Open::Control { session: [44; 32] })
        )
    })
    .await?;
    ensure!(native.is_err());
    helper?;
    ensure!(client.ready().await.is_err());
    ensure!(route(&routes).is_none());
    Ok(())
}

#[tokio::test]
async fn retirement_interrupts_unacknowledged_installation_and_joins_control() -> Result<()> {
    let (client, routes, controller, _driver) = setup();
    let (host, mut peer) = tokio::io::duplex(1024);
    let (entered, started) = oneshot::channel();
    tokio::time::timeout(Duration::from_secs(3), async {
        let application = async {
            client.ready().await?;
            let mut waiting = Box::pin(client.install(target(), "unacknowledged".to_owned()));
            ensure!(futures_util::poll!(waiting.as_mut()).is_pending());
            started.await?;
            routes.retire();
            ensure!(waiting.await.is_err());
            ensure!(route(&routes).is_none());
            Ok::<_, anyhow::Error>(())
        };
        let helper = async {
            wire::write_open(&mut peer, &Open::Control { session: SESSION }).await?;
            read_install(&mut peer, 1).await?;
            entered
                .send(())
                .map_err(|_| anyhow!("application stopped before entry"))?;
            ensure!(peer.read(&mut [0]).await? == 0);
            Ok::<_, anyhow::Error>(())
        };
        let (native, application, helper) = tokio::join!(controller.run(host), application, helper);
        native?;
        application?;
        helper?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn idle_peer_disconnect_retires_routes_and_joins_controller() -> Result<()> {
    let (client, routes, controller, _driver) = setup::<String>();
    let (host, mut peer) = tokio::io::duplex(1024);
    let (ready, wait_ready) = oneshot::channel();
    tokio::time::timeout(Duration::from_secs(3), async {
        let application = async {
            client.ready().await?;
            ready
                .send(())
                .map_err(|_| anyhow!("helper stopped before readiness"))?;
            Ok::<_, anyhow::Error>(())
        };
        let helper = async {
            wire::write_open(&mut peer, &Open::Control { session: SESSION }).await?;
            wait_ready.await?;
            drop(peer);
            Ok::<_, anyhow::Error>(())
        };
        let (native, application, helper) = tokio::join!(controller.run(host), application, helper);
        ensure!(native.is_err());
        application?;
        helper?;
        ensure!(client.ready().await.is_err());
        ensure!(route(&routes).is_none());
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn helper_acknowledges_only_completed_monotonic_network_changes() -> Result<()> {
    let (mut host, peer) = tokio::io::duplex(1024);
    let effects = Arc::new(AtomicUsize::new(0));
    let installed = effects.clone();
    let (entered, started) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let mut entered = Some(entered);
    let mut released = Some(released);
    tokio::time::timeout(Duration::from_secs(3), async {
        let helper = serve(peer, SESSION, move |_, targets| {
            let effects = installed.clone();
            let entered = entered.take().unwrap();
            let released = released.take().unwrap();
            async move {
                ensure!(targets == [target()]);
                entered.send(()).map_err(|_| anyhow!("host disappeared"))?;
                released.await?;
                effects.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        });
        let native = async {
            ensure!(wire::read_open(&mut host).await? == Open::Control { session: SESSION });
            wire::write_control(
                &mut host,
                &Control::Install {
                    revision: 1,
                    targets: vec![target()],
                },
            )
            .await?;
            started.await?;
            let mut ack_wait = Box::pin(wire::read_control(&mut host));
            ensure!(futures_util::poll!(ack_wait.as_mut()).is_pending());
            release
                .send(())
                .map_err(|_| anyhow!("helper disappeared"))?;
            ensure!(ack_wait.await? == Control::Installed { revision: 1 });
            ensure!(effects.load(Ordering::SeqCst) == 1);
            wire::write_control(
                &mut host,
                &Control::Install {
                    revision: 1,
                    targets: vec![target()],
                },
            )
            .await?;
            ensure!(host.read(&mut [0]).await? == 0);
            Ok::<_, anyhow::Error>(())
        };
        let (helper, native) = tokio::join!(helper, native);
        ensure!(helper.unwrap_err().to_string().contains("stale"));
        native?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}
