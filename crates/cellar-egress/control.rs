// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Run-owned rule installation. The owner drives and joins `Controller::run`;
//! cancelling an individual installer does not cancel an entered installation.

use crate::{
    transport::Link,
    wire::{self, Control, Open},
    Destination, HttpTarget, Rules,
};
use anyhow::{anyhow, ensure, Result};
use futures_util::future::poll_fn;
use std::{
    future::Future,
    pin::pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot, watch},
};

pub const PENDING_INSTALLS: usize = 8;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Status {
    Starting,
    Ready,
    Closed,
}

struct Shared<T> {
    rules: Mutex<Rules<T>>,
    retired: AtomicBool,
    status: watch::Sender<Status>,
    link: Link,
}

impl<T> Shared<T> {
    fn retire(&self) {
        if self.retired.swap(true, Ordering::AcqRel) {
            return;
        }
        // Exhaustion also clears the table and permanently refuses new rules.
        let _ = self.rules.lock().unwrap().retire();
        self.status.send_replace(Status::Closed);
        self.link.close();
    }
}

struct Update<T> {
    target: HttpTarget,
    handler: T,
    reply: oneshot::Sender<Result<()>>,
}

pub struct Installer<T> {
    updates: mpsc::Sender<Update<T>>,
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Installer<T> {
    fn clone(&self) -> Self {
        Self {
            updates: self.updates.clone(),
            shared: self.shared.clone(),
        }
    }
}

/// A short synchronous lookup for each request, including requests on an
/// existing HTTP connection. Retained handlers belong only to accepted work.
pub struct Routes<T>(Arc<Shared<T>>);

impl<T> Clone for Routes<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Routes<T> {
    pub fn resolve(&self, destination: &Destination<'_>) -> Option<Arc<T>> {
        if self.0.retired.load(Ordering::Acquire) {
            return None;
        }
        self.0.rules.lock().unwrap().resolve(destination)
    }

    /// Synchronous retirement also wakes a controller waiting for an ack or a
    /// new command. The owner must still join that controller and transport.
    pub fn retire(&self) {
        self.0.retire();
    }
}

pub struct Controller<T> {
    receive: mpsc::Receiver<Update<T>>,
    shared: Arc<Shared<T>>,
    session: [u8; 32],
}

/// Create one controller per container run. The session is an engine-owned
/// identity check on the private attach stream, not an application credential.
pub fn channel<T>(link: Link, session: [u8; 32]) -> (Installer<T>, Routes<T>, Controller<T>) {
    let shared = Arc::new(Shared {
        rules: Mutex::new(Rules::default()),
        retired: AtomicBool::new(false),
        status: watch::channel(Status::Starting).0,
        link,
    });
    let (updates, receive) = mpsc::channel(PENDING_INSTALLS);
    (
        Installer {
            updates,
            shared: shared.clone(),
        },
        Routes(shared.clone()),
        Controller {
            receive,
            shared,
            session,
        },
    )
}

impl<T> Installer<T> {
    pub async fn ready(&self) -> Result<()> {
        let mut status = self.shared.status.subscribe();
        let state = *status
            .wait_for(|state| *state != Status::Starting)
            .await
            .map_err(|_| anyhow!("egress controller stopped"))?;
        ensure!(
            state == Status::Ready && !self.shared.retired.load(Ordering::Acquire),
            "egress run is retired"
        );
        Ok(())
    }

    /// Queue admission is bounded and immediate. Once the controller starts
    /// the update, it finishes the protocol even if this waiter is dropped.
    pub async fn install(&self, target: HttpTarget, handler: T) -> Result<()> {
        ensure!(
            !self.shared.retired.load(Ordering::Acquire),
            "egress run is retired"
        );
        let (reply, receive) = oneshot::channel();
        self.updates
            .try_send(Update {
                target,
                handler,
                reply,
            })
            .map_err(|_| anyhow!("egress installation queue is closed or full"))?;
        receive
            .await
            .map_err(|_| anyhow!("egress installation outcome is unknown; run retired"))?
    }
}

impl<T> Controller<T> {
    pub async fn run(mut self, mut io: impl AsyncRead + AsyncWrite + Unpin) -> Result<()> {
        let mut status = self.shared.status.subscribe();
        let mut retirement = pin!(status.wait_for(|state| *state == Status::Closed));
        let mut work = pin!(async {
            ensure!(
                wire::read_open(&mut io).await?
                    == Open::Control {
                        session: self.session
                    },
                "invalid egress control session"
            );
            {
                // Serialize readiness with retirement so a racing handshake
                // cannot overwrite Closed and strand the controller.
                let _rules = self.shared.rules.lock().unwrap();
                ensure!(
                    !self.shared.retired.load(Ordering::Acquire),
                    "egress run is retired"
                );
                self.shared.status.send_replace(Status::Ready);
            }
            let (mut input, mut output) = tokio::io::split(&mut io);
            loop {
                // Keep the same decoder alive across queue admission and
                // rejected candidates. Cancelling a partial frame here would
                // lose bytes and corrupt the next acknowledgement.
                let mut acknowledgement = pin!(wire::read_control(&mut input));
                let (prepared, reply) = loop {
                    let update = poll_fn(|cx| {
                        if let std::task::Poll::Ready(message) = acknowledgement.as_mut().poll(cx) {
                            return std::task::Poll::Ready(Err(message.err().unwrap_or_else(
                                || anyhow!("unsolicited egress control message"),
                            )));
                        }
                        self.receive.poll_recv(cx).map(Ok)
                    })
                    .await?;
                    let Some(update) = update else {
                        return Ok(());
                    };
                    if update.reply.is_closed() {
                        continue;
                    }
                    let prepared = self
                        .shared
                        .rules
                        .lock()
                        .unwrap()
                        .prepare(update.target, update.handler);
                    match prepared {
                        Ok(prepared) => break (prepared, update.reply),
                        Err(error) => {
                            let _ = update.reply.send(Err(error));
                        }
                    }
                };
                let revision = prepared.revision();
                let targets = prepared.targets().cloned().collect();
                // A peer may have applied a change even when its ack is lost.
                // Any protocol/I/O failure exits and retires the entire run.
                wire::write_control(&mut output, &Control::Install { revision, targets }).await?;
                ensure!(
                    acknowledgement.await? == Control::Installed { revision },
                    "unexpected egress installation acknowledgement"
                );
                self.shared.rules.lock().unwrap().commit(prepared)?;
                let _ = reply.send(Ok(()));
            }
        });
        poll_fn(|cx| {
            // Revocation takes precedence over installing or acknowledging work.
            if retirement.as_mut().poll(cx).is_ready() {
                return std::task::Poll::Ready(Ok(()));
            }
            work.as_mut().poll(cx)
        })
        .await
    }
}

impl<T> Drop for Controller<T> {
    fn drop(&mut self) {
        self.shared.retire();
    }
}

/// Helper-side control loop. `install` must finish actual network/DNS changes
/// before returning; a failure closes control without acknowledging success.
pub async fn serve<I, F, Fut>(mut io: I, session: [u8; 32], mut install: F) -> Result<()>
where
    I: AsyncRead + AsyncWrite + Unpin,
    F: FnMut(u64, Vec<HttpTarget>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    wire::write_open(&mut io, &Open::Control { session }).await?;
    let mut current = 0;
    loop {
        let Control::Install { revision, targets } = wire::read_control(&mut io).await? else {
            anyhow::bail!("unexpected helper control message");
        };
        ensure!(revision > current, "stale helper installation");
        install(revision, targets).await?;
        current = revision;
        wire::write_control(&mut io, &Control::Installed { revision }).await?;
    }
}

#[cfg(test)]
#[path = "control_tests.rs"]
mod tests;
