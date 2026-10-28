// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cooperative idle removal: cancellation cannot interrupt an entered delete.

use super::{CellContainer, ContainerEngine};
use crate::asyncrt;
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

pub(super) struct Sweeper {
    cancel: watch::Sender<bool>,
    task: Option<asyncrt::TaskHandle<()>>,
}

impl Sweeper {
    pub fn start(engine: Arc<ContainerEngine>, cell: Arc<CellContainer>, window: Duration) -> Self {
        let (cancel, mut cancelled) = watch::channel(false);
        let task = asyncrt::spawn(async move {
            asyncrt::select_biased! { "cancel idle work before its deadline is admitted";
                _ = cancelled.changed() => return,
                _ = asyncrt::sleep(window) => {}
            }
            loop {
                {
                    // Activation and removal share this lock. Cancellation
                    // while queued refuses before effects; once admitted,
                    // removal must finish even if its owner cancels the timer.
                    let _control = asyncrt::select_biased! { "cancel queued idle removal before taking SDK control";
                        _ = cancelled.changed() => return,
                        control = cell.sdk_control.lock() => control,
                    };
                    match engine.forget_locked(&cell).await {
                        Ok(()) => return,
                        Err(error) => {
                            tracing::warn!(cell = %cell.scope, %error, "idle container removal failed; retrying")
                        }
                    }
                }
                asyncrt::select_biased! { "cancel idle retry before another removal attempt";
                    _ = cancelled.changed() => return,
                    _ = asyncrt::sleep(Duration::from_secs(30)) => {}
                }
            }
        });
        Self {
            cancel,
            task: Some(task),
        }
    }

    /// Call while holding SDK control, after any entered removal has finished.
    pub async fn cancel_and_join(mut self) -> anyhow::Result<()> {
        self.cancel.send_replace(true);
        self.task
            .take()
            .expect("idle task joined only once")
            .await?;
        Ok(())
    }
}

impl Drop for Sweeper {
    fn drop(&mut self) {
        // Dropping a TaskHandle only detaches it. Explicit cooperative
        // cancellation also covers domain closure and replacement on release.
        self.cancel.send_replace(true);
    }
}
