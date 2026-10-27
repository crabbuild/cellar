// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::Vercel;
use anyhow::ensure;
use cellar::sdk::{
    containers::{
        BackendIdentity, BindingCheckpoint, BindingJournal, DeleteSafety, MaintenanceAction,
        MaintenanceBackend,
    },
    Context, HandlerFuture,
};
use std::sync::Arc;

impl MaintenanceBackend for Vercel {
    fn identity(&self) -> BackendIdentity {
        BackendIdentity {
            binding: self.0.binding.clone(),
            provider: "vercel".into(),
            configuration_digest: self.0.digest(),
        }
    }

    fn delete_safety(&self) -> DeleteSafety {
        // destroy() inspects the original allocation and verifies its complete
        // account/configuration/request metadata before every repeated delete.
        DeleteSafety::RevalidateExactResource
    }

    fn maintain(
        self: Arc<Self>,
        context: Context,
        expected: BindingCheckpoint,
        action: MaintenanceAction,
    ) -> HandlerFuture<()> {
        Box::pin(async move {
            let journal = BindingJournal::open(&context, &self.0.binding)?;
            ensure!(
                journal.load(&context)?.as_ref() == Some(&expected),
                "maintenance names a superseded provider checkpoint"
            );
            match action {
                MaintenanceAction::Inspect => {
                    self.0.reconcile_binding(&context).await?;
                }
                MaintenanceAction::Delete => {
                    self.0.destroy(&context).await?;
                }
            }
            Ok::<_, anyhow::Error>(())
        })
    }
}
