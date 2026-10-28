// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![doc = include_str!("../vercel.md")]
mod config;
mod files;
mod lifecycle;
mod process;
mod supervision;
mod wire;
use crate::{
    Environment, Fencing, Limits, Operation, Provider, ProviderFuture, ProviderSession, Validation,
};
use anyhow::Context as _;
use cellar::sdk::Context;
pub use config::{Config, Ownership};
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone)]
pub struct Vercel(Arc<Config>);

impl Vercel {
    pub fn new(config: Config) -> Self {
        Self(Arc::new(config))
    }
}

impl Provider for Vercel {
    fn maintenance_backend(&self) -> Option<Arc<dyn cellar::sdk::containers::MaintenanceBackend>> {
        Some(Arc::new(self.clone()))
    }

    fn name(&self) -> &str {
        "vercel"
    }

    fn binding_name(&self) -> Option<&str> {
        Some(&self.0.binding)
    }

    fn environment(&self) -> Environment {
        Environment::Linux
    }

    fn fencing(&self) -> Fencing {
        Fencing::LocalChecksAndExpiry
    }

    fn max_resources(&self) -> Option<usize> {
        Some(0)
    }

    fn operations(&self) -> &[Operation] {
        &[
            Operation::Running,
            Operation::Reconcile,
            Operation::Start,
            Operation::Destroy,
            Operation::ExecOutput,
            Operation::ReadFile,
            Operation::WriteFile,
            Operation::StatFile,
            Operation::ListFiles,
        ]
    }

    fn open(&self) -> Box<dyn ProviderSession> {
        Box::new(Session(self.0.clone()))
    }
}

struct Session(Arc<Config>);

impl ProviderSession for Session {
    fn execute<'a>(
        &'a self,
        context: &'a Context,
        operation: Operation,
        input: Value,
        limits: &'a Limits,
    ) -> ProviderFuture<'a> {
        Box::pin(async move {
            let journal = cellar::sdk::containers::BindingJournal::open(context, &self.0.binding)?;
            let records_activity = matches!(
                operation,
                Operation::ExecOutput
                    | Operation::ReadFile
                    | Operation::WriteFile
                    | Operation::StatFile
                    | Operation::ListFiles
            );
            let generation = journal
                .load(context)?
                .map(|checkpoint| checkpoint.record.generation);
            if !matches!(
                operation,
                Operation::Running | Operation::Reconcile | Operation::Destroy
            ) {
                journal.check_retention(context)?;
            }
            let execute = async {
                let value = async {
                    match operation {
                        Operation::Start => self.0.start(context, input).await,
                        Operation::Running => self.0.running(context).await,
                        Operation::Reconcile => self.0.reconcile_binding(context).await,
                        Operation::Destroy => self.0.destroy(context).await,
                        Operation::ExecOutput => {
                            self.0
                                .validate_command(&input["options"])
                                .map_err(|error| error.context(Validation))?;
                            let session = self.0.connected(context).await?;
                            self.0
                                .command(
                                    context,
                                    &session,
                                    &input["options"],
                                    input["max_bytes"].as_u64().unwrap() as usize,
                                )
                                .await
                        }
                        Operation::ReadFile
                        | Operation::WriteFile
                        | Operation::StatFile
                        | Operation::ListFiles => {
                            self.0.file(context, operation, input, limits).await
                        }
                        _ => Err(anyhow::anyhow!(Validation)),
                    }
                }
                .await?;
                if records_activity {
                    journal
                        .activity(
                            context,
                            generation.context(
                                "provider binding changed during operation; outcome may be unknown",
                            )?,
                        )
                        .await?;
                }
                Ok::<Value, anyhow::Error>(value)
            };
            cellar::asyncrt::timeout(std::time::Duration::from_millis(self.0.timeout_ms), execute)
                .await
                .map_err(|_| {
                    anyhow::anyhow!("Vercel operation timed out; outcome may be unknown")
                })?
        })
    }
}
