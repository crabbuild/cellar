// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![doc = include_str!("../e2b.md")]
mod config;
mod files;
mod lifecycle;
mod process;
use crate::remote_http as transport;
mod wire;
use crate::{
    Environment, Fencing, Limits, Operation, Provider, ProviderFuture, ProviderSession, Validation,
};
use cellar::sdk::Context;
pub use config::{Config, Ownership};
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone)]
pub struct E2b(Arc<Config>);

impl E2b {
    pub fn new(config: Config) -> Self {
        Self(Arc::new(config))
    }
}

impl Provider for E2b {
    fn name(&self) -> &str {
        "e2b"
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
            let operation = async {
                match operation {
                    Operation::Start => self.0.start(context, input).await,
                    Operation::Running => self.0.running(context).await,
                    Operation::Reconcile => self.0.reconcile_binding(context).await,
                    Operation::Destroy => self.0.destroy(context).await,
                    Operation::ExecOutput => {
                        self.0
                            .validate_command(&input["options"])
                            .map_err(|error| error.context(Validation))?;
                        let sandbox = self.0.connected(context).await?;
                        let result = self
                            .0
                            .command(
                                context,
                                &sandbox,
                                &input["options"],
                                input["max_bytes"].as_u64().unwrap() as usize,
                                None,
                            )
                            .await?;
                        Ok(result)
                    }
                    Operation::ReadFile
                    | Operation::WriteFile
                    | Operation::StatFile
                    | Operation::ListFiles => self.0.file(context, operation, input, limits).await,
                    _ => Err(anyhow::anyhow!(Validation)),
                }
            };
            cellar::asyncrt::timeout(
                std::time::Duration::from_millis(self.0.timeout_ms),
                operation,
            )
            .await
            .map_err(|_| anyhow::anyhow!("E2B operation timed out; outcome may be unknown"))?
        })
    }
}
