// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Controlled local image tests may report native failures to their own logs.
//! Production guest failure envelopes continue to hide backend exception text.
use cellar::sdk::Context;
use cellar_container::{
    Environment, Fencing, Limits, Native, Operation, Provider, ProviderFuture, ProviderSession,
};
use serde_json::Value;

pub struct DiagnosticNative;

impl Provider for DiagnosticNative {
    fn name(&self) -> &str {
        "native"
    }

    fn environment(&self) -> Environment {
        Native.environment()
    }

    fn fencing(&self) -> Fencing {
        Native.fencing()
    }

    fn max_resources(&self) -> Option<usize> {
        Native.max_resources()
    }

    fn synchronous_running(&self) -> bool {
        Native.synchronous_running()
    }

    fn queued_lifecycle(&self) -> bool {
        Native.queued_lifecycle()
    }

    fn operations(&self) -> &[Operation] {
        Native.operations()
    }

    fn open(&self) -> Box<dyn ProviderSession> {
        Box::new(Session(Native.open()))
    }
}

struct Session(Box<dyn ProviderSession>);

impl ProviderSession for Session {
    fn preflight(
        &self,
        context: &Context,
        operation: Operation,
        input: &Value,
    ) -> anyhow::Result<()> {
        self.0.preflight(context, operation, input)
    }

    fn running_snapshot(&self, context: &Context) -> anyhow::Result<bool> {
        self.0.running_snapshot(context)
    }

    fn execute<'a>(
        &'a self,
        context: &'a Context,
        operation: Operation,
        input: Value,
        limits: &'a Limits,
    ) -> ProviderFuture<'a> {
        Box::pin(async move {
            let result = self.0.execute(context, operation, input, limits).await;
            if let Err(error) = &result {
                eprintln!(
                    "native image operation {} failed: {error:#}",
                    operation.name()
                );
            }
            result
        })
    }
}
