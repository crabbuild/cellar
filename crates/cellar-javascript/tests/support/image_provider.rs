// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Controlled local image tests may report native failures to their own logs.
//! Production guest failure envelopes continue to hide backend exception text.
use cellar::sdk::Context;
use cellar_container::{
    Environment, Fencing, Limits, Native, Operation, Provider, ProviderFuture, ProviderSession,
};
use serde_json::Value;

pub struct DiagnosticNative<P = Native>(pub P);

impl<P: Provider> Provider for DiagnosticNative<P> {
    fn exec_pty(&self) -> bool {
        self.0.exec_pty()
    }

    fn name(&self) -> &str {
        self.0.name()
    }

    fn environment(&self) -> Environment {
        self.0.environment()
    }

    fn fencing(&self) -> Fencing {
        self.0.fencing()
    }

    fn max_resources(&self) -> Option<usize> {
        self.0.max_resources()
    }

    fn synchronous_running(&self) -> bool {
        self.0.synchronous_running()
    }

    fn queued_lifecycle(&self) -> bool {
        self.0.queued_lifecycle()
    }

    fn operations(&self) -> &[Operation] {
        self.0.operations()
    }

    fn open(&self) -> Box<dyn ProviderSession> {
        Box::new(Session(self.0.open()))
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
