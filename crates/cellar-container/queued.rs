// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded, one-shot admission for synchronous Workers lifecycle calls.
use crate::{Operation, Provider, Validation};
use anyhow::{ensure, Context as _, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const MAX_COMMANDS: usize = 64;

pub(crate) fn validate(operation: Operation, input: &Value) -> Result<()> {
    if operation != Operation::Start {
        return Ok(());
    }
    let options = &input["options"];
    if let Some(env) = options["env"].as_array() {
        for pair in env {
            let key = pair[0].as_str().unwrap(); // shape checked by native::validate
            let value = pair[1].as_str().unwrap();
            ensure!(
                !key.contains(['=', '\0']) && !value.contains('\0'),
                "invalid container environment"
            );
        }
    }
    if let Some(args) = options["entrypoint"].as_array() {
        ensure!(
            args.iter().all(|arg| !arg.as_str().unwrap().contains('\0')),
            "invalid container entrypoint"
        );
    }
    if let Some(labels) = options["labels"].as_array() {
        ensure!(labels.len() <= 10, "too many container labels");
        for pair in labels {
            let key = pair[0].as_str().unwrap();
            let value = pair[1].as_str().unwrap();
            ensure!(
                !key.is_empty()
                    && key.len() <= 16
                    && value.len() <= 64
                    && !key.chars().any(char::is_control)
                    && !value.chars().any(char::is_control),
                "invalid container label"
            );
        }
    }
    Ok(())
}

pub(crate) fn supported(provider: &dyn Provider) -> bool {
    provider.queued_lifecycle()
        && provider.synchronous_running()
        && [
            Operation::Running,
            Operation::Start,
            Operation::Monitor,
            Operation::Signal,
            Operation::Destroy,
        ]
        .iter()
        .all(|operation| provider.operations().contains(operation))
}

#[derive(Default)]
struct Usage {
    commands: usize,
    bytes: usize,
    starting: bool,
}

struct Permit {
    usage: Arc<Mutex<Usage>>,
    bytes: usize,
    start: bool,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut usage = self.usage.lock().unwrap();
        usage.commands -= 1;
        usage.bytes -= self.bytes;
        if self.start {
            usage.starting = false;
        }
    }
}

pub(crate) struct Command {
    operation: Operation,
    input: Value,
    _permit: Permit,
}

impl Command {
    pub async fn run(
        self,
        session: &crate::Session,
        context: &cellar::sdk::Context,
        limits: &crate::Limits,
    ) -> Result<Value> {
        let Self {
            operation,
            input,
            _permit,
        } = self;
        let result = session.execute(context, operation, input, limits).await;
        drop(_permit);
        result
    }
}

#[derive(Default)]
pub(crate) struct Queue {
    pending: Mutex<HashMap<String, Command>>,
    usage: Arc<Mutex<Usage>>,
}

impl Queue {
    pub fn admit(&self, operation: Operation, input: Value, limit: usize) -> Result<String> {
        struct Count {
            size: usize,
            limit: usize,
        }

        impl std::io::Write for Count {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if bytes.len() > self.limit.saturating_sub(self.size) {
                    return Err(std::io::Error::other(
                        "queued container inputs exceed limit",
                    ));
                }
                self.size += bytes.len();
                Ok(bytes.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut count = Count { size: 0, limit };
        serde_json::to_writer(&mut count, &input)?;
        let start = operation == Operation::Start;
        let ticket = cellar::native::crypto::random_uuid()?;
        let permit = {
            let mut usage = self.usage.lock().unwrap();
            ensure!(
                usage.commands < MAX_COMMANDS,
                "queued container command limit exceeded"
            );
            ensure!(
                count.size <= limit.saturating_sub(usage.bytes),
                "queued container inputs exceed limit"
            );
            ensure!(
                !start || !usage.starting,
                "container startup is already queued"
            );
            usage.commands += 1;
            usage.bytes += count.size;
            usage.starting |= start;
            Permit {
                usage: self.usage.clone(),
                bytes: count.size,
                start,
            }
        };
        let mut pending = self.pending.lock().unwrap();
        ensure!(
            !pending.contains_key(&ticket),
            "container ticket identity collision"
        );
        pending.insert(
            ticket.clone(),
            Command {
                operation,
                input,
                _permit: permit,
            },
        );
        Ok(ticket)
    }

    pub fn take(&self, ticket: &str) -> Result<Command> {
        self.pending
            .lock()
            .unwrap()
            .remove(ticket)
            .context("container ticket is absent or already consumed")
            .context(Validation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn tickets_are_session_local_one_shot_and_hold_quota_until_completion() {
        let queue = Queue::default();
        let ticket = queue.admit(Operation::Start, json!({}), 2).unwrap();
        assert!(Queue::default().take(&ticket).is_err());
        let command = queue.take(&ticket).unwrap();
        assert!(queue.take(&ticket).is_err());
        assert!(queue.admit(Operation::Start, json!({}), 1024).is_err());
        assert!(queue.admit(Operation::Destroy, json!({}), 2).is_err());
        drop(command);
        assert!(queue.admit(Operation::Start, json!({}), 2).is_ok());
    }

    #[test]
    fn byte_and_command_limits_refuse_before_retaining_more_input() {
        let queue = Queue::default();
        assert!(queue
            .admit(Operation::Destroy, json!({"large":"x".repeat(100)}), 8)
            .is_err());
        for _ in 0..MAX_COMMANDS {
            queue.admit(Operation::Destroy, json!({}), 4096).unwrap();
        }
        assert!(queue.admit(Operation::Destroy, json!({}), 4096).is_err());
    }
}
