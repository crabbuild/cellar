// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit optional native profiles keep default capability claims unchanged.

use crate::{Environment, Fencing, Native, Operation, Provider, ProviderSession};
use std::sync::OnceLock;

pub(super) fn base_operations() -> &'static [Operation] {
    static OPERATIONS: OnceLock<Vec<Operation>> = OnceLock::new();
    OPERATIONS.get_or_init(|| {
        Operation::ALL
            .iter()
            .copied()
            .filter(|operation| *operation != Operation::InterceptOutboundHttp)
            .collect()
    })
}

pub(super) struct HttpInterception;

impl Provider for HttpInterception {
    fn name(&self) -> &str {
        Native.name()
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

    fn exec_pty(&self) -> bool {
        Native.exec_pty()
    }

    fn synchronous_running(&self) -> bool {
        Native.synchronous_running()
    }

    fn queued_lifecycle(&self) -> bool {
        Native.queued_lifecycle()
    }

    fn operations(&self) -> &[Operation] {
        Operation::ALL
    }

    fn open(&self) -> Box<dyn ProviderSession> {
        Native.open()
    }
}
