// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{envelope, Failure};
use crate::application::{Invocation, Output};

/// Reply policy captured before application code takes ownership of a request.
/// Only the host's pre-factory catalog path may bypass this policy and report
/// a proven refusal. A custom cell has the same obligations as a generated one.
pub(crate) struct Entered {
    typed: bool,
}

impl Entered {
    pub(crate) fn new(invocation: &Invocation) -> Self {
        Self {
            typed: matches!(invocation, Invocation::CellRpc { args, .. }
                if envelope::view(args).is_ok()),
        }
    }

    pub(crate) fn finish(self, output: Output) -> Result<Output, Failure> {
        match output {
            Output::Rpc(data) if self.typed => envelope::entered_reply(data).map(Output::Rpc),
            output => Ok(output),
        }
    }
}

#[cfg(test)]
#[path = "provenance_tests.rs"]
mod tests;
