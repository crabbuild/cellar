//! Adapter budgets retain the original host clock without retaining a context.
use super::{Code, Failure};
use crate::{application::Context, host_io::IoContext};
use std::sync::{Arc, Weak};

/// A clock and policy captured by an adapter on its original native invocation.
/// Guest threads may mint budgets without consulting their own clock or the
/// thread-local execution domain. This is not serializable SDK authority.
#[derive(Clone)]
pub struct BudgetSource {
    owner: Weak<IoContext>,
    cancellation: Option<Arc<crate::runtime::InvocationCancellation>>,
    domain: crate::asyncrt::DomainToken,
    host_ms: u64,
    parent: Option<u64>,
}

impl BudgetSource {
    /// Capture policy and clock ownership during the context's native SDK turn.
    pub fn capture(context: &Context) -> Result<Self, Failure> {
        context
            .check_turn()
            .map_err(|_| Failure::refused(Code::ContextExpired))?;
        let services = context.io.host_services();
        Ok(Self {
            owner: Arc::downgrade(&context.io),
            cancellation: context.io.rpc_cancellation.get().cloned(),
            domain: services
                .domain_token()
                .ok_or_else(|| Failure::refused(Code::Unavailable))?,
            host_ms: services
                .operation_deadline_ms()
                .map_err(|_| Failure::refused(Code::Unavailable))?,
            parent: context.io.rpc_deadline_ms.get().copied(),
        })
    }

    fn now(&self) -> Result<u64, Failure> {
        // Do not upgrade the weak I/O context on a guest thread: dropping a
        // last temporary Arc there could move native resource cleanup to it.
        // This is a clock read, not admission; the adapter and Client repeat
        // original-scope/liveness checks before accepting an operation.
        if self.owner.strong_count() == 0
            || self
                .cancellation
                .as_ref()
                .is_some_and(|scope| scope.is_revoked())
        {
            return Err(Failure::refused(Code::ContextExpired));
        }
        self.domain
            .mono_ms()
            .map_err(|_| Failure::refused(Code::ContextExpired))
    }

    /// Start before guest validation or codecs, using the host/default and
    /// inherited parent cap. A requested timeout can only shorten this budget.
    pub fn start(&self) -> Result<CallBudget, Failure> {
        let started = self.now()?;
        Ok(CallBudget {
            source: self.clone(),
            started,
            deadline: started
                .saturating_add(self.host_ms)
                .min(self.parent.unwrap_or(u64::MAX)),
        })
    }

    /// Whether a budget belongs to this source's original invocation.
    pub fn owns(&self, budget: &CallBudget) -> bool {
        Weak::ptr_eq(&self.owner, &budget.source.owner)
    }
}

/// Opaque, immutable call deadline. It contains no guest-supplied timestamp and
/// cannot be transferred to another invocation or extend its parent deadline.
#[derive(Clone)]
pub struct CallBudget {
    source: BudgetSource,
    started: u64,
    deadline: u64,
}

impl CallBudget {
    /// Return a clone capped from the original start. Repeated limits can only
    /// shorten the deadline; they never start a fresh relative window.
    pub fn limit(&self, timeout_ms: u32) -> Self {
        Self {
            deadline: self
                .deadline
                .min(self.started.saturating_add(u64::from(timeout_ms))),
            ..self.clone()
        }
    }

    /// Remaining whole milliseconds, refusing at the original deadline.
    /// Adapters must map a refusal after submission to an unknown effect outcome.
    pub fn remaining_ms(&self) -> Result<u64, Failure> {
        let remaining = self.deadline.saturating_sub(self.source.now()?);
        if remaining == 0 {
            return Err(Failure::refused(Code::DeadlineExceeded));
        }
        Ok(remaining)
    }

    pub(super) fn deadline_for(&self, context: &Context) -> Result<u64, Failure> {
        if !Weak::ptr_eq(&self.source.owner, &Arc::downgrade(&context.io)) {
            return Err(Failure::refused(Code::ContextExpired));
        }
        self.remaining_ms()?;
        Ok(self.deadline)
    }
}
