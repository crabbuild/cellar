// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded native deadlines, separate from effect/mutation admission.
use cellar::sdk::rpc::{BudgetSource, CallBudget, Code, Failure};
use std::collections::HashMap;

#[derive(Default)]
pub(super) struct Budgets(HashMap<u64, HashMap<u64, CallBudget>>);

fn code(error: Failure) -> &'static str {
    match error.code() {
        Code::ContextExpired => "ContextExpired",
        Code::DeadlineExceeded => "DeadlineExceeded",
        _ => "Unavailable",
    }
}

impl Budgets {
    pub fn start(
        &mut self,
        invocation: u64,
        id: u64,
        source: Option<&BudgetSource>,
        maximum: usize,
    ) -> Result<u32, &'static str> {
        if self
            .0
            .get(&invocation)
            .is_some_and(|entries| entries.len() >= maximum || entries.contains_key(&id))
        {
            return Err("ResourceExhausted");
        }
        let budget = source.ok_or("Unavailable")?.start().map_err(code)?;
        let remaining = budget
            .remaining_ms()
            .map_err(code)?
            .min(u64::from(u32::MAX)) as u32;
        self.0.entry(invocation).or_default().insert(id, budget);
        Ok(remaining)
    }

    pub fn get(&self, invocation: u64, id: u64) -> Result<CallBudget, &'static str> {
        self.0
            .get(&invocation)
            .and_then(|entries| entries.get(&id))
            .cloned()
            .ok_or("ContextMismatch")
    }

    #[cfg(feature = "embed")]
    pub fn remaining(&self, invocation: u64, id: u64) -> Result<u32, &'static str> {
        Ok(self
            .get(invocation, id)?
            .remaining_ms()
            .map_err(code)?
            .min(u64::from(u32::MAX)) as u32)
    }

    pub fn limit(&mut self, invocation: u64, id: u64, timeout: u32) -> Result<(), &'static str> {
        let budget = self
            .0
            .get_mut(&invocation)
            .and_then(|entries| entries.get_mut(&id))
            .ok_or("ContextMismatch")?;
        *budget = budget.limit(timeout);
        Ok(())
    }

    pub fn release(&mut self, invocation: u64, id: u64) {
        if let Some(entries) = self.0.get_mut(&invocation) {
            entries.remove(&id);
            if entries.is_empty() {
                self.0.remove(&invocation);
            }
        }
    }

    pub fn has(&self, invocation: u64) -> bool {
        self.0
            .get(&invocation)
            .is_some_and(|entries| !entries.is_empty())
    }

    pub fn retire(&mut self, invocation: u64) {
        self.0.remove(&invocation);
    }
}
