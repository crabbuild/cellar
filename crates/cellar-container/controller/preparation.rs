// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Private metadata preparation; the Session gate remains mandatory before
//! constructing this call and fence_execution still activates the exact claim.
use super::{ControllerProvider, PreparedProviderCall};
use crate::Operation;
use anyhow::Result;
use cellar::sdk::{containers::*, Context};
use serde_json::Value;

impl ControllerProvider {
    pub(super) fn prepare_call<'a>(
        &'a self,
        context: &'a Context,
        operation: Operation,
        input: Value,
        lifecycle: Option<&'a BindingLifecycleGuard>,
    ) -> Result<PreparedProviderCall<'a>> {
        if self.identity.provider == super::docker::PROVIDER {
            super::docker::validate_parameters(operation, &input)?;
        }
        let mut start = None;
        if let Some(lifecycle) = lifecycle {
            let journal = BindingJournal::open(context, &self.identity.binding)?;
            if operation == Operation::Start {
                start = Some(self.prepare_start(context, &journal, &input)?);
            } else if operation == Operation::Destroy {
                self.prepare_delete_intent(context, &journal)?;
            }
            if journal
                .load(context)?
                .is_some_and(|current| !current.record.is_retired())
            {
                // This reserves only metadata. The ordinary adapter gate proves
                // the unfinished command, business writes, intent and claim in
                // one checkpoint. fence_execution rechecks it after that gate;
                // no controller request can use this unproven reservation.
                journal.prepare_execution_claim(context, lifecycle, self.controller.as_ref())?;
            }
        }
        Ok(Box::new(move || {
            Box::pin(self.execute(context, operation, input, lifecycle, start))
        }))
    }

    pub(super) fn prepare_delete_intent(
        &self,
        context: &Context,
        journal: &BindingJournal,
    ) -> Result<()> {
        let Some(current) = journal.load(context)? else {
            return Ok(());
        };
        if !current.record.is_retired()
            && current.record.resource_id.is_some()
            && current.record.intent != BindingIntent::Deleted
        {
            let mut record = current.record;
            record.intent = BindingIntent::Deleted;
            record.observation = BindingObservation::Pending;
            journal.prepare_exchange(context, Some(current.revision), record)?;
        }
        Ok(())
    }
}
