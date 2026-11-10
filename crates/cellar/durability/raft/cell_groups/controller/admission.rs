// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Node maintenance and work accounting become visible in one short admission
//! section. Physical producers and undelivered responses share the work lease.
use super::*;
use crate::application::consensus::admission::{Access, Admission, RegistrationRefusal};
use crate::host_services::{ApplicationWork, ApplicationWorkGuard};
use std::sync::MutexGuard;

pub(super) struct Public {
    admission: Arc<Admission>,
    work: Arc<ApplicationWork>,
}

pub(super) struct Registration<'a> {
    // Serializes initial binding with standalone admissions. Never crosses an
    // await, filesystem operation or application callback.
    _configuration: MutexGuard<'a, ()>,
    _gate: Option<MutexGuard<'a, ()>>,
    pub work: Option<ApplicationWorkGuard>,
}

pub(super) struct Retention {
    pub _permit: crate::raft::budget::Permit,
    pub _work: Option<ApplicationWorkGuard>,
}

impl Shared {
    pub(super) fn registration(&self) -> Result<Registration<'_>, RuntimeError> {
        let configuration = self
            .admission_configuration
            .lock()
            .map_err(|_| RuntimeError::Closed)?;
        if self.closing.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        let (gate, work) = match self.public.get() {
            Some(public) => {
                let gate = public
                    .admission
                    .registration_reason(Access::Application)
                    .map_err(|refusal| match refusal {
                        RegistrationRefusal::Closed => RuntimeError::Closed,
                        RegistrationRefusal::Maintenance => RuntimeError::NotReady,
                    })?;
                (Some(gate), Some(public.work.enter()))
            }
            None => (None, None),
        };
        Ok(Registration {
            _configuration: configuration,
            _gate: gate,
            work,
        })
    }

    pub(super) fn application_paused(&self) -> bool {
        self.public
            .get()
            .is_some_and(|public| public.admission.application_paused())
    }
}

impl CellGroupController {
    /// Conservative shutdown observation behind the sealed public barrier.
    /// Poison, an unbound owner, held responses, pending provisioning or any
    /// accepted physical application work make this proof unavailable.
    pub(crate) fn local_producers_quiesced(&self) -> bool {
        let shared = &self.handle.0;
        let Ok(_configuration) = shared.admission_configuration.lock() else {
            return false;
        };
        let Some(public) = shared.public.get() else {
            return false;
        };
        if !public.admission.is_draining()
            || public.admission.is_poisoned()
            || public.work.snapshot().pending_events != 0
            || shared.commands.usage().1 != 0
            || shared.active_effects.load(Ordering::Acquire) != 0
            || !shared.groups.producers_quiesced()
        {
            return false;
        }
        let Ok(provisioning) = shared.provisioning.lock() else {
            return false;
        };
        provisioning.as_ref().is_none_or(|config| {
            config.preparations.usage().1 == 0 && config.waiters.usage().1 == 0
        })
    }

    /// Install the canonical SDK's node admission before exposing ingress. A
    /// different owner, or previously admitted untracked work, cannot be rebound.
    pub(crate) fn bind_public_admission(
        &self,
        admission: Arc<Admission>,
        work: Arc<ApplicationWork>,
    ) -> Result<(), CellGroupControllerError> {
        let shared = &self.handle.0;
        let _configuration = shared
            .admission_configuration
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?;
        if let Some(old) = shared.public.get() {
            return if Arc::ptr_eq(&old.admission, &admission) && Arc::ptr_eq(&old.work, &work) {
                Ok(())
            } else {
                Err(CellGroupControllerError::Invalid("public admission owner"))
            };
        }
        if shared.closing.load(Ordering::Acquire)
            || shared.commands.usage().1 != 0
            || shared.active_effects.load(Ordering::Acquire) != 0
        {
            return Err(CellGroupControllerError::Invalid(
                "public admission already active/closed",
            ));
        }
        shared
            .public
            .set(Public { admission, work })
            .map_err(|_| CellGroupControllerError::Invalid("public admission already bound"))
    }
}
