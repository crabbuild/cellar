// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Node maintenance and work accounting become visible in one short admission
//! section. Physical producers and undelivered responses share the work lease.
use super::*;
use crate::application::consensus::admission::{Access, Admission, RegistrationRefusal};
use crate::host_services::{ApplicationWork, ApplicationWorkGuard};
use cellar_logic::cell_group::CellHistory;
use std::sync::MutexGuard;

/// Only active callbacks retain entries. A response timeout cannot release the
/// cell while its physical invocation or result still retains node admission.
/// The slot count is fixed at controller start by `max_callbacks`.
pub(super) struct CallbackCells(Mutex<Vec<Option<CellHistory>>>);

pub(super) struct CallbackCell {
    cells: Arc<CallbackCells>,
    cell: CellHistory,
}

impl CallbackCells {
    pub fn new(slots: usize) -> Self {
        Self(Mutex::new(vec![None; slots]))
    }

    pub fn contains(&self, cell: &CellHistory) -> Result<bool, RuntimeError> {
        Ok(self
            .0
            .lock()
            .map_err(|_| RuntimeError::Closed)?
            .iter()
            .any(|value| value.as_ref() == Some(cell)))
    }

    /// Every slot is retained by an active callback. This is a scheduling
    /// hint only: admission still acquires a free slot atomically.
    pub fn is_full(&self) -> Result<bool, RuntimeError> {
        Ok(self
            .0
            .lock()
            .map_err(|_| RuntimeError::Closed)?
            .iter()
            .all(Option::is_some))
    }

    pub fn acquire(self: &Arc<Self>, cell: &CellHistory) -> Result<CallbackCell, RuntimeError> {
        let mut cells = self.0.lock().map_err(|_| RuntimeError::Closed)?;
        // Acquire the bounded callback slots first. Fixed metadata never
        // grows with idle cells, failed admissions or duplicate discoveries.
        if cells.iter().any(|value| value.as_ref() == Some(cell)) {
            return Err(RuntimeError::Overloaded);
        }
        let slot = cells
            .iter_mut()
            .find(|value| value.is_none())
            .ok_or(RuntimeError::Overloaded)?;
        *slot = Some(cell.clone());
        Ok(CallbackCell {
            cells: self.clone(),
            cell: cell.clone(),
        })
    }
}

impl Drop for CallbackCell {
    fn drop(&mut self) {
        let mut cells = self.cells.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(slot) = cells
            .iter_mut()
            .find(|value| value.as_ref() == Some(&self.cell))
        {
            *slot = None;
        }
    }
}

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
    pub _callback: Option<CallbackCell>,
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
    /// Conservative shutdown observation behind a successful terminal join and
    /// the sealed public barrier. An empty live queue is not a closure proof.
    /// Poison, an unbound owner, held responses, pending provisioning or any
    /// accepted physical application work make this proof unavailable.
    pub(crate) fn local_producers_quiesced(&self) -> bool {
        let shared = &self.handle.0;
        // The terminal result is installed only after the worker, transport,
        // archive, SQL and runtime owners have joined and their charges released.
        // A failed join, poisoned observation or still-running worker supplies
        // no proof, even if its queues happen to be empty at this instant.
        if !shared
            .terminal
            .lock()
            .is_ok_and(|terminal| terminal.as_ref().is_some_and(|result| result.is_ok()))
        {
            return false;
        }
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
            || shared.callbacks.usage().1 != 0
            || shared.active_effects.load(Ordering::Acquire) != 0
            || !shared.groups.shutdown_states_valid()
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
            || shared.callbacks.usage().1 != 0
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

#[cfg(test)]
mod tests {
    use super::*;
    use cellar_logic::consensus::{ClusterId, IncarnationId, NamespaceId};

    #[test]
    fn callback_cell_survives_waiter_drop_and_preserves_other_cell_progress() {
        let cell = CellHistory {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            incarnation: IncarnationId::new([3; 16]).unwrap(),
            cell: cellar_logic::cell_group::CellId::new("first").unwrap(),
        };
        let mut other = cell.clone();
        other.cell = cellar_logic::cell_group::CellId::new("second").unwrap();
        let cells = Arc::new(CallbackCells::new(2));
        let budget = crate::raft::budget::Budget::new(1024, 2);
        let owner = Arc::new(Retention {
            _permit: budget.acquire(64).unwrap(),
            _work: None,
            _callback: Some(cells.acquire(&cell).unwrap()),
        });
        let physical = owner.clone();
        drop(owner);
        assert!(cells.acquire(&cell).is_err());
        let peer = Arc::new(Retention {
            _permit: budget.acquire(64).unwrap(),
            _work: None,
            _callback: Some(cells.acquire(&other).unwrap()),
        });
        assert!(cells.contains(&cell).unwrap());
        drop(physical);
        let returned = Arc::new(Retention {
            _permit: budget.acquire(64).unwrap(),
            _work: None,
            _callback: Some(cells.acquire(&cell).unwrap()),
        });
        drop((peer, returned));
        assert!(!cells.contains(&cell).unwrap());
        assert_eq!(budget.usage().1, 0);
    }

    #[test]
    fn configured_callback_slots_bound_distinct_cells() {
        let history = |name: &str| CellHistory {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            incarnation: IncarnationId::new([3; 16]).unwrap(),
            cell: cellar_logic::cell_group::CellId::new(name).unwrap(),
        };
        let cells = Arc::new(CallbackCells::new(4));
        let mut held: Vec<_> = ["a", "b", "c", "d"]
            .into_iter()
            .map(|name| cells.acquire(&history(name)).unwrap())
            .collect();
        // Every configured slot is in use: a further cell refuses, and a
        // retained cell can never receive a second overlapping callback.
        assert!(cells.is_full().unwrap());
        assert!(matches!(
            cells.acquire(&history("e")),
            Err(RuntimeError::Overloaded)
        ));
        drop(held.remove(1));
        assert!(matches!(
            cells.acquire(&history("a")),
            Err(RuntimeError::Overloaded)
        ));
        assert!(!cells.is_full().unwrap());
        let reused = cells.acquire(&history("e")).unwrap();
        assert!(cells.is_full().unwrap());
        assert!(cells.contains(&history("e")).unwrap());
        assert!(!cells.contains(&history("b")).unwrap());
        drop(reused);
        assert!(!cells.contains(&history("e")).unwrap());
        assert!(cells.contains(&history("a")).unwrap());
        drop(held);
        assert!(!cells.contains(&history("a")).unwrap());
    }
}
