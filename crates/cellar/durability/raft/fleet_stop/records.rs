// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded engine metadata; a synced guard is not a joined-owner proof.
use crate::raft::storage::LogError;
use cellar_logic::consensus::{
    DiskLineage, EngineInstanceId, IncarnationId, NodeId, ProcessGeneration, ProposalId,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Physical boot metadata. Possessing or decoding this value grants no stop,
/// membership, capacity or joined-owner authority.
pub struct BootBinding {
    pub catalog: [u8; 32],
    pub fleet: [u8; 16],
    pub node: [u8; 16],
    pub disk: [u8; 16],
    pub engine: [u8; 16],
    pub boot: [u8; 16],
    pub process: [u8; 16],
    pub epoch: u64,
    pub join_key: [u8; 32],
}

impl BootBinding {
    pub(crate) fn validate(&self) -> Result<(), LogError> {
        let invalid = || LogError::Invalid("fleet boot binding".into());
        IncarnationId::new(self.fleet).map_err(|_| invalid())?;
        NodeId::new(self.node).map_err(|_| invalid())?;
        DiskLineage::new(self.disk).map_err(|_| invalid())?;
        EngineInstanceId::new(self.engine).map_err(|_| invalid())?;
        ProposalId::new(self.boot).map_err(|_| invalid())?;
        ProcessGeneration::new(self.process).map_err(|_| invalid())?;
        let key = ed25519_dalek::VerifyingKey::from_bytes(&self.join_key).map_err(|_| invalid())?;
        if self.catalog == [0; 32] || self.epoch == 0 || self.epoch >= u64::MAX - 2 || key.is_weak()
        {
            return Err(invalid());
        }
        Ok(())
    }

    fn same_engine_owner(&self, other: &Self) -> bool {
        self.catalog == other.catalog
            && self.fleet == other.fleet
            && self.node == other.node
            && self.disk == other.disk
            && self.engine == other.engine
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LifecyclePurpose {
    Evacuation,
    Maintenance,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StopSelector {
    pub purpose: LifecyclePurpose,
    pub boot: BootBinding,
    pub epoch: u64,
    pub operation: [u8; 16],
    pub finalization: [u8; 16],
    pub request: [u8; 16],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FleetStopAction {
    Stop,
    Fence,
    Quiesce,
}

/// Untrusted request data. The receiver verifies its current rooted operation
/// and exact local boot before the engine can admit either action.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetStopRequest {
    pub(crate) selector: StopSelector,
    pub(crate) action: FleetStopAction,
}

/// Historical local guard admission, never proof of physical closure.
#[derive(Clone, Debug, Serialize)]
pub struct FleetStopAcceptance {
    selector: StopSelector,
    action: FleetStopAction,
}

impl FleetStopRequest {
    pub fn request(&self) -> [u8; 16] {
        self.selector.request
    }

    pub fn request_id(&self) -> String {
        hex::encode(self.selector.request)
    }

    pub fn action(&self) -> FleetStopAction {
        self.action
    }

    pub(crate) fn new(selector: StopSelector, action: FleetStopAction) -> Result<Self, LogError> {
        selector.validate()?;
        Ok(Self { selector, action })
    }

    pub(crate) fn acceptance(&self) -> FleetStopAcceptance {
        FleetStopAcceptance {
            selector: self.selector.clone(),
            action: self.action,
        }
    }
}

impl FleetStopAcceptance {
    pub fn request(&self) -> [u8; 16] {
        self.selector.request
    }

    pub fn operation(&self) -> [u8; 16] {
        self.selector.operation
    }

    pub fn process(&self) -> [u8; 16] {
        self.selector.boot.process
    }

    pub fn action(&self) -> FleetStopAction {
        self.action
    }
}

impl StopSelector {
    pub fn validate(&self) -> Result<(), LogError> {
        self.boot.validate()?;
        for id in [self.operation, self.finalization, self.request] {
            ProposalId::new(id).map_err(|_| LogError::Invalid("fleet stop identity".into()))?;
        }
        if self.epoch <= self.boot.epoch || self.epoch >= u64::MAX - 2 {
            return Err(LogError::Invalid("fleet stop epoch".into()));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
enum Phase {
    Open,
    Quiescing {
        selector: Box<StopSelector>,
    },
    Fenced {
        epoch: u64,
        operation: [u8; 16],
        request: [u8; 16],
    },
    StopAccepted {
        selector: Box<StopSelector>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Guard {
    pub boot: BootBinding,
    phase: Phase,
}

impl Guard {
    pub fn accepted(&self) -> Option<&StopSelector> {
        match &self.phase {
            Phase::StopAccepted { selector } => Some(selector),
            _ => None,
        }
    }

    pub fn quiescing(&self) -> Option<&StopSelector> {
        match &self.phase {
            Phase::Quiescing { selector } => Some(selector),
            _ => None,
        }
    }

    fn epoch(&self) -> u64 {
        match &self.phase {
            Phase::Open => self.boot.epoch,
            Phase::Fenced { epoch, .. } => *epoch,
            Phase::StopAccepted { selector } | Phase::Quiescing { selector } => selector.epoch,
        }
    }

    fn validate(&self) -> Result<(), LogError> {
        self.boot.validate()?;
        if matches!(&self.phase, Phase::Quiescing { selector }
            if selector.purpose != LifecyclePurpose::Maintenance)
        {
            return Err(LogError::WrongIdentity);
        }
        match &self.phase {
            Phase::Open => (),
            Phase::Fenced {
                epoch,
                operation,
                request,
            } => {
                ProposalId::new(*operation).map_err(|_| LogError::WrongIdentity)?;
                ProposalId::new(*request).map_err(|_| LogError::WrongIdentity)?;
                if *epoch <= self.boot.epoch || *epoch >= u64::MAX - 2 {
                    return Err(LogError::WrongIdentity);
                }
            }
            Phase::StopAccepted { selector } | Phase::Quiescing { selector } => {
                selector.validate()?;
                if selector.boot != self.boot {
                    return Err(LogError::WrongIdentity);
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LifecycleRecord {
    format: u8,
    #[serde(deserialize_with = "nullable")]
    pub guard: Option<Guard>,
}

fn nullable<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    Option::deserialize(deserializer)
}

impl LifecycleRecord {
    pub fn empty() -> Self {
        Self {
            format: 1,
            guard: None,
        }
    }

    pub fn validate(&self) -> Result<(), LogError> {
        if self.format != 1 {
            return Err(LogError::Corrupt("fleet lifecycle marker".into()));
        }
        if let Some(guard) = &self.guard {
            guard.validate()?;
        }
        Ok(())
    }

    pub fn change(&self, update: LifecycleUpdate) -> Result<Self, LogError> {
        self.validate()?;
        let guard = match update {
            LifecycleUpdate::Boot { boot, .. } => {
                boot.validate()?;
                if let Some(old) = &self.guard {
                    if old.boot == boot {
                        return Ok(self.clone());
                    }
                    if !boot.same_engine_owner(&old.boot)
                        || boot.boot == old.boot.boot
                        || boot.epoch <= old.epoch()
                    {
                        return Err(LogError::WrongIdentity);
                    }
                }
                Guard {
                    boot,
                    phase: Phase::Open,
                }
            }
            LifecycleUpdate::Quiesce { selector, .. } => {
                selector.validate()?;
                let old = self.guard.as_ref().ok_or(LogError::WrongIdentity)?;
                if selector.purpose != LifecyclePurpose::Maintenance || old.boot != selector.boot {
                    return Err(LogError::WrongIdentity);
                }
                if old.quiescing() == Some(&selector) {
                    return Ok(self.clone());
                }
                if old.accepted().is_some() || selector.epoch <= old.epoch() {
                    return Err(LogError::WrongIdentity);
                }
                Guard {
                    boot: old.boot.clone(),
                    phase: Phase::Quiescing {
                        selector: Box::new(selector),
                    },
                }
            }
            LifecycleUpdate::MaintenanceStop {
                selector,
                quiescence,
                ..
            } => {
                selector.validate()?;
                quiescence.validate()?;
                let old = self.guard.as_ref().ok_or(LogError::WrongIdentity)?;
                if selector.purpose != LifecyclePurpose::Maintenance
                    || quiescence.purpose != LifecyclePurpose::Maintenance
                    || selector.boot != quiescence.boot
                    || selector.boot != old.boot
                    || selector.epoch != quiescence.epoch
                    || selector.operation != quiescence.operation
                {
                    return Err(LogError::WrongIdentity);
                }
                if old.accepted() == Some(&selector) {
                    return Ok(self.clone());
                }
                if old.quiescing() != Some(quiescence.as_ref()) {
                    return Err(LogError::WrongIdentity);
                }
                Guard {
                    boot: old.boot.clone(),
                    phase: Phase::StopAccepted {
                        selector: Box::new(selector),
                    },
                }
            }
            LifecycleUpdate::Stop { selector, .. } => {
                selector.validate()?;
                let old = self.guard.as_ref().ok_or(LogError::WrongIdentity)?;
                if old.boot != selector.boot
                    || selector.purpose != LifecyclePurpose::Evacuation
                    || old.quiescing().is_some()
                    || selector.epoch < old.epoch()
                {
                    return Err(LogError::WrongIdentity);
                }
                match &old.phase {
                    Phase::StopAccepted { selector: accepted } if **accepted == selector => {
                        return Ok(self.clone());
                    }
                    Phase::StopAccepted { .. } => return Err(LogError::AlreadyExists),
                    Phase::Fenced { epoch, .. } if selector.epoch <= *epoch => {
                        return Err(LogError::WrongIdentity);
                    }
                    _ => (),
                }
                Guard {
                    boot: old.boot.clone(),
                    phase: Phase::StopAccepted {
                        selector: Box::new(selector),
                    },
                }
            }
            LifecycleUpdate::Fence {
                boot,
                epoch,
                operation,
                request,
                ..
            } => {
                let old = self.guard.as_ref().ok_or(LogError::WrongIdentity)?;
                let next = Guard {
                    boot,
                    phase: Phase::Fenced {
                        epoch,
                        operation,
                        request,
                    },
                };
                next.validate()?;
                if old.boot != next.boot || epoch < old.epoch() {
                    return Err(LogError::WrongIdentity);
                }
                if old.accepted().is_some() {
                    return Err(LogError::AlreadyExists);
                }
                if epoch == old.epoch() && *old != next && old.quiescing().is_none() {
                    return Err(LogError::WrongIdentity);
                }
                next
            }
        };
        let next = Self {
            format: 1,
            guard: Some(guard),
        };
        next.validate()?;
        Ok(next)
    }
}

pub(crate) enum LifecycleUpdate {
    MaintenanceStop {
        selector: StopSelector,
        quiescence: Box<StopSelector>,
        deadline: u64,
    },
    Quiesce {
        selector: StopSelector,
        deadline: u64,
    },
    Boot {
        boot: BootBinding,
        deadline: u64,
    },
    Stop {
        selector: StopSelector,
        deadline: u64,
    },
    Fence {
        boot: BootBinding,
        epoch: u64,
        operation: [u8; 16],
        request: [u8; 16],
        deadline: u64,
    },
}

impl LifecycleUpdate {
    pub fn boot(&self) -> &BootBinding {
        match self {
            Self::Boot { boot, .. } | Self::Fence { boot, .. } => boot,
            Self::Stop { selector, .. }
            | Self::Quiesce { selector, .. }
            | Self::MaintenanceStop { selector, .. } => &selector.boot,
        }
    }

    pub fn deadline(&self) -> u64 {
        match self {
            Self::Boot { deadline, .. }
            | Self::MaintenanceStop { deadline, .. }
            | Self::Stop { deadline, .. }
            | Self::Quiesce { deadline, .. }
            | Self::Fence { deadline, .. } => *deadline,
        }
    }
}
