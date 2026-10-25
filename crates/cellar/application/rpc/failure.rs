// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use serde::{Deserialize, Serialize};

/// A fixed framework failure code. Application exception text is never a code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, prost::Enumeration)]
#[repr(i32)]
pub enum Code {
    Unspecified = 0,
    InvalidArgument = 1,
    MethodNotFound = 2,
    ContractMismatch = 3,
    ResourceExhausted = 4,
    ContextExpired = 5,
    Cancelled = 6,
    DeadlineExceeded = 7,
    Unavailable = 8,
    OwnershipLost = 9,
    DurabilityUnproven = 10,
    InvalidResponse = 11,
    Internal = 12,
}

/// Evidence about the target call, independent of whether retry is appropriate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, prost::Enumeration)]
#[serde(rename_all = "snake_case")]
#[repr(i32)]
pub enum EffectOutcome {
    Unknown = 0,
    NotCommitted = 1,
}

/// A sanitized infrastructure failure. Expected business errors belong in the
/// application's response message. Returning this from an entered handler never
/// proves that its writes rolled back, even when a nested operation was refused.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Failure {
    code: Code,
    outcome: EffectOutcome,
}

impl Failure {
    /// Construct a handler failure. The effect outcome is always unknown.
    pub fn new(code: Code) -> Self {
        Self {
            code,
            outcome: EffectOutcome::Unknown,
        }
    }

    pub fn code(&self) -> Code {
        self.code
    }

    pub fn outcome(&self) -> EffectOutcome {
        self.outcome
    }

    pub(super) fn refused(code: Code) -> Self {
        Self {
            code,
            outcome: EffectOutcome::NotCommitted,
        }
    }

    pub(super) fn received(code: i32, outcome: i32) -> Result<Self, Self> {
        let code = Code::try_from(code).map_err(|_| Self::new(Code::InvalidResponse))?;
        let outcome =
            EffectOutcome::try_from(outcome).map_err(|_| Self::new(Code::InvalidResponse))?;
        if code == Code::Unspecified {
            return Err(Self::new(Code::InvalidResponse));
        }
        Ok(Self { code, outcome })
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cellar RPC {:?} ({:?})", self.code, self.outcome)
    }
}

impl std::error::Error for Failure {}
