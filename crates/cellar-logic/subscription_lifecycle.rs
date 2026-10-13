//! Administrative subscription transitions. Adapters authenticate the caller,
//! compare durable incarnation identity and persist the plan with its audit.

use crate::event_stream::{cursor, Failure};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Active,
    Paused,
    Expired,
    Deleted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Pause,
    Resume,
    Reset { first: i64 },
    Delete { force: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Facts {
    pub state: State,
    pub checkpoint: i64,
    pub earliest: i64,
    pub end: i64,
    pub required: bool,
    pub other_required: usize,
    pub leased: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Change {
    pub state: State,
    pub checkpoint: i64,
    pub invalidate: bool,
    pub skipped: Option<(i64, i64)>,
}

pub fn change(facts: Facts, action: Action) -> Result<Change, Failure> {
    if facts.state == State::Deleted {
        return Err(Failure::SubscriptionReset);
    }
    if facts.checkpoint < 0
        || facts.earliest <= 0
        || facts.end < facts.earliest
        || facts.checkpoint >= facts.end
    {
        return Err(Failure::InvalidCursor);
    }
    let first = facts
        .checkpoint
        .checked_add(1)
        .ok_or(Failure::InvalidCursor)?;
    let mut result = Change {
        state: facts.state,
        checkpoint: facts.checkpoint,
        invalidate: false,
        skipped: None,
    };
    match action {
        Action::Pause | Action::Resume => {
            if facts.state == State::Expired || first < facts.earliest {
                return Err(Failure::CursorExpired {
                    earliest: facts.earliest,
                });
            }
            result.state = if action == Action::Pause {
                State::Paused
            } else {
                State::Active
            };
        }
        Action::Reset { first: target } => {
            cursor(facts.earliest, facts.end, target)?;
            result.state = State::Active;
            result.checkpoint = target - 1;
            result.invalidate = true;
            if target > first {
                result.skipped = Some((first, target - 1));
            }
        }
        Action::Delete { force } => {
            if facts.required && facts.other_required == 0 {
                return Err(Failure::LastRequiredSubscription);
            }
            if !force && (first < facts.end || facts.leased) {
                return Err(Failure::PendingDelivery);
            }
            result.state = State::Deleted;
            result.invalidate = true;
            if first < facts.end {
                result.skipped = Some((first, facts.end - 1));
            }
        }
    }
    Ok(result)
}
