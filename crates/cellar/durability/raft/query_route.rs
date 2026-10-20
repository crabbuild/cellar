//! Bounded read attempts. Every destination receives the original contract,
//! input and consistency; attempts consume one shared invocation deadline.

use super::budget::Permit;
use super::query_wire::QueryCall;
use super::runtime::RaftRuntime;
use super::transport::{RaftTransport, TransportError};
use crate::application::consensus::client::AttemptControl;
use crate::application::consensus::{
    CommandFailure, CommittedVersion, MemberId, OperationContract, QueryOptions, QueryResult,
    QueryRouting, RaftCellHandle, VersionKeys,
};
use std::{collections::BTreeSet, sync::Arc, time::Duration};

pub(super) struct RouteContext {
    pub local: RaftCellHandle,
    pub member: MemberId,
    pub runtime: RaftRuntime,
    pub transport: RaftTransport,
    pub keys: VersionKeys,
    pub control: Arc<AttemptControl>,
    pub minimum: Option<CommittedVersion>,
}

pub(super) struct RoutedOutput {
    pub result: QueryResult,
    pub bytes: Permit,
}

enum Attempt {
    Success(RoutedOutput),
    Retry(CommandFailure),
    Fatal(CommandFailure),
}

impl RouteContext {
    pub async fn run(
        self,
        contract: OperationContract,
        input: Vec<u8>,
        mut options: QueryOptions,
    ) -> Result<RoutedOutput, CommandFailure> {
        let QueryRouting::Replicas {
            members,
            attempt_timeout,
            leader_fallback,
        } = std::mem::take(&mut options.routing)
        else {
            return Err(CommandFailure::UnsupportedConsistency);
        };
        if members.is_empty()
            || members.len() > 8
            || attempt_timeout.as_millis() == 0
            || attempt_timeout > Duration::from_secs(60)
        {
            return Err(CommandFailure::UnsupportedConsistency);
        }
        let mut tried = BTreeSet::new();
        let mut last = CommandFailure::VersionUnavailable;
        for member in members {
            if !tried.insert(member) {
                return Err(CommandFailure::UnsupportedConsistency);
            }
            match self
                .attempt(member, &contract, &input, &options, attempt_timeout)
                .await
            {
                Attempt::Success(result) => return Ok(result),
                Attempt::Fatal(error) => return Err(error),
                Attempt::Retry(error) => last = error,
            }
        }
        if leader_fallback && !self.control.cancelled() {
            // This is a routing hint only. The destination still obtains the
            // requested read proof; a stale leader hint grants no authority.
            let status = self.runtime.status(self.local.scope());
            let status = crate::asyncrt::select! { _ = self.control.cancellation() => return Err(CommandFailure::VersionUnavailable), value = status => value };
            if let Ok(status) = status {
                if let Some(member) = status
                    .leader
                    .and_then(|id| MemberId::new(id).ok())
                    .filter(|member| tried.insert(*member))
                {
                    match self
                        .attempt(member, &contract, &input, &options, attempt_timeout)
                        .await
                    {
                        Attempt::Success(result) => return Ok(result),
                        Attempt::Fatal(error) => return Err(error),
                        Attempt::Retry(error) => last = error,
                    }
                }
            }
        }
        if self.control.cancelled() {
            Err(CommandFailure::VersionUnavailable)
        } else {
            Err(last)
        }
    }

    async fn attempt(
        &self,
        member: MemberId,
        contract: &OperationContract,
        input: &[u8],
        options: &QueryOptions,
        timeout: Duration,
    ) -> Attempt {
        if self.control.cancelled() {
            return Attempt::Fatal(CommandFailure::VersionUnavailable);
        }
        let domain = &self.local.handle.domain;
        let Ok(now) = domain.mono_ms() else {
            return Attempt::Fatal(CommandFailure::Unavailable);
        };
        let deadline = self
            .control
            .deadline()
            .min(now.saturating_add(timeout.as_millis() as u64));
        let Some(remaining) = deadline.checked_sub(now).filter(|ms| *ms > 0) else {
            return Attempt::Fatal(CommandFailure::VersionUnavailable);
        };
        let Ok(timer) = domain.sleep_until(deadline) else {
            return Attempt::Fatal(CommandFailure::Unavailable);
        };
        let mut options = options.clone();
        options.routing = QueryRouting::Local;
        options.timeout = Duration::from_millis(remaining);
        let query = async {
            let result = if member == self.member {
                self.local
                    .query(contract.clone(), input.to_vec(), options)
                    .await
            } else {
                let delivery = self.transport.query(
                    self.local.scope(),
                    member,
                    QueryCall {
                        contract: contract.clone(),
                        input: input.to_vec(),
                        options,
                    },
                    self.keys.clone(),
                    deadline,
                )?;
                delivery.receive().await?.value
            };
            Ok::<_, TransportError>(result)
        };
        let result = crate::asyncrt::select! {
            _ = self.control.cancellation() => return Attempt::Fatal(CommandFailure::VersionUnavailable),
            _ = timer => return Attempt::Retry(CommandFailure::VersionUnavailable),
            result = query => result,
        };
        if self.control.cancelled() {
            return Attempt::Fatal(CommandFailure::VersionUnavailable);
        }
        match result {
            Ok(Ok(result)) => {
                if result.served_by != member
                    || result.version.scope != *self.local.scope()
                    || self
                        .minimum
                        .as_ref()
                        .is_some_and(|minimum| result.version.covers(minimum) != Ok(true))
                {
                    return Attempt::Fatal(CommandFailure::CorruptState);
                }
                let Some(bytes) = self.local.handle.budget.acquire(result.result.len()) else {
                    return Attempt::Retry(CommandFailure::Overloaded);
                };
                Attempt::Success(RoutedOutput { result, bytes })
            }
            Ok(Err(error @ (CommandFailure::WrongIncarnation | CommandFailure::CorruptState))) => {
                Attempt::Fatal(error)
            }
            Ok(Err(error)) => Attempt::Retry(error),
            Err(
                TransportError::Authentication
                | TransportError::Identity
                | TransportError::Protocol
                | TransportError::Replay,
            ) => Attempt::Fatal(CommandFailure::Unavailable),
            Err(TransportError::Overloaded) => Attempt::Retry(CommandFailure::Overloaded),
            Err(_) => Attempt::Retry(CommandFailure::VersionUnavailable),
        }
    }
}
