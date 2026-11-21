// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Remote creation retries preserve the captured identity, inputs and route budget.
use super::*;
use crate::raft::query_wire::enrollment::CellEnrollmentCall;
use crate::raft::transport::{CellRemoteEnrollmentResponse, RaftTransport, TransportError};
use cellar_logic::consensus::MemberId;

impl CellEnrollmentInvocation {
    pub(crate) fn bind_remote_route(&mut self, route: super::super::RemoteMutationRoute) {
        self.remote_route = Some(route);
    }

    pub fn attempt(&self) -> Option<&CellRouteAttempt> {
        self.attempt.as_ref()
    }

    /// Select a remote attempt. Once selected, this invocation cannot be handed
    /// to a local controller as a fresh creation, even after transport failure.
    pub async fn begin(&mut self) -> Result<(), CellEnrollmentError> {
        if self.origin.is_some() || self.remote_route.is_some() {
            return Err(self.error(CellDispatchError::Invalid("local enrollment ownership")));
        }
        self.started = true;
        self.dispatched = true;
        let attempt = self
            .session
            .begin()
            .await
            .map_err(|e| self.error(CellDispatchError::Routing(e)))?;
        self.attempt = Some(attempt);
        self.dispatched = false;
        Ok(())
    }

    pub async fn retry(&mut self) -> Result<(), CellEnrollmentError> {
        let previous = self.current()?.clone();
        self.dispatched = true;
        let next = self
            .session
            .retry(&previous)
            .await
            .map_err(|e| self.error(CellDispatchError::Routing(e)))?;
        self.attempt = Some(next);
        self.dispatched = false;
        Ok(())
    }

    pub async fn redirect(&mut self, hint: &GroupRoute) -> Result<(), CellEnrollmentError> {
        let previous = self.current()?.clone();
        self.dispatched = true;
        let next = self
            .session
            .redirect(&previous, hint)
            .await
            .map_err(|e| self.error(CellDispatchError::Routing(e)))?;
        self.attempt = Some(next);
        self.dispatched = false;
        Ok(())
    }

    fn current(&self) -> Result<&CellRouteAttempt, CellEnrollmentError> {
        self.attempt
            .as_ref()
            .ok_or_else(|| self.error(CellDispatchError::Invalid("missing enrollment route")))
    }

    /// One consumed remote attempt. Unknown transmission remains ambiguous.
    /// An authenticated matching non-entry reply can settle only this send;
    /// it never erases uncertainty retained from an earlier attempt.
    pub async fn execute_remote(
        &mut self,
        transport: &RaftTransport,
        member: MemberId,
    ) -> Result<CellRemoteEnrollmentResponse, CellEnrollmentError> {
        let attempt = self.current()?.clone();
        self.session
            .validate_attempt(&attempt)
            .map_err(|e| self.error(CellDispatchError::Routing(e)))?;
        if self.dispatched || self.origin.is_some() || self.remote_route.is_some() {
            return Err(self.error(CellDispatchError::Invalid(
                "consumed or local enrollment attempt",
            )));
        }
        if !transport.uses_command_domain(&self.dispatcher.domain) {
            return Err(self.error(CellDispatchError::Invalid("enrollment transport domain")));
        }
        let now = self
            .dispatcher
            .domain
            .mono_ms()
            .map_err(|_| self.error(closed()))?;
        let timeout_ms = self
            .session
            .deadline_ms()
            .checked_sub(now)
            .filter(|n| *n > 0)
            .ok_or_else(|| {
                self.error(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded))
            })?;
        let call = CellEnrollmentCall {
            request: CellEnrollmentRequest {
                creation: self.creation,
                durability: self.durability,
            },
            inputs: self.inputs,
            preview: self.preview,
            timeout_ms,
            route_epoch: attempt.route().epoch.get(),
            birth_digest: attempt.route().birth_digest,
        };
        self.session
            .mark_submitted(&attempt)
            .map_err(|e| self.error(CellDispatchError::Routing(e)))?;
        self.dispatched = true;
        let previously_ambiguous = self.ambiguous;
        self.ambiguous = true;
        let result = self
            .dispatcher
            .domain
            .run(transport.enroll_cell(
                attempt.route(),
                &self.cell,
                member,
                call,
                self.dispatcher.keys.clone(),
                self.session.deadline_ms(),
            ))
            .await
            .map_err(|_| self.error(closed()))?;
        let response = result.map_err(|error| {
            self.error(match error {
                TransportError::Command(failure) => {
                    CellDispatchError::Execution(ExecutionError::Refused(failure))
                }
                other => CellDispatchError::Transport(other),
            })
        })?;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                if error.creation != self.creation {
                    return Err(self.error(CellDispatchError::Invalid(
                        "remote enrollment failure binding",
                    )));
                }
                let error = preserve_ambiguity(error, previously_ambiguous);
                self.ambiguous = error.ambiguous;
                return Err(error);
            }
        };
        self.session
            .validate_attempt(&attempt)
            .map_err(|e| self.error(CellDispatchError::Routing(e)))?;
        Ok(response)
    }
}

fn preserve_ambiguity(mut returned: CellEnrollmentError, previous: bool) -> CellEnrollmentError {
    returned.ambiguous |= previous;
    returned
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authenticated_non_entry_never_erases_prior_uncertainty() {
        let creation = ProposalId::new([23; 16]).unwrap();
        for previous in [false, true] {
            for reported in [false, true] {
                let error = preserve_ambiguity(
                    CellEnrollmentError {
                        creation,
                        ambiguous: reported,
                        cause: Box::new(CellDispatchError::Runtime(RuntimeError::NotReady)),
                    },
                    previous,
                );
                assert_eq!(error.creation, creation);
                assert_eq!(error.ambiguous, previous || reported);
                assert!(matches!(
                    error.cause.as_ref(),
                    CellDispatchError::Runtime(RuntimeError::NotReady)
                ));
            }
        }
    }
}
