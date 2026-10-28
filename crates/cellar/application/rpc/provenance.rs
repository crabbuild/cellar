// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{envelope, Code, Failure, Limits};
use crate::application::{Invocation, Output};

/// Reply policy captured before application code takes ownership of a request.
/// Only the host's pre-factory catalog path may bypass this policy and report
/// a proven refusal. A custom cell has the same obligations as a generated one.
pub(crate) struct Entered {
    digest: Option<String>,
    response: Option<(crate::rpc_response::Admission, usize)>,
}

impl Entered {
    pub(crate) fn new(invocation: &Invocation) -> Self {
        let digest = match invocation {
            Invocation::CellRpc { args, .. } => envelope::view(args).ok().map(|(digest, _)| digest),
            _ => None,
        };
        Self {
            digest,
            response: None,
        }
    }

    /// Host admission precedes factories, handlers and framework response
    /// codecs. The owned allowance crosses suspension and output-proof waits.
    pub(crate) fn admit_response(
        &mut self,
        limits: Limits,
    ) -> Result<Option<crate::engine_api::RpcData>, Failure> {
        if let Some(digest) = &self.digest {
            let limit = envelope::response_transport_limit(limits);
            let held = match crate::asyncrt::services()
                .reserve_rpc_response(crate::rpc_response::encoding_weight(limit)?)
            {
                Ok(held) => held,
                // Fixed control fits the request's existing control
                // admission. Echo the call's digest so all peer clients
                // preserve its code; this supplies no rollback proof.
                Err(failure) => {
                    return envelope::reply(digest, Err(Failure::new(failure.code())), limits)
                        .map(Some)
                }
            };
            self.response = Some((held, limit));
        }
        Ok(None)
    }

    pub(crate) fn finish(self, output: Output) -> Result<Output, Failure> {
        match output {
            Output::Rpc(data) if self.digest.is_some() => {
                let data = envelope::entered_reply(data)?;
                let Some((held, limit)) = self.response else {
                    return Ok(Output::Rpc(data));
                };
                let crate::engine_api::RpcData::Structured(bytes) = data else {
                    return Err(Failure::new(Code::InvalidResponse));
                };
                if bytes.len() > limit {
                    return Err(Failure::new(Code::InvalidResponse));
                }
                Ok(Output::Rpc(crate::engine_api::RpcData::Structured(
                    held.normalize(bytes)?,
                )))
            }
            output => Ok(output),
        }
    }
}

#[cfg(test)]
#[path = "provenance_tests.rs"]
mod tests;
