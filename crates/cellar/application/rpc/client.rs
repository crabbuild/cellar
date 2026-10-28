// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{contract::Contract, envelope, Code, Failure, HandlerFuture};
use crate::application::{cells::Stub, Context};
use cellar_native::web::AbortSignal;
use prost::Message;
use std::{sync::Arc, time::Duration};

/// Options apply to the entire call, including encoding and output proof waits.
/// Cancellation never promises rollback and generated clients never retry.
#[derive(Clone, Default)]
pub struct CallOptions {
    /// Relative budget from call creation, capped by host policy and any
    /// inherited parent RPC deadline. Encoding uses this same budget.
    pub timeout: Option<Duration>,
    pub signal: Option<AbortSignal>,
}

/// A typed namespace handle tied to the context of one executing invocation.
#[derive(Clone)]
pub struct Client {
    context: Context,
    stub: Stub,
    contract: Arc<Contract>,
}

/// The wire frame owns response capacity while its business copy crosses
/// native preflight/decoder admission. Raw-byte APIs hand that copy to their
/// caller; native typed APIs keep this owner through response decoding.
struct Received {
    bytes: Vec<u8>,
    deadline: u64,
    method: super::Method,
    _frame: crate::engine_api::RpcData,
}

impl Client {
    pub fn bind(
        context: &Context,
        binding: &str,
        name: &str,
        contract: Arc<Contract>,
    ) -> Result<Self, Failure> {
        context
            .check_turn()
            .map_err(|_| Failure::refused(Code::ContextExpired))?;
        // Generated code carries the schema with conservative defaults. A
        // provider or consumer declaration owns the actual application limits.
        let contract = context
            .rpc_catalog
            .as_deref()
            .and_then(|catalog| catalog.contract(contract.digest()))
            .unwrap_or(contract);
        let stub = context
            .namespace(binding)
            .map_err(|_| Failure::refused(Code::InvalidArgument))?
            .get_by_name(name);
        Ok(Self {
            context: context.clone(),
            stub,
            contract,
        })
    }

    pub fn call<Request, Response>(
        &self,
        method: &str,
        request: Request,
        options: CallOptions,
    ) -> HandlerFuture<Response>
    where
        Request: Message + super::MessageGraph,
        Response: Message + Default + Send + 'static,
    {
        let started = crate::asyncrt::mono_ms();
        let deadline = self.deadline(&options, started);
        let admission = self
            .context
            .check_turn()
            .map_err(|_| Failure::refused(Code::ContextExpired))
            .and_then(|_| {
                if self.contract.method(method).is_some() {
                    Ok(())
                } else {
                    Err(Failure::refused(Code::MethodNotFound))
                }
            })
            .and_then(|_| {
                if options
                    .signal
                    .as_ref()
                    .is_some_and(|signal| signal.throw_if_aborted().is_err())
                {
                    Err(Failure::refused(Code::Cancelled))
                } else {
                    Ok(())
                }
            })
            .and_then(|_| {
                if deadline
                    .as_ref()
                    .is_ok_and(|deadline| *deadline <= crate::asyncrt::mono_ms())
                {
                    Err(Failure::refused(Code::DeadlineExceeded))
                } else {
                    deadline.as_ref().map(|_| ()).map_err(Clone::clone)
                }
            });
        let result = match admission {
            Ok(()) => super::encode_message(request, self.contract.limits())
                .map_err(|failure| Failure::refused(failure.code())),
            Err(failure) => {
                super::discard_message(request);
                Err(failure)
            }
        };
        let signal = options.signal.clone();
        let contract = self.contract.clone();
        let context = self.context.clone();
        let call = self.start(method, result, options, deadline);
        Box::pin(async move {
            let received = call.await?;
            let admission =
                super::DecodeReservation::new(&contract, &received.method.output, &received.bytes);
            check_completion(received.deadline, signal.as_ref())?;
            let _admission = admission?;
            let response = Response::decode(received.bytes.as_slice())
                .map_err(|_| Failure::new(Code::InvalidResponse));
            check_completion(received.deadline, signal.as_ref())?;
            context
                .check_turn()
                .map_err(|_| Failure::new(Code::ContextExpired))?;
            response
        })
    }

    /// Binary transport used by generated language clients. Arguments are
    /// already owned and pass descriptor preflight before any native submission.
    pub fn call_bytes(
        &self,
        method: &str,
        request: Vec<u8>,
        options: CallOptions,
    ) -> HandlerFuture<Vec<u8>> {
        let deadline = self.deadline(&options, crate::asyncrt::mono_ms());
        let call = self.start(method, Ok(request), options, deadline);
        Box::pin(async move { call.await.map(|received| received.bytes) })
    }

    /// Submit guest-encoded bytes under a budget minted before its codecs ran.
    /// The opaque budget must belong to this client's original invocation.
    pub fn call_bytes_in_budget(
        &self,
        method: &str,
        request: Vec<u8>,
        options: CallOptions,
        budget: super::CallBudget,
    ) -> HandlerFuture<Vec<u8>> {
        let deadline = budget.deadline_for(&self.context).and_then(|deadline| {
            self.deadline(&options, crate::asyncrt::mono_ms())
                .map(|current| deadline.min(current))
        });
        let call = self.start(method, Ok(request), options, deadline);
        Box::pin(async move { call.await.map(|received| received.bytes) })
    }

    fn deadline(&self, options: &CallOptions, started: u64) -> Result<u64, Failure> {
        let host_ms = self
            .context
            .io
            .host_services()
            .operation_deadline_ms()
            .map_err(|_| Failure::refused(Code::Unavailable))?;
        Ok(call_deadline(
            started,
            host_ms,
            options.timeout,
            self.context.io.rpc_deadline_ms.get().copied(),
        ))
    }

    fn start(
        &self,
        method: &str,
        request: Result<Vec<u8>, Failure>,
        options: CallOptions,
        deadline: Result<u64, Failure>,
    ) -> HandlerFuture<Received> {
        let context = self.context.clone();
        let stub = self.stub.clone();
        let contract = self.contract.clone();
        let prepared = (|| {
            context
                .check_turn()
                .map_err(|_| Failure::refused(Code::ContextExpired))?;
            let method = contract
                .method(method)
                .cloned()
                .ok_or_else(|| Failure::refused(Code::MethodNotFound))?;
            let deadline = deadline?;
            if deadline <= crate::asyncrt::mono_ms() {
                return Err(Failure::refused(Code::DeadlineExceeded));
            }
            let request = request?;
            contract.validate_message(&method.input, &request)?;
            let args = super::encode_envelope(
                contract.digest(),
                request,
                contract.limits().max_message_bytes,
            )?;
            if deadline <= crate::asyncrt::mono_ms() {
                return Err(Failure::refused(Code::DeadlineExceeded));
            }
            if options
                .signal
                .as_ref()
                .is_some_and(|s| s.throw_if_aborted().is_err())
            {
                return Err(Failure::refused(Code::Cancelled));
            }
            Ok((method, args, deadline))
        })();
        Box::pin(async move {
            let (method, args, deadline) = prepared?;
            context
                .check_turn()
                .map_err(|_| Failure::refused(Code::ContextExpired))?;
            let remaining = deadline.saturating_sub(crate::asyncrt::mono_ms());
            if remaining == 0 {
                return Err(Failure::refused(Code::DeadlineExceeded));
            }
            if options
                .signal
                .as_ref()
                .is_some_and(|signal| signal.throw_if_aborted().is_err())
            {
                return Err(Failure::refused(Code::Cancelled));
            }
            // Once the native future is polled, failures without call-local
            // proof are conservative. Dropping it cancels its exact request.
            let response_signal = options.signal.clone();
            let operation = async {
                let result = stub
                    .rpc_scoped(
                        &context,
                        &method.name,
                        args,
                        crate::host_rpc::CallScope::in_context(
                            &context,
                            Some(deadline),
                            response_signal.clone(),
                        )
                        .with_reply_limit(envelope::response_transport_limit(contract.limits())),
                    )
                    .await;
                check_completion(deadline, response_signal.as_ref())?;
                let data = result.map_err(|error| {
                    Failure::new(
                        error
                            .downcast_ref::<Failure>()
                            .map_or(Code::Unavailable, Failure::code),
                    )
                })?;
                context
                    .check_turn()
                    .map_err(|_| Failure::new(Code::ContextExpired))?;
                let bytes = envelope::response(contract.digest(), &data, contract.limits())?;
                contract
                    .validate_message(&method.output, &bytes)
                    .map_err(|_| Failure::new(Code::InvalidResponse))?;
                check_completion(deadline, response_signal.as_ref())?;
                Ok(Received {
                    bytes,
                    deadline,
                    method,
                    _frame: data,
                })
            };
            let cancellation = async {
                match options.signal {
                    Some(signal) => {
                        signal.cancelled().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            };
            crate::asyncrt::select_biased! { "RPC deadline and cancellation win an undelivered result";
                _ = crate::asyncrt::sleep(Duration::from_millis(remaining)) => Err(Failure::new(Code::DeadlineExceeded)),
                _ = cancellation => Err(Failure::new(Code::Cancelled)),
                result = operation => result,
            }
        })
    }
}

// Compute from the original start, before request sizing/encoding. A nested
// call can shorten its parent budget but cannot buy another host-sized window.
fn call_deadline(
    started: u64,
    host_ms: u64,
    requested: Option<Duration>,
    parent: Option<u64>,
) -> u64 {
    let timeout = requested
        .map(|value| u64::try_from(value.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(host_ms)
        .min(host_ms);
    started
        .saturating_add(timeout)
        .min(parent.unwrap_or(u64::MAX))
}

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;

// A ready operation can spend time decoding without yielding to the timer.
// Recheck after synchronous codec work before releasing an otherwise valid reply.
fn check_completion(deadline: u64, signal: Option<&AbortSignal>) -> Result<(), Failure> {
    if crate::asyncrt::mono_ms() >= deadline {
        return Err(Failure::new(Code::DeadlineExceeded));
    }
    if signal.is_some_and(|signal| signal.throw_if_aborted().is_err()) {
        return Err(Failure::new(Code::Cancelled));
    }
    Ok(())
}
