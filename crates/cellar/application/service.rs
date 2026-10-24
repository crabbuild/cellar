// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Host-minted service routes and outbound authority for native applications.
use super::*;
use crate::host_channels::{begin_call_cancellation, SvcCallReq, SvcRpcReq};
use crate::host_gates::{egress_gate_request, EgressGate};
use crate::http_streams::RequestBodyGuard;
use crate::worker_config::{EgressPolicy, LoaderEnvRoute, OutboundBroker};
use cellar_native::clone::{Object, StructuredValue, Value};
use tokio::sync::oneshot;

pub(crate) const ENV_SERVICE_MARKER: &str = "__cellar$loaderSvc";
const DENIED: &str = "This worker is not permitted to access the internet via global functions like fetch(). It must use capabilities (such as bindings in 'env') to talk to the outside world.";

#[derive(Clone)]
/// A service route minted from a declared binding or a supplied capability.
/// Its deployment generation remains the origin of every routed call.
/// Loaded loopback handles follow their loader or original facet activation.
pub struct Fetcher {
    pub(crate) generation: crate::generation::GenerationId,
    script: String,
    entrypoint: Option<crate::WorkerFetchEntrypoint>,
    // A derived loader lifetime or weak activation authority; no Slot cycle.
    loaded: Option<super::loader::CallOrigin>,
    host: std::sync::Weak<crate::host_services::HostServices>,
    props_limit: usize,
}

impl Fetcher {
    pub(crate) fn from_route(
        generation: crate::generation::GenerationId,
        route: &LoaderEnvRoute,
        host: &Arc<crate::host_services::HostServices>,
    ) -> Self {
        Self {
            generation,
            script: route.script.clone(),
            loaded: None,
            host: Arc::downgrade(host),
            props_limit: host.loader_policy().max_props_bytes,
            entrypoint: route
                .entrypoint
                .clone()
                .map(|name| crate::WorkerFetchEntrypoint {
                    name,
                    props: route.props.clone(),
                }),
        }
    }

    /// Charge the copied route fields before allocating their copies.
    pub(crate) fn route(&self, remaining: &mut usize) -> Result<LoaderEnvRoute> {
        let bytes = self
            .script
            .len()
            .checked_add(self.entrypoint.as_ref().map_or(0, |e| e.name.len()))
            .and_then(|n| n.checked_add(self.entrypoint.as_ref().map_or(0, |e| e.props.len())))
            .ok_or_else(|| anyhow!("worker loader: environment exceeds size limit"))?;
        *remaining = remaining
            .checked_sub(bytes)
            .ok_or_else(|| anyhow!("worker loader: environment exceeds size limit"))?;
        Ok(LoaderEnvRoute {
            script: self.script.clone(),
            entrypoint: self.entrypoint.as_ref().map(|e| e.name.clone()),
            props: self
                .entrypoint
                .as_ref()
                .map(|e| e.props.clone())
                .unwrap_or_default(),
        })
    }

    pub(crate) fn broker(&self) -> OutboundBroker {
        OutboundBroker {
            generation: self.generation,
            script: self.script.clone(),
            entrypoint: self.entrypoint.clone(),
        }
    }

    pub(crate) fn check_context(&self, context: &Context) -> Result<()> {
        context.check_turn()?;
        anyhow::ensure!(
            self.host.upgrade().is_some_and(
                |host| Arc::ptr_eq(&host, context.io.host_services()) && host.accepts_work()
            ),
            "service capability belongs to another or closed execution domain"
        );
        Ok(())
    }

    pub fn with_props(&self, props: &StructuredValue) -> Result<Self> {
        let name = self
            .entrypoint
            .as_ref()
            .map(|e| e.name.clone())
            .unwrap_or_else(|| "default".into());
        Ok(Self {
            entrypoint: Some(crate::WorkerFetchEntrypoint {
                name,
                props: super::loader::props_bytes(props, self.props_limit)?,
            }),
            ..self.clone()
        })
    }

    pub fn fetch(&self, context: &Context, mut request: Request) -> HandlerFuture<HttpResponse> {
        if let Err(error) = self.check_context(context) {
            return Box::pin(async move { Err(error) });
        }
        // Incoming dispatch metadata is not request-selected authority. A
        // forwarded request always uses this handle's target and properties.
        request.entrypoint = None;
        if let Some(worker) = &self.loaded {
            return worker
                .routed_entrypoint(self.entrypoint.as_ref())
                .fetch(context, request);
        }
        let prepared = (|| -> Result<_> {
            context.check_turn()?;
            request.method = super::http::normalize_method(&request.method)?;
            context.io.charge_subrequest().map_err(anyhow::Error::msg)?;
            let guard = body_guard(context, Some(&request.body))?;
            Ok((
                guard,
                egress_gate_request(&context.io, cellar_logic::Channel::Service),
            ))
        })();
        match prepared {
            Ok((guard, gate)) => self.fetch_prepared(context, request, guard, gate),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }

    pub(crate) fn fetch_prepared(
        &self,
        context: &Context,
        request: Request,
        guard: RequestBodyGuard,
        gate: EgressGate,
    ) -> HandlerFuture<HttpResponse> {
        if let Err(error) = self.check_context(context) {
            return Box::pin(async move { Err(error) });
        }
        if let Some(worker) = &self.loaded {
            return worker
                .routed_entrypoint(self.entrypoint.as_ref())
                .fetch_prepared(context, request, guard, gate);
        }
        let fetcher = self.clone();
        let context = context.clone();
        Box::pin(async move {
            let (_, cancel, mut cancellation) = begin_call_cancellation();
            let (reply, receive) = oneshot::channel();
            let result = crate::host_service::fetch(
                gate,
                SvcCallReq {
                    cancel: Some(cancel),
                    generation: fetcher.generation,
                    script: fetcher.script,
                    entrypoint: fetcher.entrypoint,
                    url: request.url,
                    method: request.method,
                    body: request.body,
                    body_guard: guard,
                    headers: request.headers,
                    reply,
                },
                receive,
            )
            .await
            .map_err(anyhow::Error::msg);
            cancellation.disarm();
            context.check_turn()?;
            result
        })
    }

    pub fn rpc(
        &self,
        context: &Context,
        operation: crate::WorkerRpcOperation,
    ) -> HandlerFuture<Vec<u8>> {
        if let Err(error) = self.check_context(context) {
            return Box::pin(async move { Err(error) });
        }
        if let Some(worker) = &self.loaded {
            return worker
                .routed_entrypoint(self.entrypoint.as_ref())
                .rpc(context, operation);
        }
        let prepared = context
            .check_turn()
            .and_then(|_| context.io.charge_subrequest().map_err(anyhow::Error::msg))
            .map(|_| egress_gate_request(&context.io, cellar_logic::Channel::Service));
        let fetcher = self.clone();
        let context = context.clone();
        Box::pin(async move {
            let gate = prepared?;
            let (reply, receive) = oneshot::channel();
            let entry = fetcher.entrypoint.unwrap_or(crate::WorkerFetchEntrypoint {
                name: "default".into(),
                props: Vec::new(),
            });
            let bytes = crate::host_service::rpc(
                gate,
                SvcRpcReq {
                    cancel: None,
                    generation: fetcher.generation,
                    script: fetcher.script,
                    entrypoint: entry.name,
                    props: entry.props,
                    operation,
                    reply,
                },
                receive,
            )
            .await
            .map_err(anyhow::Error::msg)?;
            context.check_turn()?;
            Ok(bytes)
        })
    }

    /// Abort an unfinished service or loaded-worker RPC without invalidating
    /// this reusable handle. The original abort reason reaches the caller.
    pub fn rpc_with_signal(
        &self,
        context: &Context,
        operation: crate::WorkerRpcOperation,
        signal: cellar_native::web::AbortSignal,
    ) -> HandlerFuture<Vec<u8>> {
        if let Err(reason) = signal.throw_if_aborted() {
            return Box::pin(async move { Err(reason.into()) });
        }
        let call = self.rpc(context, operation);
        Box::pin(async move {
            crate::asyncrt::select_biased! { "a service RPC caller abort wins an undelivered reply";
                reason=signal.cancelled()=>Err(reason.into()),result=call=>result,
            }
        })
    }

    pub fn fetch_with_signal(
        &self,
        context: &Context,
        request: Request,
        signal: cellar_native::web::AbortSignal,
    ) -> HandlerFuture<HttpResponse> {
        if let Err(reason) = signal.throw_if_aborted() {
            return Box::pin(async move { Err(reason.into()) });
        }
        let call = self.fetch(context, request);
        Box::pin(async move {
            crate::asyncrt::select_biased! { "a service caller abort wins an undelivered reply";
                reason=signal.cancelled()=>Err(reason.into()),result=call=>result,
            }
        })
    }
}

pub(super) fn body_guard(
    context: &Context,
    body: Option<&RequestBody>,
) -> Result<RequestBodyGuard> {
    match body.and_then(RequestBody::stream_id) {
        Some(id) => context
            .io
            .transfer_body_stream(id)
            .ok_or_else(|| anyhow!("fetch: body stream is not owned")),
        None => Ok(RequestBodyGuard(None)),
    }
}

impl Context {
    pub fn service(&self, name: &str) -> Result<Fetcher> {
        self.check_turn()?;
        let (_, script, entrypoint) = self
            .worker_config
            .services
            .iter()
            .find(|(binding, _, _)| binding == name)
            .ok_or_else(|| anyhow!("no service binding named {name}"))?;
        Ok(Fetcher {
            generation: self.generation,
            script: script.clone(),
            loaded: None,
            host: Arc::downgrade(self.io.host_services()),
            props_limit: self.io.host_services().loader_policy().max_props_bytes,
            entrypoint: entrypoint
                .as_ref()
                .map(|name| crate::WorkerFetchEntrypoint {
                    name: name.clone(),
                    props: Vec::new(),
                }),
        })
    }

    pub fn exported_entrypoint(&self, name: impl Into<String>) -> Result<Fetcher> {
        self.check_turn()?;
        let loaded = match self.worker_config.origin {
            crate::worker_config::WorkerOrigin::Deployment => None,
            crate::worker_config::WorkerOrigin::Dynamic => {
                Some(super::loader::CallOrigin::for_context(self)?)
            }
        };
        Ok(Fetcher {
            generation: self.generation,
            script: self.worker_config.script_name.clone(),
            loaded,
            host: Arc::downgrade(self.io.host_services()),
            props_limit: self.io.host_services().loader_policy().max_props_bytes,
            entrypoint: Some(crate::WorkerFetchEntrypoint {
                name: name.into(),
                props: Vec::new(),
            }),
        })
    }

    pub fn environment_fetcher(&self, value: &Value) -> Result<Fetcher> {
        self.check_turn()?;
        let Value::Reference(index) = value else {
            return Err(anyhow!("environment value is not a service capability"));
        };
        let graph = self
            .loaded_environment
            .as_ref()
            .ok_or_else(|| anyhow!("this worker has no loaded environment"))?;
        let Some(Object::Object(fields)) = graph.objects.get(*index as usize) else {
            return Err(anyhow!("environment value is not a service capability"));
        };
        let key = Value::from(ENV_SERVICE_MARKER);
        let route = fields
            .iter()
            .find_map(|(name, value)| (name == &key).then_some(value))
            .ok_or_else(|| anyhow!("environment value is not a service capability"))?;
        let Value::Number(route) = route else {
            return Err(anyhow!("invalid environment service capability"));
        };
        anyhow::ensure!(
            route.is_finite() && *route >= 0.0 && route.fract() == 0.0,
            "invalid environment service capability"
        );
        let index = *route as usize;
        let env = self
            .worker_config
            .loader_env
            .as_ref()
            .ok_or_else(|| anyhow!("missing environment route table"))?;
        let route = env
            .routes
            .get(index)
            .ok_or_else(|| anyhow!("environment service capability has no supplied route"))?;
        if let Some(fetcher) = self.worker_config.native_environment_fetchers.get(index) {
            return Ok(fetcher.clone());
        }
        Ok(Fetcher::from_route(
            self.generation,
            route,
            self.io.host_services(),
        ))
    }

    /// Perform SDK outbound HTTP through this worker's configured authority.
    /// This policy does not sandbox arbitrary Rust network access.
    pub fn fetch(&self, mut request: super::http::OutboundRequest) -> HandlerFuture<HttpResponse> {
        let prepared = (|| -> Result<_> {
            self.check_turn()?;
            request.method = super::http::normalize_method(&request.method)?;
            self.io.charge_subrequest().map_err(anyhow::Error::msg)?;
            anyhow::ensure!(self.worker_config.egress != EgressPolicy::Deny, "{DENIED}");
            let guard = body_guard(self, request.body.as_ref())?;
            Ok((
                guard,
                egress_gate_request(&self.io, cellar_logic::Channel::Fetch),
            ))
        })();
        let (guard, gate) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        if let Some(fetcher) = &self.worker_config.native_outbound_broker {
            return fetcher.fetch_prepared(
                self,
                Request {
                    url: request.url,
                    method: request.method,
                    headers: request.headers,
                    body: request.body.unwrap_or_else(|| Vec::new().into()),
                    entrypoint: None,
                },
                guard,
                gate,
            );
        }
        let context = self.clone();
        Box::pin(async move {
            let (_, cancel, mut cancellation) = begin_call_cancellation();
            let result = match context.worker_config.egress.clone() {
                EgressPolicy::Broker(broker) => {
                    let (reply, receive) = oneshot::channel();
                    crate::host_service::fetch(
                        gate,
                        SvcCallReq {
                            cancel: Some(cancel),
                            generation: broker.generation,
                            script: broker.script,
                            entrypoint: broker.entrypoint,
                            url: request.url,
                            method: request.method,
                            body: request.body.unwrap_or_else(|| Vec::new().into()),
                            body_guard: guard,
                            headers: request.headers,
                            reply,
                        },
                        receive,
                    )
                    .await
                    .map_err(anyhow::Error::msg)
                }
                EgressPolicy::Allow => {
                    let head = crate::host_fetch::fetch(
                        context.io.clone(),
                        request.method,
                        request.url,
                        request.body,
                        request.headers,
                        request.redirect.as_str(),
                        guard,
                        gate,
                        Some(cancel),
                        context.trace,
                    )
                    .await
                    .map_err(anyhow::Error::msg)?;
                    context.check_turn()?;
                    head.into_http().map_err(anyhow::Error::msg)
                }
                EgressPolicy::Deny => {
                    unreachable!("outbound authority was checked before suspension")
                }
            };
            cancellation.disarm();
            context.check_turn()?;
            result
        })
    }

    pub fn fetch_with_signal(
        &self,
        request: super::http::OutboundRequest,
        signal: cellar_native::web::AbortSignal,
    ) -> HandlerFuture<HttpResponse> {
        if let Err(reason) = signal.throw_if_aborted() {
            return Box::pin(async move { Err(reason.into()) });
        }
        let call = self.fetch(request);
        Box::pin(async move {
            crate::asyncrt::select_biased! { "an outbound caller abort wins an undelivered reply";
                reason=signal.cancelled()=>Err(reason.into()),result=call=>result,
            }
        })
    }
}
