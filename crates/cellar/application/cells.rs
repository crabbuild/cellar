// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Business cell identities and callable namespaces over existing host routing.
use super::*;
use cellar_native::cell_id;
pub use cellar_native::cell_id::Id;
use cellar_native::clone::{encode_with_limits, Limits, Object, StructuredValue, Value};

/// A configured cell namespace. Equal business names address the same cell
/// across instances, restarts and nodes using the existing namespace protocol.
#[derive(Clone, Debug)]
pub struct Namespace(cell_id::Namespace);

impl Namespace {
    pub fn id_from_name(&self, name: &str) -> Id {
        self.0.id_from_name(name)
    }

    pub fn id_from_name_utf16(&self, name: &[u16]) -> Id {
        self.0.id_from_name_utf16(name)
    }

    pub fn id_from_string(&self, id: &str) -> Result<Id> {
        Ok(self.0.id_from_string(id)?)
    }

    pub fn new_unique_id(&self) -> Result<Id> {
        Ok(self.0.new_unique_id()?)
    }

    pub fn jurisdiction(&self, value: Option<&str>) -> Result<Self> {
        Ok(Self(self.0.jurisdiction(value)?))
    }

    pub fn get_by_name(&self, name: &str) -> Stub {
        self.stub(self.id_from_name(name))
    }

    pub fn get(&self, id: &Id) -> Result<Stub> {
        self.0.accepts(id)?;
        Ok(self.stub(id.clone()))
    }

    fn stub(&self, id: Id) -> Stub {
        Stub {
            name: id.dispatch_name().map(Arc::from),
            id,
            broken: Arc::new(Mutex::new(None)),
        }
    }
}

/// A business entity's remote handle. Clone aliases share failure state;
/// obtaining another stub from its namespace starts a fresh handle.
#[derive(Clone, Debug)]
pub struct Stub {
    id: Id,
    name: Option<Arc<str>>,
    broken: Arc<Mutex<Option<Arc<str>>>>,
}

impl Stub {
    pub fn id(&self) -> &Id {
        &self.id
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn name_utf16(&self) -> Option<&[u16]> {
        self.name.as_ref().and(self.id.name_utf16())
    }

    fn check(&self, context: &Context) -> Result<()> {
        context.check_turn()?;
        if let Some(reason) = self.broken.lock().unwrap().as_ref() {
            return Err(anyhow!(reason.to_string()));
        }
        Ok(())
    }

    fn finish<T>(&self, result: Result<T>) -> Result<T> {
        result.map_err(|error| {
            // Typed routing errors are retryable host verdicts, not a reason
            // to permanently break the entity handle.
            if error
                .downcast_ref::<crate::cell_dispatch::RoutedRequestError>()
                .is_some()
            {
                return error;
            }
            let message = error.to_string();
            for marker in ["__CELLAR_ACTOR_ABORT__:", "__CELLAR_PROCESS_EXIT__:"] {
                if let Some(start) = message.find(marker) {
                    let reason = &message[start + marker.len()..];
                    let mut broken = self.broken.lock().unwrap();
                    *broken = Some(Arc::from(reason));
                    return anyhow!(reason.to_string());
                }
            }
            error
        })
    }

    pub fn fetch(&self, context: &Context, mut request: Request) -> HandlerFuture<HttpResponse> {
        if let Err(error) = self.check(context).and_then(|_| {
            request.method = super::http::normalize_method(&request.method)?;
            Ok(())
        }) {
            return Box::pin(async move { Err(error) });
        }
        request.entrypoint = None;
        let call = context.fetch_cell(
            self.id.scope(),
            self.name.as_deref().map(str::to_string),
            request,
        );
        let context = context.clone();
        let stub = self.clone();
        Box::pin(async move {
            let response = call.await;
            context.check_turn()?;
            stub.finish(response)
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
            crate::asyncrt::select_biased! { "entity fetch abort wins an undelivered reply";
                reason=signal.cancelled()=>Err(reason.into()), result=call=>result,
            }
        })
    }

    pub fn rpc(
        &self,
        context: &Context,
        method: impl Into<String>,
        args: RpcData,
    ) -> HandlerFuture<RpcData> {
        self.rpc_scoped(
            context,
            method,
            args,
            crate::host_rpc::CallScope::in_context(
                context,
                context.io.rpc_deadline_ms.get().copied(),
                None,
            ),
        )
    }

    pub(crate) fn rpc_scoped(
        &self,
        context: &Context,
        method: impl Into<String>,
        args: RpcData,
        call_scope: crate::host_rpc::CallScope,
    ) -> HandlerFuture<RpcData> {
        if let Err(error) = self.check(context) {
            return Box::pin(async move { Err(error) });
        }
        let call = crate::native_worker::rpc_cell(
            context,
            self.id.scope(),
            self.name.as_deref().map(str::to_string),
            method.into(),
            args,
            call_scope,
        );
        let context = context.clone();
        let stub = self.clone();
        Box::pin(async move {
            let result = call.await;
            context.check_turn()?;
            stub.finish(result)
        })
    }

    pub fn rpc_with_signal(
        &self,
        context: &Context,
        method: impl Into<String>,
        args: RpcData,
        signal: cellar_native::web::AbortSignal,
    ) -> HandlerFuture<RpcData> {
        if let Err(reason) = signal.throw_if_aborted() {
            return Box::pin(async move { Err(reason.into()) });
        }
        let call = self.rpc_scoped(
            context,
            method,
            args,
            crate::host_rpc::CallScope::in_context(
                context,
                context.io.rpc_deadline_ms.get().copied(),
                Some(signal.clone()),
            ),
        );
        Box::pin(async move {
            crate::asyncrt::select_biased! { "a cell RPC abort wins an undelivered reply";
                reason=signal.cancelled()=>Err(reason.into()), result=call=>result,
            }
        })
    }

    /// Invoke with a structured argument array and decode the result graph.
    /// Cycles, shared references, typed buffers and UTF-16 values remain intact.
    pub fn call(
        &self,
        context: &Context,
        method: impl Into<String>,
        arguments: &StructuredValue,
    ) -> HandlerFuture<StructuredValue> {
        self.call_with_limits(context, method, arguments, Limits::default())
    }

    /// Apply clone byte/graph limits to arguments and routed replies, including
    /// thrown values. Reply refusal can follow effects in the remote method.
    pub fn call_with_limits(
        &self,
        context: &Context,
        method: impl Into<String>,
        arguments: &StructuredValue,
        limits: Limits,
    ) -> HandlerFuture<StructuredValue> {
        let prepared = self.check(context).and_then(|_| {
            let Value::Reference(index) = &arguments.root else {
                anyhow::bail!("cell RPC arguments must be an array");
            };
            anyhow::ensure!(
                matches!(
                    arguments.objects.get(*index as usize),
                    Some(Object::DenseArray { .. } | Object::SparseArray { .. })
                ),
                "cell RPC arguments must be an array"
            );
            Ok(RpcData::Structured(
                encode_with_limits(arguments, limits)?.into(),
            ))
        });
        let args = match prepared {
            Ok(args) => args,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let call = self.rpc(context, method, args);
        let stub = self.clone();
        Box::pin(async move {
            // Remote throws arrive as successful transport replies with an
            // error envelope. Decode before applying the stub failure protocol.
            stub.finish(super::decode_rpc_result_with_limits(&call.await?, limits))
        })
    }

    pub fn call_with_signal(
        &self,
        context: &Context,
        method: impl Into<String>,
        arguments: &StructuredValue,
        signal: cellar_native::web::AbortSignal,
    ) -> HandlerFuture<StructuredValue> {
        if let Err(reason) = signal.throw_if_aborted() {
            return Box::pin(async move { Err(reason.into()) });
        }
        let call = self.call(context, method, arguments);
        Box::pin(async move {
            crate::asyncrt::select_biased! { "a structured cell RPC abort wins an undelivered reply";
                reason=signal.cancelled()=>Err(reason.into()), result=call=>result,
            }
        })
    }
}

impl Context {
    /// Resolve a declared business cell binding in the current invocation.
    /// Retain its stubs between turns and pass each turn's context when calling.
    ///
    /// ```
    /// use cellar::application::{Invocation, Output, Registry};
    /// let app = Registry::new(|context, invocation| Box::pin(async move {
    ///     let Invocation::Fetch(request) = invocation else {
    ///         anyhow::bail!("expected an HTTP request");
    ///     };
    ///     let accounts = context.namespace("ACCOUNTS")?;
    ///     let account = accounts.get_by_name("tenant:acme/account:42");
    ///     Ok(Output::Http(account.fetch(&context, request).await?))
    /// }));
    /// ```
    pub fn namespace(&self, binding: &str) -> Result<Namespace> {
        self.check_turn()?;
        let (_, class) = self
            .worker_config
            .bindings
            .iter()
            .find(|(name, _)| name == binding)
            .ok_or_else(|| anyhow!("no cell namespace binding named {binding}"))?;
        self.exported_namespace(class)
    }

    pub fn exported_namespace(&self, class: &str) -> Result<Namespace> {
        self.check_turn()?;
        anyhow::ensure!(
            self.worker_config
                .do_classes
                .iter()
                .any(|name| name == class),
            "no Durable Object namespace key for {class}"
        );
        Ok(Namespace(cell_id::Namespace::new(
            class,
            crate::engine_api::namespace_key(&self.worker_config.script_name, class),
        )))
    }
}
