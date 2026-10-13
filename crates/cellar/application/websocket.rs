//! Server upgrades and outbound WebSockets for Rust cells and Workers.

use super::{Activation, Context, EventState, HandlerFuture};
use crate::engine_api::{HttpResponse, HttpResponseWebSocket};
use crate::worker_config::EgressPolicy;
use crate::{host_websocket as host, ws_registry as ws};
use anyhow::{anyhow, Result};
use cellar_native::clone::StructuredValue;
use std::sync::{Arc, Mutex, Weak};

pub use ws::{WsIn as Message, WsPull as SocketEvent};

/// The server end sends frames; the client end transfers it to an HTTP 101.
/// Socket ids and facet routes are allocated by the host.
pub struct WebSocketPair {
    pub client: Upgrade,
    pub server: WebSocket,
}

pub struct Upgrade(WebSocket);

/// An outbound HTTP upgrade preserves a server's ordinary declined response.
pub enum UpgradeResponse {
    Upgraded(WebSocket),
    Declined(HttpResponse),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadyState {
    Connecting,
    Open,
    Closing,
    Closed,
}

#[derive(Clone)]
pub struct WebSocket {
    id: u64,
    owner: Owner,
    state: Arc<Mutex<SocketState>>,
}

#[derive(Clone)]
enum Owner {
    Activation(Weak<Activation>),
    Event(Weak<EventState>),
}

#[derive(Default)]
pub(crate) struct SocketState {
    accepted: bool,
    closed: bool,
    closing: bool,
    connecting: bool,
    protocol: String,
    url: Option<String>,
    polled: bool,
    pull: Option<ws::WsPullQueue>,
    transferred: bool,
    attachment: Option<Vec<u8>>,
}

impl Owner {
    fn new(context: &Context) -> Self {
        match &context.io.native_activation {
            Some(activation) => Self::Activation(Arc::downgrade(activation)),
            None => Self::Event(Arc::downgrade(&context.state)),
        }
    }

    fn check(&self, context: &Context) -> Result<()> {
        context.check_turn()?;
        let matches = match self {
            Self::Activation(original) => original.upgrade().is_some_and(|original| {
                context
                    .io
                    .native_activation
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(&original, current))
            }),
            Self::Event(original) => original
                .upgrade()
                .is_some_and(|original| Arc::ptr_eq(&original, &context.state)),
        };
        anyhow::ensure!(
            matches,
            "WebSocket belongs to another or retired activation"
        );
        Ok(())
    }
}

impl WebSocketPair {
    pub fn new(context: &Context) -> Result<Self> {
        context.check_turn()?;
        let server = WebSocket {
            id: ws::ws_next_id(),
            owner: Owner::new(context),
            state: Arc::new(Mutex::new(SocketState::default())),
        };
        Ok(Self {
            client: Upgrade(server.clone()),
            server,
        })
    }
}

impl WebSocket {
    pub fn ready_state(&self) -> ReadyState {
        let state = self.state.lock().unwrap();
        if state.closed {
            ReadyState::Closed
        } else if state.closing {
            ReadyState::Closing
        } else if state.connecting {
            ReadyState::Connecting
        } else {
            ReadyState::Open
        }
    }

    pub fn protocol(&self) -> String {
        self.state.lock().unwrap().protocol.clone()
    }

    pub fn url(&self) -> Option<String> {
        self.state.lock().unwrap().url.clone()
    }

    pub(crate) fn record_open(context: &Context, id: u64, protocol: &str) {
        if let Ok(socket) = context.websocket(id) {
            let mut state = socket.state.lock().unwrap();
            state.connecting = false;
            state.protocol = protocol.into();
        }
    }

    fn cache(&self, context: &Context) {
        if let Some(activation) = &context.io.native_activation {
            let registry = ws::ws_registry();
            let registry = registry.lock().unwrap();
            let mut states = activation.websocket_states.lock().unwrap();
            states.retain(|id, _| registry.metadata.contains_key(id));
            states.insert(self.id, self.state.clone());
        }
    }

    fn restored(context: &Context, id: u64, attachment: Option<Vec<u8>>) -> Self {
        let activation = context
            .io
            .native_activation
            .as_ref()
            .expect("cell socket has an activation");
        let registry = ws::ws_registry();
        let registry = registry.lock().unwrap();
        let mut states = activation.websocket_states.lock().unwrap();
        states.retain(|id, _| registry.metadata.contains_key(id));
        let state = states
            .entry(id)
            .or_insert_with(|| {
                Arc::new(Mutex::new(SocketState {
                    accepted: true,
                    attachment,
                    ..SocketState::default()
                }))
            })
            .clone();
        Self {
            id,
            owner: Owner::new(context),
            state,
        }
    }

    pub(crate) fn record_close(context: &Context, id: u64) {
        if let Ok(socket) = context.websocket(id) {
            socket.state.lock().unwrap().closed = true;
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// Regular cell sockets retain residency. Stateless sockets receive their
    /// inbound events through `next` and remain owned by the opening request.
    pub fn accept(&self, context: &Context) -> Result<()> {
        self.owner.check(context)?;
        let mut state = self.state.lock().unwrap();
        if state.accepted {
            return Ok(());
        }
        match &context.identity {
            Some(identity) => {
                host::accept_regular(self.id, &identity.scope);
                host::cell_target(&context.io, self.id, &identity.scope)
                    .map_err(anyhow::Error::msg)?;
                context.io.sockets.lock().unwrap().push(self.id);
            }
            None => host::prepare_worker_handoff(&context.io, self.id),
        }
        if let Some(bytes) = &state.attachment {
            host::attachment_set(self.id, bytes.clone());
        }
        state.accepted = true;
        drop(state);
        self.cache(context);
        Ok(())
    }

    /// Hibernation metadata lives in the host and survives eviction of the
    /// cell instance. Facet routes retain their original opening proof.
    pub fn accept_hibernating(&self, context: &Context, tags: Vec<String>) -> Result<()> {
        self.owner.check(context)?;
        let identity = context
            .identity
            .as_ref()
            .ok_or_else(|| anyhow!("hibernatable WebSockets require a cell"))?;
        let facet =
            host::dialing_facet(&context.io, &identity.scope).map_err(anyhow::Error::msg)?;
        let mut state = self.state.lock().unwrap();
        host::accept(self.id, &identity.scope, tags);
        if let Some(facet) = facet {
            ws::ws_attach_facet(self.id, &identity.scope, facet);
        }
        if !state.accepted {
            context.io.sockets.lock().unwrap().push(self.id);
        }
        if let Some(bytes) = &state.attachment {
            host::attachment_set(self.id, bytes.clone());
        }
        state.accepted = true;
        drop(state);
        self.cache(context);
        Ok(())
    }

    pub fn send_text(&self, context: &Context, text: impl Into<String>) -> Result<()> {
        self.send(context, ws::WsOut::Text(text.into()))
    }

    pub fn send_binary(&self, context: &Context, bytes: impl Into<Vec<u8>>) -> Result<()> {
        self.send(context, ws::WsOut::Binary(bytes.into()))
    }

    fn send(&self, context: &Context, frame: ws::WsOut) -> Result<()> {
        self.owner.check(context)?;
        let state = self.state.lock().unwrap();
        anyhow::ensure!(
            state.accepted && !state.closed && !state.closing && !state.connecting,
            "WebSocket is not open"
        );
        emit(context, self.id, frame);
        Ok(())
    }

    pub fn close(&self, context: &Context, code: u16, reason: impl Into<String>) -> Result<()> {
        self.owner.check(context)?;
        anyhow::ensure!(
            code == 1000 || (3000..=4999).contains(&code),
            "WebSocket close code {code} is not permitted"
        );
        let reason = reason.into();
        anyhow::ensure!(
            reason.len() <= 123,
            "WebSocket close reason must not exceed 123 bytes"
        );
        let mut state = self.state.lock().unwrap();
        anyhow::ensure!(state.accepted, "WebSocket is not accepted");
        // Inbound sockets may answer a peer close after their close event.
        state.closing = true;
        emit(context, self.id, ws::WsOut::Close(code, reason));
        Ok(())
    }

    pub fn serialize_attachment(&self, context: &Context, value: &StructuredValue) -> Result<()> {
        self.owner.check(context)?;
        let bytes = cellar_native::clone::encode(value)?;
        let mut state = self.state.lock().unwrap();
        if state.accepted {
            host::attachment_set(self.id, bytes.clone());
        }
        state.attachment = Some(bytes);
        Ok(())
    }

    pub fn deserialize_attachment(&self, context: &Context) -> Result<Option<StructuredValue>> {
        self.owner.check(context)?;
        self.state
            .lock()
            .unwrap()
            .attachment
            .as_deref()
            .map(cellar_native::clone::decode)
            .transpose()
    }

    pub fn tags(&self, context: &Context) -> Result<Vec<String>> {
        self.owner.check(context)?;
        let registry = ws::ws_registry();
        let registry = registry.lock().unwrap();
        let meta = registry
            .metadata
            .get(&self.id)
            .ok_or_else(|| anyhow!("socket is not registered"))?;
        anyhow::ensure!(meta.hibernatable, "socket is not hibernatable");
        Ok(meta.tags.clone())
    }

    pub fn auto_response_timestamp(&self, context: &Context) -> Result<Option<f64>> {
        self.owner.check(context)?;
        Ok(host::auto_response_timestamp(self.id))
    }

    /// Only stateless server sockets have a pull queue; cell sockets enter
    /// through the ordinary WebSocket invocation protocol.
    pub fn next(&self, context: &Context) -> HandlerFuture<SocketEvent> {
        let prepared = self.owner.check(context).and_then(|()| {
            anyhow::ensure!(
                context.identity.is_none() || self.state.lock().unwrap().polled,
                "cell sockets receive WebSocket invocations"
            );
            anyhow::ensure!(
                self.state.lock().unwrap().accepted,
                "WebSocket is not accepted"
            );
            Ok(self
                .state
                .lock()
                .unwrap()
                .pull
                .clone()
                .or_else(|| ws::ws_pull().lock().unwrap().get(&self.id).cloned()))
        });
        let context = context.clone();
        let owner = self.owner.clone();
        let state = self.state.clone();
        Box::pin(async move {
            owner.check(&context)?;
            let event = match prepared? {
                Some(queue) => queue
                    .lock()
                    .await
                    .recv()
                    .await
                    .unwrap_or_else(|| SocketEvent::Close(1006, String::new(), false)),
                None => SocketEvent::Close(1006, "socket is not registered".into(), false),
            };
            owner.check(&context)?;
            if matches!(&event, SocketEvent::Close(..)) {
                state.lock().unwrap().closed = true;
            }
            Ok(event)
        })
    }
}

impl Upgrade {
    pub fn into_response(
        self,
        context: &Context,
        headers: Vec<(String, String)>,
    ) -> Result<HttpResponse> {
        let socket = self.0;
        socket.owner.check(context)?;
        let mut state = socket.state.lock().unwrap();
        anyhow::ensure!(
            state.accepted,
            "WebSocket must be accepted before returning an upgrade"
        );
        anyhow::ensure!(!state.transferred, "WebSocket upgrade already transferred");
        let websocket = match &context.identity {
            Some(identity) => HttpResponseWebSocket::Cell(
                host::cell_target(&context.io, socket.id, &identity.scope)
                    .map_err(anyhow::Error::msg)?,
            ),
            None => HttpResponseWebSocket::Worker(
                host::transfer_worker_handoff(&context.io, socket.id)
                    .ok_or_else(|| anyhow!("WebSocket handoff is not registered"))?,
            ),
        };
        state.transferred = true;
        Ok(HttpResponse {
            status: 101,
            body: Vec::new(),
            stream: None,
            headers,
            websocket: Some(websocket),
            write_position: None,
            observed_position: None,
        })
    }
}

impl Context {
    /// Consume the response's one upgrade capability. Forwarding an unconsumed
    /// response retains its original transport; accepting it creates a typed,
    /// polled client endpoint for local cell, Worker or cross-node targets.
    pub fn accept_websocket(&self, response: &mut HttpResponse) -> HandlerFuture<WebSocket> {
        let prepared = (|| -> Result<_> {
            self.check_turn()?;
            anyhow::ensure!(
                response.status == 101,
                "WebSocket acceptance requires an HTTP 101 response"
            );
            let target = response.websocket.take().ok_or_else(|| {
                anyhow!("WebSocket response was already consumed or has no upgrade")
            })?;
            let protocol = response
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("sec-websocket-protocol"))
                .map(|(_, value)| value.clone())
                .unwrap_or_default();
            let id = ws::ws_next_id();
            let scope = self
                .identity
                .as_ref()
                .map(|identity| identity.scope.clone())
                .unwrap_or_default();
            let binding = host::bind(self.io.clone(), id, scope, target)?;
            let socket = WebSocket {
                id,
                owner: Owner::new(self),
                state: Arc::new(Mutex::new(SocketState {
                    accepted: true,
                    connecting: true,
                    polled: true,
                    protocol,
                    pull: ws::ws_pull().lock().unwrap().get(&id).cloned(),
                    ..SocketState::default()
                })),
            };
            socket.cache(self);
            Ok((socket, binding))
        })();
        let (socket, binding) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let guard = OpeningState {
            id: socket.id,
            state: socket.state.clone(),
            completed: false,
        };
        let context = self.clone();
        Box::pin(async move {
            let mut guard = guard;
            socket.owner.check(&context)?;
            binding.await?;
            socket.owner.check(&context)?;
            socket.state.lock().unwrap().connecting = false;
            guard.completed = true;
            Ok(socket)
        })
    }

    /// Dial through the worker's outbound authority and durability gate.
    /// Cell sockets deliver `Invocation::WebSocketOpen/Message/Close` and
    /// retain their activation. Worker sockets use `next` and live only while
    /// the original event has handler, response-body or background work.
    pub fn connect_websocket(
        &self,
        url: impl Into<String>,
        protocols: Vec<String>,
    ) -> HandlerFuture<WebSocket> {
        let opening = self.open_websocket(url.into(), protocols, Vec::new(), false);
        Box::pin(async move {
            match opening.await? {
                UpgradeResponse::Upgraded(socket) => Ok(socket),
                UpgradeResponse::Declined(_) => unreachable!("a dial refuses declined upgrades"),
            }
        })
    }

    /// HTTP(S) upgrade form, retaining a declined status, headers and body.
    /// Headers use the shared HTTP facade, including combined subprotocols.
    pub fn upgrade_websocket(
        &self,
        url: impl Into<String>,
        headers: cellar_native::web::Headers,
    ) -> HandlerFuture<UpgradeResponse> {
        let protocols = headers.get("sec-websocket-protocol");
        match protocols {
            Ok(protocols) => self.open_websocket(
                url.into(),
                protocols
                    .map(|value| value.split(',').map(|p| p.trim().into()).collect())
                    .unwrap_or_default(),
                headers.entries().collect(),
                true,
            ),
            Err(error) => Box::pin(async move { Err(error.into()) }),
        }
    }

    fn open_websocket(
        &self,
        url: String,
        protocols: Vec<String>,
        headers: Vec<(String, String)>,
        want_response: bool,
    ) -> HandlerFuture<UpgradeResponse> {
        self.open_websocket_with_authority(url, protocols, headers, want_response, false)
    }

    pub(super) fn container_websocket(
        &self,
        url: String,
        headers: cellar_native::web::Headers,
    ) -> HandlerFuture<UpgradeResponse> {
        let protocols = match headers.get("sec-websocket-protocol") {
            Ok(value) => value
                .map(|value| value.split(',').map(|p| p.trim().into()).collect())
                .unwrap_or_default(),
            Err(error) => return Box::pin(async move { Err(error.into()) }),
        };
        self.open_websocket_with_authority(url, protocols, headers.entries().collect(), true, true)
    }

    fn open_websocket_with_authority(
        &self,
        url: String,
        protocols: Vec<String>,
        headers: Vec<(String, String)>,
        want_response: bool,
        container: bool,
    ) -> HandlerFuture<UpgradeResponse> {
        let prepared = (|| -> Result<_> {
            self.check_turn()?;
            if !container {
                match self.worker_config.egress {
                    EgressPolicy::Deny => anyhow::bail!(
                        "This worker is not permitted to access the internet via global functions."
                    ),
                    EgressPolicy::Broker(_) => {
                        anyhow::bail!(
                            "A globalOutbound Fetcher cannot broker WebSockets in cellar."
                        )
                    }
                    EgressPolicy::Allow => {}
                }
                anyhow::ensure!(
                    self.worker_config.native_outbound_broker.is_none(),
                    "A globalOutbound Fetcher cannot broker WebSockets in cellar."
                );
            }
            let parsed = url::Url::parse(&url)
                .map_err(|_| anyhow!("WebSocket Constructor: The url is invalid."))?;
            anyhow::ensure!(
                matches!(parsed.scheme(), "ws" | "wss")
                    || (want_response && matches!(parsed.scheme(), "http" | "https")),
                "WebSocket Constructor: The url scheme must be ws or wss."
            );
            let mut unique = std::collections::HashSet::new();
            for protocol in &protocols {
                anyhow::ensure!(
                    !protocol.is_empty()
                        && protocol
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)),
                    "WebSocket Constructor: The subprotocol '{protocol}' is invalid."
                );
                anyhow::ensure!(
                    unique.insert(protocol),
                    "WebSocket Constructor: The subprotocols must be unique."
                );
            }
            let id = ws::ws_next_id();
            let scope = self
                .identity
                .as_ref()
                .map(|identity| identity.scope.clone())
                .unwrap_or_default();
            let opening = host::connect(
                self.io.clone(),
                id,
                scope,
                url.clone(),
                protocols,
                headers,
                want_response,
            )?;
            let socket = WebSocket {
                id,
                owner: Owner::new(self),
                state: Arc::new(Mutex::new(SocketState {
                    accepted: true,
                    connecting: true,
                    url: (!want_response).then_some(url),
                    ..SocketState::default()
                })),
            };
            socket.cache(self);
            Ok((socket, opening))
        })();
        let (socket, opening) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let guard = OpeningState {
            id: socket.id,
            state: socket.state.clone(),
            completed: false,
        };
        let context = self.clone();
        Box::pin(async move {
            let mut guard = guard;
            socket.owner.check(&context)?;
            let open = opening.await?;
            socket.owner.check(&context)?;
            if let Some(declined) = open.declined {
                return Ok(UpgradeResponse::Declined(HttpResponse {
                    status: declined.status,
                    headers: declined.headers,
                    body: declined.body,
                    stream: None,
                    websocket: None,
                    write_position: None,
                    observed_position: None,
                }));
            }
            {
                let mut state = socket.state.lock().unwrap();
                state.connecting = false;
                state.protocol = open.protocol.unwrap_or_default();
            }
            guard.completed = true;
            Ok(UpgradeResponse::Upgraded(socket))
        })
    }

    pub fn websocket_pair(&self) -> Result<WebSocketPair> {
        WebSocketPair::new(self)
    }

    pub fn websockets(&self, tag: Option<&str>) -> Result<Vec<WebSocket>> {
        self.check_turn()?;
        let identity = self
            .identity
            .as_ref()
            .ok_or_else(|| anyhow!("hibernatable WebSockets require a cell"))?;
        host::list(&identity.scope, tag)
            .into_iter()
            .map(|row| self.websocket(row.id))
            .collect()
    }

    /// Resolve an incoming socket only within the current cell. An application
    /// cannot turn a numeric id from another cell into a socket capability.
    pub fn websocket(&self, id: u64) -> Result<WebSocket> {
        self.check_turn()?;
        let identity = self
            .identity
            .as_ref()
            .ok_or_else(|| anyhow!("socket lookup requires a cell"))?;
        let registry = ws::ws_registry();
        let registry = registry.lock().unwrap();
        let meta = registry
            .metadata
            .get(&id)
            .ok_or_else(|| anyhow!("socket is not registered"))?;
        anyhow::ensure!(
            meta.scope == identity.scope,
            "WebSocket belongs to another cell"
        );
        if let Some(facet) = &meta.facet {
            anyhow::ensure!(
                crate::storage::facet_opening(&identity.scope) == Some(facet.opening),
                "WebSocket belongs to a retired facet opening"
            );
        }
        let attachment = meta.attachment.clone();
        drop(registry);
        Ok(WebSocket::restored(self, id, attachment))
    }

    pub fn set_websocket_auto_response(&self, pair: Option<(String, String)>) -> Result<()> {
        self.check_turn()?;
        let identity = self
            .identity
            .as_ref()
            .ok_or_else(|| anyhow!("WebSocket auto-responses require a cell"))?;
        if let Some((request, response)) = &pair {
            anyhow::ensure!(
                request.len() <= 2048,
                "Request cannot be larger than 2048 bytes. A request of size {} was provided.",
                request.len()
            );
            anyhow::ensure!(
                response.len() <= 2048,
                "Response cannot be larger than 2048 bytes. A response of size {} was provided.",
                response.len()
            );
        }
        host::auto_response_set(&identity.scope, pair);
        Ok(())
    }

    pub fn websocket_auto_response(&self) -> Result<Option<(String, String)>> {
        self.check_turn()?;
        let identity = self
            .identity
            .as_ref()
            .ok_or_else(|| anyhow!("WebSocket auto-responses require a cell"))?;
        Ok(host::auto_response_get(&identity.scope))
    }
}

struct OpeningState {
    id: u64,
    state: Arc<Mutex<SocketState>>,
    completed: bool,
}

impl Drop for OpeningState {
    fn drop(&mut self) {
        if !self.completed {
            self.state.lock().unwrap().closed = true;
            ws::ws_pull_unregister(self.id);
            ws::ws_unregister(self.id);
        }
    }
}

fn emit(context: &Context, id: u64, frame: ws::WsOut) {
    let mut capture = context.state.websocket_capture.lock().unwrap();
    if ws::ws_channel(id) == cellar_logic::Channel::WsHibernatable {
        if let Some(batch) = capture.as_mut() {
            batch.capture(id, frame);
            return;
        }
    }
    drop(capture);
    crate::host_gates::ws_emit_uncaptured(&context.io, id, frame);
}
