// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Invocation-owned streaming and socket capabilities shared by language adapters.
use crate::{
    failure::Validation,
    values::{self, field},
    Limits,
};
use anyhow::{bail, ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::sdk::{
    streams::{ByteReadable, ByteWriter, IdentityTransformStream},
    websocket::{Message, SocketEvent, Upgrade, WebSocket},
    Context, Invocation, InvocationAuthority, Output,
};
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock,
    },
};
use tokio::sync::{oneshot, watch};

type Head = oneshot::Sender<Result<Output>>;

/// Capabilities are local to one invocation. Socket IDs cross as decimal strings;
/// every use still passes the native activation/turn checks.
#[derive(Default)]
pub struct Session {
    request_body: crate::request_body::Reader,
    authority: OnceLock<InvocationAuthority>,
    head: Mutex<Option<Head>>,
    pipe: Option<(ByteReadable, ByteWriter)>,
    streaming: Mutex<bool>,
    body_started: AtomicBool,
    body_finished: AtomicBool,
    sockets: Mutex<HashMap<u64, WebSocket>>,
    upgrades: Mutex<HashMap<u64, Upgrade>>,
    responses: Mutex<HashMap<String, cellar::engine_api::HttpResponse>>,
    cancellations: Mutex<HashMap<String, watch::Sender<bool>>>,
    sequence: std::sync::atomic::AtomicU64,
}

/// Retain the language driver after an early response head. The native event
/// driver polls it with the same authority as the original handler. Dropping the
/// HTTP body cancels even a producer waiting on language-side I/O.
pub async fn serve<F, Fut>(context: &Context, make: F) -> Result<Output>
where
    F: FnOnce(Arc<Session>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<Output>> + Send + 'static,
{
    serve_with_lifetime(context, make, false).await
}

/// Retain a Workers-compatible producer through native background work even if
/// its request or response consumer disconnects. Response writes still observe
/// their own backpressure/cancellation; independent background jobs may finish.
pub async fn serve_retained<F, Fut>(context: &Context, make: F) -> Result<Output>
where
    F: FnOnce(Arc<Session>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<Output>> + Send + 'static,
{
    serve_with_lifetime(context, make, true).await
}

async fn serve_with_lifetime<F, Fut>(context: &Context, make: F, retained: bool) -> Result<Output>
where
    F: FnOnce(Arc<Session>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<Output>> + Send + 'static,
{
    let pipe = IdentityTransformStream::new();
    let writer = pipe.writable.get_writer()?;
    let (head, mut receive) = oneshot::channel();
    let owned = Arc::new(Session {
        head: Mutex::new(Some(head)),
        pipe: Some((pipe.readable.clone(), writer)),
        ..Default::default()
    });
    let producer_context = context.clone();
    let mut driver = Box::pin(async move {
        struct Producer(ByteReadable, bool);

        impl Drop for Producer {
            fn drop(&mut self) {
                if !self.1 {
                    self.0.error(cellar::sdk::web::AbortReason::error(
                        "guest response producer failed or stopped",
                    ));
                }
            }
        }
        let mut producer = Producer(owned.pipe.as_ref().unwrap().0.clone(), false);
        let writer = &owned.pipe.as_ref().unwrap().1;
        let result = if retained {
            make(owned.clone()).await
        } else {
            cellar::asyncrt::select_biased! { "disconnect revokes streaming invocation authority";
                _ = async {
                    // Producer EOF is distinct from consumer cancellation.
                    if writer.closed().await.is_ok() {
                        std::future::pending::<()>().await;
                    }
                } => bail!("guest response consumer closed"),
                result = make(owned.clone()) => result,
            }
        };
        let head = owned.head.lock().unwrap().take();
        if let Some(head) = head {
            let _ = head.send(result);
        } else {
            result?;
        }
        if !owned.body_finished.load(Ordering::Acquire) {
            if owned.body_started.load(Ordering::Acquire) {
                producer_context.finish_stream(writer).await?;
            } else {
                writer.close().await?;
            }
        }
        producer.1 = true;
        Ok(())
    });

    struct Cancel(ByteReadable, bool);

    impl Drop for Cancel {
        fn drop(&mut self) {
            if self.1 {
                let _ = self.0.cancel(None);
            }
        }
    }
    let mut cancel = Cancel(pipe.readable.clone(), true);
    if retained {
        context.wait_until(driver)?;
        cancel.1 = false;
        return receive.await.context("guest response producer stopped")?;
    }
    // Before the response head, poll SDK operations through the actual handler
    // future. Wrappers that pause/cancel that future must retain control. Only
    // a streaming response or upgrade transfers the remaining driver to the
    // native invocation's owned background work.
    let (result, completed) = cellar::asyncrt::select_biased! { "publish an early response before continuing its producer";
        head = &mut receive => (head.context("guest response producer stopped")?, false),
        result = &mut driver => {
            result?;
            (receive.await.context("guest response producer stopped")?, true)
        },
    };
    if !completed {
        context.wait_until_byte_stream(&pipe.readable, driver)?;
    }
    cancel.1 = false;
    result
}

impl Session {
    /// Admit one native incoming body without consuming it. Per-read bounds,
    /// invocation checks and cancellation apply before every source pull.
    pub fn request(&self, context: &Context, request: cellar::sdk::Request) -> Result<Value> {
        self.check(context)?;
        values::headers(&request.headers)?;
        self.request_body
            .install(context.take_body(request.body)?)?;
        Ok(
            json!({"url":request.url,"method":request.method,"headers":request.headers,"body":"","body_stream":true}),
        )
    }

    fn check(&self, context: &Context) -> Result<()> {
        let candidate = context.invocation_authority()?;
        self.authority.get_or_init(|| candidate).check(context)
    }

    pub(crate) fn response_capacity(&self, context: &Context, limits: &Limits) -> Result<()> {
        self.check(context)?;
        ensure!(
            self.cancellations.lock().unwrap().len() < limits.max_batch_operations,
            Validation
        );
        Ok(())
    }

    pub(crate) fn retain_response(
        &self,
        context: &Context,
        response: cellar::engine_api::HttpResponse,
        limits: &Limits,
    ) -> Result<Value> {
        self.check(context)?;
        values::headers(&response.headers)?;
        let mut cancellations = self.cancellations.lock().unwrap();
        let mut responses = self.responses.lock().unwrap();
        ensure!(
            cancellations.len() < limits.max_batch_operations,
            "response capability limit"
        );
        let retained = responses.values().fold(0usize, |total, response| {
            total.saturating_add(response.body.len())
        });
        ensure!(
            retained.saturating_add(response.body.len()) <= limits.max_body_bytes,
            "response prefixes exceed body limit"
        );
        let id = self
            .sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .to_string();
        let metadata = json!({"forward":id,"status":response.status,"headers":response.headers,"body":STANDARD.encode(&response.body),"stream":response.stream.is_some(),"websocket":response.websocket.is_some()});
        cancellations.insert(id.clone(), watch::channel(false).0);
        responses.insert(id, response);
        Ok(metadata)
    }

    /// Publish a completed response while the original driver drains background
    /// work. A disconnected consumer does not revoke unrelated native jobs.
    pub fn publish(&self, output: Result<Output>) -> Result<()> {
        if let Some(head) = self.head.lock().unwrap().take() {
            let _ = head.send(output);
        } else {
            ensure!(
                matches!(output, Ok(Output::Done) | Err(_)),
                "response already published"
            );
            if output.is_err() {
                if let Some((readable, _)) = &self.pipe {
                    // Fail the body immediately while independent retained
                    // work drains under the original invocation authority.
                    readable.error(cellar::sdk::web::AbortReason::error(
                        "guest response producer failed",
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn streaming(&self) -> bool {
        *self.streaming.lock().unwrap()
    }

    pub fn output(&self, context: &Context, value: &Value) -> Result<Option<Output>> {
        self.check(context)?;
        if let Some(id) = value.get("forward").and_then(Value::as_str) {
            ensure!(!self.streaming(), "response already started");
            let headers: Option<Vec<(String, String)>> = value
                .get("headers")
                .map(|headers| serde_json::from_value(headers.clone()))
                .transpose()?;
            if let Some(headers) = &headers {
                values::headers(headers)?;
            }
            self.cancellations.lock().unwrap().remove(id);
            let mut response = self
                .responses
                .lock()
                .unwrap()
                .remove(id)
                .context("unknown response capability")?;
            if let Some(headers) = headers {
                response.headers = headers;
            }
            return Ok(Some(Output::Http(response)));
        }
        if self.streaming() {
            ensure!(
                value["streamed"] == true,
                "streaming handler returned another response"
            );
            return Ok(Some(Output::Done));
        }
        if let Some(id) = value.get("websocket") {
            let id = id.as_str().context("invalid socket capability")?.parse()?;
            let upgrade = self
                .upgrades
                .lock()
                .unwrap()
                .remove(&id)
                .context("unknown upgrade")?;
            let headers: Vec<(String, String)> = serde_json::from_value(value["headers"].clone())?;
            values::headers(&headers)?;
            return Ok(Some(Output::Http(upgrade.into_response(context, headers)?)));
        }
        Ok(None)
    }

    fn socket(&self, context: &Context, value: &Value) -> Result<WebSocket> {
        let id = field(value, "id")?.parse().context("invalid socket ID")?;
        if let Some(socket) = self.sockets.lock().unwrap().get(&id) {
            return Ok(socket.clone());
        }
        context.websocket(id)
    }

    pub async fn execute(
        &self,
        context: &Context,
        name: &str,
        value: Value,
        limits: &Limits,
    ) -> Result<Value> {
        self.check(context)?;
        values::check_depth(&value).context(Validation)?;
        let result = self.execute_inner(context, name, value, limits).await;
        self.check(context)?;
        result
    }

    async fn execute_inner(
        &self,
        context: &Context,
        name: &str,
        value: Value,
        limits: &Limits,
    ) -> Result<Value> {
        match name {
            "request.body.read" | "request.body.cancel" => {
                ensure!(
                    value.as_object().is_some_and(serde_json::Map::is_empty),
                    Validation
                );
                if name == "request.body.cancel" {
                    self.request_body.cancel()?;
                    return Ok(Value::Null);
                }
                let bytes = self.request_body.read(limits.max_body_bytes).await?;
                Ok(
                    json!({"done":bytes.is_none(),"body":STANDARD.encode(bytes.unwrap_or_default())}),
                )
            }
            "cell.fetch_stream" | "http.fetch_stream" => {
                self.response_capacity(context, limits)?;
                let request =
                    values::input(&value["request"], limits.max_body_bytes).context(Validation)?;
                let response = if name == "cell.fetch_stream" {
                    context
                        .namespace(field(&value, "binding")?)?
                        .get_by_name(field(&value, "name")?)
                        .fetch(context, request)
                        .await?
                } else {
                    context
                        .fetch(cellar::sdk::http::OutboundRequest {
                            url: request.url,
                            method: request.method,
                            headers: request.headers,
                            body: Some(request.body),
                            redirect: cellar::sdk::http::Redirect::Follow,
                        })
                        .await?
                };
                self.retain_response(context, response, limits)
            }
            "response.read" => {
                let id = field(&value, "id")?;
                let mut cancelled = self
                    .cancellations
                    .lock()
                    .unwrap()
                    .get(id)
                    .context("unknown response capability")?
                    .subscribe();
                let mut response = self
                    .responses
                    .lock()
                    .unwrap()
                    .remove(id)
                    .context("unknown response capability")?;
                // The initial prefix was already returned to the language reader.
                response.body.clear();
                let bytes = cellar::asyncrt::select_biased! { "response cancellation wins a pending read";
                    _ = cancelled.changed() => bail!("response reader cancelled"),
                    result = async {
                        match response.stream.as_mut() {
                            Some(stream) => stream.next().await.transpose().map_err(anyhow::Error::msg),
                            None => Ok(None),
                        }
                    } => result?,
                };
                if let Some(bytes) = &bytes {
                    ensure!(
                        bytes.len() <= limits.max_body_bytes,
                        "stream chunk exceeds body limit"
                    );
                }
                self.responses.lock().unwrap().insert(id.into(), response);
                Ok(
                    json!({"done":bytes.is_none(),"body":STANDARD.encode(bytes.unwrap_or_default())}),
                )
            }
            "response.cancel" => {
                let id = field(&value, "id")?;
                if let Some(cancelled) = self.cancellations.lock().unwrap().remove(id) {
                    let _ = cancelled.send(true);
                }
                self.responses.lock().unwrap().remove(id);
                Ok(Value::Null)
            }
            "response.upgrade" => {
                let Output::Http(response) =
                    self.output(context, &value)?.context("upgrade required")?
                else {
                    bail!("upgrade required")
                };
                self.head
                    .lock()
                    .unwrap()
                    .take()
                    .context("response already sent")?
                    .send(Ok(Output::Http(response)))
                    .map_err(|_| anyhow::anyhow!("response consumer closed"))?;
                *self.streaming.lock().unwrap() = true;
                Ok(Value::Null)
            }
            "response.finish" => {
                ensure!(
                    self.body_started.load(Ordering::Acquire),
                    "response body has not started"
                );
                if !self.body_finished.load(Ordering::Acquire) {
                    context
                        .finish_stream(&self.pipe.as_ref().unwrap().1)
                        .await?;
                    self.body_finished.store(true, Ordering::Release);
                }
                Ok(Value::Null)
            }
            "response.closed" => {
                ensure!(
                    self.body_started.load(Ordering::Acquire),
                    "response body has not started"
                );
                let writer = &self.pipe.as_ref().context("streaming requires fetch")?.1;
                // Both a clean close and a consumer cancellation end production.
                let _ = writer.closed().await;
                Ok(Value::Null)
            }
            "response.start" => {
                ensure!(!self.streaming(), "response already started");
                // Validate headers/status before publishing the head.
                let mut response =
                    values::output_body(context, &value, Vec::new(), limits.max_body_bytes)
                        .context(Validation)?;
                let (readable, _) = self.pipe.as_ref().context("streaming requires fetch")?;
                response.stream = Some(cellar::sdk::streams::http_byte_stream(readable)?);
                self.body_started.store(true, Ordering::Release);
                let head = self
                    .head
                    .lock()
                    .unwrap()
                    .take()
                    .context("response already sent")?;
                *self.streaming.lock().unwrap() = true;
                head.send(Ok(Output::Http(response)))
                    .map_err(|_| anyhow::anyhow!("response consumer closed"))?;
                Ok(Value::Null)
            }
            "response.write" => {
                ensure!(self.streaming(), "response has not started");
                let bytes = STANDARD
                    .decode(field(&value, "body")?)
                    .context(Validation)?;
                ensure!(
                    bytes.len() <= limits.max_body_bytes,
                    "stream chunk exceeds body limit"
                );
                context
                    .write_stream_chunk(&self.pipe.as_ref().unwrap().1, bytes)
                    .await?;
                Ok(Value::Null)
            }
            "websocket.from_response" => {
                ensure!(
                    self.sockets.lock().unwrap().len() < limits.max_batch_operations,
                    Validation
                );
                let response_id = field(&value, "response")?;
                self.cancellations.lock().unwrap().remove(response_id);
                let mut response = self
                    .responses
                    .lock()
                    .unwrap()
                    .remove(response_id)
                    .context("unknown response capability")?;
                let socket = context.accept_websocket(&mut response).await?;
                let id = socket.id();
                self.sockets.lock().unwrap().insert(id, socket);
                Ok(json!({"id":id.to_string()}))
            }
            "websocket.pair" => {
                ensure!(
                    self.sockets.lock().unwrap().len() < limits.max_batch_operations,
                    Validation
                );
                let pair = context.websocket_pair()?;
                let id = pair.server.id();
                self.upgrades.lock().unwrap().insert(id, pair.client);
                self.sockets.lock().unwrap().insert(id, pair.server);
                Ok(json!({"id":id.to_string()}))
            }
            "websocket.connect" => {
                ensure!(
                    self.sockets.lock().unwrap().len() < limits.max_batch_operations,
                    Validation
                );
                let protocols =
                    serde_json::from_value(value["protocols"].clone()).context(Validation)?;
                let socket = context
                    .connect_websocket(field(&value, "url")?, protocols)
                    .await?;
                let id = socket.id();
                self.sockets.lock().unwrap().insert(id, socket);
                Ok(json!({"id":id.to_string()}))
            }
            "websocket.list" => {
                let tag = value
                    .get("tag")
                    .filter(|v| !v.is_null())
                    .map(|v| v.as_str().context("invalid tag"))
                    .transpose()?;
                Ok(json!(context
                    .websockets(tag)?
                    .iter()
                    .map(|s| s.id().to_string())
                    .collect::<Vec<_>>()))
            }
            "websocket.auto_response.get" => Ok(json!(context.websocket_auto_response()?)),
            "websocket.auto_response" => {
                let pair = serde_json::from_value(value["pair"].clone()).context(Validation)?;
                context.set_websocket_auto_response(pair)?;
                Ok(Value::Null)
            }
            _ => {
                let socket = self.socket(context, &value)?;
                match name {
                    "websocket.accept" => {
                        ensure!(
                            value.get("polled").is_none_or(Value::is_boolean),
                            Validation
                        );
                        if value["polled"] == true {
                            ensure!(value["hibernating"] != true, Validation);
                            socket.accept_polled(context)?;
                        } else if value["hibernating"] == true {
                            socket.accept_hibernating(
                                context,
                                serde_json::from_value(value["tags"].clone())
                                    .context(Validation)?,
                            )?;
                        } else {
                            socket.accept(context)?;
                        }
                    }
                    "websocket.send" => {
                        if let Some(text) = value.get("text") {
                            let text = text.as_str().context("text must be a string")?;
                            ensure!(
                                text.len() <= limits.max_body_bytes,
                                "socket frame exceeds body limit"
                            );
                            socket.send_text(context, text)?;
                        } else {
                            let bytes = STANDARD
                                .decode(field(&value, "body")?)
                                .context(Validation)?;
                            ensure!(
                                bytes.len() <= limits.max_body_bytes,
                                "socket frame exceeds body limit"
                            );
                            socket.send_binary(context, bytes)?;
                        }
                    }
                    "websocket.close" => socket.close(
                        context,
                        serde_json::from_value(value["code"].clone()).context(Validation)?,
                        field(&value, "reason")?,
                    )?,
                    "websocket.closed" => {
                        return socket_event(socket.closed(context).await?, limits.max_body_bytes)
                    }
                    "websocket.next" => {
                        return socket_event(socket.next(context).await?, limits.max_body_bytes)
                    }
                    "websocket.attachment.set" => socket.serialize_attachment(
                        context,
                        &cellar::sdk::clone::StructuredValue::from_json(value["value"].clone())?,
                    )?,
                    "websocket.attachment.get" => {
                        return Ok(socket
                            .deserialize_attachment(context)?
                            .map(|v| v.to_json())
                            .transpose()?
                            .unwrap_or(Value::Null))
                    }
                    "websocket.tags" => return Ok(json!(socket.tags(context)?)),
                    "websocket.auto_response_timestamp" => {
                        return Ok(json!(socket.auto_response_timestamp(context)?))
                    }
                    "websocket.info" => {
                        return Ok(
                            json!({"protocol":socket.protocol(),"url":socket.url(),"state":format!("{:?}",socket.ready_state())}),
                        )
                    }
                    _ => bail!("unsupported transport operation"),
                }
                Ok(Value::Null)
            }
        }
    }
}

pub fn invocation(event: Invocation, limit: usize) -> Result<(&'static str, Value)> {
    Ok(match event {
        Invocation::WebSocketOpen { id, protocol } => (
            "websocket_open",
            json!({"id":id.to_string(),"protocol":protocol}),
        ),
        Invocation::WebSocketMessage { id, data } => (
            "websocket_message",
            match data {
                Message::Text(text) => {
                    frame_limit(text.len(), limit)?;
                    json!({"id":id.to_string(),"text":text})
                }
                Message::Binary(body) => {
                    frame_limit(body.len(), limit)?;
                    json!({"id":id.to_string(),"body":STANDARD.encode(body)})
                }
            },
        ),
        Invocation::WebSocketClose {
            id,
            code,
            reason,
            was_clean,
        } => (
            "websocket_close",
            json!({"id":id.to_string(),"code":code,"reason":reason,"was_clean":was_clean}),
        ),
        _ => bail!("unsupported guest invocation"),
    })
}

fn socket_event(event: SocketEvent, limit: usize) -> Result<Value> {
    Ok(match event {
        SocketEvent::Open(protocol) => json!({"type":"open","protocol":protocol}),
        SocketEvent::Text(text) => {
            frame_limit(text.len(), limit)?;
            json!({"type":"message","text":text})
        }
        SocketEvent::Binary(bytes) => {
            frame_limit(bytes.len(), limit)?;
            json!({"type":"message","body":STANDARD.encode(bytes)})
        }
        SocketEvent::Close(code, reason, was_clean) => {
            json!({"type":"close","code":code,"reason":reason,"was_clean":was_clean})
        }
    })
}

fn frame_limit(bytes: usize, limit: usize) -> Result<()> {
    ensure!(bytes <= limit, "incoming socket frame exceeds body limit");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inbound_frames_use_raw_byte_limits_before_language_encoding() {
        for limit in [0, 1, 2, 3] {
            let binary = || Invocation::WebSocketMessage {
                id: u64::MAX,
                data: Message::Binary(vec![255; 2]),
            };
            let text = || Invocation::WebSocketMessage {
                id: u64::MAX,
                data: Message::Text("é".into()),
            };
            assert_eq!(invocation(binary(), limit).is_ok(), limit >= 2);
            assert_eq!(invocation(text(), limit).is_ok(), limit >= 2);
            assert_eq!(
                socket_event(SocketEvent::Binary(vec![255; 2]), limit).is_ok(),
                limit >= 2
            );
            assert_eq!(
                socket_event(SocketEvent::Text("é".into()), limit).is_ok(),
                limit >= 2
            );
        }
        assert_eq!(
            invocation(
                Invocation::WebSocketMessage {
                    id: u64::MAX,
                    data: Message::Text("é".into())
                },
                2
            )
            .unwrap()
            .1["id"],
            u64::MAX.to_string()
        );
    }
}
