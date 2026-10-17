//! A credit-based state subscription: at most one compact update is pending
//! per connection. ACK catches up to the current durable version after a slow
//! client or reconnect; clients use the HTTP history cursor for every change.
use crate::state;
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{clone::StructuredValue, websocket::WebSocket, Context, Output, Request};
use serde_json::{json, Value};

fn frame(context: &Context) -> Result<String> {
    let value = state::state(&context.storage()?)?;
    Ok(serde_json::to_string(
        &json!({"version":value["version"],"health":value["health"],
        "scope":context.identity().context("missing subscription identity")?.scope}),
    )?)
}

fn attachment(context: &Context, socket: &WebSocket) -> Result<Value> {
    let value = socket
        .deserialize_attachment(context)?
        .context("subscription attachment absent")?
        .to_json()?;
    ensure!(
        value["scope"]
            == context
                .identity()
                .context("subscription outside a cell")?
                .scope,
        "subscription authority changed"
    );
    Ok(value)
}

type Ordinary = std::sync::Mutex<std::collections::BTreeSet<u64>>;

pub fn upgrade(
    context: &Context,
    tenant: usize,
    request: &Request,
    ids: &Ordinary,
) -> Result<Output> {
    ensure!(
        context.websockets(None)?.len() + ids.lock().unwrap().len() < 8,
        "device subscription capacity exceeded"
    );
    let pair = context.websocket_pair()?;
    let ordinary = url::Url::parse(&request.url)?
        .query_pairs()
        .any(|(k, v)| k == "mode" && v == "ordinary");
    if ordinary {
        pair.server.accept(context)?;
        ids.lock().unwrap().insert(pair.server.id());
    } else {
        pair.server
            .accept_hibernating(context, vec![format!("tenant-{tenant}"), "state-v1".into()])?;
    }
    let state = state::state(&context.storage()?)?;
    pair.server.serialize_attachment(context,&StructuredValue::from_json(json!({"tenant":tenant,
        "scope":context.identity().context("subscription outside a cell")?.scope,"sent":state["version"],"acked":0,"binary_pending":false}))?)?;
    context.set_websocket_auto_response(Some(("PING".into(), "PONG".into())))?;
    pair.server.send_text(context, frame(context)?)?;
    Ok(Output::Http(
        pair.client.into_response(context, Vec::new())?,
    ))
}

pub fn broadcast(context: &Context, ids: &Ordinary) -> Result<()> {
    let state = state::state(&context.storage()?)?;
    let mut sockets = context.websockets(None)?;
    for id in ids.lock().unwrap().iter() {
        sockets.push(context.websocket(*id)?);
    }
    for socket in sockets {
        let mut value = attachment(context, &socket)?;
        if value["sent"] == value["acked"] && state["version"].as_u64() > value["sent"].as_u64() {
            socket.send_text(context, frame(context)?)?;
            value["sent"] = state["version"].clone();
            socket.serialize_attachment(context, &StructuredValue::from_json(value)?)?;
        }
    }
    Ok(())
}

pub fn message(context: &Context, id: u64, data: cellar::ws_registry::WsIn) -> Result<Output> {
    let socket = context.websocket(id)?;
    let mut value = attachment(context, &socket)?;
    match data {
        cellar::ws_registry::WsIn::Text(text) => {
            ensure!(text.len() <= 4096, "subscription control too large");
            if text == "PING" {
                socket.send_text(context, "PONG")?;
                return Ok(Output::Done);
            }
            if text == "CLOSE" {
                socket.close(context, 1000, "client done")?;
                return Ok(Output::Done);
            }
            let ack: Value = serde_json::from_str(&text)?;
            let version = ack["ack"].as_u64().context("expected ACK version")?;
            ensure!(
                Some(version) == value["sent"].as_u64(),
                "ACK does not name the pending version"
            );
            value["acked"] = json!(version);
            value["binary_pending"] = json!(false);
            let current = state::state(&context.storage()?)?;
            if current["version"]
                .as_u64()
                .context("missing state version")?
                > version
            {
                socket.send_text(context, frame(context)?)?;
                value["sent"] = current["version"].clone();
            }
        }
        cellar::ws_registry::WsIn::Binary(bytes) => {
            ensure!(
                bytes.len() <= 65536 && value["binary_pending"] == false,
                "binary frame has no credit"
            );
            socket.send_binary(context, bytes)?;
            value["binary_pending"] = json!(true);
        }
    }
    socket.serialize_attachment(context, &StructuredValue::from_json(value)?)?;
    Ok(Output::Done)
}
