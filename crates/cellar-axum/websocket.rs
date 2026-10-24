// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cellar-owned WebSocket upgrades for Axum handlers.
use crate::CellContext;
use anyhow::Result;
use axum::{
    extract::FromRequestParts,
    http::{request::Parts, StatusCode},
    response::Response,
};
use cellar::sdk::{websocket::WebSocket, Context};

/// Upgrades retain Cellar's opening proof, ownership, and socket event dispatch.
/// Regular sockets need a `Context::wait_until` consumer. Hibernating sockets
/// deliver events to the cell's SDK delegate through `AxumCell::with_cell`.
pub struct CellWebSocketUpgrade {
    context: Context,
}

impl<S: Send + Sync> FromRequestParts<S> for CellWebSocketUpgrade {
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let CellContext(context) = CellContext::from_request_parts(parts, state).await?;
        let token = |name: &str, expected: &str| {
            parts.headers.get_all(name).iter().any(|value| {
                value.to_str().is_ok_and(|value| {
                    value
                        .split(',')
                        .any(|token| token.trim().eq_ignore_ascii_case(expected))
                })
            })
        };
        if parts.method != axum::http::Method::GET || !token("upgrade", "websocket") {
            return Err((StatusCode::BAD_REQUEST, "invalid WebSocket upgrade"));
        }
        // SDK binding upgrades transfer a capability without an RFC6455
        // network handshake. The public node transport validates Connection,
        // version and key before dispatching a real network upgrade.
        Ok(Self { context })
    }
}

impl CellWebSocketUpgrade {
    pub fn accept(self) -> Result<(WebSocket, Response)> {
        self.accept_with(None)
    }

    pub fn accept_hibernating(self, tags: Vec<String>) -> Result<(WebSocket, Response)> {
        self.accept_with(Some(tags))
    }

    fn accept_with(self, tags: Option<Vec<String>>) -> Result<(WebSocket, Response)> {
        let pair = self.context.websocket_pair()?;
        match tags {
            Some(tags) => pair.server.accept_hibernating(&self.context, tags)?,
            None => pair.server.accept(&self.context)?,
        }
        let response =
            crate::http::response(pair.client.into_response(&self.context, Vec::new())?)?;
        Ok((pair.server, response))
    }
}
