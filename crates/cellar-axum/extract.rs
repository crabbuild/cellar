//! Extract fresh invocation authority; never put an event Context in Router State.

use axum::extract::FromRequestParts;
use axum::http::{request::Parts, StatusCode};
use cellar::sdk::{CellIdentity, Context};

#[derive(Clone)]
pub struct CellContext(pub Context);

impl<S: Send + Sync> FromRequestParts<S> for CellContext {
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        parts.extensions.get::<Context>().cloned().map(Self).ok_or((
            StatusCode::INTERNAL_SERVER_ERROR,
            "handler requires a Cellar invocation",
        ))
    }
}

#[derive(Clone, Debug)]
pub struct CurrentCell(pub CellIdentity);

impl<S: Send + Sync> FromRequestParts<S> for CurrentCell {
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        let CellContext(context) = CellContext::from_request_parts(parts, &()).await?;
        context.identity().cloned().map(Self).ok_or((
            StatusCode::INTERNAL_SERVER_ERROR,
            "handler requires a cell invocation",
        ))
    }
}
