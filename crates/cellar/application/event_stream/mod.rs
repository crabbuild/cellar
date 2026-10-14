//! Durable event resource handles. Handles are minted by a bound application
//! context and retain the execution domain that granted their authority.

use super::{Context, Result};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;

pub(crate) mod authority;
mod binding_config;
pub(crate) mod broker;
pub(crate) mod catalog;
pub(crate) mod catalog_inventory;
mod catalog_schema;
mod client;
mod config;
pub(crate) mod decimal;
pub(crate) mod encoding;
mod error;
mod inventory;
mod lifecycle;
mod lifecycle_store;
pub(crate) mod name_resolution;
mod producer;
pub(crate) mod readiness;
mod retirement;
pub(crate) mod retirement_proof;
mod retirement_store;
mod schema;
pub(crate) mod service;
mod source_inventory;
pub(crate) mod statistics;
mod store;
mod subscription;
mod subscription_store;
pub use authority::Authority;
pub use binding_config::{BindingConfig, ResourceConfig};
pub use catalog_inventory::{
    CatalogEntry, CatalogListOptions, CatalogNamespace, CatalogPage, CatalogState,
};
pub use client::{Configuration, Cursor, Page, ReadOptions, Record};
pub use config::{
    Definition, Grant, Retention, Start, SubscriptionDefinition, MAX_DEFINITION_BYTES,
};
pub use error::{DeliveryError, ErrorCode};
pub use inventory::{SubscriptionListOptions, SubscriptionPage};
pub use lifecycle::{
    AuditPage, AuditRecord, Change, ChangeAction, LeaseSummary, LifecycleReceipt, OffsetRange,
    SubscriptionIdentity, SubscriptionObservation, SubscriptionStatus,
};
pub use producer::SealStatus;
pub use retirement::{Retirement, RetirementMode, RetirementStatus};
pub use retirement_proof::RetirementProof;
pub use source_inventory::{SourceListOptions, SourcePage, SourceStatus};
pub use statistics::{StreamObservation, StreamStats};
pub use subscription::{Acknowledgment, Batch, BatchOptions, Receipt, Subscription};

pub(crate) async fn call_bound(
    context: &Context,
    binding: &Binding,
    action: authority::Action,
    method: &str,
    args: cellar_native::clone::StructuredValue,
) -> Result<serde_json::Value> {
    if matches!(action, authority::Action::Read | authority::Action::Consume) {
        binding_ready(context)?;
    }
    let authority = Authority::mint(context, binding, action)?;
    let result = crate::native_worker::event_rpc(
        context,
        cell_scope(&binding.resource)?,
        Some(cell_name(&binding.resource)?),
        method.into(),
        super::encode_rpc(&args)?,
        authority,
    )
    .await?;
    error::unwrap_reply(super::decode_rpc_result(&result)?.to_json()?)
}

fn binding_ready(context: &Context) -> Result<()> {
    context.worker_config.event_readiness.check()
}

pub(crate) async fn call_authorized(
    context: &Context,
    authority: Authority,
    scope: String,
    name: Option<String>,
    method: &str,
    args: serde_json::Value,
) -> Result<serde_json::Value> {
    let args = cellar_native::clone::StructuredValue::from_json(args)?;
    let result = crate::native_worker::event_rpc(
        context,
        scope,
        name,
        method.into(),
        super::encode_rpc(&args)?,
        authority,
    )
    .await?;
    error::unwrap_reply(super::decode_rpc_result(&result)?.to_json()?)
}

pub(crate) fn is_event_scope(scope: &str) -> bool {
    scope.split_once(':').is_some_and(|(class, _)| {
        class == crate::deploy::EVENT_STREAM_CLASS || class == crate::deploy::EVENT_CATALOG_CLASS
    })
}

pub(crate) fn validate_target(scope: &str, authority: &Authority) -> Result<()> {
    authority.validate()?;
    anyhow::ensure!(
        scope == cell_scope(&authority.resource)? || scope == catalog_scope(&authority.resource)?,
        "event RPC authority does not match its target"
    );
    Ok(())
}

pub(crate) fn catalog_name(resource: &Resource) -> Result<String> {
    resource.validate()?;
    Ok(serde_json::to_string(&(
        &resource.tenant,
        &resource.application,
    ))?)
}

pub(crate) fn catalog_scope(resource: &Resource) -> Result<String> {
    let name = catalog_name(resource)?;
    let id = crate::engine_api::durable_object_id_for_name(
        crate::engine_api::EVENT_CATALOG_NAMESPACE_KEY,
        &name,
    );
    Ok(format!(
        "{}:{}",
        crate::deploy::EVENT_CATALOG_CLASS,
        crate::engine_api::durable_object_id_hex(&id)
    ))
}

pub(crate) fn cell_name(resource: &Resource) -> Result<String> {
    resource.validate()?;
    Ok(serde_json::to_string(&(
        &resource.tenant,
        &resource.application,
        &resource.id,
        0u32,
    ))?)
}

pub(crate) fn cell_scope(resource: &Resource) -> Result<String> {
    let name = cell_name(resource)?;
    let id = crate::engine_api::durable_object_id_for_name(
        crate::engine_api::EVENT_STREAM_NAMESPACE_KEY,
        &name,
    );
    Ok(format!(
        "{}:{}",
        cellar_logic::event_stream::RESERVED_CLASS,
        crate::engine_api::durable_object_id_hex(&id)
    ))
}

pub(crate) fn lane_identity(
    resource: &Resource,
    source: &str,
    incarnation: &str,
) -> Result<String> {
    use sha2::{Digest, Sha256};
    let identity = serde_json::to_vec(&(
        source,
        incarnation,
        &resource.tenant,
        &resource.application,
        &resource.id,
        0u32,
    ))?;
    Ok(hex::encode(Sha256::digest(identity)))
}

pub trait Event: Serialize {
    const FAMILY: &'static str;
    const VERSION: u32;

    fn key(&self) -> &str;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resource {
    pub tenant: String,
    pub application: String,
    pub id: String,
    pub name: String,
    pub family: String,
}

impl Resource {
    pub fn same_identity(&self, other: &Self) -> bool {
        self.tenant == other.tenant
            && self.application == other.application
            && self.id == other.id
            && self.family == other.family
    }

    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.id.len() == 32
                && self
                    .id
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "stream resource incarnation must be 32 lowercase hexadecimal digits"
        );
        for (field, value) in [
            ("tenant", &self.tenant),
            ("application", &self.application),
            ("name", &self.name),
            ("family", &self.family),
        ] {
            anyhow::ensure!(
                !value.is_empty() && value.len() <= 128,
                "invalid stream resource {field}"
            );
        }
        Ok(())
    }
}

/// Deployment-resolved publication and consumption authority. A resource ID
/// alone is not a grant; receivers authorize the principal independently.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub environment: String,
    pub resource: Resource,
    pub principal: String,
    pub grant: String,
    #[serde(default)]
    pub publish: bool,
    #[serde(default)]
    pub read: bool,
    #[serde(default)]
    pub subscriptions: Vec<String>,
    #[serde(default)]
    pub administer: bool,
}

impl Binding {
    pub fn validate(&self) -> Result<()> {
        self.resource.validate()?;
        for (name, value) in [
            ("binding", &self.environment),
            ("principal", &self.principal),
            ("grant", &self.grant),
        ] {
            anyhow::ensure!(
                !value.is_empty() && value.len() <= 256,
                "invalid stream {name}"
            );
        }
        anyhow::ensure!(
            self.subscriptions.len() <= 128,
            "too many stream subscription grants"
        );
        anyhow::ensure!(
            self.subscriptions
                .iter()
                .all(|name| !name.is_empty() && name.len() <= 128),
            "invalid subscription grant"
        );
        Ok(())
    }
}

pub struct Stream<E> {
    pub(crate) readiness: Arc<readiness::Readiness>,
    pub(crate) binding: Arc<Binding>,
    pub(crate) services: Arc<crate::host_services::HostServices>,
    event: PhantomData<fn() -> E>,
}

impl<E> Clone for Stream<E> {
    fn clone(&self) -> Self {
        Self {
            readiness: self.readiness.clone(),
            binding: self.binding.clone(),
            services: self.services.clone(),
            event: PhantomData,
        }
    }
}

impl<E> Stream<E> {
    pub(crate) fn check_ready(&self) -> Result<()> {
        self.check_domain()?;
        self.readiness.check()
    }

    pub fn resource(&self) -> &Resource {
        &self.binding.resource
    }

    pub(crate) fn check_domain(&self) -> Result<()> {
        if !Arc::ptr_eq(&self.services, &crate::asyncrt::services()) {
            return Err(error::error(
                ErrorCode::WrongDomain,
                "stream handle belongs to another execution domain",
            ));
        }
        anyhow::ensure!(
            self.services.accepts_work(),
            "stream execution domain is closed"
        );
        Ok(())
    }
}

impl Context {
    pub fn stream<E: Event>(&self, name: &str) -> Result<Stream<E>> {
        self.check_turn()?;
        let binding = self
            .worker_config
            .stream_bindings
            .iter()
            .find(|binding| binding.environment == name)
            .ok_or_else(|| anyhow::anyhow!("no event stream binding named {name}"))?;
        let binding = self.worker_config.event_readiness.binding(binding)?;
        if binding.resource.family != E::FAMILY {
            return Err(error::error(
                ErrorCode::EventFamilyMismatch,
                "event family does not match stream binding",
            ));
        }
        Ok(Stream {
            readiness: self.worker_config.event_readiness.clone(),
            binding: Arc::new(binding),
            services: self.io.host_services().clone(),
            event: PhantomData,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    pub id: String,
    pub lane: String,
    #[serde(with = "decimal")]
    pub sequence: i64,
    pub source: String,
    pub source_incarnation: String,
    pub family: String,
    pub version: u32,
    pub key: String,
    #[serde(with = "decimal")]
    pub created_at_ms: i64,
    pub payload: serde_json::Value,
}

impl Envelope {
    pub fn decode<E: Event + serde::de::DeserializeOwned>(&self) -> Result<E> {
        anyhow::ensure!(
            self.family == E::FAMILY && self.version == E::VERSION,
            "unsupported event family or schema version"
        );
        Ok(serde_json::from_value(self.payload.clone())?)
    }
}
