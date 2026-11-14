// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One-shot namespace provisioning through a registered native Raft class.
#![allow(clippy::disallowed_methods)]

use super::native::CompiledApplications;
use crate::cli_options::{FleetFlags, Storage};
use crate::cli_output::{Format, Output};
use crate::sdk::{consensus::*, ExecutionDomain};
use anyhow::{ensure, Context as _, Result};
use serde::{Deserialize, Serialize};
use std::{ffi::OsString, io::Read, path::Path, time::Duration};

pub(super) const FLAG: &str = "--provision-sql-namespace";
const PLAN_BYTES: u64 = 16 * 1024;
const TIMEOUT: Duration = Duration::from_secs(25);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    catalog_id: String,
    cluster_id: String,
    incarnation_id: String,
    coordination_prefix: String,
    catalog_prefix: String,
    namespace_id: String,
    script: String,
    version: String,
    class: String,
    principal: String,
    request_id: String,
    operation_id: String,
    expected_revision: u64,
    #[serde(default)]
    configuration: crate::application::consensus::NativeCellConfiguration,
    #[serde(default)]
    image_storage: ImageStorage,
    #[serde(default)]
    minimum_durability: MinimumDurability,
}

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum MinimumDurability {
    #[default]
    Quorum,
    S3,
}

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ImageStorage {
    #[default]
    Materialized,
    Extents,
}

impl From<ImageStorage> for RaftImageStorage {
    fn from(storage: ImageStorage) -> Self {
        match storage {
            ImageStorage::Materialized => Self::Materialized,
            ImageStorage::Extents => Self::Extents,
        }
    }
}

fn identity<const N: usize>(value: &str, label: &str) -> Result<[u8; N]> {
    let mut bytes = [0; N];
    hex::decode_to_slice(value, &mut bytes).with_context(|| format!("invalid {label}"))?;
    ensure!(
        hex::encode(bytes) == value && bytes != [0; N],
        "invalid {label}"
    );
    Ok(bytes)
}

fn inputs(mut arguments: impl Iterator<Item = OsString>) -> Result<(Plan, Storage)> {
    let path = arguments
        .next()
        .context("--provision-sql-namespace requires a plan path")?;
    let mut arguments = arguments.map(|arg| {
        arg.into_string()
            .map_err(|_| anyhow::anyhow!("storage flags require UTF-8"))
    });
    let mut fleet = FleetFlags::default();
    while let Some(argument) = arguments.next().transpose()? {
        ensure!(
            fleet.consume(&argument, &mut |flag| {
                arguments
                    .next()
                    .transpose()?
                    .with_context(|| format!("{flag} requires a value"))
            })?,
            "unexpected namespace provisioning option {argument}"
        );
    }
    let mut bytes = Vec::new();
    std::fs::File::open(Path::new(&path))?
        .take(PLAN_BYTES + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= PLAN_BYTES,
        "namespace provision plan exceeds 16 KiB"
    );
    let plan = serde_json::from_slice(&bytes).context("invalid namespace provision plan")?;
    Ok((plan, fleet.resolve(FLAG)?))
}

pub(super) fn run(
    applications: CompiledApplications,
    arguments: impl Iterator<Item = OsString>,
) -> Result<()> {
    let (plan, storage) = inputs(arguments)?;
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let value = runtime.block_on(async {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let result = domain
            .run(provision(&domain, applications, plan, storage))
            .await;
        domain.shutdown().await;
        result?
    })?;
    Output::new(Format::Json).line(format_args!("{}", serde_json::to_string(&value)?))
}

async fn provision(
    domain: &ExecutionDomain,
    applications: CompiledApplications,
    plan: Plan,
    storage: Storage,
) -> Result<serde_json::Value> {
    let cluster = ClusterId::new(identity(&plan.cluster_id, "cluster ID")?)?;
    let incarnation = IncarnationId::new(identity(&plan.incarnation_id, "fleet incarnation ID")?)?;
    let namespace = NamespaceId::new(identity(&plan.namespace_id, "namespace ID")?)?;
    let catalog_id: [u8; 32] = identity(&plan.catalog_id, "catalog ID")?;
    let request = FleetRequestIdentity {
        id: identity(&plan.request_id, "request ID")?,
        operation: identity(&plan.operation_id, "operation ID")?,
        principal: plan.principal,
        expected_revision: plan.expected_revision,
    };
    let class = applications.namespace_class(
        &plan.script,
        &plan.version,
        &plan.class,
        &plan.configuration,
    )?;
    let bucket = storage.open().await?;
    let fleet = FleetCatalog::new(
        domain,
        bucket.clone(),
        cluster,
        incarnation,
        &plan.coordination_prefix,
    )?;
    ensure!(
        fleet.catalog_id()? == catalog_id,
        "fleet catalog identity differs from the plan"
    );
    let catalog = Catalog::new(
        bucket.raft_store(),
        CatalogOptions {
            prefix: plan.catalog_prefix,
            ..Default::default()
        },
    )?;
    let mut config = NamespaceConfig::new(
        cluster,
        namespace,
        NamespaceMode::Raft,
        class.schema_digest(),
    );
    config.image_storage = plan.image_storage.into();
    config.minimum_durability = match plan.minimum_durability {
        MinimumDurability::Quorum => Durability::Quorum,
        MinimumDurability::S3 => Durability::S3,
    };
    let registered = fleet
        .provision_managed_namespace(
            &request,
            &catalog,
            config,
            class.as_ref(),
            DirectoryLimits::default(),
            TIMEOUT,
        )
        .await?;
    Ok(serde_json::json!({
        "catalog_id": plan.catalog_id,
        "namespace_id": plan.namespace_id,
        "request_id": plan.request_id,
        "operation_id": plan.operation_id,
        "expected_revision": request.expected_revision,
        "class": registered.class,
        "image_storage": plan.image_storage,
        "minimum_durability": plan.minimum_durability,
        "schema_digest": hex::encode(registered.namespace.schema_digest),
    }))
}
