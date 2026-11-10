// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit fresh-namespace setup for the managed Docker qualification. Ordinary
//! controller/voter opens never call this initializer or infer missing roots.
use super::*;
use cellar::sdk::{consensus::*, fleet_operations::*};
use cellar_logic::NamespaceMode;
use serde::Deserialize;
use std::io::Read as _;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    endpoint: String,
    bucket: String,
    prefix: String,
    access: String,
    secret: String,
    bucket_prefix: String,
    cluster: [u8; 16],
    incarnation: [u8; 16],
    namespace: [u8; 16],
    coordination_prefix: String,
    namespace_prefix: String,
    image_storage: RaftImageStorage,
    replica_separation: FleetReplicaSeparation,
    request: FleetRequestIdentity,
}

pub(super) async fn run(domain: &ExecutionDomain, options: &Options) -> Result<()> {
    let path = options.config.clone();
    let config = domain
        .blocking(move || -> Result<Configuration> {
            let mut bytes = Vec::new();
            std::fs::File::open(path)?
                .take(256 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            ensure!(
                bytes.len() <= 256 * 1024,
                "bootstrap configuration too large"
            );
            serde_json::from_slice(&bytes)
                .map_err(|_| anyhow::anyhow!("invalid bootstrap configuration"))
        })?
        .await??;
    ensure!(
        !config.access.is_empty() && !config.secret.is_empty(),
        "explicit S3 credentials required"
    );
    ensure!(
        config.request.id != [0; 16]
            && config.request.operation != [0; 16]
            && config.request.expected_revision > 0
            && config.request.expected_revision < u64::MAX - 2
            && !config.request.principal.is_empty()
            && config.request.principal.len() <= 128
            && !config.request.principal.chars().any(char::is_control),
        "explicit bootstrap request identity required"
    );
    let deadline = tokio::time::Instant::now() + bounds().timeout;
    let result = tokio::time::timeout(bounds().timeout, initialize(domain, &config, deadline))
        .await
        .unwrap_or_else(|_| {
            Err(anyhow::anyhow!(
                "bootstrap deadline expired; reconcile the original request"
            ))
        });
    crate::managed_recovery::reply(result, &config.access, &config.secret)
}

async fn initialize(
    domain: &ExecutionDomain,
    config: &Configuration,
    deadline: tokio::time::Instant,
) -> Result<Value> {
    let remaining = || -> Result<Duration> {
        let value = deadline.saturating_duration_since(tokio::time::Instant::now());
        ensure!(
            value.as_millis() > 0,
            "bootstrap deadline expired; reconcile the original request"
        );
        Ok(value)
    };
    let bucket = crate::managed_store::open(
        &config.endpoint,
        &config.bucket,
        &config.prefix,
        &config.access,
        &config.secret,
        &config.bucket_prefix,
    )?;
    let cluster = ClusterId::new(config.cluster)?;
    let fleet = FleetCatalog::new(
        domain,
        bucket.clone(),
        cluster,
        IncarnationId::new(config.incarnation)?,
        &config.coordination_prefix,
    )?;
    let class = crate::managed_voter::counter()?;
    let adapter = Catalog::new(
        bucket.cas_store.clone(),
        CatalogOptions {
            prefix: config.namespace_prefix.clone(),
            ..Default::default()
        },
    )?;
    let mut namespace = NamespaceConfig::new(
        cluster,
        NamespaceId::new(config.namespace)?,
        NamespaceMode::Raft,
        class.schema_digest(),
    );
    namespace.image_storage = config.image_storage;
    fleet
        .initialize_with_replica_separation(config.replica_separation, remaining()?)
        .await?;
    let registration = fleet
        .provision_managed_namespace(
            &config.request,
            &adapter,
            namespace,
            &class,
            DirectoryLimits::default(),
            remaining()?,
        )
        .await?;
    let connection = RaftFleetConnection {
        cluster_id: hex::encode(config.cluster),
        incarnation_id: hex::encode(config.incarnation),
        coordination_prefix: config.coordination_prefix.clone(),
        namespaces: vec![RaftFleetNamespace {
            namespace_id: hex::encode(config.namespace),
            schema_digest: hex::encode(class.schema_digest()),
            catalog_prefix: config.namespace_prefix.clone(),
            scope: RaftFleetNamespaceScope::StoreRoot,
            managed: true,
            image_storage: config.image_storage,
            group_directory: DirectoryLimits::default(),
        }],
    };
    let control =
        RaftFleetControl::connect(domain, bucket, connection.clone(), remaining()?).await?;
    let revision = fleet.read(remaining()?).await?.revision();
    Ok(
        json!({"connection":connection, "catalog_id":control.catalog_id(),
        "namespace_digest":hex::encode(control.namespace_digest(&hex::encode(config.namespace))?),
        "capability_digest":hex::encode(RaftManagedHostOptions::capability_digest(&[registration])?),
        "fleet_revision":revision, "replica_separation":config.replica_separation}),
    )
}
