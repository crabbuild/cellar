// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Disposable operator handoff: each invocation opens the existing S3 roots
//! and private replay stores. The infrastructure controller signs externally.
use super::*;
use cellar::{
    bucket::{Bucket, StorageBackend},
    sdk::fleet_operations::{
        RaftFleetConnection, RaftFleetControl, RaftFleetTransitionRecoverySelection,
    },
};
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore},
    prefix::PrefixStore,
};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagedConfig {
    rooted: bool,
    endpoint: String,
    bucket: String,
    prefix: String,
    access: String,
    secret: String,
    bucket_prefix: String,
    connection: RaftFleetConnection,
    principal: String,
    namespace_id: String,
    operation_id: String,
    intent_digest: String,
    selection: Option<RaftFleetTransitionRecoverySelection>,
    fence_public: [u8; 32],
    certificate: Vec<u8>,
}

struct NoListing;

#[async_trait::async_trait]
impl PaginatedListStore for NoListing {
    async fn list_paginated(
        &self,
        _: Option<&str>,
        _: PaginatedListOptions,
    ) -> object_store::Result<PaginatedListResult> {
        Err(object_store::Error::NotSupported {
            source: "managed recovery uses exact rooted keys".into(),
        })
    }
}

pub(super) async fn run(domain: &ExecutionDomain, options: &Options) -> Result<()> {
    ensure!(
        matches!(
            options.action.as_str(),
            "managed-challenge" | "managed-fence" | "managed-reopen"
        ),
        "unknown managed operator action"
    );
    let bytes = std::fs::read(&options.config)?;
    ensure!(bytes.len() <= 256 * 1024, "managed config too large");
    let config: ManagedConfig = serde_json::from_slice(&bytes)?;
    ensure!(
        (config.endpoint == "http://rustfs:9000"
            || config.endpoint.starts_with("http://127.0.0.1:"))
            && (config.prefix.starts_with("cellar-cg-dr-")
                || config.prefix.starts_with("cell-groups/")),
        "isolated RustFS run required"
    );
    ensure!(
        config.connection.namespaces.len() == 1
            && config.connection.namespaces[0].namespace_id == config.namespace_id
            && (config.connection.namespaces[0].image_storage == RaftImageStorage::Extents)
                == config.rooted,
        "exact namespace and image mode required"
    );
    let store = AmazonS3Builder::new()
        .with_endpoint(&config.endpoint)
        .with_bucket_name(&config.bucket)
        .with_region("us-east-1")
        .with_access_key_id(&config.access)
        .with_secret_access_key(&config.secret)
        .with_allow_http(true)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()?;
    let store = Arc::new(PrefixStore::new(store, config.prefix.as_str()));
    let bucket = Bucket::from_clients(
        store.clone(),
        store,
        Arc::new(NoListing),
        StorageBackend::S3,
        config.bucket,
        config.bucket_prefix,
    )?;
    let control = RaftFleetControl::connect(domain, bucket, config.connection, bounds().timeout)
        .await?
        .with_transition_preparer(
            verifier(domain, options.data.clone(), config.rooted).await?,
            CellGroupRecoveryPool::new(Default::default())?,
        );
    if options.action == "managed-challenge" {
        let intent = control
            .reopen_transition_recovery_intent(
                &config.principal,
                &config.namespace_id,
                &config.operation_id,
                bounds().timeout,
            )
            .await?
            .context("retained recovery intent")?;
        ensure!(
            hex::encode(intent.digest()) == config.intent_digest,
            "different recovery intent"
        );
        let (selection, challenge) = control
            .transition_recovery_challenge(
                &config.principal,
                &config.namespace_id,
                &intent,
                bounds().timeout,
            )
            .await?;
        return output(
            json!({"selection":selection,"challenge":serde_json::from_slice::<Value>(&challenge)?}),
        );
    }
    let selection = config.selection.context("retained recovery selection")?;
    ensure!(
        selection.namespace_id == config.namespace_id
            && selection.operation_id == config.operation_id
            && selection.intent_digest == config.intent_digest,
        "different retained recovery selection"
    );
    let authority = CellGroupPermanentFenceAuthority::new(config.fence_public)?;
    let fenced = if options.action == "managed-fence" {
        control
            .fence_selected_transition_recovery(
                &config.principal,
                &selection,
                &authority.with_certificate(config.certificate)?,
                bounds().timeout,
            )
            .await?
    } else {
        ensure!(
            config.certificate.is_empty(),
            "read-only reopen uses the pinned public key"
        );
        control
            .reopen_fenced_transition_recovery(
                &config.principal,
                &selection,
                &authority,
                bounds().timeout,
            )
            .await?
    };
    output(
        json!({"selection":selection,"fence_digest":hex::encode(fenced.request().digest()),
        "receipt_digest":hex::encode(fenced.receipt_digest()),"ranges":fenced.archives().len()}),
    )
}

async fn verifier(
    domain: &ExecutionDomain,
    directory: PathBuf,
    rooted: bool,
) -> Result<FleetGroupArchiveVerifier> {
    let base = directory.clone();
    let images = domain
        .blocking(move || physical::Physical::open(&base, rooted).map(|physical| physical.images))?
        .await??;
    let tails = CellGroupArchiveTailLimits {
        max_operations: 2,
        max_retained_bytes: 2 * CellGroupArchiveTailLimits::default().max_retained_bytes,
        ..Default::default()
    };
    Ok(FleetGroupArchiveVerifier::new(
        CellGroupArchivePool::new(Default::default())?,
        CellGroupArchiveTailPool::new(tails)?,
        CellGroupCheckpointPool::new(CellGroupCheckpointLimits {
            max_sources: 2,
            max_retained_bytes: 32 * 1024 * 1024,
            max_cells: 32,
            max_transfer_bytes: 32 * 1024 * 1024,
            max_chunk_bytes: 4093,
        })?,
        physical::scratch(8)?,
        FleetGroupArchiveReplayOptions {
            directory,
            filesystem: Arc::new(DirectFileSystem),
            materialized: (!rooted).then(|| images.clone()),
            extents: rooted.then_some(images),
            manifest: ManifestLimits {
                max_cells: 32,
                max_depth: 16,
            },
            entries: Default::default(),
            max_pages: 1024,
            max_page_bytes: 16 * 1024 * 1024,
            max_retained_roots: 16,
            max_effect_rows: 100,
        },
    )?)
}
