// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use futures_util::future::BoxFuture;

pub(super) const PURPOSE: &[u8] = b"cellar-docker-cell-group-fence-1\0";

pub(super) struct SignedFence {
    public: ed25519_dalek::VerifyingKey,
    certificate: Vec<u8>,
}

impl SignedFence {
    pub fn new(config: &Config) -> Result<Self> {
        Ok(Self {
            public: ed25519_dalek::VerifyingKey::from_bytes(&config.fence_public)?,
            certificate: config.certificate.clone(),
        })
    }

    fn check(&self, request: &CellGroupDisasterFenceRequest, evidence: &[u8]) -> Result<()> {
        ensure!(
            evidence.len() == 96 && evidence[..32] == request.digest(),
            "missing or foreign permanent fence certificate"
        );
        let mut message = PURPOSE.to_vec();
        message.extend_from_slice(&request.digest());
        self.public
            .verify_strict(
                &message,
                &ed25519_dalek::Signature::from_slice(&evidence[32..])?,
            )
            .context("untrusted Docker controller fence")
    }
}

impl DisasterFenceAuthority<CellGroupDisasterFenceRequest> for SignedFence {
    fn fence<'a>(
        &'a self,
        request: &'a CellGroupDisasterFenceRequest,
    ) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            self.check(request, &self.certificate)?;
            Ok(self.certificate.clone())
        })
    }

    fn verify<'a>(
        &'a self,
        request: &'a CellGroupDisasterFenceRequest,
        evidence: &'a [u8],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.check(request, evidence) })
    }
}

pub(super) async fn fenced(
    domain: &ExecutionDomain,
    config: &Config,
    data: &std::path::Path,
    directory: &RangeDirectory,
) -> Result<FencedCellGroupRecovery> {
    let request = directory.decode_group_recovery(&config.recovery)?;
    let route = request.source().route().clone();
    let rooted = config.rooted;
    let base = data.join("cold-source-verifier");
    let store = domain
        .blocking(move || physical::Physical::open(&base, rooted)?.archive(route))?
        .await??;
    Ok(directory
        .fence_group_recovery(
            domain,
            &CellGroupArchivePool::new(Default::default())?,
            &CellGroupArchiveTailPool::new(Default::default())?,
            &request,
            physical::checkpoint_pool()?.receive_archive(
                store,
                physical::scratch(8)?,
                data.into(),
            )?,
            &SignedFence::new(config)?,
        )
        .await?)
}

pub(super) async fn seed(
    domain: &ExecutionDomain,
    config: &Config,
    options: &Options,
) -> Result<()> {
    ensure!(
        options.generation == 1 && !options.retained,
        "fresh target seed required"
    );
    let app = service::application()?;
    let directory = config.directory(domain, app.class.schema_digest()).await?;
    let proof = Box::pin(fenced(domain, config, &options.data, &directory)).await?;
    let route = proof.request().replacement().route().clone();
    let base = options.data.join("state");
    let rooted = config.rooted;
    let destination = domain
        .blocking(move || physical::Physical::open(&base, rooted)?.archive(route))?
        .await??;
    let prepared = CellGroupRecoveryPool::new(Default::default())?
        .prepare(
            domain,
            proof,
            destination,
            physical::scratch(8)?,
            options.data.clone(),
        )
        .await?;
    let log = ReplicaLog::open(
        config.log_options(options, LogOpenMode::Create)?,
        Arc::new(DirectFileSystem),
    )
    .await?;
    let seeded = directory
        .seed_group_recovery(domain, prepared, &log, &SignedFence::new(config)?)
        .await?;
    let value = json!({"seeded":true,"member":seeded.group().identity().member.get(),"binding":hex::encode(seeded.binding()),"cells":seeded.manifest().pinned_images()});
    drop(seeded);
    log.close().await?;
    output(value)
}

pub(super) async fn operator(
    domain: &ExecutionDomain,
    config: &Config,
    options: &Options,
    action: &str,
) -> Result<()> {
    let app = service::application()?;
    let schema = app.class.schema_digest();
    if action == "init" {
        let catalog = Catalog::new(config.store()?, Default::default())?;
        let namespace = catalog.open_namespace(config::namespace(schema)?).await?;
        let directory = RangeDirectory::new(namespace, Default::default())?;
        let birth = directory.prepare_group_birth(config.spec(0)?)?;
        directory.reserve_group_birth(&birth).await?;
        let mutation = directory
            .prepare_initialization(ProposalId::new([7; 16])?, &[birth.route().clone()])?;
        directory.publish(&mutation).await?;
        let birth = directory.materialize_group_birth(&birth).await?;
        directory
            .prepare_group_archive_binding(
                domain,
                &CellGroupArchivePool::new(Default::default())?,
                &birth,
            )
            .await?
            .commit(domain)
            .await?;
        return output(json!({"initialized":true}));
    }
    let directory = config.directory(domain, schema).await?;
    if action == "probe-old" {
        let log = ReplicaLog::open(
            config.log_options(options, LogOpenMode::Create)?,
            Arc::new(DirectFileSystem),
        )
        .await?;
        let birth = directory.prepare_group_birth(config.spec(0)?)?;
        let refused = matches!(
            directory.materialize_group_birth(&birth).await,
            Err(CatalogError::Fenced)
        );
        log.close().await?;
        ensure!(
            refused,
            "old birth was not fenced on a recreated empty disk"
        );
        return output(json!({"old_identity_refused":true}));
    }
    if action == "fence" {
        let proof = Box::pin(fenced(domain, config, &options.data, &directory)).await?;
        return output(json!({"fenced":true,"request":hex::encode(proof.request().digest())}));
    }
    let request = directory.decode_group_recovery(&config.recovery)?;
    let authority = SignedFence::new(config)?;
    match action {
        "publish" => {
            directory
                .publish_group_recovery(&request, &authority)
                .await?;
            output(json!({"published":true}))
        }
        "target-archive" => {
            let birth = directory
                .materialize_selected_group(request.replacement().route())
                .await?;
            directory
                .prepare_group_archive_binding(
                    domain,
                    &CellGroupArchivePool::new(Default::default())?,
                    &birth,
                )
                .await?
                .commit(domain)
                .await?;
            output(json!({"initialized":true}))
        }
        "verify-old-tokens" => {
            let ledger: Value = serde_json::from_slice(&std::fs::read(
                options
                    .config
                    .parent()
                    .context("config parent")?
                    .join("ledger.json"),
            )?)?;
            let now = cellar::asyncrt::wall_ms().max(0) as u64;
            for name in ["alpha", "beta", "gamma"] {
                let token = CellVersionToken::from_encoded(
                    ledger["observations"][name]["token"]
                        .as_str()
                        .context("source token")?,
                )?;
                keys()?.verify_cell(&token, &config::cell(0, name)?, schema, now)?;
            }
            output(json!({"old_tokens_valid":true}))
        }
        _ => anyhow::bail!("unknown operator action"),
    }
}
