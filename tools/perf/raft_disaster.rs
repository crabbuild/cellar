// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Local Docker qualification adapter. The controller owns the private signing
//! key outside every voter/operator mount and signs only after destructive fencing.
//! The library intentionally does not embed this fixture's trust configuration.
use anyhow::{ensure, Context, Result};
use cellar::sdk::consensus::{verification::Catalog, *};
use futures_util::future::BoxFuture;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

const PURPOSE: &[u8] = b"cellar-docker-disaster-fence-1\0";

pub fn node_id(node: u8, generation: u8) -> Result<NodeId> {
    ensure!(
        generation <= 1 && (1..=5).contains(&node),
        "invalid fixture identity"
    );
    Ok(NodeId::new([node + generation * 32; 16])?)
}

pub fn scope(cell: usize, class: &str, generation: u8) -> Result<GroupScope> {
    ensure!(generation <= 1, "invalid fixture history");
    let mut group = [5 + generation; 16];
    group[..8].copy_from_slice(&(cell as u64 + 1).to_be_bytes());
    Ok(GroupScope {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: format!("{class}:{cell}"),
        group: GroupId::new(group)?,
        incarnation: IncarnationId::new([4 + generation; 16])?,
    })
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    cell: usize,
    archive: ArchiveReference,
    fence_digest: String,
}

fn request(plan: &Plan) -> Result<DisasterRecoveryRequest> {
    Ok(DisasterRecoveryRequest {
        source: scope(plan.cell, "Workload", 0)?,
        archive: plan.archive.clone(),
        replacement: BirthProposal {
            scope: scope(plan.cell, "Workload", 1)?,
            voters: (1..=3)
                .map(|n| {
                    Ok(InitialMember {
                        member: MemberId::new(n.into())?,
                        node: node_id(n, 1)?,
                    })
                })
                .collect::<Result<_>>()?,
            learners: vec![],
        },
    })
}

struct SignedFence {
    public: ed25519_dalek::VerifyingKey,
    receipts: BTreeMap<String, String>,
}

impl SignedFence {
    fn check(&self, request: &DisasterFenceRequest, evidence: &[u8]) -> Result<()> {
        ensure!(
            evidence.len() == 96 && evidence[..32] == request.digest(),
            "wrong fence operation"
        );
        let signature = ed25519_dalek::Signature::from_slice(&evidence[32..])?;
        let mut message = PURPOSE.to_vec();
        message.extend_from_slice(&request.digest());
        self.public
            .verify_strict(&message, &signature)
            .context("untrusted infrastructure fence")
    }
}

impl DisasterFenceAuthority for SignedFence {
    fn fence<'a>(&'a self, request: &'a DisasterFenceRequest) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            let receipt = self
                .receipts
                .get(&hex::encode(request.digest()))
                .context("no controller fence certificate for this plan")?;
            let evidence = hex::decode(receipt)?;
            self.check(request, &evidence)?;
            Ok(evidence)
        })
    }

    fn verify<'a>(
        &'a self,
        request: &'a DisasterFenceRequest,
        evidence: &'a [u8],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.check(request, evidence) })
    }
}

pub async fn run(
    action: &str,
    store: Arc<dyn ObjectStore>,
    schema: [u8; 32],
    total: usize,
) -> Result<()> {
    ensure!(
        (1..=24).contains(&total),
        "bounded disaster qualification only"
    );
    let namespace = Catalog::new(
        store,
        CatalogOptions {
            prefix: "perf/multi-raft".into(),
            ..Default::default()
        },
    )?
    .existing_namespace(NamespaceConfig::new(
        ClusterId::new([1; 16])?,
        NamespaceId::new([2; 16])?,
        NamespaceMode::Raft,
        schema,
    ))
    .await?;
    if action == "plan" {
        let mut plans = Vec::new();
        for cell in 0..total {
            let source = scope(cell, "Workload", 0)?;
            let birth = namespace
                .birth(&source.cell)
                .await?
                .context("missing source birth")?;
            ensure!(birth.scope() == &source, "source history changed");
            let archive = namespace.cell_archive(
                &birth,
                cellar_ltx::LtxHost::default(),
                ArchiveLimits::default(),
            )?;
            let mut plan = Plan {
                cell,
                archive: archive
                    .recovery_metadata()
                    .await?
                    .context("archive missing")?
                    .reference(),
                fence_digest: String::new(),
            };
            plan.fence_digest = hex::encode(
                namespace
                    .disaster_fence_request(request(&plan)?)
                    .await?
                    .digest(),
            );
            plans.push(plan);
        }
        cellar::cli_output::Output::new(cellar::cli_output::Format::Json)
            .line(format_args!("{}", serde_json::to_string(&plans)?))?;
        return Ok(());
    }
    ensure!(action == "recover", "unknown disaster action");
    let bytes = std::fs::read("/config/dr-plan.json")?;
    ensure!(bytes.len() <= 256 * 1024, "plan too large");
    let plans: Vec<Plan> = serde_json::from_slice(&bytes)?;
    ensure!(
        plans.len() == total && plans.iter().enumerate().all(|(i, p)| i == p.cell),
        "plan population changed"
    );
    let public: [u8; 32] = std::fs::read("/config/fence-public")?
        .try_into()
        .map_err(|_| anyhow::anyhow!("fence key length"))?;
    let receipts = std::fs::read("/config/fence-evidence.json")?;
    ensure!(receipts.len() <= 256 * 1024, "fence evidence too large");
    let authority = SignedFence {
        public: ed25519_dalek::VerifyingKey::from_bytes(&public)?,
        receipts: serde_json::from_slice(&receipts)?,
    };
    for plan in plans {
        let request = request(&plan)?;
        ensure!(
            hex::encode(
                namespace
                    .disaster_fence_request(request.clone())
                    .await?
                    .digest()
            ) == plan.fence_digest,
            "plan binding changed"
        );
        namespace
            .recover_from_disaster(
                request,
                &authority,
                cellar_ltx::LtxHost::default(),
                ArchiveLimits::default(),
            )
            .await?;
    }
    cellar::cli_output::Output::new(cellar::cli_output::Format::Json)
        .line(format_args!("{{\"recovered_births\":{total}}}"))?;
    Ok(())
}
