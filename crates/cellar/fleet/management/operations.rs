use super::FleetClient;
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopMode {
    Handoff,
    Preserve,
}

impl From<StopMode> for crate::actor::ShutdownMode {
    fn from(mode: StopMode) -> Self {
        match mode {
            StopMode::Handoff => Self::Handoff,
            StopMode::Preserve => Self::Preserve,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StopProof {
    /// Explicitly absent host or its actual joined observations. Required in
    /// format 1: old/missing evidence cannot imply a completed Raft stop.
    pub raft: super::RaftStopProof,
    pub mode: StopMode,
    pub ingress_drained: bool,
    pub application_work_drained: bool,
    pub pending_application_events: usize,
    pub cells_drained: bool,
    pub pending_dispatches: usize,
    pub log_quiesced: bool,
    pub local_durability_closed: bool,
    pub clean_reload_prepared: bool,
    pub drain_timed_out: bool,
    pub deadline_elapsed: bool,
}

impl From<&crate::node_lifecycle::ShutdownReport> for StopProof {
    fn from(report: &crate::node_lifecycle::ShutdownReport) -> Self {
        Self {
            raft: report
                .raft
                .as_deref()
                .map_or(super::RaftStopProof::NotRegistered, Into::into),
            mode: match report.mode {
                crate::actor::ShutdownMode::Handoff => StopMode::Handoff,
                crate::actor::ShutdownMode::Preserve => StopMode::Preserve,
            },
            ingress_drained: report.ingress_drained,
            application_work_drained: report.application_work_drained,
            pending_application_events: report.pending_application_events,
            cells_drained: report.cells_drained,
            pending_dispatches: report.pending_dispatches,
            log_quiesced: report.log_quiesced,
            local_durability_closed: report.local_durability_closed,
            clean_reload_prepared: report.clean_reload_prepared,
            drain_timed_out: report.drain_timed_out,
            deadline_elapsed: report.deadline_elapsed,
        }
    }
}

impl StopProof {
    pub fn complete(&self, requested: StopMode) -> bool {
        self.mode == requested
            && self.ingress_drained
            && self.application_work_drained
            && self.pending_application_events == 0
            && self.cells_drained
            && self.pending_dispatches == 0
            && self.log_quiesced
            && self.local_durability_closed
            && (self.mode != StopMode::Preserve || self.clean_reload_prepared)
            && !self.drain_timed_out
            && !self.deadline_elapsed
            && self.raft.complete()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StopReceipt {
    pub version: u32,
    pub node: String,
    pub generation: String,
    pub operation_id: String,
    pub requested_mode: StopMode,
    pub phase: String,
    pub sequence: u64,
    pub requested_at: u64,
    pub observed_at: u64,
    pub domain_joined: bool,
    pub proof: Option<StopProof>,
    pub error: Option<String>,
}

pub(crate) fn receipt_key(node: &str, generation: &str, operation: &str) -> Result<String> {
    for value in [node, generation, operation] {
        ensure!(
            !value.is_empty()
                && value.len() <= 128
                && value
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"-_".contains(&c)),
            "invalid node operation identity"
        );
    }
    Ok(format!(
        "management/stops/{node}/{generation}/{operation}.json"
    ))
}

impl FleetClient {
    /// Receipts remain readable after the node listener and lease disappear.
    pub async fn stop_receipt(
        &self,
        node: &str,
        generation: &str,
        operation: &str,
    ) -> Result<Option<StopReceipt>> {
        let key = receipt_key(node, generation, operation)?;
        let Some((body, _)) = self.bucket.get(&key).await? else {
            return Ok(None);
        };
        ensure!(
            body.len() <= 64 * 1024,
            "node operation receipt exceeds 64 KiB"
        );
        let receipt: StopReceipt = serde_json::from_slice(&body)?;
        if let Some(proof) = &receipt.proof {
            proof.raft.validate()?;
        }
        ensure!(
            receipt.version == 1
                && receipt.node == node
                && receipt.generation == generation
                && receipt.operation_id == operation,
            "node operation receipt identity mismatch"
        );
        let proven = receipt
            .proof
            .as_ref()
            .is_some_and(|proof| proof.complete(receipt.requested_mode));
        ensure!(
            match receipt.phase.as_str() {
                "complete" => receipt.domain_joined && proven && receipt.error.is_none(),
                "incomplete" => receipt.domain_joined && receipt.proof.is_some() && !proven,
                "failed" =>
                    receipt.domain_joined && receipt.proof.is_none() && receipt.error.is_some(),
                "accepted" | "draining_ingress" | "draining_cells" | "closing_durability"
                | "joining_domain" => !receipt.domain_joined,
                _ => false,
            },
            "node operation receipt has inconsistent completion evidence"
        );
        Ok(Some(receipt))
    }
}
