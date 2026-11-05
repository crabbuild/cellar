// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Operator receipts must match the exact first-group request.
use super::*;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ApplyPlan {
    reservation: RaftFleetFirstGroupPlan,
    publication: MutationIdentity,
    archive: MutationIdentity,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationIdentity {
    request_id: String,
    expected_revision: u64,
}

impl ApplyPlan {
    pub fn validate(&self, namespace: &str, catalog: &str) -> Result<()> {
        if self.reservation.catalog_id != catalog {
            return Err(invalid("first group plan belongs to another catalog"));
        }
        self.publication.request(&self.reservation).validate(namespace)?;
        self.archive.request(&self.reservation).validate(namespace)?;
        if self.archive.request_id == self.publication.request_id
            || self.archive.expected_revision <= self.publication.expected_revision
        {
            return Err(invalid("first group archive must follow publication with its own identity"));
        }
        Ok(())
    }
}

impl MutationIdentity {
    fn request(&self, plan: &RaftFleetFirstGroupPlan) -> RaftFleetFirstGroupMutation {
        RaftFleetFirstGroupMutation {
            plan: plan.clone(),
            request_id: self.request_id.clone(),
            expected_revision: self.expected_revision,
        }
    }
}

pub(super) async fn apply(
    client: &http::Client,
    plan: &ApplyPlan,
    timeout: Duration,
) -> Result<Value> {
    crate::asyncrt::timeout(timeout, async {
        let reservation = reserve(client, &plan.reservation)
            .await
            .map_err(|error| stage_error("reserve", error))?;
        let publication = mutate(
            client,
            &plan.publication.request(&plan.reservation),
            "publish",
            RaftFleetFirstGroupStage::Published,
        )
        .await
        .map_err(|error| stage_error("publish", error))?;
        let archive = mutate(
            client,
            &plan.archive.request(&plan.reservation),
            "archive",
            RaftFleetFirstGroupStage::ArchiveInitialized,
        )
        .await
        .map_err(|error| stage_error("archive", error))?;
        Ok(json!({ "reservation": reservation, "publication": publication, "archive": archive }))
    })
    .await
    .map_err(|_| unavailable())?
}

fn stage_error(stage: &str, mut error: RaftFleetFailure) -> RaftFleetFailure {
    error.message = format!("First group {stage} stage. {}", error.message);
    error
}

pub(super) async fn standby(
    client: &http::Client,
    catalog: &str,
    namespace: &str,
    timeout: Duration,
) -> Result<Value> {
    let observed = client
        .get::<RaftFleetFirstGroupStandby>(
            &format!("namespaces/{namespace}/first-group/standby"),
            None,
            None,
            timeout,
        )
        .await?;
    observed.validate().map_err(|_| unavailable())?;
    if observed.catalog_id != catalog || observed.namespace_id != namespace {
        return Err(unavailable());
    }
    serde_json::to_value(observed).map_err(|_| unavailable())
}

pub(super) async fn reserve(
    client: &http::Client,
    plan: &RaftFleetFirstGroupPlan,
) -> Result<Value> {
    let response = client
        .post::<RaftFleetFirstGroupAcceptance>(
            &format!("namespaces/{}/first-group/reserve", plan.namespace_id),
            plan,
        )
        .await?;
    let expected = RaftFleetFirstGroupAcceptance::from_plan(
        plan,
        &plan.request_id,
        plan.expected_revision,
        RaftFleetFirstGroupStage::Reserved,
    );
    checked(response, expected)
}

pub(super) async fn mutate(
    client: &http::Client,
    request: &RaftFleetFirstGroupMutation,
    verb: &str,
    stage: RaftFleetFirstGroupStage,
) -> Result<Value> {
    let response = client
        .post::<RaftFleetFirstGroupAcceptance>(
            &format!(
                "namespaces/{}/first-group/{verb}",
                request.plan.namespace_id
            ),
            request,
        )
        .await?;
    let expected = RaftFleetFirstGroupAcceptance::from_plan(
        &request.plan,
        &request.request_id,
        request.expected_revision,
        stage,
    );
    checked(response, expected)
}

fn checked(
    response: RaftFleetFirstGroupAcceptance,
    expected: RaftFleetFirstGroupAcceptance,
) -> Result<Value> {
    if response != expected {
        return Err(unavailable());
    }
    serde_json::to_value(response).map_err(|_| unavailable())
}
