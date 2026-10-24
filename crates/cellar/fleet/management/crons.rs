// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use super::{FleetClient, Reachable, Subject};
use crate::{
    deployment_cron,
    protocol::{DeployPointer, Manifest},
};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CronConfiguration {
    pub application: String,
    pub deployment_revision: String,
    pub prefix: String,
    pub revision: Option<String>,
    pub expressions: Vec<String>,
    pub declared_expressions: Vec<String>,
    pub primary: bool,
}

impl FleetClient {
    /// Observe the effective deployment-scoped configuration, without activating a cell.
    pub async fn cron_configuration(&self, application: &str) -> Result<CronConfiguration> {
        ensure!(
            !application.is_empty()
                && application.len() <= 128
                && application
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"-_".contains(&c)),
            "invalid application name"
        );
        let (bytes, deployment_revision) = self
            .bucket
            .get(&format!("deploy/{application}/current.json"))
            .await?
            .context("application not found")?;
        let pointer: DeployPointer = serde_json::from_slice(&bytes)?;
        ensure!(
            pointer.script_name.as_deref() == Some(application),
            "application pointer identity mismatch"
        );
        let (bytes, _) = self
            .bucket
            .get(&format!("{}/manifest.json", pointer.prefix))
            .await?
            .context("manifest unavailable")?;
        let manifest: Manifest = serde_json::from_slice(&bytes)?;
        ensure!(
            manifest.script_name == application && manifest.version == pointer.version,
            "manifest identity mismatch"
        );
        let (expressions, revision) = match deployment_cron::read(
            &self.bucket,
            application,
            &pointer.prefix,
            &manifest.crons,
        )
        .await?
        {
            Some((expressions, revision)) => (expressions, Some(revision)),
            None => (manifest.crons.clone(), None),
        };
        let primary = self
            .bucket
            .get("deploy/current.json")
            .await?
            .map(|(bytes, _)| serde_json::from_slice::<DeployPointer>(&bytes))
            .transpose()?
            .is_some_and(|current| {
                current.prefix == pointer.prefix
                    && current.script_name.as_deref() == Some(application)
            });
        Ok(CronConfiguration {
            application: application.into(),
            deployment_revision,
            prefix: pointer.prefix,
            revision,
            expressions,
            declared_expressions: manifest.crons,
            primary,
        })
    }

    /// CAS only the schedule document belonging to the observed deployment.
    /// A racing deploy can make the result inactive, but cannot apply it to a new prefix.
    pub async fn update_crons(
        &self,
        application: &str,
        deployment_revision: &str,
        expected_revision: Option<&str>,
        expressions: Vec<String>,
    ) -> Result<CronConfiguration> {
        deployment_cron::validate(&expressions)?;
        let current = self.cron_configuration(application).await?;
        ensure!(
            current.primary,
            "only the fleet primary application runs cron schedules"
        );
        ensure!(
            current.deployment_revision == deployment_revision,
            "deployment changed; refresh before saving"
        );
        ensure!(
            current.revision.as_deref() == expected_revision,
            "cron configuration changed; refresh before saving"
        );
        let document = deployment_cron::Schedule {
            version: 1,
            application: application.into(),
            prefix: current.prefix.clone(),
            expressions: expressions.clone(),
        };
        let revision = self
            .bucket
            .put_cas(
                &deployment_cron::key(&current.prefix, &current.declared_expressions),
                serde_json::to_vec(&document)?,
                expected_revision,
            )
            .await?
            .context("cron configuration changed; refresh before saving")?;
        // Return the exact write receipt. Callers must observe the current selector
        // and node revisions separately; this does not assert propagation.
        Ok(CronConfiguration {
            revision: Some(revision),
            expressions,
            ..current
        })
    }
}

#[derive(Serialize)]
pub struct CronNodeObservation {
    pub node: String,
    pub generation: String,
    pub status: &'static str,
    pub observed_revision: Option<String>,
    pub observed_at: u64,
    pub error: Option<String>,
}

impl FleetClient {
    /// Bounded, fresh observations; adoption is not evidence that an occurrence ran.
    pub async fn cron_propagation(
        &self,
        application: &str,
        mut request: super::PageRequest,
    ) -> Result<super::Page<CronNodeObservation>> {
        use futures_util::{stream, StreamExt};
        let desired = self.cron_configuration(application).await?;
        request.limit = Some(request.limit()?.min(20));
        let nodes = self.nodes(request).await?;
        let observations = stream::iter(nodes.items)
            .map(|node| {
                let desired = &desired;
                async move {
                    let mut observation = CronNodeObservation {
                        node: node.id.clone(),
                        generation: node.generation.clone(),
                        status: "unavailable",
                        observed_revision: None,
                        observed_at: crate::ownership_store::now_ms(),
                        error: None,
                    };
                    if !node.lease_current {
                        observation.status = "lease_expired";
                        return observation;
                    }
                    match crate::asyncrt::timeout(
                        std::time::Duration::from_secs(5),
                        self.node_action(&node.id, &node.generation, super::NodeAction::State),
                    )
                    .await
                    {
                        Ok(Ok(state)) => {
                            let deployment = &state["deployment"];
                            observation.observed_revision =
                                deployment["cron_revision"].as_str().map(str::to_string);
                            observation.status = if deployment.get("cron_revision").is_none()
                                || !deployment["cron_expressions"].is_array()
                            {
                                "unsupported"
                            } else if deployment["prefix"] != desired.prefix
                                || deployment["application"] != desired.application
                            {
                                "different_deployment"
                            } else if observation.observed_revision == desired.revision
                                && deployment["cron_expressions"]
                                    == serde_json::json!(desired.expressions)
                            {
                                "adopted"
                            } else {
                                "pending"
                            };
                        }
                        Ok(Err(error)) => observation.error = Some(error.to_string()),
                        Err(_) => observation.error = Some("node observation timed out".into()),
                    }
                    observation
                }
            })
            .buffer_unordered(4)
            .collect::<Vec<_>>()
            .await;
        Ok(super::Page::new(
            observations,
            nodes.next_cursor,
            nodes.warnings,
        ))
    }
}

impl FleetClient {
    /// Real retained attempts from the scheduling cell. Does not arm or run a schedule.
    pub async fn cron_history(
        &self,
        application: &str,
        before: Option<u64>,
        limit: u32,
        expression: Option<&str>,
    ) -> Result<serde_json::Value> {
        let query = crate::application::cron::HistoryQuery {
            before,
            limit,
            expression: expression.map(str::to_string),
        };
        query.validate()?;
        // Only the primary application owns the fleet's scheduling cell.
        // Its retained records remain inspectable after schedules are removed.
        let configuration = self.cron_configuration(application).await?;
        ensure!(
            configuration.primary,
            "only the primary application owns a scheduling cell"
        );
        let scope = cellar_logic::cron::reserved_cell(application);
        Reachable::from_entrances(
            &self.bucket,
            Subject {
                noun: "cron history",
                source: "dashboard",
                timeout: Duration::from_secs(30),
            },
            scope.clone(),
            None,
            self.routing_entrances(&scope).await?,
        )
        .await?
        .with_response_limit(8 * 1024 * 1024)
        .with_cron_history()
        .call(serde_json::json!({"before":before,"limit":limit,"expression":expression}))
        .await
    }
}
