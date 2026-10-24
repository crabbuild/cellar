// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{set, Context, CreateOptions, Definition, Retention, StructuredValue, Workflow};
use anyhow::{anyhow, ensure, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Binding defaults are captured by the deployment, never read from process environment.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BindingSettings {
    pub default_retention: Retention,
    pub schedules: Vec<String>,
    /// Optional versioned documentation for the compiled workflow.
    pub definition: Option<Definition>,
}

impl BindingSettings {
    pub fn validate(&self) -> Result<()> {
        if let Some(definition) = &self.definition {
            definition.validate()?;
        }
        for duration in [
            &self.default_retention.success,
            &self.default_retention.error,
        ]
        .into_iter()
        .flatten()
        {
            let ms = duration.milliseconds()?;
            ensure!(
                ms.is_finite() && ms >= 0.0 && ms < i64::MAX as f64,
                "WORKFLOW_ERROR: invalid binding retention"
            );
        }
        for expression in &self.schedules {
            cellar_logic::cron::parse(expression).map_err(|error| {
                anyhow!("WORKFLOW_ERROR: invalid workflow schedule {expression:?}: {error}")
            })?;
        }
        Ok(())
    }

    pub(crate) fn from_binding(value: &serde_json::Value) -> Result<Self> {
        let settings: Self = serde_json::from_value(json!({
            "default_retention": value.get("default_retention").cloned().unwrap_or(json!({})),
            "schedules": value.get("schedules").cloned().unwrap_or(json!([])),
            "definition": value.get("definition").cloned().unwrap_or(json!(null)),
        }))?;
        settings.validate()?;
        Ok(settings)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CronSchedule {
    pub cron: String,
    pub scheduled_ms: i64,
}

impl CronSchedule {
    pub(crate) fn instance_id(&self) -> String {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(self.cron.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        format!("cron-{}-{}", digest, self.scheduled_ms)
    }
}

impl Workflow {
    pub(crate) async fn create_scheduled(
        &self,
        context: &Context,
        schedule: CronSchedule,
    ) -> Result<()> {
        let id = schedule.instance_id();
        let (instance, mut arguments) = self.options(CreateOptions {
            id: Some(id),
            ..Default::default()
        })?;
        set(
            &mut arguments,
            "schedule",
            &StructuredValue::from_json(serde_json::to_value(schedule)?)?,
        )?;
        set(
            &mut arguments,
            "skipExisting",
            &StructuredValue::from_json(json!(true))?,
        )?;
        instance
            .call(context, "__wfCreate", Some(arguments))
            .await?;
        Ok(())
    }
}
