// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use serde::{Deserialize, Serialize};

/// The modern batch API: shared options with generated IDs, or individual options.
#[derive(Clone, Debug)]
pub enum BatchCreateOptions {
    Count {
        count: usize,
        params: Option<StructuredValue>,
        retention: Retention,
    },
    Instances(Vec<CreateOptions>),
}

impl BatchCreateOptions {
    pub fn count(count: usize) -> Self {
        Self::Count {
            count,
            params: None,
            retention: Retention::default(),
        }
    }

    pub fn instances(instances: Vec<CreateOptions>) -> Self {
        Self::Instances(instances)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BatchCreateError {
    pub index: usize,
    pub id: Option<String>,
    pub code: u32,
    pub message: String,
}

#[derive(Clone, Debug, Default)]
pub struct BatchCreateResult {
    pub created: Vec<Instance>,
    pub errors: Vec<BatchCreateError>,
}

impl Workflow {
    /// Create a batch with per-input errors. Invalid options fail before any RPC.
    pub async fn create_batch_with(
        &self,
        context: &Context,
        options: BatchCreateOptions,
    ) -> Result<BatchCreateResult> {
        let batch = match options {
            BatchCreateOptions::Instances(batch) => batch,
            BatchCreateOptions::Count {
                count,
                params,
                retention,
            } => {
                ensure!(
                    (1..=limits().max_batch_size).contains(&count),
                    "WORKFLOW_ERROR: invalid batch count"
                );
                if let Some(params) = &params {
                    check_value(params, "the workflow params")?;
                }
                let options = CreateOptions {
                    params,
                    retention,
                    ..Default::default()
                };
                let mut batch = Vec::new();
                batch
                    .try_reserve_exact(count)
                    .map_err(|cause| error(format!("batch cannot be allocated: {cause}")))?;
                batch.resize(count, options);
                batch
            }
        };
        ensure!(
            (1..=limits().max_batch_size).contains(&batch.len()),
            "WORKFLOW_ERROR: invalid batch size"
        );
        let mut prepared = Vec::with_capacity(batch.len());
        for options in batch {
            if let Some(params) = &options.params {
                check_value(params, "the workflow params")?;
            }
            prepared.push(self.options(options)?);
        }
        let mut result = BatchCreateResult::default();
        let mut ids = IndexSet::new();
        for (index, (instance, mut arguments)) in prepared.into_iter().enumerate() {
            let duplicate = !ids.insert(instance.id().to_owned());
            let code = if duplicate {
                Some(10415)
            } else {
                set(
                    &mut arguments,
                    "skipExisting",
                    &StructuredValue::from_json(json!(true))?,
                )?;
                match instance.call(context, "__wfCreate", Some(arguments)).await {
                    Ok(reply)
                        if property(&reply, "created")?
                            .is_some_and(|v| v.root == CloneValue::Boolean(true)) =>
                    {
                        None
                    }
                    Ok(_) => Some(10405),
                    Err(_) => Some(10001),
                }
            };
            if let Some(code) = code {
                result.errors.push(BatchCreateError {
                    index,
                    id: Some(instance.id().into()),
                    code,
                    message: match code {
                        10415 => "duplicate instance id in batch",
                        10405 => "instance already exists",
                        _ => "workflows.api.error.internal_server",
                    }
                    .into(),
                });
            } else {
                result.created.push(instance);
            }
        }
        Ok(result)
    }
}
