// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use futures_util::{stream, StreamExt};

impl FleetClient {
    pub async fn workflow(
        &self,
        application: &str,
        binding: &BindingView,
        instance: &str,
        command: WorkflowAction,
        expected_generation: Option<&str>,
    ) -> Result<Value> {
        ensure!(binding.kind == "workflow", "binding is not a workflow");
        ensure!(
            !instance.is_empty() && instance.len() <= 256,
            "invalid workflow instance ID"
        );
        let history = matches!(
            command,
            WorkflowAction::History { .. } | WorkflowAction::HistorySearch { .. }
        );
        let scope =
            crate::engine_api::workflow_cell_scope(application, &binding.identity, instance);
        let mut result=self.call(
            scope.clone(),
            None,
            json!({"binding":binding.name,"instance":instance,"command":command,"expected_generation":expected_generation}),
        )
        .await?;
        if history {
            let fields = result
                .as_object_mut()
                .ok_or_else(|| anyhow::anyhow!("workflow history is not an object"))?;
            fields.insert("scope".into(), json!(scope));
            fields.insert("bindingIdentity".into(), json!(binding.identity));
        }
        Ok(result)
    }

    /// Bound each page by visited scopes, including instances of other bindings.
    /// Continuation advances over the application class even if no match is found.
    pub async fn workflow_instances(
        &self,
        application: &str,
        binding: &str,
        request: PageRequest,
    ) -> Result<Page<Value>> {
        let limit = request.limit()?.min(20);
        let class = crate::deploy::workflow_class(application);
        let page = self
            .cells(PageRequest {
                prefix: Some(format!("{class}:")),
                limit: Some(limit),
                cursor: request.cursor,
            })
            .await?;
        let mut warnings = page.warnings;
        let mut items = Vec::new();
        let mut results = stream::iter(page.items.into_iter().map(|cell| async move {
            let value = self
                .call(
                    cell.scope.clone(),
                    None,
                    json!({"binding":binding,"command":{"operation":"inspect"}}),
                )
                .await;
            (cell.scope, value)
        }))
        .buffered(4);
        while let Some((scope, result)) = results.next().await {
            match result {
                Ok(Value::Null) => {}
                Ok(mut value) => {
                    value["scope"] = json!(scope);
                    items.push(value);
                }
                Err(_) => warnings.push(format!("Could not inspect workflow cell {scope}")),
            }
        }
        Ok(Page::new(items, page.next_cursor, warnings))
    }
}
