//! Bounded, declarative diagrams for compiled workflows. These do not execute code.
use super::*;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Definition {
    pub version: String,
    pub nodes: Vec<DefinitionNode>,
    pub edges: Vec<DefinitionEdge>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DefinitionNode {
    pub id: String,
    pub label: String,
    pub kind: DefinitionKind,
    /// Exact durable step name, when this node corresponds to recorded work.
    pub step: Option<String>,
    #[serde(default)]
    pub description: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DefinitionKind {
    Start,
    Activity,
    Decision,
    Parallel,
    Sleep,
    Wait,
    End,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DefinitionEdge {
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub label: String,
}

impl Definition {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.version.trim().is_empty() && self.version.len() <= 128,
            "invalid workflow definition version"
        );
        ensure!(
            !self.nodes.is_empty() && self.nodes.len() <= 128 && self.edges.len() <= 256,
            "workflow definitions allow 1–128 nodes and at most 256 edges"
        );
        ensure!(
            serde_json::to_vec(self)?.len() <= 64 * 1024,
            "workflow definition exceeds 64 KiB"
        );
        let mut ids = BTreeSet::new();
        let mut steps = BTreeSet::new();
        for node in &self.nodes {
            ensure!(
                !node.id.is_empty() && node.id.len() <= 128 && ids.insert(&node.id),
                "invalid or duplicate workflow node ID"
            );
            ensure!(
                !node.label.trim().is_empty()
                    && node.label.len() <= 256
                    && node.description.len() <= 2048,
                "invalid workflow node label or description"
            );
            if let Some(step) = &node.step {
                ensure!(
                    !step.is_empty() && step.len() <= 256 && steps.insert(step),
                    "invalid or duplicate workflow step mapping"
                );
            }
        }
        let mut edges = BTreeSet::new();
        for edge in &self.edges {
            ensure!(
                ids.contains(&edge.from) && ids.contains(&edge.to) && edge.label.len() <= 256,
                "invalid workflow edge"
            );
            ensure!(
                edges.insert((&edge.from, &edge.to, &edge.label)),
                "duplicate workflow edge"
            );
        }
        Ok(())
    }

    /// Content identity, including the declared version and diagram order.
    pub fn revision(&self) -> Result<String> {
        self.validate()?;
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(self)?)))
    }
}

pub(super) fn snapshot(
    context: &Context,
    storage: &crate::application::Storage,
    meta: &mut StructuredValue,
) -> Result<()> {
    let name = super::cell::text(meta, "workflowName")?;
    let mut declared = context
        .workflow_bindings
        .values()
        .filter(|binding| binding.name() == name);
    let definition = declared
        .next()
        .and_then(|binding| binding.settings.definition.as_ref());
    ensure!(
        declared.all(|binding| binding.settings.definition.as_ref() == definition),
        "conflicting workflow definitions for {name}"
    );
    let generation = super::cell::text(meta, "generation")?;
    let snapshot = match definition {
        Some(definition) => json!({"definition": definition, "revision": definition.revision()?}),
        None => json!({"definition": null, "revision": null}),
    };
    let snapshot = StructuredValue::from_json(snapshot)?;
    storage.put_clone(&format!("__wf.definition.{generation}"), &snapshot)?;
    set(meta, "definitionSnapshot", &snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagram_identity_and_references_are_validated() -> Result<()> {
        let mut definition: Definition = serde_json::from_value(
            json!({"version":"1", "nodes":[{"id":"a","label":"Prepare","kind":"activity","step":"prepare"}],"edges":[]}),
        )?;
        let revision = definition.revision()?;
        definition.version = "2".into();
        assert_ne!(revision, definition.revision()?);
        definition.edges.push(DefinitionEdge {
            from: "a".into(),
            to: "missing".into(),
            label: String::new(),
        });
        assert!(definition.validate().is_err());
        definition.edges.clear();
        definition.nodes.push(definition.nodes[0].clone());
        assert!(definition.validate().is_err());
        Ok(())
    }
}
