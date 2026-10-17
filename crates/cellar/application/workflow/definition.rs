//! Capture the compiled workflow definition in its instance journal.
use super::*;

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
