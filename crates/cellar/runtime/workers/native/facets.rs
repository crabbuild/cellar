// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld: crates/celld/js.rs.

use super::*;
use crate::application::FacetContext;
use cellar_native::clone::{decode, Object, StructuredValue, Value};

#[cfg(test)]
#[path = "facets_tests.rs"]
pub(super) mod tests;

pub(super) fn identity_name(id: &StructuredValue) -> Result<Option<String>> {
    match &id.root {
        Value::String(_) => Ok(None),
        Value::Reference(index) => {
            let Some(Object::DenseArray { elements, .. }) = id.objects.get(*index as usize) else {
                anyhow::bail!("facet identity must be a string or Durable Object ID tuple");
            };
            anyhow::ensure!(
                elements.len() == 3
                    && matches!(elements[0], Value::String(_))
                    && matches!(elements[1], Value::String(_)),
                "invalid facet Durable Object ID tuple"
            );
            match &elements[2] {
                Value::Undefined | Value::Null => Ok(None),
                // The structured ID retains the exact UTF-16 name. The
                // existing host identity label is a Rust string.
                Value::String(name) => Ok(Some(String::from_utf16_lossy(name))),
                _ => anyhow::bail!("invalid facet Durable Object ID name"),
            }
        }
        _ => anyhow::bail!("facet identity must be a string or Durable Object ID tuple"),
    }
}

impl NativeWorker {
    pub(super) fn own_embedded_cell(
        &mut self,
        scope: &str,
        parent: &storage::StorageIdentity,
        name: &str,
        id_sc: Vec<u8>,
        props_sc: Vec<u8>,
        file: (std::path::PathBuf, bool),
    ) -> Result<u64> {
        let _installed = self.cells.install();
        let _clock = cellar_native::web::WallClockGuard::enter(primitive_wall_ms);
        if let Some(opening) = storage::facet_opening(scope) {
            anyhow::ensure!(
                storage::activation_epoch(scope) == Some(parent.epoch),
                "facet belongs to a retired root activation"
            );
            storage::refresh_embedded_root(scope, parent);
            return Ok(opening);
        }
        anyhow::ensure!(
            storage::activation_epoch(scope).is_none(),
            "facet scope is already resident"
        );
        let (class, _) = scope.split_once(':').context("facet scope has no class")?;
        anyhow::ensure!(
            self.application.supports_cell(class),
            "no native cell class named {class}"
        );
        let id = decode(&id_sc).context("decode facet identity")?;
        identity_name(&id)?;
        let props = if props_sc.is_empty() {
            StructuredValue {
                root: Value::Undefined,
                objects: Vec::new(),
            }
        } else {
            decode(&props_sc).context("decode facet props")?
        };
        let (path, _) = file;
        storage::open_embedded(scope, parent, name, &path, self.config.compat.sqlite_vec)
            .context("facet storage open failed")?;
        self.facets.insert(
            scope.to_string(),
            Arc::new(FacetContext {
                id,
                props,
                depth: parent.facet_path.len() + 1,
            }),
        );
        self.activations.insert(
            scope.to_string(),
            Arc::new(Activation {
                worker_config: self.config.clone(),
                scope: scope.to_string(),
                epoch: parent.epoch,
                delete_all_deletes_alarm: self.config.compat.delete_all_deletes_alarm,
                resources: Default::default(),
                websocket_states: Default::default(),
            }),
        );
        storage::facet_opening(scope).context("the facet opened without an embedded backing")
    }

    // Called with this worker's original storage installed. Drop instances
    // and retire retained contexts before their connections can be reused.
    pub(super) fn close_native_cell(&mut self, scope: &str) {
        if let Some(activation) = self.activations.get(scope) {
            retire_activation(activation);
        }
        self.instances.remove(scope);
        self.activations.remove(scope);
        self.facets.remove(scope);
        storage::close(scope);
    }

    pub(super) fn retire_facet_descendants(&mut self, scope: &str) {
        for scope in storage::embedded_descendants(scope) {
            self.close_native_cell(&scope);
        }
    }

    pub(super) fn close_facet_tree(
        &mut self,
        scope: &str,
        root: &str,
        facet_path: &[String],
    ) -> (Vec<String>, Result<()>) {
        let _installed = self.cells.install();
        let mut closed = storage::embedded_facets_below(root, facet_path);
        closed.push(scope.to_string());
        for scope in &closed {
            self.close_native_cell(scope);
        }
        (closed, Ok(()))
    }
}
