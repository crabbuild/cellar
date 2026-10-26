// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Context as _, Result};
use cellar::native::clone::{self, Object, StructuredValue, Value as CloneValue};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Publication {
    pub format: u8,
    pub group: String,
    pub count: usize,
    pub max_body_bytes: usize,
    pub value: String,
}

pub(super) fn graph(encoded: &str, maximum: usize) -> Result<StructuredValue> {
    // Reuse the bounded, value-only native clone codec. A marker has authority
    // only alongside the separately retained native lease and registry entry.
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    let bytes = STANDARD.decode(encoded)?;
    clone::decode_with_limits(&bytes, limits(maximum))
}

pub(super) fn limits(bytes: usize) -> clone::Limits {
    clone::Limits {
        bytes,
        objects: 100_000,
        values: 200_000,
        depth: 64,
    }
}

pub(super) fn markers(encoded: &str, count: usize, maximum: usize) -> Result<()> {
    let graph = graph(encoded, maximum)?;

    fn array<'a>(graph: &'a StructuredValue, value: &CloneValue) -> Result<&'a [CloneValue]> {
        let CloneValue::Reference(id) = value else {
            anyhow::bail!("expected RPC array")
        };
        match graph.objects.get(*id as usize) {
            Some(Object::DenseArray {
                elements,
                properties,
            }) if properties.is_empty() => Ok(elements),
            _ => anyhow::bail!("expected RPC array"),
        }
    }
    let root = array(&graph, &graph.root)?;
    ensure!(root.len() == 3, "invalid callable RPC graph");
    let references = array(&graph, &root[1])?;
    let kinds = array(&graph, &root[2])?;
    ensure!(
        count > 0 && references.len() == count && kinds.len() == count,
        "invalid RPC delegate count"
    );
    for kind in kinds {
        ensure!(
            *kind == CloneValue::from("function")
                || *kind == CloneValue::from("readable")
                || *kind == CloneValue::from("writable"),
            "invalid RPC delegate kind"
        );
    }
    let mut seen = HashSet::new();
    for reference in references {
        let CloneValue::Reference(id) = reference else {
            anyhow::bail!("invalid RPC marker")
        };
        ensure!(seen.insert(*id), "duplicate RPC marker");
        ensure!(
            matches!(graph.objects.get(*id as usize), Some(Object::Object(properties)) if properties.is_empty()),
            "invalid RPC marker"
        );
    }
    Ok(())
}

pub(super) fn arguments(encoded: &str, maximum: usize) -> Result<()> {
    let graph = graph(encoded, maximum)?;
    let CloneValue::Reference(id) = graph.root else {
        anyhow::bail!("RPC arguments must be an array")
    };
    ensure!(
        matches!(
            graph.objects.get(id as usize),
            Some(Object::DenseArray { .. } | Object::SparseArray { .. })
        ),
        "RPC arguments must be an array"
    );
    Ok(())
}

pub(super) fn publication(
    data: cellar::engine_api::RpcData,
    maximum: usize,
) -> Result<Publication> {
    let cellar::engine_api::RpcData::Json(value) = data else {
        anyhow::bail!("invalid RPC publication")
    };
    ensure!(
        value.len() <= maximum,
        "RPC publication exceeds byte budget"
    );
    let mut publication: Publication =
        serde_json::from_str(&value).context("invalid RPC publication")?;
    ensure!(
        publication.format == 1 && publication.group.len() <= 64 && publication.max_body_bytes > 0,
        "invalid RPC publication metadata"
    );
    markers(&publication.value, publication.count, maximum)?;
    publication.max_body_bytes = publication.max_body_bytes.min(maximum);
    Ok(publication)
}

#[cfg(test)]
#[path = "wire_tests.rs"]
mod tests;

// Input markers are admitted only with an invocation-owned native reservation.
// Function callbacks require a separate profile; data cannot select that power.
pub(super) fn inputs(encoded: &str, count: usize, maximum: usize) -> Result<()> {
    markers(encoded, count, maximum)?;
    let graph = graph(encoded, maximum)?;
    let CloneValue::Reference(root) = graph.root else {
        anyhow::bail!("missing RPC input graph")
    };
    let Object::DenseArray { elements, .. } = &graph.objects[root as usize] else {
        anyhow::bail!("missing RPC input envelope")
    };
    let CloneValue::Reference(args) = elements[0] else {
        anyhow::bail!("RPC inputs require an argument array")
    };
    ensure!(
        matches!(
            graph.objects.get(args as usize),
            Some(Object::DenseArray { .. } | Object::SparseArray { .. })
        ),
        "RPC inputs require an argument array"
    );
    let CloneValue::Reference(kinds) = elements[2] else {
        anyhow::bail!("missing RPC input kinds")
    };
    let Object::DenseArray { elements, .. } = &graph.objects[kinds as usize] else {
        anyhow::bail!("missing RPC input kinds array")
    };
    ensure!(
        elements
            .iter()
            .all(|kind| *kind == CloneValue::from("readable")
                || *kind == CloneValue::from("writable")),
        "RPC input callbacks require a separate profile"
    );
    Ok(())
}
