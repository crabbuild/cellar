// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Context, Result};
use prost_types::FileDescriptorProto;
use std::collections::{BTreeMap, BTreeSet};

/// File visibility is local declarations plus direct imports and their public
/// exports. Merely including a file in the descriptor set grants no visibility.
pub(super) struct Imports {
    visible: Vec<BTreeSet<usize>>,
}

impl Imports {
    pub(super) fn new(files: &[FileDescriptorProto]) -> Result<Self> {
        let mut names = BTreeMap::new();
        for (index, file) in files.iter().enumerate() {
            let name = file
                .name
                .as_deref()
                .context("RPC descriptor file has no name")?;
            ensure!(
                name.ends_with(".proto")
                    && name.len() <= super::MAX_NAME_BYTES
                    && !name.contains(['\\', '\0'])
                    && name
                        .split('/')
                        .all(|part| !part.is_empty() && part != "." && part != ".."),
                "RPC descriptor filename must be a canonical relative proto path"
            );
            ensure!(
                names.insert(name, index).is_none(),
                "duplicate RPC descriptor file"
            );
        }

        let mut edges = Vec::with_capacity(files.len());
        let mut public = Vec::with_capacity(files.len());
        for (index, file) in files.iter().enumerate() {
            let mut dependencies = Vec::new();
            let mut seen = BTreeSet::new();
            for dependency in &file.dependency {
                let &target = names
                    .get(dependency.as_str())
                    .context("missing RPC descriptor import")?;
                ensure!(target != index, "RPC descriptor cannot import itself");
                ensure!(seen.insert(target), "duplicate RPC descriptor import");
                dependencies.push(target);
            }
            let public_indexes = modifier_indexes(&file.public_dependency, dependencies.len())?;
            let weak_indexes = modifier_indexes(&file.weak_dependency, dependencies.len())?;
            ensure!(
                public_indexes.is_disjoint(&weak_indexes),
                "RPC import cannot be both public and weak"
            );
            public.push(
                public_indexes
                    .into_iter()
                    .map(|i| dependencies[i])
                    .collect::<Vec<_>>(),
            );
            edges.push(dependencies);
        }

        // Dependency-first order detects cycles in every file, even an unused
        // import. Recursion is bounded by the loader's 256-file inventory cap.
        let mut states = vec![0; files.len()];
        let mut order = Vec::with_capacity(files.len());
        for index in 0..files.len() {
            visit(index, &edges, &mut states, &mut order)?;
        }
        let mut exports = vec![BTreeSet::new(); files.len()];
        for index in order {
            let mut exported = BTreeSet::from([index]);
            for &dependency in &public[index] {
                exported.extend(exports[dependency].iter().copied());
            }
            exports[index] = exported;
        }
        let visible = edges
            .iter()
            .enumerate()
            .map(|(index, dependencies)| {
                let mut visible = BTreeSet::from([index]);
                for &dependency in dependencies {
                    visible.extend(exports[dependency].iter().copied());
                }
                visible
            })
            .collect();
        Ok(Self { visible })
    }

    pub(super) fn can_reference(&self, source: usize, target: usize) -> bool {
        self.visible[source].contains(&target)
    }
}

fn modifier_indexes(indexes: &[i32], count: usize) -> Result<BTreeSet<usize>> {
    let mut result = BTreeSet::new();
    for &index in indexes {
        let index = usize::try_from(index).context("negative RPC import modifier index")?;
        ensure!(index < count, "RPC import modifier index is out of range");
        ensure!(result.insert(index), "duplicate RPC import modifier index");
    }
    Ok(result)
}

fn visit(
    index: usize,
    edges: &[Vec<usize>],
    states: &mut [u8],
    order: &mut Vec<usize>,
) -> Result<()> {
    ensure!(states[index] != 1, "cyclic RPC descriptor imports");
    if states[index] == 2 {
        return Ok(());
    }
    states[index] = 1;
    for &dependency in &edges[index] {
        visit(dependency, edges, states, order)?;
    }
    states[index] = 2;
    order.push(index);
    Ok(())
}
