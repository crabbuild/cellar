// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exact partition geometry and immutable directory-plan refusals.
use crate::{managed_fixture::fault, route};
use anyhow::{ensure, Result};
use cellar::application::consensus::{
    verification::{
        cell_groups::*, Catalog, CatalogError, CatalogOptions, InitialMember, NamespaceConfig,
    },
    *,
};
use cellar::sdk::ExecutionDomain;
use cellar_logic::consensus::{MemberId, NodeId};
use std::sync::{atomic::Ordering, Arc};

#[path = "cell_group_directory_fixture.rs"]
mod fixture;
use fixture::*;

pub(crate) async fn split_equal_union_whole_cells() -> Result<()> {
    // Force digest ties at both extremes: the complete CellKey orders cells,
    // so a hash collision cannot give one database to both children.
    let tied: Vec<_> = [0, 1, 127, 255]
        .into_iter()
        .flat_map(|digest| {
            ["a", "aa", "b", "é"]
                .map(|name| CellKey::from_digest([digest; 32], CellId::new(name).unwrap()))
        })
        .collect();
    for pivot in &tied {
        let parent = CellRange::all();
        let (left, right) = parent.split(pivot.clone())?;
        ensure!(left.merge(&right)? == parent);
        ensure!(!left.contains(pivot) && right.contains(pivot));
        for key in &tied {
            ensure!(usize::from(left.contains(key)) + usize::from(right.contains(key)) == 1);
        }
        ensure!(left.split(pivot.clone()).is_err());
        ensure!(right.split(pivot.clone()).is_err());
    }

    let f = Fixture::new(DirectoryLimits::default()).await?;
    let result = check_split(&f).await;
    f.domain.shutdown().await;
    result
}

async fn check_split(f: &Fixture) -> Result<()> {
    let d = &f.directory;
    let mut parent = assigned(4000, CellRange::all(), 1);
    f.reserve(std::slice::from_mut(&mut parent)).await?;
    let init = d.prepare_initialization(operation(4001), std::slice::from_ref(&parent))?;
    f.run(d.publish(&init)).await?;
    let view = f.run(d.observe()).await?;
    let mut keys: Vec<_> = (0..32)
        .map(|n| {
            cell_routing_key(
                parent.history.namespace,
                &CellId::new(format!("cell-{n}")).unwrap(),
            )
        })
        .collect();
    keys.sort();
    // The edge pivots have an empty occupied side. Empty occupancy is valid;
    // the range itself must remain nonempty and cover the parent's full union.
    for (index, pivot) in [keys[0].clone(), keys[16].clone(), keys[31].clone()]
        .into_iter()
        .enumerate()
    {
        let (left, right) = parent.range.split(pivot)?;
        let id = 4100 + index as u64 * 10;
        let mut children = vec![assigned(id, left, 2), assigned(id + 1, right, 2)];
        f.reserve(&mut children).await?;
        let plan = d.prepare_split(&view, operation(id + 2), &parent, &children)?;
        ensure!(d.decode_mutation(plan.encode())?.encode() == plan.encode());
        ensure!(children[0].range.merge(&children[1].range)? == parent.range);
        ensure!(children
            .iter()
            .all(|c| c.history.group != parent.history.group));
        ensure!(children[0].history != children[1].history);
        for key in &keys {
            ensure!(children.iter().filter(|c| c.range.contains(key)).count() == 1);
            ensure!(f.run(d.lookup(&view, key.cell())).await? == parent);
        }
        let reversed = [children[1].clone(), children[0].clone()];
        ensure!(d
            .prepare_split(&view, operation(id + 3), &parent, &reversed)
            .is_err());
        let mut hole = children.clone();
        hole[1].range = hole[0].range.clone();
        ensure!(d
            .prepare_split(&view, operation(id + 4), &parent, &hole)
            .is_err());
        let mut reused = children.clone();
        reused[0].history = parent.history;
        ensure!(d
            .prepare_split(&view, operation(id + 5), &parent, &reused)
            .is_err());
    }
    ensure!(f.run(d.observe()).await?.digest() == view.digest());
    ensure!(f.run(d.active_transition()).await?.is_none());
    ensure!(f.run(f.reopen().await?.observe()).await?.digest() == view.digest());
    ensure!(f.store.reject_listing.load(Ordering::Acquire));
    Ok(())
}

pub(crate) async fn merge_requires_adjacent_compatible_ranges() -> Result<()> {
    let f = Fixture::new(DirectoryLimits::default()).await?;
    let result = check_merge(&f).await;
    f.domain.shutdown().await;
    result
}

async fn check_merge(f: &Fixture) -> Result<()> {
    let d = &f.directory;
    let parent = assigned(5000, CellRange::all(), 1);
    let mut parents = split(&parent, 5100);
    for route in &mut parents {
        route.epoch = RouteEpoch::new(1)?;
    }
    f.reserve(&mut parents).await?;
    let init = d.prepare_initialization(operation(5001), &parents)?;
    f.run(d.publish(&init)).await?;
    let view = f.run(d.observe()).await?;
    let mut child = assigned(5200, CellRange::all(), 2);
    f.reserve(std::slice::from_mut(&mut child)).await?;
    let valid = d.prepare_merge(&view, operation(5201), &parents, &child)?;
    ensure!(d.decode_mutation(valid.encode())?.encode() == valid.encode());
    for field in ["namespace", "cluster", "schema", "gap", "overlap", "order"] {
        let mut invalid = parents.clone();
        match field {
            "namespace" => invalid[1].history.namespace = NamespaceId::new([88; 16])?,
            "cluster" => invalid[1].history.cluster = ClusterId::new([88; 16])?,
            "schema" => invalid[1].schema_digest = [88; 32],
            "gap" => invalid[1].range = split(&parents[1], 5300)[1].range.clone(),
            "overlap" => invalid[1].range = CellRange::all(),
            "order" => invalid.reverse(),
            _ => unreachable!(),
        }
        ensure!(
            d.prepare_merge(&view, operation(5202), &invalid, &child)
                .is_err(),
            "accepted {field}"
        );
        ensure!(f.run(d.observe()).await?.digest() == view.digest());
        ensure!(f.run(d.active_transition()).await?.is_none());
    }
    Ok(())
}
