// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Crash the shared physical journal while it publishes three group roots.

use super::*;
use anyhow::{bail, Context};
use cellar::application::consensus::verification::{
    CellGroupBootstrap, GroupWrite, LogOpenMode, ReplicaLog,
};
use cellar_logic::consensus::OperationId;
use cellar_ltx::FileSystem;
use raft::{GetEntriesContext, Storage};
use std::path::Path;

const GROUPS: [u8; 3] = [10, 11, 12];
const BASE: u64 = 2;
const ADVANCED: u64 = 3;

fn group_route(id: u8) -> GroupRoute {
    let mut route = route();
    route.history.group = GroupId::new([id; 16]).unwrap();
    route.history.incarnation = IncarnationId::new([id + 10; 16]).unwrap();
    route.birth_digest = [id + 20; 32];
    route
}

fn payload(id: u8, index: u64) -> Vec<u8> {
    vec![id; index as usize * 31]
}

async fn open(
    root: &Path,
    mode: LogOpenMode,
    process: u8,
    filesystem: Arc<dyn FileSystem>,
) -> Result<(ExecutionDomain, ReplicaLog)> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = domain
        .run(ReplicaLog::open(
            engine_options(&root.join("node"), mode, process),
            filesystem,
        ))
        .await??;
    Ok((domain, log))
}

fn prepare(root: &Path, log: &ReplicaLog, index: u64) -> Result<Vec<GroupWrite<GroupHistory>>> {
    let mut writes = Vec::new();
    for id in GROUPS {
        let route = group_route(id);
        let group = log.cell_group(&route.history)?;
        let previous = group.group_manifest()?.context("previous group manifest")?;
        assert_eq!(previous.route(), &route);
        assert_eq!(previous.position(), position(index - 1));
        let mut pages = ManifestPageStore::open(
            &root.join(format!("pages-{id}")),
            Arc::new(DirectFileSystem),
            32,
            1024 * 1024,
            16,
        )?;
        let tree = ManifestTree::new(route, ManifestLimits::default())?;
        let prepared = tree.prepare_complete(
            &group,
            &mut pages,
            previous.root(),
            position(index),
            previous.membership_revision(),
            |_| unreachable!("empty storage fixture"),
        )?;
        let mut write = root_write(&group, prepared)?;
        write.entries[0].data = payload(id, index).into();
        writes.push(write);
    }
    Ok(writes)
}

async fn persist(
    log: &ReplicaLog,
    index: u64,
    writes: Vec<GroupWrite<GroupHistory>>,
) -> Result<()> {
    let expected: Vec<_> = writes
        .iter()
        .map(|write| write.group.identity().clone())
        .collect();
    let operation = OperationId::new(index);
    let receipt = log.persist_cells(operation, writes).await?;
    assert_eq!(receipt.operation, operation);
    assert_eq!(receipt.groups, expected);
    assert!(receipt.sequence > 0);
    Ok(())
}

pub(super) async fn run() -> Result<()> {
    for cut in ["before-sync", "after-sync", "acknowledged"] {
        let temp = tempfile::tempdir()?;
        let root = temp.path();
        let (domain, log) = open(root, LogOpenMode::Create, 1, Arc::new(DirectFileSystem)).await?;
        let local = MemberId::new(1)?;
        for id in GROUPS {
            log.create_cell_group(CellGroupBootstrap {
                route: group_route(id),
                member: local,
                voters: vec![local],
                learners: vec![],
            })
            .await?;
        }
        persist(&log, BASE, prepare(root, &log, BASE)?).await?;
        log.close().await?;
        domain.shutdown().await;

        crash_fixture::kill_at(root, "batch_crash_child", false, cut).await?;
        let (domain, log) = open(root, LogOpenMode::Resume, 3, Arc::new(DirectFileSystem)).await?;
        assert_eq!(log.cell_groups()?.len(), GROUPS.len());
        let first = log.cell_group(&group_route(GROUPS[0]).history)?;
        let committed = first.initial_state()?.hard_state.commit;
        if cut == "before-sync" {
            assert!([BASE, ADVANCED].contains(&committed));
        } else {
            assert_eq!(committed, ADVANCED);
        }
        for id in GROUPS {
            let route = group_route(id);
            let group = log.cell_group(&route.history)?;
            assert_eq!(
                group.initial_state()?.hard_state.commit,
                committed,
                "partial batch at {cut}"
            );
            assert_eq!(group.last_index()?, committed);
            let marker = group.group_manifest()?.context("reopened group manifest")?;
            assert_eq!(marker.route(), &route);
            assert_eq!(
                marker.position(),
                position(committed),
                "partial applied batch at {cut}"
            );
            assert_eq!(marker.membership_revision(), 1);
            assert_eq!(marker.root().cells(), 0);
            let entries =
                group.entries(BASE, committed + 1, None, GetEntriesContext::empty(false))?;
            assert_eq!(entries.len() as u64, committed - BASE + 1);
            for (index, entry) in (BASE..=committed).zip(entries) {
                assert_eq!((entry.index, entry.term), (index, 1));
                assert_eq!(&entry.data[..], &payload(id, index));
            }
        }
        log.close().await?;
        domain.shutdown().await;
    }
    Ok(())
}

pub(super) async fn child() -> Result<()> {
    let (root, _, cut) = crash_fixture::parameters()?;
    let gate = engine_fault::Gate::new(Some(root.join("signal")));
    let (_domain, log) = open(
        &root,
        LogOpenMode::Resume,
        2,
        Arc::new(engine_fault::FaultFileSystem { gate: gate.clone() }),
    )
    .await?;
    let writes = prepare(&root, &log, ADVANCED)?;
    let gate_mode = match cut.as_str() {
        "before-sync" => engine_fault::CRASH_BEFORE_SYNC,
        "after-sync" => engine_fault::CRASH_AFTER_SYNC,
        "acknowledged" => {
            persist(&log, ADVANCED, writes).await?;
            return crash_fixture::stop_at(&root).await;
        }
        _ => bail!("unknown batch crash cut"),
    };
    let _release = gate.arm(gate_mode);
    persist(&log, ADVANCED, writes).await?;
    bail!("batch persistence returned without entering its sync gate")
}
