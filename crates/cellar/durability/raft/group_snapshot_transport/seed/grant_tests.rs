// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Local guard transition fixtures do not establish a sync or seed authority.
use super::*;
use crate::raft::fleet_stop::{LifecyclePurpose, LifecycleRecord, LifecycleUpdate, StopSelector};

fn boot() -> BootBinding {
    BootBinding {
        catalog: [1; 32],
        fleet: [2; 16],
        node: [3; 16],
        disk: [4; 16],
        engine: [5; 16],
        boot: [6; 16],
        process: [7; 16],
        epoch: 1,
        join_key: crate::peer_probe::PeerProbeSigner::from_secret([8; 32]).public_key_bytes(),
    }
}

#[test]
fn managed_seed_guard_requires_exact_open_boot() {
    let boot = boot();
    assert!(check_boot(None, &boot).is_err());
    let open = LifecycleRecord::empty()
        .change(LifecycleUpdate::Boot {
            boot: boot.clone(),
            deadline: 1000,
        })
        .unwrap();
    check_boot(open.guard.as_ref(), &boot).unwrap();
    for part in 0..3 {
        let mut changed = boot.clone();
        match part {
            0 => changed.process = [9; 16],
            1 => changed.engine = [10; 16],
            _ => changed.boot = [11; 16],
        }
        assert!(check_boot(open.guard.as_ref(), &changed).is_err());
    }
}

#[test]
fn managed_seed_guard_refuses_quiescence_and_fence() {
    let boot = boot();
    let open = LifecycleRecord::empty()
        .change(LifecycleUpdate::Boot {
            boot: boot.clone(),
            deadline: 1000,
        })
        .unwrap();
    let selector = StopSelector {
        purpose: LifecyclePurpose::Maintenance,
        boot: boot.clone(),
        epoch: 2,
        operation: [12; 16],
        finalization: [13; 16],
        request: [14; 16],
    };
    let quiescing = open
        .change(LifecycleUpdate::Quiesce {
            selector: selector.clone(),
            deadline: 1000,
        })
        .unwrap();
    assert!(check_boot(quiescing.guard.as_ref(), &boot).is_err());
    let fenced = quiescing
        .change(LifecycleUpdate::Fence {
            boot: boot.clone(),
            epoch: 2,
            operation: selector.operation,
            request: [15; 16],
            deadline: 1000,
        })
        .unwrap();
    assert!(check_boot(fenced.guard.as_ref(), &boot).is_err());
}
