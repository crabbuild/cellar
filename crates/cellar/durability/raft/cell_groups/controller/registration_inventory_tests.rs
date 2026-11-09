// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::consensus::{GroupId, IncarnationId};

fn history(id: u128, namespace: u8) -> GroupHistory {
    GroupHistory {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([namespace; 16]).unwrap(),
        group: GroupId::new(id.to_be_bytes()).unwrap(),
        incarnation: IncarnationId::new((id + 100_000).to_be_bytes()).unwrap(),
    }
}

#[test]
fn scoped_pages_are_complete_bounded_and_refuse_capacity_overflow() {
    let local = (1..=128).map(|id| history(id, 2)).collect::<Vec<_>>();
    let inventory = Inventory::new(
        local
            .iter()
            .copied()
            .chain((129..=640).map(|id| history(id, 3))),
    );
    let scope = (local[0].cluster, local[0].namespace);
    let scopes = [scope, scope];
    let revision = inventory.revision(&scopes).unwrap();
    let mut after = None;
    let mut complete = Vec::new();
    let mut pages = 0;
    loop {
        let page = inventory
            .page(&scopes, Some(revision), after, 128, 16)
            .unwrap();
        assert_eq!(page.revision, revision);
        assert!(!page.groups.is_empty() && page.groups.len() <= 16);
        complete.extend(page.groups);
        pages += 1;
        after = page.next;
        if after.is_none() {
            break;
        }
    }
    assert_eq!(pages, 8);
    assert_eq!(complete, local);
    for after in [None, Some(local[15])] {
        assert!(matches!(
            inventory.page(&scopes, Some(revision), after, 127, 16),
            Err(RuntimeError::Overloaded)
        ));
    }
    for limit in [0, 17, 129] {
        assert!(matches!(
            inventory.page(&scopes, None, None, 128, limit),
            Err(RuntimeError::Overloaded)
        ));
    }
}

#[test]
fn foreign_changes_do_not_invalidate_scoped_cursors_but_owner_changes_do() {
    let first = history(1, 2);
    let second = history(2, 2);
    let scopes = [(first.cluster, first.namespace)];
    let mut inventory = Inventory::new([first, second].into_iter());
    let page = inventory.page(&scopes, None, None, 2, 1).unwrap();
    let foreign = history(3, 3);
    inventory.insert(foreign);
    assert_eq!(inventory.revision(&scopes).unwrap(), page.revision);
    assert_eq!(
        inventory
            .page(&scopes, Some(page.revision), page.next, 2, 1)
            .unwrap()
            .groups,
        vec![second]
    );
    inventory.remove(&foreign);
    assert_eq!(inventory.revision(&scopes).unwrap(), page.revision);
    inventory.remove(&second);
    assert!(matches!(
        inventory.page(&scopes, Some(page.revision), page.next, 2, 1),
        Err(RuntimeError::NotReady)
    ));
    let current = inventory.revision(&scopes).unwrap();
    assert!(matches!(
        inventory.page(&scopes, Some(current), Some(foreign), 2, 1),
        Err(RuntimeError::NotReady)
    ));
    assert!(matches!(
        inventory.page(&scopes, Some(current), Some(second), 2, 1),
        Err(RuntimeError::NotReady)
    ));
}

#[test]
fn empty_scope_reactivation_and_generation_exhaustion_never_reuse_a_cursor() {
    let group = history(1, 2);
    let scopes = [(group.cluster, group.namespace)];
    let mut inventory = Inventory::new([group].into_iter());
    let before = inventory.revision(&scopes).unwrap();
    inventory.remove(&group);
    assert!(inventory.scopes.is_empty());
    let empty = inventory.revision(&scopes).unwrap();
    assert_ne!(before, empty);
    inventory.insert(group);
    assert_ne!(inventory.revision(&scopes).unwrap(), before);
    assert_ne!(inventory.revision(&scopes).unwrap(), empty);
    assert!(matches!(
        inventory.page(&scopes, Some(before), Some(group), 1, 1),
        Err(RuntimeError::NotReady)
    ));
    inventory.revision = u64::MAX;
    inventory.remove(&group);
    inventory.insert(group);
    assert!(matches!(
        inventory.revision(&scopes),
        Err(RuntimeError::Overloaded)
    ));
}
