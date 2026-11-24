// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::{
    cell_group::CellId,
    consensus::{ClusterId, IncarnationId, NamespaceId},
};

fn cell(name: &str) -> CellHistory {
    CellHistory {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: CellId::new(name).unwrap(),
        incarnation: IncarnationId::new([3; 16]).unwrap(),
    }
}

#[test]
fn colliding_lookup_tags_only_discard_advice() {
    let original = cell("__Queue:a");
    let mut cluster = original.clone();
    cluster.cluster = ClusterId::new([9; 16]).unwrap();
    let mut namespace = original.clone();
    namespace.namespace = NamespaceId::new([9; 16]).unwrap();
    let mut incarnation = original.clone();
    incarnation.incarnation = IncarnationId::new([9; 16]).unwrap();
    for (other, schema) in [
        (cell("__Queue:b"), [4; 32]),
        (cluster, [4; 32]),
        (namespace, [4; 32]),
        (incarnation, [4; 32]),
        (original.clone(), [9; 32]),
    ] {
        let hints = Hints::default();
        // Force a lookup collision; correctness cannot depend on its rarity.
        let tag = Target([7; 8]);
        hints.remember(tag, Hint::new(&original, [4; 32], 5, 100, 10).unwrap(), 10);
        assert_eq!(hints.take(tag).unwrap().sequence(&other, schema, 20), None);
        assert!(hints.take(tag).is_none());

        hints.remember(tag, Hint::new(&original, [4; 32], 5, 100, 10).unwrap(), 10);
        hints.remember(tag, Hint::new(&other, schema, 8, 100, 10).unwrap(), 10);
        assert_eq!(
            hints.take(tag).unwrap().sequence(&original, [4; 32], 20),
            None
        );
        assert!(hints.take(tag).is_none());
    }
}
