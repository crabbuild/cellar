// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Descriptive disk-codec fixtures confer no seed, membership or serving grant.
use super::*;
use crate::raft::{cache, manifest};
use cellar_logic::{
    cell_group::{CellRange, GroupRoute, RouteEpoch},
    consensus::MemberId,
};
use std::sync::Arc;

fn marker(route: GroupRoute, transfer: bool) -> GroupManifest {
    let mut value = serde_json::to_value(GroupManifest::genesis(route).unwrap()).unwrap();
    value["index"] = 5.into();
    value["term"] = 2.into();
    if transfer {
        value["transfer"] = serde_json::json!({
            "operation": vec![7u8; 16], "plan": vec![8u8; 32], "sealed_sources": vec![9u8; 32],
            "activation": {"publication": vec![10u8; 32], "manifest": vec![11u8; 32], "index": 2, "term": 2}
        });
    }
    serde_json::from_value(value).unwrap()
}

fn fixture(transfer: bool) -> (MappingRecord, CachedGroup) {
    let route = GroupRoute {
        history: GroupHistory {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            group: GroupId::new([3; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        },
        range: CellRange::all(),
        epoch: RouteEpoch::new(1).unwrap(),
        birth_digest: [5; 32],
        schema_digest: [6; 32],
    };
    let conf = ConfState {
        voters: vec![1, 2, 3],
        ..Default::default()
    };
    let bootstrap = CellGroupBootstrap {
        route: route.clone(),
        member: MemberId::new(4).unwrap(),
        voters: (1..=3).map(|id| MemberId::new(id).unwrap()).collect(),
        learners: vec![],
    };
    let mapping = MappingRecord::new_cells(1, &bootstrap, &conf).unwrap();
    let anchor = marker(route, transfer);
    let mut cache = cache::genesis(conf.clone());
    cache.group_manifest = Some(anchor.clone());
    cache.checkpoint_manifest = Some(anchor.clone());
    cache.group_seed = Some(5);
    cache.seed_anchor = Some(Arc::new(anchor.clone()));
    cache.applied.position = anchor.position();
    cache.hard_state.term = 2;
    cache.hard_state.commit = 5;
    cache.snapshot.mut_metadata().index = 5;
    cache.snapshot.mut_metadata().term = 2;
    cache.snapshot.data = serde_json::to_vec(&serde_json::json!({
        "domain": "cellar-cell-group-checkpoint-1", "root": anchor,
        "conf": manifest::encode_conf(&conf).unwrap(),
        "membership": cache.membership, "transition": "active"
    }))
    .unwrap()
    .into();
    cache.validate(10).unwrap();
    (mapping, cache)
}

#[test]
fn seed_lineage_roundtrips_and_cannot_be_replaced() {
    for transfer in [false, true] {
        let (mapping, cache) = fixture(transfer);
        assert!(mapping.validate_cache(&cache).is_err());
        let anchor = cache.seed_anchor.as_ref().unwrap();
        let seeded = mapping.with_seed_anchor(anchor).unwrap();
        let record = GroupRecord::from_cache(seeded.clone(), &cache).unwrap();
        let encoded = encode(&record).unwrap();
        let decoded: GroupRecord = decode(&encoded).unwrap();
        let reopened = decoded.metadata_cache().unwrap();
        reopened.validate(10).unwrap();
        assert_eq!(reopened.seed_anchor, cache.seed_anchor);
        assert_eq!(reopened.group_manifest, cache.group_manifest);
        assert!(seeded.with_seed_anchor(anchor).is_ok());
        let mut changed = serde_json::to_value(anchor.as_ref()).unwrap();
        changed["index"] = 6.into();
        let changed: GroupManifest = serde_json::from_value(changed).unwrap();
        assert!(seeded.with_seed_anchor(&changed).is_err());
        let mut foreign = anchor.route().clone();
        foreign.history.group = GroupId::new([12; 16]).unwrap();
        assert!(mapping
            .with_seed_anchor(&marker(foreign, transfer))
            .is_err());
    }
}

#[test]
fn seed_lineage_requires_original_receipt_and_provenance() {
    for transfer in [false, true] {
        let (mapping, mut cache) = fixture(transfer);
        let anchor = cache.seed_anchor.as_ref().unwrap().clone();
        let seeded = mapping.with_seed_anchor(&anchor).unwrap();
        cache.group_seed = Some(6);
        assert!(seeded.validate_cache(&cache).is_err());
        cache.group_seed = Some(5);
        let mut changed = serde_json::to_value(cache.group_manifest.as_ref().unwrap()).unwrap();
        if transfer {
            changed["transfer"]["operation"] = serde_json::json!(vec![13u8; 16]);
        } else {
            changed["transfer"] = serde_json::to_value(marker(anchor.route().clone(), true))
                .unwrap()["transfer"]
                .clone();
        }
        cache.group_manifest = Some(serde_json::from_value(changed).unwrap());
        assert!(seeded.validate_cache(&cache).is_err());
        assert!(cache.validate(10).is_err());
    }
}

#[test]
fn seeded_mapping_refuses_receipts_without_the_durable_anchor() {
    for transfer in [false, true] {
        let (mapping, cache) = fixture(transfer);
        let seeded = mapping
            .with_seed_anchor(cache.seed_anchor.as_ref().unwrap())
            .unwrap();
        let mut record = GroupRecord::from_cache(seeded, &cache).unwrap();
        record.mapping = mapping;
        let decoded: GroupRecord = decode(&encode(&record).unwrap()).unwrap();
        assert!(decoded.metadata_cache().is_err());
    }
}
