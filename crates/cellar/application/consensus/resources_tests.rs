// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn transport_resource_counters_preserve_full_integer_precision() {
    let empty = RaftBudgetUsage {
        bytes: 0,
        items: 0,
        max_bytes: 0,
        max_items: 0,
    };
    let banks = RaftTransportBanks {
        control: empty,
        data: empty,
        query: empty,
    };
    let usage = RaftTransportResources {
        incoming: banks,
        outgoing: banks,
        incoming_connections: RaftConnectionReservations {
            control: 0,
            data: 0,
            query: 0,
        },
        outgoing_connections: RaftConnectionReservations {
            control: 0,
            data: 0,
            query: 0,
        },
        incoming_connection_limits: RaftConnectionReservations {
            control: 128,
            data: 128,
            query: 128,
        },
        snapshot_source: empty,
        snapshot_receive: empty,
        seed_metadata: empty,
        inspection_metadata: empty,
        tasks: 0,
        peers: 0,
        member_routes: 0,
        router_routes: 0,
        accepted: u64::MAX,
        refused: (1u64 << 53) + 1,
        failed: 0,
    };
    let json = serde_json::to_value(usage).unwrap();
    assert_eq!(json["accepted"], u64::MAX.to_string());
    assert_eq!(json["refused"], "9007199254740993");
}

#[test]
fn unavailable_resource_sources_do_not_emit_zero_budgets() {
    let empty = RaftBudgetUsage {
        bytes: 0,
        items: 0,
        max_bytes: 4096,
        max_items: 1,
    };
    let usage = RaftResourceUsage {
        extents: None,
        business: empty,
        control: empty,
        archive_capture: empty,
        query_pins: RaftQueryPinUsage {
            in_use: 0,
            limit: 1,
        },
        sql: None,
        images: None,
        groups: None,
        controller: None,
        transport: None,
    };
    let text = usage.prometheus();
    assert!(text.contains("cellar_raft_sql_sample_available 0\n"));
    assert!(text.contains("cellar_raft_images_sample_available 0\n"));
    assert!(text.contains("cellar_raft_extents_sample_available 0\n"));
    assert!(!text.contains("cellar_raft_extents_bytes"));
    assert!(text.contains("cellar_raft_groups_sample_available 0\n"));
    assert!(!text.contains("cellar_raft_groups_leaders"));
    assert!(!text.contains("cellar_raft_images_bytes"));
    assert!(text.contains("cellar_raft_transport_sample_available 0\n"));
    assert!(!text.contains("cellar_raft_sql_running_workers"));
    assert!(!text.contains("cellar_raft_transport_budget_bytes"));
    assert!(serde_json::to_value(usage).unwrap()["transport"].is_null());
}

#[test]
fn image_group_and_controller_observations_preserve_integer_precision() {
    let images = RaftImageUsage {
        stores: 1,
        generations: 1,
        bytes: (1u64 << 53) + 1,
        retained: 1,
        busy: 0,
        max_generations: 64,
        max_bytes: u64::MAX,
    };
    let json = serde_json::to_value(images).unwrap();
    assert_eq!(json["bytes"], "9007199254740993");
    assert_eq!(json["max_bytes"], u64::MAX.to_string());
    let groups = RaftGroupResources {
        apply_lag_entries: u64::MAX,
        sample_age_ms: (1u64 << 53) + 1,
        ..Default::default()
    };
    let json = serde_json::to_value(groups).unwrap();
    assert_eq!(json["apply_lag_entries"], u64::MAX.to_string());
    assert_eq!(json["sample_age_ms"], "9007199254740993");
    let controller = RaftControllerResources {
        enabled: true,
        recovering: false,
        busy: false,
        metadata: RaftBudgetUsage {
            bytes: 0,
            items: 0,
            max_bytes: 4096,
            max_items: 1,
        },
        probe_rounds: u64::MAX,
        repair_requests: 0,
        activation_requests: 0,
        transfer_attempts: (1u64 << 53) + 1,
        completed_repairs: 0,
        sample_age_ms: 1,
    };
    let json = serde_json::to_value(controller).unwrap();
    assert_eq!(json["probe_rounds"], u64::MAX.to_string());
    assert_eq!(json["transfer_attempts"], "9007199254740993");
}
