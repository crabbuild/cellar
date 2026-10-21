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
        business: empty,
        control: empty,
        archive_capture: empty,
        query_pins: RaftQueryPinUsage {
            in_use: 0,
            limit: 1,
        },
        sql: None,
        transport: None,
    };
    let text = usage.prometheus();
    assert!(text.contains("cellar_raft_sql_sample_available 0\n"));
    assert!(text.contains("cellar_raft_transport_sample_available 0\n"));
    assert!(!text.contains("cellar_raft_sql_running_workers"));
    assert!(!text.contains("cellar_raft_transport_budget_bytes"));
    assert!(serde_json::to_value(usage).unwrap()["transport"].is_null());
}
