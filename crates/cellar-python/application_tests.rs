use super::*;

#[test]
fn initial_reads_require_registered_events_unique_keys_and_bounded_count() {
    let source = Source::new("x=1", "app.py").cell("Counter");
    assert_eq!(
        source.descriptor(),
        json!({"events":[],"cells":{"Counter":["fetch"]}})
    );
    for invalid in [
        source
            .clone()
            .cell_initial_reads("Unknown", EventKind::Fetch, ["a"]),
        source
            .clone()
            .cell_initial_reads("Counter", EventKind::Alarm, ["a"]),
        source
            .clone()
            .cell_initial_reads("Counter", EventKind::Fetch, ["a", "a"]),
        source
            .clone()
            .cell_initial_reads("Counter", EventKind::Fetch, ["a", "b", "c"]),
    ] {
        assert!(invalid.validate_initial_reads(2).is_err());
    }
    let declared = source.cell_initial_reads("Counter", EventKind::Fetch, ["", "__sys_é"]);
    declared.validate_initial_reads(2).unwrap();
    assert_eq!(
        declared.descriptor()["initial_reads"],
        json!({"Counter":{"fetch":["", "__sys_é"]}})
    );
    assert!(declared.validate_initial_reads(1).is_err());
}
