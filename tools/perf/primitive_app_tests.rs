// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn all_public_bindings_validate_against_the_actual_raft_definitions() -> Result<()> {
    for family in ["sql", "kv", "queue"] {
        let options = options_for(family)?;
        for raft in [false, true] {
            let application = application_for(family, "test-token".into(), raft)?;
            application.validate_configuration(&options)?;
            if raft {
                assert_eq!(
                    application.resolve_raft_class(class(family))?.name(),
                    class(family)
                );
            }
        }
        assert_eq!(
            options.sql_bindings.len(),
            if family == "sql" { ENTITIES } else { 0 }
        );
        assert_eq!(
            options.kv_bindings.len(),
            if family == "kv" { ENTITIES } else { 0 }
        );
        assert_eq!(
            options.queue_bindings.len(),
            if family == "queue" { 2 * ENTITIES } else { 0 }
        );
        assert_eq!(
            options.queue_consumers.len(),
            if family == "queue" { ENTITIES } else { 0 }
        );
    }
    assert!(options_for("unknown").is_err());
    Ok(())
}

#[test]
fn queue_and_dead_letter_identities_pass_the_production_scope_gate() -> Result<()> {
    let options = options_for("queue")?;
    for binding in &options.queue_bindings {
        assert!(cellar_logic::cell::valid_cell_scope(&binding.queue));
    }
    for consumer in &options.queue_consumers {
        assert!(cellar_logic::cell::valid_cell_scope(&consumer.queue));
        let dead = consumer
            .dead_letter_queue
            .as_ref()
            .context("dead-letter queue")?;
        assert!(cellar_logic::cell::valid_cell_scope(dead));
        assert_ne!(dead, &consumer.queue);
    }
    Ok(())
}
