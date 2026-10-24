// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn visits() -> Vec<Visit> {
    let mut visits = Vec::new();
    for (caller, _, _) in fleet::CLASSES {
        for (_, _, class) in fleet::CLASSES {
            visits.push(Visit {
                node: "ingress".into(),
                class: None,
                scope: None,
                epoch: None,
                method: format!("ingress:{caller}"),
            });
            visits.push(Visit {
                node: "owner".into(),
                class: Some(class.into()),
                scope: Some(format!("{class}:alice")),
                epoch: Some(7),
                method: "example.counter.v1.Counter/Increment".into(),
            });
        }
    }
    visits
}

#[test]
fn remote_audit_requires_every_pair_and_the_actual_remote_domain() {
    let valid = visits();
    let (pairs, owners) = execution(&valid, "ingress", "owner").unwrap();
    assert_eq!(pairs.len(), 16);
    assert_eq!(owners.len(), 4);
    assert!(execution(&valid, "owner", "owner").is_err());
    let mut incomplete = valid.clone();
    incomplete.pop();
    assert!(execution(&incomplete, "ingress", "owner").is_err());
    let mut local = valid.clone();
    local[1].node = "ingress".into();
    assert!(execution(&local, "ingress", "owner").is_err());
    let mut forwarded_ingress = valid.clone();
    forwarded_ingress[0].node = "owner".into();
    assert!(execution(&forwarded_ingress, "ingress", "owner").is_err());
}

#[test]
fn remote_audit_refuses_cross_epoch_cross_scope_and_unregistered_execution() {
    for mutate in [
        |visit: &mut Visit| visit.epoch = Some(8),
        |visit: &mut Visit| visit.scope = Some("other-cell".into()),
        |visit: &mut Visit| visit.class = Some("Unregistered".into()),
        |visit: &mut Visit| visit.method = "example.counter.v1.Counter/PrivateHelper".into(),
    ] {
        let mut invalid = visits();
        mutate(&mut invalid[9]); // Same target already observed in the first row.
        assert!(execution(&invalid, "ingress", "owner").is_err());
    }
    let mut no_parent = visits();
    no_parent.remove(0);
    assert!(execution(&no_parent, "ingress", "owner").is_err());
}
