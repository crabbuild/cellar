// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Administrative metadata must independently match its operation and scope.
use super::super::Resource;
use super::*;

fn resource() -> Resource {
    Resource {
        tenant: "tenant".into(),
        application: "app".into(),
        id: "a".repeat(32),
        name: "events".into(),
        family: "events".into(),
    }
}

fn pause() -> LifecycleReceipt {
    let identity = SubscriptionIdentity {
        resource: resource().id,
        name: "worker".into(),
        id: "b".repeat(32),
        generation: "c".repeat(32),
    };
    LifecycleReceipt {
        operator: None,
        operation: "e".repeat(32),
        previous: identity.clone(),
        current: identity,
        action: ChangeAction::Pause,
        state: "paused".into(),
        checkpoint: 12,
        skipped: None,
        reason: "operator action".into(),
        at_ms: 100,
    }
}

fn reset() -> LifecycleReceipt {
    let mut receipt = pause();
    receipt.current.generation = "d".repeat(32);
    receipt.action = ChangeAction::Reset {
        cursor: Cursor {
            resource: resource().id,
            partition: 0,
            next: 20,
        },
    };
    receipt.state = "active".into();
    receipt.checkpoint = 19;
    receipt.skipped = Some(OffsetRange {
        first: 13,
        last: 19,
    });
    receipt
}

fn request(receipt: &LifecycleReceipt) -> Change {
    Change {
        operation: receipt.operation.clone(),
        expected: receipt.previous.clone(),
        action: receipt.action.clone(),
        reason: receipt.reason.clone(),
    }
}

fn page() -> AuditPage {
    let first = pause();
    let mut second = reset();
    second.operation = "f".repeat(32);
    AuditPage {
        records: vec![
            AuditRecord {
                sequence: 2,
                principal: "app".into(),
                grant: "admin".into(),
                receipt: first,
            },
            AuditRecord {
                sequence: 5,
                principal: "another-app".into(),
                grant: "management".into(),
                receipt: second,
            },
        ],
        next: 5,
        has_more: false,
    }
}

#[test]
fn lifecycle_acceptance_matches_action_reason_operator_and_full_identity() -> Result<()> {
    let receipt = reset();
    let change = request(&receipt);
    receipt.validate_acceptance(&change, None)?;
    for field in [
        "action",
        "reason",
        "operator",
        "operation",
        "previous",
        "current",
    ] {
        let mut bad = receipt.clone();
        match field {
            "action" => bad.action = ChangeAction::Resume,
            "reason" => bad.reason = "changed".into(),
            "operator" => bad.operator = Some("other".into()),
            "operation" => bad.operation = "f".repeat(32),
            "previous" => bad.previous.id = "f".repeat(32),
            _ => bad.current.id = "f".repeat(32),
        }
        assert!(
            bad.validate_acceptance(&change, None).is_err(),
            "accepted {field}"
        );
    }
    Ok(())
}

#[test]
fn pause_and_resume_never_replace_incarnation_or_claim_loss() -> Result<()> {
    let mut receipt = pause();
    receipt.validate()?;
    receipt.action = ChangeAction::Resume;
    receipt.state = "active".into();
    receipt.validate()?;
    let mut bad = receipt.clone();
    bad.current.generation = "d".repeat(32);
    assert!(bad.validate().is_err());
    let mut bad = receipt.clone();
    bad.skipped = Some(OffsetRange { first: 1, last: 12 });
    assert!(bad.validate().is_err());
    receipt.state = "deleted".into();
    assert!(receipt.validate().is_err());
    Ok(())
}

#[test]
fn reset_requires_new_incarnation_exact_target_and_bounded_skipped_range() -> Result<()> {
    let receipt = reset();
    receipt.validate()?;
    let mut bad = receipt.clone();
    bad.checkpoint = 20;
    assert!(bad.validate().is_err());
    let mut bad = receipt.clone();
    bad.current.generation = bad.previous.generation.clone();
    assert!(bad.validate().is_err());
    let mut bad = receipt.clone();
    bad.skipped.as_mut().unwrap().last = 18;
    assert!(bad.validate().is_err());
    let mut bad = receipt.clone();
    if let ChangeAction::Reset { cursor } = &mut bad.action {
        cursor.partition = 1;
    }
    assert!(bad.validate().is_err());
    let mut backwards = receipt;
    backwards.checkpoint = 4;
    backwards.skipped = None;
    if let ChangeAction::Reset { cursor } = &mut backwards.action {
        cursor.next = 5;
    }
    backwards.validate()?;
    Ok(())
}

#[test]
fn deletion_requires_invalidation_and_precise_skipped_start() -> Result<()> {
    let mut receipt = pause();
    receipt.action = ChangeAction::Delete { force: true };
    receipt.state = "deleted".into();
    receipt.current.generation = "d".repeat(32);
    receipt.skipped = Some(OffsetRange {
        first: 13,
        last: 40,
    });
    receipt.validate()?;
    let mut bad = receipt.clone();
    bad.skipped.as_mut().unwrap().first = 14;
    assert!(bad.validate().is_err());
    let mut bad = receipt.clone();
    bad.action = ChangeAction::Delete { force: false };
    assert!(bad.validate().is_err());
    receipt.skipped = None;
    receipt.action = ChangeAction::Delete { force: false };
    receipt.validate()?;
    Ok(())
}

#[test]
fn lifecycle_rejects_invalid_counter_time_reason_and_uuid() -> Result<()> {
    for (field, value) in [
        ("checkpoint", serde_json::json!("-1")),
        ("checkpoint", serde_json::json!(i64::MAX.to_string())),
        ("at_ms", serde_json::json!("-1")),
        ("operation", serde_json::json!("wrong")),
        ("reason", serde_json::json!(" ")),
    ] {
        let mut value_map = serde_json::to_value(pause())?;
        value_map[field] = value;
        let receipt: LifecycleReceipt = serde_json::from_value(value_map)?;
        let failure = receipt
            .validate()
            .expect_err("accepted malformed lifecycle metadata");
        assert!(failure
            .downcast_ref::<super::super::DeliveryError>()
            .is_some());
    }
    Ok(())
}

#[test]
fn audit_acceptance_preserves_gapped_sequence_and_other_authorized_principals() -> Result<()> {
    let mut page = page();
    page.validate(&resource(), 0, 2)?;
    page.records[0].sequence = i64::MAX - 1;
    page.records[1].sequence = i64::MAX;
    page.next = i64::MAX;
    page.validate(&resource(), 0, 2)?;
    let decoded: AuditPage = serde_json::from_value(serde_json::to_value(page.clone())?)?;
    assert_eq!(decoded.next, i64::MAX);
    Ok(())
}

#[test]
fn audit_rejects_count_order_scope_and_unmatched_continuations() {
    let original = page();
    assert!(original.validate(&resource(), 0, 1).is_err());
    let mut bad = original.clone();
    bad.records[1].sequence = 2;
    assert!(bad.validate(&resource(), 0, 2).is_err());
    let mut duplicated = original.clone();
    duplicated.records[1].receipt.operation = duplicated.records[0].receipt.operation.clone();
    assert!(duplicated.validate(&resource(), 0, 2).is_err());
    let mut bad = original.clone();
    bad.next = 6;
    assert!(bad.validate(&resource(), 0, 2).is_err());
    let mut bad = original.clone();
    bad.records[0].receipt.previous.resource = "f".repeat(32);
    bad.records[0].receipt.current.resource = "f".repeat(32);
    assert!(bad.validate(&resource(), 0, 2).is_err());
    let mut bad = original.clone();
    bad.records[1].principal = "".into();
    assert!(bad.validate(&resource(), 0, 2).is_err());
    assert!(original.validate(&resource(), 2, 2).is_err());
    let empty = AuditPage {
        records: vec![],
        next: 9,
        has_more: false,
    };
    empty.validate(&resource(), 9, 2).unwrap();
    let blocked = AuditPage {
        has_more: true,
        ..empty
    };
    assert!(blocked.validate(&resource(), 9, 2).is_err());
}

#[test]
fn audit_bounds_record_count_before_typed_receipt_decode() -> Result<()> {
    let failure = accept_audit(
        &resource(),
        0,
        2,
        serde_json::json!({"records":[null,null,null],"next":"3","has_more":false}),
    )
    .expect_err("accepted oversized audit page");
    assert_eq!(
        failure
            .downcast_ref::<super::super::DeliveryError>()
            .unwrap()
            .code,
        ErrorCode::PayloadTooLarge
    );
    let original = page();
    accept_audit(&resource(), 0, 2, serde_json::to_value(original)?)?;
    Ok(())
}
