use super::*;
use serde_json::json;

fn resource() -> super::super::Resource {
    super::super::Resource {
        tenant: "tenant".into(),
        application: "app".into(),
        id: "a".repeat(32),
        name: "events".into(),
        family: "events".into(),
    }
}

fn completed() -> serde_json::Value {
    json!({"resource":resource().id,"operation":"b".repeat(32),"state":"deleted","last_error":null,"operator":null,"affected":{"first":"10","last":"11"},"records":"2","bytes":"200"})
}

#[test]
fn retirement_acceptance_requires_terminal_loss_accounting_and_keeps_pending_intent() -> Result<()>
{
    accept_status(&resource(), &"b".repeat(32), completed())?;
    accept_status(
        &resource(),
        &"b".repeat(32),
        json!({"resource":resource().id,"operation":"b".repeat(32),"state":"retiring","last_error":"offline"}),
    )?;
    for (field, value) in [
        ("records", json!("3")),
        ("bytes", json!("0")),
        ("state", json!("active")),
        ("operation", json!("c".repeat(32))),
        ("operator", json!("other")),
        ("last_error", json!("failed")),
    ] {
        let mut bad = completed();
        bad[field] = value;
        assert!(
            accept_status(&resource(), &"b".repeat(32), bad).is_err(),
            "accepted {field}"
        );
    }
    Ok(())
}

#[test]
fn retirement_rejects_invalid_ranges_and_noncanonical_counters() -> Result<()> {
    for (field, value) in [
        ("first", json!("0")),
        ("last", json!("9")),
        ("last", json!(i64::MAX.to_string())),
    ] {
        let mut bad = completed();
        bad["affected"][field] = value;
        assert!(accept_status(&resource(), &"b".repeat(32), bad).is_err());
    }
    let mut bad = completed();
    bad["records"] = json!("02");
    assert!(accept_status(&resource(), &"b".repeat(32), bad).is_err());
    let mut empty = completed();
    empty["records"] = json!("0");
    empty["bytes"] = json!("0");
    empty["affected"] = serde_json::Value::Null;
    accept_status(&resource(), &"b".repeat(32), empty.clone())?;
    empty["bytes"] = json!("1");
    assert!(accept_status(&resource(), &"b".repeat(32), empty).is_err());
    Ok(())
}

#[test]
fn pending_retirement_does_not_accept_partial_loss_metadata() {
    for (field, data) in [
        ("operator", json!(null)),
        ("affected", json!({"first":"1","last":"2"})),
        ("records", json!("2")),
        ("bytes", json!("200")),
    ] {
        let mut value = json!({"resource":resource().id,"operation":"b".repeat(32),"state":"retiring","last_error":null});
        value[field] = data;
        assert!(accept_status(&resource(), &"b".repeat(32), value).is_err());
    }
}
