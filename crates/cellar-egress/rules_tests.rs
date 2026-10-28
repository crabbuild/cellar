// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn install(rules: &mut Rules<&'static str>, target: HttpTarget, value: &'static str) -> Result<()> {
    let prepared = rules.prepare(target, value)?;
    rules.commit(prepared)
}

#[test]
fn registration_ack_controls_visibility_and_replacement_retains_request_handles() -> Result<()> {
    let mut rules = Rules::default();
    let host = Host::parse("api.example")?;
    let destination = Destination {
        protocol: crate::Protocol::Http,
        socket: "192.0.2.1:80".parse()?,
        host: Some(&host),
    };
    let prepared = rules.prepare(HttpTarget::parse("API.EXAMPLE")?, "first")?;
    assert!(rules.resolve(&destination).is_none());
    assert!(!rules.resolves(&host, DnsFamily::V4));
    rules.commit(prepared)?;
    let first = rules.resolve(&destination).unwrap();
    let replacement = rules.prepare(HttpTarget::parse("api.example.")?, "replacement")?;
    assert_eq!(rules.resolve(&destination).as_deref(), Some(&"first"));
    rules.commit(replacement)?;
    assert_eq!(rules.resolve(&destination).as_deref(), Some(&"replacement"));
    assert_eq!(*first, "first");
    assert_eq!(rules.entry_count(), 2);
    Ok(())
}

#[test]
fn pending_updates_cannot_overwrite_another_commit_or_revive_retired_rules() -> Result<()> {
    let mut rules = Rules::default();
    let first = rules.prepare(HttpTarget::all(), "first")?;
    let obsolete = rules.prepare(HttpTarget::all(), "obsolete")?;
    rules.commit(first)?;
    assert!(rules.commit(obsolete).is_err());
    let obsolete = rules.prepare(HttpTarget::all(), "obsolete")?;
    rules.retire()?;
    assert_eq!(rules.entry_count(), 0);
    assert!(rules.commit(obsolete).is_err());
    let host = Host::parse("api.example")?;
    assert!(!rules.resolves(&host, DnsFamily::V4));
    assert!(rules
        .resolve(&Destination {
            protocol: crate::Protocol::Http,
            socket: "192.0.2.1:80".parse()?,
            host: Some(&host)
        })
        .is_none());
    Ok(())
}

#[test]
fn prepared_routes_cannot_cross_registries_and_exhaustion_retires_admission() -> Result<()> {
    let mut first = Rules::default();
    let mut other = Rules::default();
    let prepared = first.prepare(HttpTarget::all(), "foreign")?;
    assert!(other.commit(prepared).is_err());
    assert_eq!(other.entry_count(), 0);
    first.revision = u64::MAX - 1;
    let prepared = first.prepare(HttpTarget::all(), "last")?;
    first.commit(prepared)?;
    assert_eq!(first.entry_count(), 2);
    assert!(first.retire().is_err());
    assert_eq!(first.entry_count(), 0);
    assert!(first.prepare(HttpTarget::all(), "revived").is_err());
    Ok(())
}

#[test]
fn shared_entry_budget_rejects_new_targets_without_disturbing_existing_rules() -> Result<()> {
    let mut rules = Rules::default();
    for index in 0..64 {
        install(
            &mut rules,
            HttpTarget::parse(&format!("host-{index}.example"))?,
            "registered",
        )?;
    }
    assert_eq!(rules.entry_count(), 128);
    let revision = rules.revision();
    assert!(rules.prepare(HttpTarget::all(), "overflow").is_err());
    assert!(rules
        .prepare(HttpTarget::parse("192.0.2.1")?, "overflow")
        .is_err());
    assert_eq!(rules.revision(), revision);
    install(
        &mut rules,
        HttpTarget::parse("HOST-0.EXAMPLE.")?,
        "replaced",
    )?;
    assert_eq!(rules.entry_count(), 128);
    rules.retire()?;
    for number in 1..=128 {
        install(
            &mut rules,
            HttpTarget::parse(&format!("192.0.2.{number}:8080"))?,
            "ip",
        )?;
    }
    assert_eq!(rules.entry_count(), 128);
    assert!(rules
        .prepare(HttpTarget::parse("extra.example")?, "overflow")
        .is_err());
    Ok(())
}

#[test]
fn catch_all_obeys_registration_order_and_replacement_does_not_reorder() -> Result<()> {
    let mut rules = Rules::default();
    install(&mut rules, HttpTarget::parse("early.example")?, "early")?;
    install(&mut rules, HttpTarget::all(), "catch-all")?;
    install(&mut rules, HttpTarget::parse("late.example")?, "late")?;
    install(&mut rules, HttpTarget::all(), "new catch-all")?;
    for (name, expected) in [
        ("early.example", "early"),
        ("late.example", "new catch-all"),
        ("unknown.example", "new catch-all"),
    ] {
        let host = Host::parse(name)?;
        assert_eq!(
            rules
                .resolve(&Destination {
                    protocol: crate::Protocol::Http,
                    socket: "192.0.2.1:80".parse()?,
                    host: Some(&host)
                })
                .as_deref(),
            Some(&expected)
        );
    }
    assert_eq!(rules.entry_count(), 6);
    Ok(())
}
