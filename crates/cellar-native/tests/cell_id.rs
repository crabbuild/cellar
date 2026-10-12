use cellar_native::cell_id::{perform, Namespace};
const KEY: &str = "cells:v1:4:demo:Account";
#[test]
fn stable_identity_retains_the_existing_hmac_address_format() {
    let ns = Namespace::new("Account", KEY);
    let named = ns.id_from_name("customer:α");
    assert_eq!(
        named.to_string(),
        "986cac9d62e8bbd541f00c98bf893a7cf3e1a1ff6a9fe3cb246286d1d53803d6"
    );
    let parsed = ns
        .id_from_string(&named.to_string().to_uppercase())
        .unwrap();
    assert!(named.equals(&parsed));
    assert_eq!(parsed.name(), None);
    assert_eq!(named.name().as_deref(), Some("customer:α"));
    assert_eq!(named.scope(), format!("Account:{named}"));
    assert_eq!(
        perform(KEY, "name", "customer:α").unwrap(),
        named.to_string()
    );
}
#[test]
fn names_preserve_utf16_and_hash_the_original_hosts_replacement_view() {
    let ns = Namespace::new("Account", KEY);
    let raw = [0xd800, 0, 0xdc00, 0xd83d, 0xde80];
    let id = ns.id_from_name_utf16(&raw);
    assert_eq!(id.name_utf16(), Some(raw.as_slice()));
    assert_eq!(id, ns.id_from_name(&String::from_utf16_lossy(&raw)));
    assert_ne!(ns.id_from_name("é"), ns.id_from_name("e\u{301}"));
    assert_ne!(ns.id_from_name(""), ns.id_from_name("\0"));
}
#[test]
fn authentication_rejects_forged_ids_and_foreign_namespaces() {
    let ns = Namespace::new("Account", KEY);
    let id = ns.id_from_name("entity");
    for index in [0, 31, 32, 63] {
        let mut tampered = id.to_string().into_bytes();
        tampered[index] = if tampered[index] == b'0' { b'1' } else { b'0' };
        let error = ns
            .id_from_string(&String::from_utf8(tampered).unwrap())
            .unwrap_err();
        assert_eq!(error.name(), "TypeError");
        assert_eq!(
            error.to_string(),
            "Durable Object ID is not valid for this namespace"
        );
    }
    for invalid in ["", "0", &"0".repeat(63), &"0".repeat(65), &"g".repeat(64)] {
        assert_eq!(
            ns.id_from_string(invalid).unwrap_err().to_string(),
            "Invalid Durable Object ID: must be 64 hex digits"
        );
    }
    assert!(Namespace::new("Account", "other-script")
        .id_from_string(&id.to_string())
        .is_err());
    assert!(Namespace::new("Other", KEY).accepts(&id).is_err());
    // get(existing_id) retains the original class-only acceptance rule;
    // id_from_string remains authenticated against this namespace's key.
    let borrowed = Namespace::new("Account", "foreign-key").id_from_name("entity");
    ns.accepts(&borrowed).unwrap();
    assert!(ns.id_from_string(&borrowed.to_string()).is_err());
}
#[test]
fn unique_ids_authenticate_and_do_not_invent_a_name_or_jurisdiction() {
    let ns = Namespace::new("Account", KEY);
    let a = ns.new_unique_id().unwrap();
    let b = ns.new_unique_id().unwrap();
    assert_ne!(a, b);
    assert_eq!(a.name(), None);
    assert_eq!(ns.id_from_string(&a.to_string()).unwrap(), a);
    ns.jurisdiction(None).unwrap();
    let error = ns.jurisdiction(Some("eu")).unwrap_err();
    assert_eq!(error.name(), "Error");
    assert_eq!(
        error.to_string(),
        "Jurisdiction restrictions are not implemented"
    );
    assert_eq!(
        perform(KEY, "unknown", "").unwrap_err().to_string(),
        "unknown Durable Object ID operation"
    );
}
#[test]
fn routing_name_budget_counts_utf8_bytes_without_truncating_identity() {
    let ns = Namespace::new("Account", KEY);
    for (name, retained) in [
        ("a".repeat(1024), true),
        ("a".repeat(1025), false),
        ("🚀".repeat(256), true),
        ("🚀".repeat(257), false),
    ] {
        let id = ns.id_from_name(&name);
        assert_eq!(id.name().as_deref(), Some(name.as_str()));
        assert_eq!(id.dispatch_name().is_some(), retained);
        assert_eq!(ns.id_from_string(&id.to_string()).unwrap(), id);
    }
}
