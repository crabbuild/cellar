// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn direct_profile_requires_explicit_weaker_guarantee_and_host_selected_https() {
    assert!(Config::new(
        "team",
        "fleet",
        "test-key",
        "template",
        Ownership::RequireProviderFence
    )
    .is_err());
    let config = Config::new(
        "team",
        "fleet",
        "test-key",
        "template",
        Ownership::LocalChecksAndExpiry,
    )
    .unwrap();
    for url in [
        "http://example.test/",
        "https://user:pass@example.test/",
        "https://example.test/?secret=value",
        "https://example.test/base",
    ] {
        assert!(config.clone().api_endpoint(url).is_err(), "{url}");
    }
    assert!(config
        .clone()
        .api_endpoint("http://127.0.0.1:1234/")
        .is_ok());
    assert!(config.clone().api_endpoint("http://[::1]:1234/").is_ok());
    assert!(config.clone().lifetime_seconds(0).is_err());
    assert!(config.clone().lifetime_seconds(3601).is_err());
    assert!(config.clone().sandbox_domain("attacker/path").is_err());
    assert!(config
        .envd_url("box-1", "process.Process/Start")
        .unwrap()
        .starts_with("https://49983-box-1.e2b.app/"));
    for id in ["../victim", "box.evil", "UPPER", "-invalid"] {
        assert!(resource_id(id).is_err());
    }
}
