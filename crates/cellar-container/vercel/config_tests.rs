// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn direct_profile_refuses_strict_fencing_and_pins_account_config_without_credentials() {
    assert!(Config::new(
        "team",
        "project",
        "fleet",
        "key",
        "image",
        Ownership::RequireProviderFence
    )
    .is_err());
    let config = Config::new(
        "team",
        "project",
        "fleet",
        "key",
        "image",
        Ownership::LocalChecksAndExpiry,
    )
    .unwrap();
    assert_eq!(
        config.digest(),
        Config::new(
            "team",
            "project",
            "fleet",
            "rotated-key",
            "image",
            Ownership::LocalChecksAndExpiry
        )
        .unwrap()
        .digest()
    );
    assert_ne!(
        config.digest(),
        Config::new(
            "other-team",
            "project",
            "fleet",
            "key",
            "image",
            Ownership::LocalChecksAndExpiry
        )
        .unwrap()
        .digest()
    );
    for endpoint in [
        "http://example.test/",
        "https://user:pass@example.test/",
        "https://example.test/?query=true",
        "https://example.test/base",
    ] {
        assert!(config.clone().api_endpoint(endpoint).is_err());
    }
    assert!(config
        .clone()
        .api_endpoint("http://127.0.0.1:1234/")
        .is_ok());
    assert!(config.clone().helper_python("python3").is_err());
    assert!(config.clone().vcpus(3).is_err());
    assert!(config.clone().lifetime_ms(0).is_err());
    assert!(config.clone().binding("../other").is_err());
    assert!(config.clone().region("../other").is_err());
    for id in [
        "../victim",
        "session?teamId=victim",
        "user@host",
        "has space",
    ] {
        assert!(identifier(id).is_err());
    }
    let url = Url::parse(&config.url("v2/sandboxes/sessions/sbx_1/cmd").unwrap()).unwrap();
    assert_eq!(url.host_str(), Some("api.vercel.com"));
    assert_eq!(url.query(), Some("teamId=team"));
}
