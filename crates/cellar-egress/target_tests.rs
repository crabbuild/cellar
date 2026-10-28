// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn destination<'a>(address: &str, host: Option<&'a Host>) -> Destination<'a> {
    Destination {
        socket: address.parse().unwrap(),
        host,
    }
}

#[test]
fn canonical_targets_round_trip_and_reject_ambiguous_inputs() -> Result<()> {
    for (input, canonical) in [
        ("API.Example.COM.", "api.example.com"),
        ("192.0.2.7/24", "192.0.2.0/24"),
        ("2001:db8::ffff/32", "2001:db8::/32"),
        ("[2001:db8::1]:8080", "[2001:db8::1]:8080"),
        ("192.0.2.7:80", "192.0.2.7"),
        ("[2001:db8::1]:80", "2001:db8::1"),
    ] {
        let target = HttpTarget::parse(input)?;
        assert_eq!(target.as_str(), canonical);
        assert_eq!(
            serde_json::from_str::<HttpTarget>(&serde_json::to_string(&target)?)?,
            target
        );
    }
    for value in [
        "",
        " api.example",
        "api.example ",
        "https://api.example/",
        "x/y/z",
        "127.1",
        "123",
        "192.0.2.1:0",
        "192.0.2.0/33",
        "::/129",
        "-bad.example",
        "a..example",
        "api.example:8080",
        "é.example",
        "<all-http>",
    ] {
        assert!(HttpTarget::parse(value).is_err(), "{value}");
    }
    assert!(Host::parse("*.example.com").is_err());
    assert_eq!(
        HttpTarget::parse("192.0.2.7")?,
        HttpTarget::parse("192.0.2.7:80")?
    );
    let longest = [
        "a".repeat(63),
        "b".repeat(63),
        "c".repeat(63),
        "d".repeat(61),
    ]
    .join(".");
    assert_eq!(HttpTarget::parse(&format!("{longest}."))?.as_str(), longest);
    assert_eq!(
        serde_json::from_str::<HttpTarget>(&serde_json::to_string(&HttpTarget::all())?)?,
        HttpTarget::all()
    );
    Ok(())
}

#[test]
fn hostname_globs_are_anchored_case_normalized_and_port_scoped() -> Result<()> {
    let rule = HttpTarget::parse("*.EXAMPLE.com.")?;
    for (name, expected) in [
        ("api.example.com", true),
        ("a.b.example.com", true),
        ("example.com", false),
        ("api.example.com.attacker", false),
        ("not-example.com", false),
    ] {
        let host = Host::parse(name)?;
        assert_eq!(
            rule.matches(&destination("192.0.2.1:80", Some(&host))),
            expected,
            "{name}"
        );
        assert_eq!(rule.resolves(&host, DnsFamily::V4), expected);
        assert_eq!(rule.resolves(&host, DnsFamily::V6), expected);
        assert!(!rule.matches(&destination("192.0.2.1:443", Some(&host))));
    }
    assert!(
        HttpTarget::parse("a*b*c.example")?.resolves(&Host::parse("axbyc.example")?, DnsFamily::V4)
    );
    assert!(!rule.matches(&destination("192.0.2.1:80", None)));
    Ok(())
}

#[test]
fn address_and_cidr_rules_use_socket_destination_and_preserve_dns_policy() -> Result<()> {
    let arbitrary = Host::parse("unregistered.example")?;
    let ipv4 = HttpTarget::parse("192.0.2.7")?;
    assert!(ipv4.matches(&destination("192.0.2.7:80", Some(&arbitrary))));
    assert!(!ipv4.matches(&destination("192.0.2.7:8080", Some(&arbitrary))));
    assert!(!ipv4.resolves(&arbitrary, DnsFamily::V4));
    let port = HttpTarget::parse("192.0.2.7:8080")?;
    assert!(port.matches(&destination("192.0.2.7:8080", None)));
    assert_eq!(port.port(), 8080);
    for (target, inside, outside) in [
        ("192.0.2.0/24", "192.0.2.255:80", "192.0.3.0:80"),
        ("2001:db8::/32", "[2001:db8:ffff::1]:80", "[2001:db9::1]:80"),
    ] {
        let rule = HttpTarget::parse(target)?;
        assert!(rule.matches(&destination(inside, None)));
        assert!(!rule.matches(&destination(outside, None)));
        assert!(!rule.resolves(&arbitrary, DnsFamily::V4));
    }
    let v4 = HttpTarget::parse("0.0.0.0/0")?;
    assert!(v4.resolves(&arbitrary, DnsFamily::V4));
    assert!(!v4.resolves(&arbitrary, DnsFamily::V6));
    assert!(!v4.matches(&destination("[2001:db8::1]:80", None)));
    let v6 = HttpTarget::parse("::/0")?;
    assert!(v6.resolves(&arbitrary, DnsFamily::V6));
    assert!(!v6.resolves(&arbitrary, DnsFamily::V4));
    assert!(!v6.matches(&destination("192.0.2.1:80", None)));
    Ok(())
}

#[test]
fn wildcard_matching_agrees_with_a_bounded_reference() {
    fn reference(pattern: &[u8], value: &[u8]) -> bool {
        match pattern.split_first() {
            None => value.is_empty(),
            Some((b'*', rest)) => {
                reference(rest, value) || (!value.is_empty() && reference(pattern, &value[1..]))
            }
            Some((first, rest)) => value.first() == Some(first) && reference(rest, &value[1..]),
        }
    }

    fn words(alphabet: &[u8], length: usize) -> Vec<Vec<u8>> {
        let mut all = vec![Vec::new()];
        let mut level = all.clone();
        for _ in 0..length {
            level = level
                .into_iter()
                .flat_map(|word| {
                    alphabet.iter().map(move |byte| {
                        let mut word = word.clone();
                        word.push(*byte);
                        word
                    })
                })
                .collect();
            all.extend(level.clone());
        }
        all
    }
    for pattern in words(b"ab*", 5) {
        for value in words(b"ab", 5) {
            assert_eq!(
                wildcard(&pattern, &value),
                reference(&pattern, &value),
                "{pattern:?} {value:?}"
            );
        }
    }
}
