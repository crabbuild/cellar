// Copyright 2026 Deno Land Inc. Apache-2.0 license.
use cellar_native::web::{
    UrlPattern, UrlPatternInit, UrlPatternInitUtf16, UrlPatternInput, UrlPatternOptions,
};
use serde_json::json;

#[test]
fn pattern_routes_canonical_urls_with_named_repeated_and_optional_groups() {
    let pattern = UrlPattern::new(
        UrlPatternInit {
            protocol: Some("https".into()),
            hostname: Some(":tenant.example.com".into()),
            pathname: Some("/books/:id(\\d+){/:chapters}*".into()),
            search: Some("q=:query".into()),
            hash: Some("*".into()),
            ..Default::default()
        },
        None,
        UrlPatternOptions::default(),
    )
    .unwrap();
    assert!(pattern.has_regexp_groups());
    assert_eq!(pattern.hostname(), ":tenant.example.com");
    let result = pattern
        .exec(
            "https://SHOP.example.com:443/books/42/one/two?q=hello%20world#frag",
            None,
        )
        .unwrap()
        .unwrap();
    assert_eq!(result.hostname.input, "shop.example.com");
    assert_eq!(result.hostname.groups["tenant"].as_deref(), Some("shop"));
    assert_eq!(result.pathname.groups["id"].as_deref(), Some("42"));
    assert_eq!(
        result.pathname.groups["chapters"].as_deref(),
        Some("one/two")
    );
    assert_eq!(
        result.search.groups["query"].as_deref(),
        Some("hello%20world")
    );
    assert_eq!(result.hash.groups["0"].as_deref(), Some("frag"));
    assert!(!pattern
        .test("https://shop.example.com/books/word?q=x", None)
        .unwrap());
    assert!(!pattern
        .test("http://shop.example.com/books/42?q=x", None)
        .unwrap());
    let result = pattern
        .exec("https://shop.example.com/books/42?q=x", None)
        .unwrap()
        .unwrap();
    assert!(result.pathname.groups.contains_key("chapters"));
    assert!(result.pathname.groups["chapters"].is_none());
    assert_eq!(
        serde_json::to_value(result).unwrap()["pathname"]["groups"],
        json!({"id":"42"})
    );
}

#[test]
fn patterns_retain_dictionary_base_rules_input_identity_and_non_matches() {
    let relative = UrlPattern::new(
        "../:name",
        Some("https://example.com/dir/page"),
        UrlPatternOptions::default(),
    )
    .unwrap();
    assert_eq!(relative.pathname(), "//:name");
    let pattern = UrlPattern::new(
        "/:name",
        Some("https://example.com/dir/page"),
        UrlPatternOptions::default(),
    )
    .unwrap();
    assert_eq!(pattern.pathname(), "/:name");
    let result = pattern
        .exec("../雪", Some("https://example.com/dir/page"))
        .unwrap()
        .unwrap();
    assert_eq!(
        result.inputs,
        vec![
            UrlPatternInput::from("../雪"),
            UrlPatternInput::from("https://example.com/dir/page")
        ]
    );
    assert_eq!(result.pathname.groups["name"].as_deref(), Some("%E9%9B%AA"));
    assert!(pattern.exec("bad", Some("invalid base")).unwrap().is_none());
    assert!(!pattern.test("http://[invalid]", None).unwrap());
    assert!(UrlPattern::new("relative", None, UrlPatternOptions::default()).is_err());
    let init = UrlPatternInit {
        pathname: Some("/:id".into()),
        ..Default::default()
    };
    let error = UrlPattern::new(
        init.clone(),
        Some("https://example.com"),
        UrlPatternOptions::default(),
    )
    .unwrap_err();
    assert_eq!(error.name(), "TypeError");
    let pattern = UrlPattern::new(init.clone(), None, UrlPatternOptions::default()).unwrap();
    assert_eq!(
        pattern.exec(init, Some("https://example.com")).unwrap_err(),
        error
    );
    let input = UrlPatternInit {
        pathname: Some("/item".into()),
        ..Default::default()
    };
    let result = pattern.exec(input.clone(), None).unwrap().unwrap();
    assert_eq!(result.inputs, vec![UrlPatternInput::Init(input)]);
    assert_eq!(result.protocol.input, "");
    assert_eq!(result.protocol.groups["0"].as_deref(), Some(""));
    assert_eq!(result.pathname.groups["id"].as_deref(), Some("item"));
}

#[test]
fn patterns_keep_usv_conversion_case_flags_and_shared_compiled_state() {
    let init = UrlPatternInitUtf16 {
        pathname: Some("/雪/".encode_utf16().chain([0xd800]).collect()),
        ..Default::default()
    };
    let pattern = UrlPattern::new(init, None, UrlPatternOptions::default()).unwrap();
    assert_eq!(pattern.pathname(), "/%E9%9B%AA/%EF%BF%BD");
    assert!(pattern
        .test(
            UrlPatternInitUtf16 {
                pathname: Some("/雪/".encode_utf16().chain([0xdc00]).collect()),
                ..Default::default()
            },
            None
        )
        .unwrap());
    let exact = UrlPattern::new(
        UrlPatternInit {
            pathname: Some("/Book/:id".into()),
            ..Default::default()
        },
        None,
        UrlPatternOptions::default(),
    )
    .unwrap();
    assert!(!exact.test("https://example.com/book/42", None).unwrap());
    let folded = UrlPattern::new(
        UrlPatternInit {
            pathname: Some("/Book/:id".into()),
            ..Default::default()
        },
        None,
        UrlPatternOptions { ignore_case: true },
    )
    .unwrap();
    assert!(folded.test("https://example.com/book/42", None).unwrap());
    let alias = folded.clone();
    let mut result = std::thread::spawn(move || {
        alias
            .exec("https://example.com/BOOK/42", None)
            .unwrap()
            .unwrap()
    })
    .join()
    .unwrap();
    result
        .pathname
        .groups
        .insert("id".into(), Some("changed".into()));
    assert_eq!(
        folded
            .exec("https://example.com/book/42", None)
            .unwrap()
            .unwrap()
            .pathname
            .groups["id"]
            .as_deref(),
        Some("42")
    );
    let proto = UrlPattern::new(
        UrlPatternInit {
            pathname: Some("/:__proto__/:constructor".into()),
            ..Default::default()
        },
        None,
        UrlPatternOptions::default(),
    )
    .unwrap();
    let result = proto
        .exec("https://example.com/a/b", None)
        .unwrap()
        .unwrap();
    assert!(!result.pathname.groups.contains_key("__proto__"));
    assert_eq!(result.pathname.groups["constructor"].as_deref(), Some("b"));
}
