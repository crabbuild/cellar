// Copyright 2026 Deno Land Inc. Apache-2.0 license.
use cellar_native::web::{Headers, SearchParams, Url};
fn units(value: &str) -> Vec<u16> {
    value.encode_utf16().collect()
}
#[test]
fn headers_retain_wire_shape_cookie_splitting_and_copy_rules() {
    let h = Headers::from_pairs([
        ("X-Z", "one"),
        ("x-z", "two"),
        ("Set-Cookie", "a=1"),
        ("SET-cookie", "b=2"),
        ("__proto__", "safe"),
        ("constructor", "also"),
    ])
    .unwrap();
    assert_eq!(
        h.wire(),
        vec![
            ("X-Z".into(), "one".into()),
            ("X-Z".into(), "two".into()),
            ("Set-Cookie".into(), "a=1".into()),
            ("Set-Cookie".into(), "b=2".into()),
            ("__proto__".into(), "safe".into()),
            ("constructor".into(), "also".into())
        ]
    );
    assert_eq!(
        h.entries().collect::<Vec<_>>(),
        vec![
            ("__proto__".into(), "safe".into()),
            ("constructor".into(), "also".into()),
            ("set-cookie".into(), "a=1".into()),
            ("set-cookie".into(), "b=2".into()),
            ("x-z".into(), "one, two".into())
        ]
    );
    assert_eq!(h.get("SET-COOKIE").unwrap().as_deref(), Some("a=1, b=2"));
    assert_eq!(h.get_set_cookie(), ["a=1", "b=2"]);
    assert_eq!(h.request_wire().unwrap(), h.wire());
    assert_eq!(h.response_wire(), h.entries().collect::<Vec<_>>());
    let copied = h.copy_list();
    let constructed = Headers::from_headers(&h);
    assert_eq!(copied.wire(), h.wire());
    assert_eq!(constructed.wire(), h.entries().collect::<Vec<_>>());
    h.set("X-z", "three").unwrap();
    assert_eq!(h.wire()[0], ("X-Z".into(), "three".into()));
    assert_eq!(h.wire().len(), 5);
    assert_eq!(copied.wire().len(), 6);
    h.delete("set-cookie").unwrap();
    assert!(h.get_set_cookie().is_empty());
    assert_eq!(copied.get_set_cookie().len(), 2);
}
#[test]
fn headers_validate_http_tokens_values_utf16_and_guard_precedence() {
    let h = Headers::new();
    for byte in 0..128 {
        let name = (byte as u8 as char).to_string();
        let valid =
            (byte as u8).is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&(byte as u8));
        assert_eq!(h.append(&name, "v").is_ok(), valid, "{byte}");
    }
    assert!(h.append("雪", "v").is_err());
    assert!(h.append("", "v").is_err());
    assert!(!h.has("missing").unwrap());
    for value in ["a\0b", "a\nb", "a\rb"] {
        assert_eq!(
            h.append("ok", value).unwrap_err().to_string(),
            "Invalid header value"
        );
    }
    h.append("Whitespace", " \n\r\t a\t b \r\n").unwrap();
    assert_eq!(h.get("whitespace").unwrap().as_deref(), Some("a\t b"));
    h.append("Other", "\u{000b}\u{00a0}").unwrap();
    assert_eq!(h.get("other").unwrap().as_deref(), Some("\u{000b}\u{00a0}"));
    h.append_utf16(&units("Lone"), &[0xd800, 0xdc00, 0xd800, 0x20, 0xdc00])
        .unwrap();
    assert_eq!(
        h.get_utf16(&units("lone")).unwrap().unwrap(),
        [0xd800, 0xdc00, 0xd800, 0x20, 0xdc00]
    );
    let before = h.wire_utf16();
    assert_eq!(h.request_wire().unwrap_err().name(), "TypeError");
    assert!(h.response_wire().is_empty());
    assert_eq!(h.wire_utf16(), before);
    let error = h.has_utf16(&[0xd800]).unwrap_err();
    assert_eq!(error.name(), "TypeError");
    assert_eq!(error.message_utf16().last(), Some(&0xd800));
    h.freeze();
    for error in [
        h.append("bad name", "\0").unwrap_err(),
        h.set("bad name", "\0").unwrap_err(),
        h.delete("bad name").unwrap_err(),
    ] {
        assert_eq!(error.to_string(), "Can't modify immutable headers.");
    }
    assert!(h.copy_list().append("mutable", "yes").is_ok());
    assert!(!Headers::from_headers(&h).is_immutable());
}
#[test]
fn header_iteration_stays_live_after_mutation_and_even_after_done() {
    let h = Headers::from_pairs([("b", "2"), ("c", "3")]).unwrap();
    let mut it = h.entries();
    assert_eq!(it.next().unwrap().0, "b");
    h.append("a", "1").unwrap();
    assert_eq!(it.next().unwrap().0, "b");
    h.delete("a").unwrap();
    assert!(it.next().is_none());
    h.append("d", "4").unwrap();
    assert_eq!(it.next().unwrap().0, "d");
    let h = Headers::from_pairs([("a", "1"), ("b", "2")]).unwrap();
    let mut seen = Vec::new();
    h.for_each(|value, name, headers| {
        seen.push((name.clone(), value));
        if name == "a" {
            headers.append("c", "3").unwrap();
        }
    });
    assert_eq!(
        seen,
        vec![
            ("a".into(), "1".into()),
            ("b".into(), "2".into()),
            ("c".into(), "3".into())
        ]
    );
    let alias = h.clone();
    std::thread::spawn(move || alias.set("A", "thread").unwrap())
        .join()
        .unwrap();
    assert_eq!(h.get("a").unwrap().as_deref(), Some("thread"));
}
#[test]
fn url_parsing_keeps_normalization_base_rules_and_mutable_components() {
    let u = Url::new(
        "../雪?q=a b#",
        Some("https://User:pass@EXAMPLE.com:443/dir/page"),
    )
    .unwrap();
    assert_eq!(u.href(), "https://User:pass@example.com/%E9%9B%AA?q=a%20b#");
    assert_eq!(u.hash(), "");
    assert_eq!(u.origin(), "https://example.com");
    assert_eq!(
        Url::new("https://雪.example/", None).unwrap().hostname(),
        "xn--gp5a.example"
    );
    assert_eq!(
        Url::new("http://[::1]:8080/", None).unwrap().hostname(),
        "[::1]"
    );
    assert!(Url::can_parse("https://valid/", Some("bad base")));
    assert!(!Url::can_parse("relative", Some("bad base")));
    assert!(Url::parse("http://[bad]", None).is_none());
    assert_eq!(Url::new("relative", None).unwrap_err().name(), "TypeError");
    u.set_protocol("http");
    u.set_port("80");
    u.set_username("author");
    u.set_password("secret");
    u.set_hostname("Mixed.EXAMPLE");
    u.set_pathname("p\ta\nth\r x");
    u.set_search("?q='雪' &x=%20\t").unwrap();
    u.set_hash("raw value\n");
    assert_eq!(
        u.href(),
        "http://author:secret@Mixed.EXAMPLE/path x?q='%E9%9B%AA'%20&x=%20#raw value"
    );
    u.set_port("not a port");
    assert_eq!(u.port(), "");
    u.set_port("00080");
    assert_eq!(u.port(), "00080");
    u.set_host("other:80");
    assert_eq!(u.host(), "other");
    u.set_protocol("HTTPS");
    assert_eq!(u.origin(), "null");
    assert_eq!(
        Url::new("blob:https://EXAMPLE.com:443/id", None)
            .unwrap()
            .origin(),
        "https://example.com"
    );
    assert_eq!(
        Url::new("data:text/plain,x", None).unwrap().origin(),
        "null"
    );
    let u = Url::new("https://host/", None).unwrap();
    u.set_username_utf16(&[0xd800]);
    assert!(u.href_utf16().contains(&0xd800));
    let before = u.href_utf16();
    assert_eq!(
        u.set_search_utf16(&[0xd800]).unwrap_err().name(),
        "URIError"
    );
    assert_eq!(u.href_utf16(), before);
}
#[test]
fn search_params_decode_encode_usv_strings_and_stable_utf16_sort() {
    let p =
        SearchParams::from_query("?a=one+two&a=%FF%FE&bom=%EF%BB%BF&literal=%Q1%2&eq=a=b&&bare");
    assert_eq!(p.get_all("a"), ["one two", "��"]);
    assert_eq!(p.get("bom").as_deref(), Some("\u{feff}"));
    assert_eq!(p.get("literal").as_deref(), Some("%Q1%2"));
    assert_eq!(p.get("eq").as_deref(), Some("a=b"));
    assert_eq!(p.get("bare").as_deref(), Some(""));
    p.append(" !'()~*雪", " +&=").unwrap();
    assert!(p
        .serialize()
        .unwrap()
        .ends_with("+%21%27%28%29%7E*%E9%9B%AA=+%2B%26%3D"));
    let p = SearchParams::from_record_utf16([
        (vec![0xd800], units("first")),
        (vec![0xdc00], units("last")),
    ]);
    assert_eq!(p.size(), 1);
    assert_eq!(p.get("�").as_deref(), Some("last"));
    let p = SearchParams::from_pairs_utf16([
        (units("\u{e000}"), units("bmp")),
        (units("🐟"), units("astral")),
        (units("a"), units("first")),
        (units("a"), units("last")),
    ]);
    p.sort().unwrap();
    assert_eq!(p.keys().collect::<Vec<_>>(), ["a", "a", "🐟", "\u{e000}"]);
    assert_eq!(p.get_all("a"), ["first", "last"]);
    p.append_utf16(&units("lone"), &[0xd800]).unwrap();
    assert_eq!(p.get_utf16(&units("lone")).unwrap(), [0xd800]);
    assert_eq!(p.serialize().unwrap_err().name(), "URIError");
}
#[test]
fn url_search_lists_keep_identity_iterators_and_failed_sync_state() {
    let u = Url::new("https://host/path?a=a%20b&dup=1&dup=2#fragment", None).unwrap();
    let p = u.search_params();
    p.set("dup", "3").unwrap();
    assert_eq!(u.search(), "?a=a+b&dup=3");
    let mut it = p.entries();
    assert_eq!(it.next().unwrap().0, "a");
    u.set_search("?x=1&y=2").unwrap();
    assert_eq!(it.next().unwrap(), ("y".into(), "2".into()));
    assert!(it.next().is_none());
    p.append("z", "3").unwrap();
    assert!(
        it.next().is_none(),
        "the search iterator is a finished generator"
    );
    u.set_href("https://next/?n=1").unwrap();
    assert_eq!(p.get("n").as_deref(), Some("1"));
    p.delete("missing", None).unwrap();
    assert_eq!(u.href(), "https://next/?n=1");
    let before = u.href();
    assert_eq!(
        p.append_utf16(&units("bad"), &[0xd800]).unwrap_err().name(),
        "URIError"
    );
    assert_eq!(u.href(), before);
    assert_eq!(p.get_utf16(&units("bad")).unwrap(), [0xd800]);
    p.delete("bad", None).unwrap();
    assert_eq!(u.href(), before);
    let copied = u.copy();
    copied.search_params().append("copy", "yes").unwrap();
    assert!(!u.search_params().has("copy", None));
    let p = SearchParams::from_query("a=1&b=2");
    let mut seen = Vec::new();
    p.for_each(|value, name, params| {
        seen.push((name.clone(), value));
        if name == "a" {
            params.append("c", "3").unwrap();
        }
    });
    assert_eq!(seen.len(), 3);
}

#[test]
fn search_entry_aliases_keep_identity_and_detach_after_replacement() {
    let url = Url::new("https://host/?a=1&a=2", None).unwrap();
    let params = url.search_params();
    let mut entries = params.entry_handles();
    let first = entries.next().unwrap();
    let duplicate = entries.next().unwrap();
    first.set_value("edited");
    assert_eq!(params.get("a").as_deref(), Some("edited"));
    assert_eq!(
        url.search(),
        "?a=1&a=2",
        "entry writes do not run list synchronization"
    );
    params.set("a", "set").unwrap();
    assert_eq!(first.value(), "set");
    assert_eq!(duplicate.value(), "2");
    duplicate.set_value("detached");
    assert_eq!(params.size(), 1);
    first.set_name("renamed");
    assert!(params.has("renamed", None));
    params.sort().unwrap();
    assert_eq!(url.search(), "?renamed=set");
    url.set_search("?fresh=yes").unwrap();
    first.set_value("old");
    assert_eq!(params.get("fresh").as_deref(), Some("yes"));
    assert!(!params.has("renamed", None));
    drop(params);
    drop(url);
    assert_eq!(first.value(), "old");
}
