// Copyright 2026 Deno Land Inc. Apache-2.0 license.
use cellar_native::web::{
    Blob, BlobOptions, BlobPart, BodyInit, File, FileOptions, FormData, FormValue, SearchParams,
};
fn units(value: &str) -> Vec<u16> {
    value.encode_utf16().collect()
}
fn text(value: FormValue) -> Vec<u16> {
    let FormValue::Text(text) = value else {
        panic!("text required")
    };
    text
}
fn file(value: FormValue) -> File {
    let FormValue::File(file) = value else {
        panic!("file required")
    };
    file
}
#[test]
fn blobs_snapshot_binary_views_utf16_and_retained_slice_rules() {
    let mut bytes = [9, 0, 255, 8];
    let nested = Blob::from_bytes(b"nested", "").unwrap();
    let blob = Blob::new(
        [
            BlobPart::Bytes(&bytes[1..3]),
            BlobPart::Utf16(&[0xd800, 0x96ea]),
            BlobPart::Blob(&nested),
        ],
        BlobOptions::new("TEXT/PLAIN"),
    )
    .unwrap();
    bytes.fill(7);
    assert_eq!(
        blob.bytes(),
        [vec![0, 255], "�雪nested".as_bytes().to_vec()].concat()
    );
    assert_eq!(blob.content_type(), "text/plain");
    let mut returned = blob.bytes();
    returned.fill(1);
    assert_eq!(blob.as_bytes()[0], 0);
    let mut stream = blob.stream();
    assert_eq!(stream.next(), Some(blob.bytes()));
    assert!(stream.next().is_none());
    assert!(stream.next().is_none());
    assert!(Blob::from_bytes(&[], "").unwrap().stream().next().is_none());
    let b = Blob::from_bytes(b"abcdef", "").unwrap();
    assert_eq!(b.slice(Some(-4.9), Some(-1.2), "TEXT/X").bytes(), b"bcd");
    assert_eq!(
        b.slice(Some(f64::NEG_INFINITY), Some(f64::INFINITY), "")
            .bytes(),
        b"abcdef"
    );
    assert!(b.slice(Some(f64::NAN), Some(3.0), "").bytes().is_empty());
    assert!(b.slice(Some(1.0), Some(f64::NAN), "").bytes().is_empty());
    assert!(b.slice(Some(4.0), Some(1.0), "").bytes().is_empty());
    assert_eq!(
        Blob::from_bytes(b"\xef\xbb\xbf\xff", "")
            .unwrap()
            .text()
            .unwrap(),
        "�"
    );
    for value in ["\ttext/plain", "x\u{7f}", "雪"] {
        assert_eq!(Blob::from_bytes(b"a", value).unwrap().content_type(), "");
    }
    let option = BlobOptions {
        endings: Some(units("transparent")),
        ..Default::default()
    };
    let error = Blob::new([], option).unwrap_err();
    assert_eq!(error.name(), "Error");
    assert_eq!(
        error.to_string(),
        "The 'endings' field on 'Options' is not implemented."
    );
    let chunk = vec![0; 8 << 20];
    let error = Blob::new(
        (0..17).map(|_| BlobPart::Bytes(&chunk)),
        BlobOptions::default(),
    )
    .unwrap_err();
    assert_eq!(error.name(), "RangeError");
    assert_eq!(
        error.to_string(),
        "Blob size 142606336 exceeds limit 134217728"
    );
}
#[test]
fn files_preserve_dom_names_timestamps_and_form_value_identity() {
    let f = File::new_utf16(
        [BlobPart::Text("body")],
        &[0xd800, 0x2f],
        FileOptions {
            blob: BlobOptions::new("APP/TEST"),
            last_modified: Some(-9.8),
        },
    )
    .unwrap();
    assert_eq!(f.name_utf16(), [0xd800, 0x2f]);
    assert_eq!(f.last_modified(), -9.0);
    assert_eq!(f.content_type(), "app/test");
    for n in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(
            File::from_blob(f.blob().clone(), "n", Some(n)).last_modified(),
            0.0
        );
    }
    let form = FormData::new();
    form.append_file("same", &f, None);
    form.append_file("rename", &f, Some("new"));
    form.append_blob("blob", f.blob(), None);
    assert!(file(form.get("same").unwrap()).ptr_eq(&f));
    let renamed = file(form.get("rename").unwrap());
    assert!(!renamed.ptr_eq(&f));
    assert_eq!(renamed.name(), "new");
    assert_eq!(renamed.last_modified(), -9.0);
    assert_eq!(renamed.bytes(), b"body");
    assert_eq!(file(form.get("blob").unwrap()).name(), "blob");
    let copied = form.copy();
    form.delete("same");
    assert!(file(copied.get("same").unwrap()).ptr_eq(&f));
}
#[test]
fn form_lists_keep_order_dom_strings_and_live_iteration() {
    let form = FormData::new();
    form.append("a", FormValue::text("first"));
    form.append("b", FormValue::text("middle"));
    form.append("a", FormValue::text("last"));
    let alias = form.clone();
    alias.set("a", FormValue::text("set"));
    assert_eq!(form.keys().collect::<Vec<_>>(), [units("a"), units("b")]);
    assert_eq!(text(form.get("a").unwrap()), units("set"));
    form.append_utf16(&[0xd800], FormValue::Text(vec![0xdc00]));
    assert_eq!(text(form.get_utf16(&[0xd800]).unwrap()), [0xdc00]);
    let mut iterator = form.entries();
    assert_eq!(iterator.next().unwrap().0, units("a"));
    form.delete("a");
    assert_eq!(iterator.next().unwrap().0, [0xd800]);
    assert!(iterator.next().is_none());
    form.append("c", FormValue::text("after-done"));
    assert_eq!(iterator.next().unwrap().0, units("c"));
    let each = FormData::new();
    each.append("a", FormValue::text("1"));
    let mut seen = Vec::new();
    each.for_each(|_, name, same| {
        seen.push(name.clone());
        if name == units("a") {
            same.append("b", FormValue::text("2"));
        }
    });
    assert_eq!(seen, [units("a"), units("b")]);
    let alias = each.clone();
    std::thread::spawn(move || alias.set("a", FormValue::text("thread")))
        .join()
        .unwrap();
    assert_eq!(text(each.get("a").unwrap()), units("thread"));
}
#[test]
fn multipart_encoding_preserves_binary_and_escaping_is_one_way() {
    let form = FormData::new();
    form.append("a\"\n\r", FormValue::text("x\r\ny"));
    form.append_utf16(&units("lone"), FormValue::Text(vec![0xd800]));
    let f = File::new(
        [BlobPart::Bytes(&[0, 255, 192, 128])],
        "f\"\n.bin",
        FileOptions {
            last_modified: Some(1.0),
            ..Default::default()
        },
    )
    .unwrap();
    form.append_file("file", &f, None);
    let wire = form.encode_with_random([0; 16]).unwrap();
    assert_eq!(
        wire.boundary,
        "----cellarFormBoundary00000000000000000000000000000000"
    );
    assert!(String::from_utf8_lossy(&wire.bytes).contains("name=\"a%22%0A%0D\""));
    let parsed = FormData::parse(&wire.bytes, &wire.content_type).unwrap();
    assert_eq!(text(parsed.get("a%22%0A%0D").unwrap()), units("x\r\ny"));
    assert_eq!(text(parsed.get("lone").unwrap()), units("�"));
    let f = file(parsed.get("file").unwrap());
    assert_eq!(f.name(), "f%22%0A.bin");
    assert_eq!(f.bytes(), [0, 255, 192, 128]);
    assert_eq!(f.content_type(), "application/octet-stream");
    form.append("bad\\", FormValue::text("x"));
    assert_eq!(
        form.encode().unwrap_err().to_string(),
        "Name or filename can't end with backslash"
    );
    let form = FormData::new();
    form.append_blob("file", &Blob::from_bytes(b"x", "").unwrap(), Some("bad\\"));
    assert!(form.encode().is_err());
}
#[test]
fn form_parsing_retains_framing_header_precedence_and_charset_errors() {
    let binary=[b"preamble\n--X\nContent-Disposition: form-data; filename=\"snow.bin\"; name=\"upload\"\nContent-Type: APP/BINARY\n\n".as_slice(),&[255,0,13],b"inline--X!\r\n--X\r\nContent-Disposition: form-data; name=bare\r\n\r\n\xef\xbb\xbfvalue\r\n--X--\r\nepilogue"].concat();
    let parsed = FormData::parse(&binary, " MULTIPART/FORM-DATA; boundary=\" X \"").unwrap();
    let f = file(parsed.get("upload").unwrap());
    assert_eq!(
        f.bytes(),
        [vec![255, 0, 13], b"inline--X!".to_vec()].concat()
    );
    assert_eq!(f.content_type(), "app/binary");
    assert_eq!(text(parsed.get("bare").unwrap()), units("value"));
    let pairs = FormData::parse(
        b"\xef\xbb\xbf?a=one+two&a=%FF&snow=%E9%9B%AA",
        "\u{feff}APPLICATION/X-WWW-FORM-URLENCODED; charset=\"UTF8\"",
    )
    .unwrap();
    assert_eq!(
        pairs.get_all("a").into_iter().map(text).collect::<Vec<_>>(),
        [units("one two"), units("�")]
    );
    assert_eq!(text(pairs.get("snow").unwrap()), units("雪"));
    let mut ct = units("application/x-www-form-urlencoded; charset=");
    ct.push(0xd800);
    let error = FormData::parse_utf16(b"x=1", &ct).unwrap_err();
    assert_eq!(error.name(), "TypeError");
    assert!(error.message_utf16().contains(&0xd800));
    for (body,ct,message) in [
        ("","text/plain","Unrecognized Content-Type header value. FormData can only parse the following MIME types: multipart/form-data, application/x-www-form-urlencoded"),
        ("","multipart/form-data","No boundary was found in the multipart/form-data Content-Type header."),
        ("inline--X","multipart/form-data; boundary=X","No boundary delimiter was found in the multipart/form-data body."),
        ("--X?","multipart/form-data; boundary=X","A boundary delimiter must be followed by CRLF, LF, or \"--\"."),
        ("--X\nheaders\n\nbody","multipart/form-data; boundary=X","The multipart/form-data body ended without a closing boundary delimiter."),
        ("--X\nheaders\n--X--","multipart/form-data; boundary=X","A FormData part's headers must be terminated by a blank line."),
        ("--X\nX: y\n\nbody\n--X--","multipart/form-data; boundary=X","Content-Disposition header is required for each FormData part."),
        ("--X\nContent-Disposition: attachment; name=a\n\nbody\n--X--","multipart/form-data; boundary=X","Content-Disposition header for FormData part must have the value \"form-data\", possibly followed by parameters. Got: \"attachment\""),
        ("--X\nContent-Disposition: form-data; filename=a\n\nbody\n--X--","multipart/form-data; boundary=X","Content-Disposition header for FormData part must have a name parameter."),
    ]{assert_eq!(FormData::parse(body.as_bytes(),ct).unwrap_err().to_string(),message);}
}
#[test]
fn typed_bodies_infer_only_the_retained_content_types() {
    assert!(BodyInit::Null.encode().unwrap().bytes.is_none());
    let text = BodyInit::Text(vec![0xd800]).encode().unwrap();
    assert_eq!(text.bytes.unwrap(), "�".as_bytes());
    assert!(text.content_type.is_none());
    assert_eq!(
        BodyInit::Blob(Blob::from_bytes(b"x", "TEXT/PLAIN").unwrap())
            .encode()
            .unwrap()
            .content_type
            .as_deref(),
        Some("text/plain")
    );
    let query = SearchParams::from_query("a=a%20b");
    let encoded = BodyInit::SearchParams(query.clone()).encode().unwrap();
    assert_eq!(encoded.bytes.unwrap(), b"a=a+b");
    assert_eq!(
        encoded.content_type.as_deref(),
        Some("application/x-www-form-urlencoded;charset=UTF-8")
    );
    query.append_utf16(&units("lone"), &[0xd800]).unwrap();
    assert_eq!(
        BodyInit::SearchParams(query).encode().unwrap_err().name(),
        "URIError"
    );
}

#[test]
fn file_defaults_follow_scoped_clocks_nested_calls_and_unwinding() {
    use cellar_native::web::WallClockGuard;
    let blob = Blob::from_bytes(b"x", "").unwrap();
    let _outer = WallClockGuard::enter(|| 1234.0);
    assert_eq!(
        File::from_blob(blob.clone(), "outer", None).last_modified(),
        1234.0
    );
    {
        let _inner = WallClockGuard::enter(|| 4567.0);
        let form = FormData::new();
        form.append_blob("file", &blob, None);
        assert_eq!(file(form.get("file").unwrap()).last_modified(), 4567.0);
        let wire = form.encode_with_random([1; 16]).unwrap();
        let parsed = FormData::parse(&wire.bytes, &wire.content_type).unwrap();
        assert_eq!(file(parsed.get("file").unwrap()).last_modified(), 4567.0);
    }
    assert!(std::panic::catch_unwind(|| {
        let _guard = WallClockGuard::enter(|| 9876.0);
        panic!("clock scope unwinds")
    })
    .is_err());
    assert_eq!(
        File::from_blob(blob.clone(), "restored", None).last_modified(),
        1234.0
    );
    let threaded = std::thread::spawn(move || {
        let _guard = WallClockGuard::enter(|| 9999.0);
        File::from_blob(blob, "thread", None).last_modified()
    })
    .join()
    .unwrap();
    assert_eq!(threaded, 9999.0);
}
