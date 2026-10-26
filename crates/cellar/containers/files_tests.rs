// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn archive(entries: &[(&str, &[u8], tar::EntryType)]) -> Vec<u8> {
    let mut archive = tar::Builder::new(Vec::new());
    for (name, bytes, kind) in entries {
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o640);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(1);
        header.set_size(bytes.len() as u64);
        header.set_entry_type(*kind);
        header.set_cksum();
        if kind.is_symlink() {
            header.set_link_name("target").unwrap();
        }
        archive.append_data(&mut header, name, *bytes).unwrap();
    }
    archive.into_inner().unwrap()
}

#[test]
fn paths_and_generated_archives_preserve_binary_and_long_utf8_names() {
    for path in ["", "relative", "/a/../b", "/a/./b", "/a//b", "/a/", "/a\0b"] {
        assert!(FilePath::new(path).is_err(), "accepted {path:?}");
    }
    let filename = "é".repeat(100);
    let path = FilePath::new(format!("/workspace/{filename}")).unwrap();
    let bytes = [0, 255, 1, 13, 10];
    let output = write_archive(&path, &bytes, 0o640).unwrap();
    assert_eq!(read_archive(&output, bytes.len()).unwrap(), bytes);
    assert!(read_archive(&output, bytes.len() - 1).is_err());
    assert!(write_archive(&FilePath::new("/").unwrap(), &bytes, 0o640).is_err());
    assert!(write_archive(&path, &bytes, 0o4755).is_err());
    assert!(route("id", "/a?b#%", false).ends_with("path=%2Fa%3Fb%23%25"));
}

#[test]
fn archives_refuse_links_multiple_files_truncation_and_excessive_sizes() {
    let linked = archive(&[("link", b"", tar::EntryType::Symlink)]);
    assert!(read_archive(&linked, 10).is_err());
    let multiple = archive(&[
        ("a", b"a", tar::EntryType::Regular),
        ("b", b"b", tar::EntryType::Regular),
    ]);
    assert!(read_archive(&multiple, 10).is_err());
    let mut header = tar::Header::new_gnu();
    header.set_path("file").unwrap();
    header.set_size(u64::MAX);
    header.set_cksum();
    assert!(read_archive(header.as_bytes(), 1024).is_err());
    let truncated = archive(&[("a", b"hello", tar::EntryType::Regular)]);
    assert!(read_archive(&truncated[..514], 10).is_err());
    assert!(list_archive(&truncated[..514], 4096, 10).is_err());
}

#[test]
fn direct_listings_are_sorted_bounded_and_keep_directory_and_link_metadata() {
    let bytes = archive(&[
        ("./", b"", tar::EntryType::Directory),
        ("b", b"payload", tar::EntryType::Regular),
        ("a/", b"", tar::EntryType::Directory),
        ("a/child", b"nested", tar::EntryType::Regular),
        ("link", b"", tar::EntryType::Symlink),
    ]);
    let entries = list_archive(&bytes, 4096, 3).unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        ["a", "b", "link"]
    );
    assert_eq!(entries[0].kind, FileKind::Directory);
    assert_eq!(entries[1].size, 7);
    assert_eq!(entries[1].modified_ms, Some(1000));
    assert_eq!(entries[2].link_target.as_deref(), Some("target"));
    assert!(list_archive(&bytes, 4096, 2).is_err());
    assert!(list_archive(&bytes, 2, 3).is_err());
    let duplicates = archive(&[
        ("a", b"", tar::EntryType::Regular),
        ("a", b"", tar::EntryType::Regular),
    ]);
    assert!(list_archive(&duplicates, 4096, 3).is_err());
}

#[test]
fn daemon_stat_maps_go_modes_without_treating_setuid_as_a_special_file() {
    for (mode, expected) in [
        (0o644, FileKind::File),
        ((1 << 23) | 0o755, FileKind::File),
        ((1 << 31) | 0o755, FileKind::Directory),
        ((1 << 27) | 0o777, FileKind::Symlink),
        ((1 << 25) | 0o600, FileKind::Other),
    ] {
        let encoded = STANDARD.encode(serde_json::to_vec(&serde_json::json!({"name":"file","size":3,"mode":mode,"mtime":"2026-01-01T00:00:00.123Z","linkTarget":""})).unwrap());
        let stat = stat_header(&encoded).unwrap();
        assert_eq!(stat.kind, expected);
        assert_eq!(stat.mode, mode & 0o777);
        assert_eq!(stat.modified_ms, Some(1767225600123));
    }
    assert!(stat_header("!!!!").is_err());
}
