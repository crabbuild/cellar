// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn checksummed(header: [u8; 48]) -> [u8; 96] {
    let mut header = header;
    let (first, second) = crate::wal_checksum(cfg!(target_endian = "big"), 0, 0, &header[..40]);
    header[40..44].copy_from_slice(&first.to_ne_bytes());
    header[44..48].copy_from_slice(&second.to_ne_bytes());
    let mut copies = [0; 96];
    copies[..48].copy_from_slice(&header);
    copies[48..].copy_from_slice(&header);
    copies
}

fn valid(page_size: u32) -> [u8; 96] {
    let mut header = [0; 48];
    header[..4].copy_from_slice(&3_007_000u32.to_ne_bytes());
    header[12] = 1;
    let encoded = if page_size == 65536 {
        1
    } else {
        page_size as u16
    };
    header[14..16].copy_from_slice(&encoded.to_ne_bytes());
    header[16..20].copy_from_slice(&5u32.to_ne_bytes());
    header[20..24].copy_from_slice(&3u32.to_ne_bytes());
    checksummed(header)
}

#[test]
fn both_complete_headers_and_native_checksum_are_required() {
    for page_size in [512, 4096, 65536] {
        let copies = valid(page_size);
        let cut = WalIndexCut::decode(&copies, page_size).unwrap();
        assert_eq!(cut.frames(), 5);
        assert_eq!(cut.pages(), 3);
        for index in 0..copies.len() {
            let mut changed = copies;
            changed[index] ^= 1;
            assert!(WalIndexCut::decode(&changed, page_size).is_err());
            // Identical corrupted copies still require their own checksum.
            changed[index % 48 + if index < 48 { 48 } else { 0 }] ^= 1;
            assert!(WalIndexCut::decode(&changed, page_size).is_err());
        }
    }
}

#[test]
fn unsupported_or_uninitialized_index_never_becomes_a_committed_cut() {
    let valid = valid(4096);
    let base: [u8; 48] = valid[..48].try_into().unwrap();
    for field in 0..6 {
        let mut header = base;
        match field {
            0 => header[..4].copy_from_slice(&3_007_001u32.to_ne_bytes()),
            1 => header[12] = 0,
            2 => header[13] = 2,
            3 => header[14..16].copy_from_slice(&8192u16.to_ne_bytes()),
            4 => header[16..20].copy_from_slice(&0u32.to_ne_bytes()),
            _ => header[20..24].copy_from_slice(&0u32.to_ne_bytes()),
        }
        assert!(WalIndexCut::decode(&checksummed(header), 4096).is_err());
    }
}

#[test]
fn wal_salts_byte_order_and_final_frame_checksum_bind_the_cut() {
    let mut copies = valid(4096);
    let mut header: [u8; 48] = copies[..48].try_into().unwrap();
    header[24..28].copy_from_slice(&0x12345678u32.to_ne_bytes());
    header[28..32].copy_from_slice(&0x87654321u32.to_ne_bytes());
    header[32..40].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
    copies = checksummed(header);
    let cut = WalIndexCut::decode(&copies, 4096).unwrap();
    let mut wal = [0; 32];
    wal[3] = 0x82;
    wal[16..24].copy_from_slice(&header[32..40]);
    assert!(cut.matches_wal(&wal));
    wal[16] ^= 1;
    assert!(!cut.matches_wal(&wal));
    wal[16] ^= 1;
    wal[3] = 0x83;
    assert!(!cut.matches_wal(&wal));
    let mut frame = [0; 24];
    frame[16..20].copy_from_slice(&0x12345678u32.to_be_bytes());
    frame[20..24].copy_from_slice(&0x87654321u32.to_be_bytes());
    assert!(cut.matches_last_frame(&frame));
    frame[23] ^= 1;
    assert!(!cut.matches_last_frame(&frame));
    assert!(!cut.matches_last_frame(&frame[..23]));
    assert!(!cut.matches_wal(&wal[..31]));
}
