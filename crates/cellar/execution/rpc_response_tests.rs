// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn lossy_json_matches_utf8_semantics_including_incomplete_suffixes() {
    for first in 0..=255u8 {
        for second in 0..=255u8 {
            let input = [b'a', first, second, b'z'];
            assert_eq!(
                json(Bytes::copy_from_slice(&input)).unwrap(),
                String::from_utf8_lossy(&input)
            );
        }
    }
    for input in [
        b"\xf0\x9f\x92".as_slice(),
        b"\xe2\x82",
        b"\xed\xa0\x80",
        b"\xf4\x90\x80\x80",
    ] {
        assert_eq!(
            json(Bytes::copy_from_slice(input)).unwrap(),
            String::from_utf8_lossy(input)
        );
    }
}

#[test]
fn aliases_retain_the_full_capacity_until_the_last_cross_thread_drop() {
    let pool = Budget::new(256 * 1024);
    let mut buffer = Buffer::new(pool.reserve(Buffer::initial_weight()).unwrap());
    buffer.extend(&[42; 4096], 8192).unwrap();
    let bytes = buffer.finish().unwrap();
    let retained = pool.usage();
    assert_eq!(retained.active_buffers, 1);
    assert_eq!(retained.reserved_response_bytes, CONTROL_BYTES + 8192);
    let slice = bytes.slice(4095..);
    let alias = slice.clone();
    drop((bytes, slice));
    assert_eq!(pool.usage(), retained);
    std::thread::spawn(move || drop(alias)).join().unwrap();
    assert_eq!(pool.usage(), Usage::default());
}

#[test]
fn growth_reserves_reallocation_overlap_before_touching_the_buffer() {
    let pool = Budget::new(CONTROL_BYTES + 2 * 8192);
    let mut buffer = Buffer::new(pool.reserve(Buffer::initial_weight()).unwrap());
    buffer.extend(&[42; 4096], 8192).unwrap();
    let before = pool.usage();
    // The final 8-KiB allocation fits, but realloc may retain the old 4 KiB
    // simultaneously. Refuse that peak instead of only charging final length.
    assert_eq!(
        buffer.extend(&[0; 4096], 8192).unwrap_err().code(),
        Code::ResourceExhausted
    );
    assert_eq!(buffer.len(), 4096);
    assert_eq!(buffer.data.capacity(), 4096);
    assert_eq!(pool.usage(), before);
    drop(buffer);
    assert_eq!(pool.usage(), Usage::default());
}

#[test]
fn normalization_drops_unknown_large_backing_and_refunds_encoding_scratch() {
    struct Backing {
        data: Vec<u8>,
        dropped: Arc<AtomicUsize>,
    }

    impl AsRef<[u8]> for Backing {
        fn as_ref(&self) -> &[u8] {
            &self.data
        }
    }

    impl Drop for Backing {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
    let dropped = Arc::new(AtomicUsize::new(0));
    let view = Bytes::from_owner(Backing {
        data: vec![42; 1024 * 1024],
        dropped: dropped.clone(),
    })
    .slice(..1);
    let pool = Budget::new(encoding_weight(1024).unwrap());
    let admission = pool.reserve(encoding_weight(1024).unwrap()).unwrap();
    let bytes = admission.normalize(view).unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(bytes.as_ref(), [42]);
    assert_eq!(pool.usage().reserved_response_bytes, CONTROL_BYTES + 2);
    drop(bytes);
    assert_eq!(pool.usage(), Usage::default());
}

#[tokio::test]
async fn domain_shutdown_keeps_retained_aliases_charged_then_refunds_them() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let services = domain.services();
    let bytes = domain
        .run(async {
            let mut buffer = Buffer::new(
                services
                    .reserve_rpc_response(Buffer::initial_weight())
                    .unwrap(),
            );
            buffer.extend(b"reply", 4096).unwrap();
            buffer.finish().unwrap()
        })
        .await?;
    domain.shutdown().await;
    assert_eq!(services.rpc_response_usage().active_buffers, 1);
    assert!(services.reserve_rpc_response(1).is_err());
    std::thread::spawn(move || drop(bytes)).join().unwrap();
    assert_eq!(services.rpc_response_usage(), Usage::default());
    Ok(())
}

#[test]
fn overflow_and_protocol_size_refusals_leave_no_new_reservations() {
    let pool = Budget::new(CONTROL_BYTES + 8192);
    assert!(pool.reserve(usize::MAX).is_err());
    assert!(encoding_weight(usize::MAX).is_err());
    let mut buffer = Buffer::new(pool.reserve(Buffer::initial_weight()).unwrap());
    let before = pool.usage();
    assert!(buffer.extend(b"too long", 1).is_err());
    assert_eq!(pool.usage(), before);
    assert_eq!(buffer.len(), 0);
    drop(buffer);
    assert_eq!(pool.usage(), Usage::default());
}
