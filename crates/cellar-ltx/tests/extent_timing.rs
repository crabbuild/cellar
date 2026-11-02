// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use cellar_ltx::internal::image_timing::{self, Phase};

#[test]
fn image_timing_scopes_restore_after_nested_work_and_panics() {
    let (_, outer) = image_timing::measure(|| {
        let _sql = image_timing::span(Phase::Sql);
        let (_, inner) = image_timing::measure(|| {
            let _read = image_timing::span(Phase::ExtentRead);
        });
        assert_eq!(inner.phases[Phase::ExtentRead as usize].calls, 1);
        assert_eq!(inner.phases[Phase::Sql as usize].calls, 0);
        let _hash = image_timing::span(Phase::ExtentHash);
    });
    assert!(!outer.unavailable);
    assert_eq!(outer.phases[Phase::Sql as usize].calls, 1);
    assert_eq!(outer.phases[Phase::ExtentHash as usize].calls, 1);
    assert_eq!(outer.phases[Phase::ExtentRead as usize].calls, 0);
    assert!(std::panic::catch_unwind(|| image_timing::measure(|| panic!("injected"))).is_err());
    let (_, next) = image_timing::measure(|| {
        let _sync = image_timing::span(Phase::FileSync);
    });
    assert!(!next.unavailable);
    assert_eq!(next.phases[Phase::FileSync as usize].calls, 1);
    assert_eq!(next.phases[Phase::Sql as usize].calls, 0);
}
