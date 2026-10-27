// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn snapshot(mode: Mode) -> Snapshot {
    let free = mode == Mode::FreeThreaded;
    Snapshot {
        version: [3, 14, 5],
        implementation: "cpython".into(),
        soabi: if free {
            "cpython-314t-darwin"
        } else {
            "cpython-314-x86_64-linux-gnu"
        }
        .into(),
        free_threaded: free,
        gil_enabled: !free,
        protobuf_version: "7.34.2".into(),
        protobuf_backend: if free { "python" } else { "upb" }.into(),
    }
}

#[test]
fn mode_requires_actual_gil_state_as_well_as_abi() {
    for mode in [Mode::Gil, Mode::FreeThreaded] {
        let observed = snapshot(mode);
        observed.validate(mode).unwrap();
        let opposite = if mode == Mode::Gil {
            Mode::FreeThreaded
        } else {
            Mode::Gil
        };
        assert!(observed.validate(opposite).is_err());
        let mut changed = observed.clone();
        changed.gil_enabled = !changed.gil_enabled;
        assert!(
            changed.validate(mode).is_err(),
            "a matching ABI concealed a different live GIL state"
        );
        let mut changed = observed;
        changed.soabi = snapshot(opposite).soabi;
        assert!(changed.validate(mode).is_err());
    }
}

#[test]
fn mode_refuses_other_interpreters_and_unpinned_codecs() {
    for mode in [Mode::Gil, Mode::FreeThreaded] {
        for field in [
            "version",
            "implementation",
            "protobuf_version",
            "protobuf_backend",
        ] {
            let mut changed = snapshot(mode);
            match field {
                "version" => changed.version[1] = 13,
                "implementation" => changed.implementation = "pypy".into(),
                "protobuf_version" => changed.protobuf_version = "7.34.1".into(),
                _ => changed.protobuf_backend = "unknown".into(),
            }
            assert!(
                changed.validate(mode).is_err(),
                "accepted mismatched {field}"
            );
        }
    }
    assert!(Mode::parse("auto").is_err());
    assert_eq!(Mode::parse("gil").unwrap(), Mode::Gil);
    assert_eq!(Mode::parse("free-threaded").unwrap(), Mode::FreeThreaded);
}
