// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn limits() -> Limits {
    Limits { max_body_bytes: 3 }
}

#[test]
fn invalid_options_are_proven_refusals_before_native_effects() {
    for (name, value) in [
        (
            "container.start",
            json!({"options":{"enableInternet":true}}),
        ),
        ("container.start", json!({"options":{"entrypoint":42}})),
        ("container.exec", json!({"options":{"cmd":[]}})),
        (
            "container.exec",
            json!({"options":{"cmd":["bad\u{0}command"]}}),
        ),
        (
            "container.exec_output",
            json!({"options":{"cmd":["tool"]},"max_bytes":4}),
        ),
        (
            "container.exec_output",
            json!({"options":{"cmd":["tool"]},"max_bytes":-1}),
        ),
        ("container.signal", json!({"signal":0})),
        ("container.signal", json!({"signal":65})),
        ("container.port.connect", json!({"port":65536})),
        ("container.port.connect", json!({"port":0})),
        (
            "container.port.wait_ready",
            json!({"port":80,"timeout_ms":86400001}),
        ),
        (
            "container.set_inactivity_timeout",
            json!({"timeout_ms":0.5}),
        ),
        ("container.exec.write", json!({"id":"1","data":"invalid"})),
        (
            "container.exec.write",
            json!({"id":"1","data":STANDARD.encode([0,1,2,3])}),
        ),
        (
            "container.websocket.send_text",
            json!({"id":"1","text":"four"}),
        ),
        (
            "container.websocket.close",
            json!({"id":"1","code":1006,"reason":""}),
        ),
        (
            "container.websocket.close",
            json!({"id":"1","code":1000,"reason":"x".repeat(124)}),
        ),
        ("container.inspect", json!({})),
    ] {
        let error = validate(name, &value, &limits())
            .unwrap_err()
            .context(Validation);
        assert!(error.is::<Validation>(), "{name}");
    }
}

#[test]
fn binary_and_numeric_boundaries_are_exact() -> Result<()> {
    for (name, value) in [
        ("container.exec", json!({"options":{"cmd":["tool", ""]}})),
        (
            "container.exec.write",
            json!({"id":"1","data":STANDARD.encode([0,255,42])}),
        ),
        (
            "container.exec_output",
            json!({"options":{"cmd":["tool"]},"max_bytes":3}),
        ),
        ("container.signal", json!({"signal":64})),
        ("container.port.connect", json!({"port":65535})),
        (
            "container.port.wait_ready",
            json!({"port":1,"timeout_ms":0}),
        ),
        (
            "container.set_inactivity_timeout",
            json!({"timeout_ms":86400000}),
        ),
        (
            "container.websocket.close",
            json!({"id":"1","code":4999,"reason":"x".repeat(123)}),
        ),
    ] {
        validate(name, &value, &limits())?;
    }
    Ok(())
}

#[test]
fn only_observations_may_overlap_a_control_operation() {
    for name in [
        "container.monitor",
        "container.exec.wait",
        "container.exec.read_stdout",
        "container.exec.read_stderr",
        "container.socket.read",
        "container.websocket.receive",
    ] {
        assert!(crate::Operation::parse(name).unwrap().is_read(), "{name}");
    }
    for name in [
        "container.start",
        "container.exec",
        "container.exec_output",
        "container.port.fetch",
        "container.port.connect",
        "container.port.connect_websocket",
        "container.exec.write",
        "container.destroy",
        "container.release",
    ] {
        assert!(!crate::Operation::parse(name).unwrap().is_read(), "{name}");
    }
}
