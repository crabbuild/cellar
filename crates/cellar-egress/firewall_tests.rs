// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn invalid_or_conflicting_ports_and_over_budget_tables_refuse_before_effects() -> Result<()> {
    for (http, dns) in [
        (0, 39190),
        (39189, 0),
        (39189, 39189),
        (53, 39190),
        (39189, 53),
        (80, 39190),
        (39189, 80),
    ] {
        assert!(Program::new(&[], http, dns, false).is_err());
    }
    for port in [53, 39189, 39190] {
        assert!(Program::new(
            &[HttpTarget::parse(&format!("192.0.2.7:{port}"))?],
            39189,
            39190,
            false
        )
        .is_err());
    }
    let targets = (0..65)
        .map(|n| HttpTarget::parse(&format!("host{n}.internal")))
        .collect::<Result<Vec<_>>>()?;
    assert!(Program::new(&targets, 39189, 39190, false).is_err());
    assert!(Program::new(&targets[..64], 39189, 39190, false).is_ok());
    assert!(Program::new(
        &[
            HttpTarget::parse("192.0.2.7")?,
            HttpTarget::parse("192.0.2.7:80")?
        ],
        39189,
        39190,
        false
    )
    .is_err());
    Ok(())
}
