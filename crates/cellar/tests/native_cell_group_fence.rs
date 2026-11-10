// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public receipt verification; platform permanence is qualified separately.
use cellar::sdk::consensus::{
    CellGroupDisasterFenceRequest, CellGroupPermanentFenceAuthority, DisasterFenceAuthority,
};
use cellar::sdk::fleet_operations::FleetTransitionRecoveryFenceRequest;
use ed25519_dalek::{Signer as _, SigningKey};

fn certificate(key: &SigningKey, request: [u8; 32], inventory: [u8; 32]) -> Vec<u8> {
    let mut body = vec![1];
    body.extend_from_slice(&request);
    body.extend_from_slice(&inventory);
    let mut message = CellGroupPermanentFenceAuthority::PURPOSE.to_vec();
    message.extend_from_slice(&body);
    body.extend_from_slice(&key.sign(&message).to_bytes());
    body
}

#[test]
fn exact_request_and_winning_receipt_are_verified_independently() -> anyhow::Result<()> {
    let key = SigningKey::from_bytes(&[7; 32]);
    let local = certificate(&key, [1; 32], [2; 32]);
    let authority = CellGroupPermanentFenceAuthority::new(key.verifying_key().to_bytes())?
        .with_certificate(local.clone())?;
    authority.verify_receipt([1; 32], &local)?;
    // The controller can issue another valid receipt after reconciling a larger
    // inventory. Verify its signature, rather than comparing to local bytes.
    let winning = certificate(&key, [1; 32], [3; 32]);
    authority.verify_receipt([1; 32], &winning)?;
    assert!(authority.verify_receipt([4; 32], &winning).is_err());

    fn shared(_: &dyn DisasterFenceAuthority<CellGroupDisasterFenceRequest>) {}

    fn managed(_: &dyn DisasterFenceAuthority<FleetTransitionRecoveryFenceRequest>) {}
    shared(&authority);
    managed(&authority);
    Ok(())
}

#[test]
fn malformed_legacy_and_incomplete_certificates_refuse() -> anyhow::Result<()> {
    let key = SigningKey::from_bytes(&[7; 32]);
    let valid = certificate(&key, [1; 32], [2; 32]);
    for length in [0, 1, 32, 65, 96, 128, 130] {
        assert!(
            CellGroupPermanentFenceAuthority::new(key.verifying_key().to_bytes())?
                .with_certificate(vec![1; length])
                .is_err()
        );
    }
    for position in [0, 1, 33, 65, 128] {
        let mut changed = valid.clone();
        changed[position] ^= 1;
        assert!(
            CellGroupPermanentFenceAuthority::new(key.verifying_key().to_bytes())?
                .with_certificate(changed)
                .is_err()
        );
    }
    for (request, inventory) in [([0; 32], [2; 32]), ([1; 32], [0; 32])] {
        assert!(
            CellGroupPermanentFenceAuthority::new(key.verifying_key().to_bytes())?
                .with_certificate(certificate(&key, request, inventory))
                .is_err()
        );
    }
    Ok(())
}

#[test]
fn another_controller_and_unsigned_inventory_refuse() -> anyhow::Result<()> {
    let key = SigningKey::from_bytes(&[7; 32]);
    let other = SigningKey::from_bytes(&[8; 32]);
    let valid = certificate(&key, [1; 32], [2; 32]);
    let authority = CellGroupPermanentFenceAuthority::new(key.verifying_key().to_bytes())?
        .with_certificate(valid.clone())?;
    assert!(
        CellGroupPermanentFenceAuthority::new(other.verifying_key().to_bytes())?
            .with_certificate(valid)
            .is_err()
    );
    assert!(authority
        .verify_receipt([1; 32], &certificate(&other, [1; 32], [2; 32]))
        .is_err());
    // A weak/identity key is never an independently pinned controller.
    let mut weak = [0; 32];
    weak[0] = 1;
    assert!(CellGroupPermanentFenceAuthority::new(weak).is_err());
    Ok(())
}
