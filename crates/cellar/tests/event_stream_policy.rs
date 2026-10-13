//! Pure protocol cases are independent of transport success or storage
//! implementation, and describe the decisions persisted by stream cells.

use cellar_logic::event_stream::{self as policy, Acknowledgment, Failure, Lease, Receipt};

fn receipt(generation: i64, first: i64, last: i64) -> Receipt {
    Receipt {
        subscription: [1; 16],
        incarnation: [2; 16],
        generation,
        token: [generation as u8; 32],
        first,
        last,
    }
}

#[test]
fn lane_retries_accept_only_a_contiguous_new_suffix() {
    let plan = policy::append(3, &[2, 3, 4, 5]).unwrap();
    assert_eq!(plan.skip, 2);
    assert_eq!(plan.accepted_through, 5);
    let duplicate = policy::append(5, &[2, 3, 4, 5]).unwrap();
    assert_eq!(duplicate.skip, 4);
    assert_eq!(duplicate.accepted_through, 5);
    assert_eq!(
        policy::append(3, &[5, 6]),
        Err(Failure::SequenceGap { expected: 4 })
    );
    assert_eq!(policy::append(3, &[3, 5]), Err(Failure::InvalidSequence));
    assert_eq!(policy::append(0, &[0]), Err(Failure::InvalidSequence));
    assert_eq!(policy::append(0, &[]), Err(Failure::InvalidSequence));
    assert_eq!(policy::append(i64::MAX, &[i64::MAX]).unwrap().skip, 1);
    assert!(policy::append(i64::MAX, &[i64::MAX, i64::MIN]).is_err());
}

#[test]
fn only_matching_unexpired_delivery_can_advance_checkpoint() {
    let original = receipt(1, 1, 3);
    let lease = Lease {
        receipt: original.clone(),
        issued_at_ms: 100,
        expires_at_ms: 200,
    };
    assert_eq!(
        policy::acknowledge([1; 16], [2; 16], 0, Some(&lease), None, &original, 199),
        Ok(Acknowledgment::Advance { through: 3 })
    );
    assert_eq!(
        policy::acknowledge([1; 16], [2; 16], 0, Some(&lease), None, &original, 200),
        Err(Failure::LeaseExpired)
    );
    let mut wrong = original.clone();
    wrong.token = [0; 32];
    assert_eq!(
        policy::acknowledge([1; 16], [2; 16], 0, Some(&lease), None, &wrong, 100),
        Err(Failure::StaleReceipt)
    );
    let skipped = receipt(2, 4, 6);
    let skipping_lease = Lease {
        receipt: skipped.clone(),
        ..lease.clone()
    };
    assert_eq!(
        policy::acknowledge(
            [1; 16],
            [2; 16],
            0,
            Some(&skipping_lease),
            None,
            &skipped,
            100
        ),
        Err(Failure::StaleReceipt)
    );
    assert_eq!(
        policy::acknowledge([1; 16], [3; 16], 0, Some(&lease), None, &original, 100),
        Err(Failure::SubscriptionReset)
    );
}

#[test]
fn lost_ack_response_can_be_retried_after_lease_expiry() {
    let old = receipt(1, 1, 3);
    assert_eq!(
        policy::acknowledge([1; 16], [2; 16], 3, None, Some(&old), &old, 900),
        Ok(Acknowledgment::AlreadyAcknowledged)
    );
    let new = receipt(2, 4, 6);
    assert_eq!(
        policy::acknowledge([1; 16], [2; 16], 6, None, Some(&new), &old, 900),
        Ok(Acknowledgment::Obsolete)
    );
    let reset = receipt(3, 7, 9);
    assert_eq!(
        policy::acknowledge([1; 16], [2; 16], 6, None, Some(&new), &reset, 900),
        Err(Failure::StaleReceipt)
    );
}

#[test]
fn retrying_extension_is_idempotent_and_never_resurrects_expired_lease() {
    let original = receipt(1, 1, 3);
    let mut lease = Lease {
        receipt: original.clone(),
        issued_at_ms: 100,
        expires_at_ms: 200,
    };
    lease.expires_at_ms = policy::extend(&lease, &original, 150, 250, 300).unwrap();
    assert_eq!(policy::extend(&lease, &original, 160, 250, 300), Ok(250));
    assert_eq!(
        policy::extend(&lease, &original, 160, 401, 300),
        Err(Failure::InvalidLease)
    );
    assert_eq!(
        policy::extend(&lease, &original, 250, 300, 300),
        Err(Failure::LeaseExpired)
    );
}

#[test]
fn retention_preserves_required_progress_and_active_batches() {
    assert_eq!(policy::deletion_frontier(100, [20, 60], [31, 81]), Ok(20));
    assert_eq!(policy::deletion_frontier(100, [], [31, 81]), Ok(30));
    assert_eq!(policy::deletion_frontier(100, [], []), Ok(100));
    assert_eq!(
        policy::cursor(31, 101, 20),
        Err(Failure::CursorExpired { earliest: 31 })
    );
    assert_eq!(policy::cursor(31, 101, 31), Ok(()));
    assert_eq!(policy::cursor(31, 101, 101), Ok(()));
    assert_eq!(policy::cursor(31, 101, 102), Err(Failure::InvalidCursor));
    assert_eq!(
        policy::cursor(101, 101, 100),
        Err(Failure::CursorExpired { earliest: 101 })
    );
}

#[test]
fn consumer_and_retention_interleavings_cannot_reclaim_unacknowledged_batch() {
    let delivered = receipt(1, 1, 10);
    let lease = Lease {
        receipt: delivered.clone(),
        issued_at_ms: 100,
        expires_at_ms: 200,
    };
    // Retention before acknowledgment sees both protection mechanisms.
    assert_eq!(policy::deletion_frontier(100, [0], [1]), Ok(0));
    let decision =
        policy::acknowledge([1; 16], [2; 16], 0, Some(&lease), None, &delivered, 150).unwrap();
    let Acknowledgment::Advance { through } = decision else {
        panic!("matching batch must advance")
    };
    // Atomic settlement clears the lease and advances progress together.
    assert_eq!(policy::deletion_frontier(100, [through], []), Ok(10));
    // Expiry alone grants no progress, even though it removes lease protection.
    assert_eq!(policy::deletion_frontier(100, [0], []), Ok(0));
}

#[test]
fn admission_limits_cannot_accept_an_event_that_no_batch_can_deliver() {
    let defaults = policy::Limits::default();
    assert!(defaults.validate().is_ok());
    assert!(policy::Limits::unrestricted().validate().is_ok());
    assert_eq!(
        policy::Limits::unrestricted().batch_bytes(),
        policy::MAX_PROTOCOL_BATCH_BYTES
    );
    assert_eq!(
        policy::Limits::unrestricted().event_bytes(),
        policy::MAX_PROTOCOL_BATCH_BYTES
    );
    assert_eq!(
        policy::Limits::unrestricted().batch_records(),
        policy::MAX_PROTOCOL_BATCH_RECORDS
    );
    let too_small = policy::Limits {
        max_batch_bytes: defaults.max_event_bytes - 1,
        ..defaults
    };
    assert_eq!(too_small.validate(), Err(Failure::InvalidLimits));
    let invalid_expiry = policy::Limits {
        max_lease_lifetime_ms: defaults.lease_ms - 1,
        ..defaults
    };
    assert_eq!(invalid_expiry.validate(), Err(Failure::InvalidLimits));
}

#[test]
fn subscription_administration_preserves_progress_or_reports_explicit_skips() {
    use cellar_logic::subscription_lifecycle::{self as lifecycle, Action, Facts, State};
    let active = Facts {
        state: State::Active,
        checkpoint: 3,
        earliest: 2,
        end: 10,
        required: true,
        other_required: 0,
        leased: true,
    };
    let paused = lifecycle::change(active, Action::Pause).unwrap();
    assert_eq!(paused.state, State::Paused);
    assert_eq!(paused.checkpoint, 3);
    assert!(!paused.invalidate);
    let resumed = lifecycle::change(
        Facts {
            state: State::Paused,
            ..active
        },
        Action::Resume,
    )
    .unwrap();
    assert_eq!(resumed.state, State::Active);
    assert!(!resumed.invalidate);
    let skipped = lifecycle::change(active, Action::Reset { first: 8 }).unwrap();
    assert_eq!(skipped.checkpoint, 7);
    assert_eq!(skipped.skipped, Some((4, 7)));
    assert!(skipped.invalidate);
    let replay = lifecycle::change(active, Action::Reset { first: 2 }).unwrap();
    assert_eq!(replay.checkpoint, 1);
    assert_eq!(replay.skipped, None);
    assert!(replay.invalidate);
    assert_eq!(
        lifecycle::change(active, Action::Delete { force: true }),
        Err(Failure::LastRequiredSubscription)
    );
    assert_eq!(
        lifecycle::change(
            Facts {
                other_required: 1,
                ..active
            },
            Action::Delete { force: false }
        ),
        Err(Failure::PendingDelivery)
    );
    let deleted = lifecycle::change(
        Facts {
            other_required: 1,
            ..active
        },
        Action::Delete { force: true },
    )
    .unwrap();
    assert_eq!(deleted.state, State::Deleted);
    assert_eq!(deleted.skipped, Some((4, 9)));
    assert!(deleted.invalidate);
    assert!(lifecycle::change(
        Facts {
            state: State::Deleted,
            ..active
        },
        Action::Reset { first: 4 }
    )
    .is_err());
    assert_eq!(
        lifecycle::change(
            Facts {
                state: State::Expired,
                ..active
            },
            Action::Resume
        ),
        Err(Failure::CursorExpired { earliest: 2 })
    );
    assert_eq!(
        lifecycle::change(active, Action::Reset { first: 1 }),
        Err(Failure::CursorExpired { earliest: 2 })
    );
}

#[test]
fn producer_sealing_requires_complete_settlement_and_preserves_retry_deduplication() {
    assert_eq!(policy::source_seal(6, 5, 0), Ok(5));
    assert_eq!(policy::source_seal(6, 4, 1), Err(Failure::PendingDelivery));
    assert_eq!(policy::source_seal(6, 4, 0), Err(Failure::InvalidSequence));
    assert_eq!(policy::source_seal(0, 0, 0), Err(Failure::InvalidSequence));
    assert_eq!(policy::destination_seal(5, 5), Ok(()));
    assert_eq!(
        policy::destination_seal(5, 6),
        Err(Failure::InvalidSequence)
    );
    assert_eq!(
        policy::destination_seal(5, 4),
        Err(Failure::InvalidSequence)
    );
    assert_eq!(policy::append(5, &[1, 2, 3]).unwrap().skip, 3);
}

#[test]
fn local_retirement_conditions_never_claim_to_prove_unknown_producers_drained() {
    assert_eq!(policy::retire(false, 0, 0, 0), Ok(()));
    assert_eq!(
        policy::retire(false, 1, 0, 0),
        Err(Failure::PendingDelivery)
    );
    assert_eq!(
        policy::retire(false, 0, 1, 0),
        Err(Failure::PendingDelivery)
    );
    assert_eq!(
        policy::retire(false, 0, 0, 1),
        Err(Failure::PendingDelivery)
    );
    assert_eq!(policy::retire(true, 1, 1, 1), Ok(()));
    assert_eq!(policy::retire(true, -1, 0, 0), Err(Failure::InvalidLimits));
}

#[test]
fn event_identity_survives_structured_rpc_above_binary64_integer_precision() -> anyhow::Result<()> {
    use cellar::application::event_stream::Envelope;
    use cellar::application::{decode_rpc, encode_rpc};
    let original = Envelope {
        id: "event".into(),
        lane: "lane".into(),
        sequence: i64::MAX,
        source: "Order:123".into(),
        source_incarnation: "incarnation".into(),
        family: "orders".into(),
        version: 1,
        key: "123".into(),
        created_at_ms: 9_007_199_254_740_993,
        payload: serde_json::json!({"order":"123"}),
    };
    let args = cellar_native::clone::StructuredValue::from_json(serde_json::to_value(&original)?)?;
    let transport = encode_rpc(&args)?;
    let decoded: Envelope = serde_json::from_value(decode_rpc(&transport)?.to_json()?)?;
    assert_eq!(decoded.sequence, original.sequence);
    assert_eq!(decoded.created_at_ms, original.created_at_ms);
    // Committed event bytes also remain opaque to the binary64 value codec.
    use base64::Engine;
    let mut precise = original.clone();
    precise.payload = serde_json::json!({"business_id":i64::MAX});
    let bytes = serde_json::to_vec(&precise)?;
    let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
    let args =
        cellar_native::clone::StructuredValue::from_json(serde_json::json!({"records":[encoded]}))?;
    let value = decode_rpc(&encode_rpc(&args)?)?.to_json()?;
    let received =
        base64::engine::general_purpose::STANDARD.decode(value["records"][0].as_str().unwrap())?;
    assert_eq!(received, bytes);
    assert_eq!(
        serde_json::from_slice::<Envelope>(&received)?.payload["business_id"].as_i64(),
        Some(i64::MAX)
    );
    let mut invalid = serde_json::to_value(original)?;
    invalid["sequence"] = "01".into();
    assert!(serde_json::from_value::<Envelope>(invalid).is_err());
    Ok(())
}
