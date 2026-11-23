// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn publication_shares_its_waiting_cohort_without_creating_an_idle_proof() {
    let mut admissions = Admissions::default();
    let first = admissions.admit().unwrap();
    let mut proof = Eligibility::publication();
    assert!(proof.allows(first));
    let during_publication = admissions.admit().unwrap();
    assert!(proof.allows(during_publication));
    proof.seal(admissions.current());
    let late = admissions.admit().unwrap();
    assert!(!proof.allows(late));
    assert!(proof.allows(first) && proof.allows(during_publication));
    proof.seal(admissions.current());
    assert!(
        !proof.allows(late),
        "cached completion must not renew authority"
    );
}

#[test]
fn background_revalidation_cannot_admit_a_later_quorum_to_s3_upgrade() {
    let mut admissions = Admissions::default();
    let mut background = Eligibility::existing(admissions.current());
    let later = admissions.admit().unwrap();
    assert!(!background.allows(later));
    background.seal(admissions.current());
    assert!(!background.allows(later));

    let revalidation = Eligibility::existing(admissions.current());
    assert!(revalidation.allows(later));
    assert!(!revalidation.allows(admissions.admit().unwrap()));
}

#[test]
fn proved_outputs_remain_reusable_only_in_the_same_physical_verification() {
    let mut admissions = Admissions::default();
    let first = admissions.admit().unwrap();
    let mut retained = Eligibility::existing(admissions.current());
    retained.prove_output();
    let later = admissions.admit().unwrap();
    assert!(retained.allows(first) && retained.allows(later));

    let mut another_verification = Eligibility::publication();
    another_verification.seal(0);
    assert!(!another_verification.allows(first));
    assert!(!another_verification.allows(later));
}

#[test]
fn admission_exhaustion_never_wraps_into_an_older_proof_cohort() {
    let mut admissions = Admissions { last: u64::MAX - 1 };
    let final_admission = admissions.admit().unwrap();
    assert_eq!(admissions.current(), u64::MAX);
    assert!(admissions.admit().is_none());
    assert_eq!(admissions.current(), u64::MAX);
    assert!(Eligibility::existing(u64::MAX).allows(final_admission));
    assert!(!Eligibility::existing(0).allows(final_admission));
}
