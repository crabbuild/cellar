// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::HttpTarget;
use hickory_proto::{op::Query, rr::Name};

#[test]
fn upstream_responses_require_matching_identity_and_bounded_record_counts() -> Result<()> {
    let query = question("backup.internal", RecordType::A);
    let request = query.to_vec()?;
    let mut reply = Message::response(query.metadata.id, OpCode::Query);
    reply.queries = query.queries.clone();
    // A truncated UDP reply must remain available for the client's TCP retry.
    reply.metadata.truncation = true;
    validate_response(&request, &reply.to_vec()?)?;
    let mut foreign = reply.clone();
    foreign.metadata.id += 1;
    assert!(validate_response(&request, &foreign.to_vec()?).is_err());
    let mut foreign = reply.clone();
    foreign.queries = question("other.internal", RecordType::A).queries;
    assert!(validate_response(&request, &foreign.to_vec()?).is_err());
    let mut foreign = reply.clone();
    foreign.metadata.message_type = MessageType::Query;
    assert!(validate_response(&request, &foreign.to_vec()?).is_err());
    let mut hostile = reply.to_vec()?;
    hostile[6..8].copy_from_slice(&u16::MAX.to_be_bytes());
    assert!(validate_response(&request, &hostile).is_err());
    assert!(validate_response(&request, &vec![0; MAX_MESSAGE_BYTES + 1]).is_err());
    let valid = reply.to_vec()?;
    for length in 0..valid.len() {
        assert!(validate_response(&request, &valid[..length]).is_err());
    }
    let mut trailing = valid;
    trailing.push(0);
    assert!(validate_response(&request, &trailing).is_err());
    Ok(())
}

fn question(host: &str, kind: RecordType) -> Message {
    let mut query = Message::new(0x8a3f, MessageType::Query, OpCode::Query);
    query.metadata.recursion_desired = true;
    let mut name = Name::from_ascii(host).unwrap();
    // Wire questions are absolute names, including inputs without a final dot.
    name.set_fqdn(true);
    query.queries.push(Query::query(name, kind));
    query
}

fn grant(rules: &mut Rules<()>, target: HttpTarget) {
    let update = rules.prepare(target, ()).unwrap();
    rules.commit(update).unwrap();
}

fn answer(query: &Message, rules: &Rules<()>) -> Message {
    let Disposition::Answer(bytes) = resolve(&query.to_vec().unwrap(), rules, false).unwrap()
    else {
        panic!("granted query received no answer");
    };
    Message::from_vec(&bytes).unwrap()
}

#[test]
fn synthetic_records_preserve_question_but_never_claim_dnssec_or_cache_grants() {
    let mut rules = Rules::default();
    grant(&mut rules, HttpTarget::parse("*.internal").unwrap());
    for (kind, expected) in [
        (RecordType::A, RData::A(A(Ipv4Addr::new(127, 0, 0, 2)))),
        (RecordType::AAAA, RData::AAAA(AAAA(Ipv6Addr::LOCALHOST))),
    ] {
        let mut query = question("BackUP.Internal.", kind);
        query.metadata.authentic_data = true;
        query.metadata.checking_disabled = true;
        let mut edns = Edns::new();
        edns.set_max_payload(u16::MAX).set_dnssec_ok(true);
        query.set_edns(edns);
        let answer = answer(&query, &rules);
        assert_eq!(answer.metadata.id, query.metadata.id);
        assert_eq!(answer.metadata.message_type, MessageType::Response);
        assert!(answer.metadata.recursion_desired);
        assert!(!answer.metadata.authentic_data);
        assert!(!answer.metadata.checking_disabled);
        assert!(!answer.metadata.recursion_available);
        assert_eq!(answer.queries, query.queries);
        assert_eq!(answer.answers.len(), 1);
        assert_eq!(answer.answers[0].data, expected);
        assert_eq!(answer.answers[0].ttl, 0);
        let edns = answer.edns.unwrap();
        assert_eq!(edns.version(), 0);
        assert_eq!(edns.max_payload(), MAX_MESSAGE_BYTES as u16);
        assert!(!edns.flags().dnssec_ok);
    }
}

#[test]
fn offline_resolution_follows_current_names_and_family_specific_zero_prefixes() {
    let mut rules = Rules::default();
    grant(&mut rules, HttpTarget::parse("192.0.2.0/24").unwrap());
    let a = question("backup.internal", RecordType::A).to_vec().unwrap();
    let aaaa = question("backup.internal", RecordType::AAAA)
        .to_vec()
        .unwrap();
    assert_eq!(resolve(&a, &rules, false).unwrap(), Disposition::Drop);
    assert_eq!(resolve(&a, &rules, true).unwrap(), Disposition::Forward);
    grant(&mut rules, HttpTarget::parse("0.0.0.0/0").unwrap());
    assert!(matches!(
        resolve(&a, &rules, false).unwrap(),
        Disposition::Answer(_)
    ));
    assert_eq!(resolve(&aaaa, &rules, false).unwrap(), Disposition::Drop);
    grant(&mut rules, HttpTarget::parse("::/0").unwrap());
    assert!(matches!(
        resolve(&aaaa, &rules, false).unwrap(),
        Disposition::Answer(_)
    ));
    rules.retire().unwrap();
    assert_eq!(resolve(&a, &rules, false).unwrap(), Disposition::Drop);
    grant(&mut rules, HttpTarget::all());
    assert!(matches!(
        resolve(&aaaa, &rules, false).unwrap(),
        Disposition::Answer(_)
    ));
}

#[test]
fn unsupported_queries_obey_explicit_fallback_and_malformed_queries_never_forward() {
    let mut rules = Rules::default();
    grant(&mut rules, HttpTarget::all());
    let txt = question("backup.internal", RecordType::TXT)
        .to_vec()
        .unwrap();
    assert_eq!(resolve(&txt, &rules, false).unwrap(), Disposition::Drop);
    assert_eq!(resolve(&txt, &rules, true).unwrap(), Disposition::Forward);
    let mut query = question("backup.internal", RecordType::A);
    let mut edns = Edns::new();
    edns.set_version(1);
    query.set_edns(edns);
    assert_eq!(
        resolve(&query.to_vec().unwrap(), &rules, false).unwrap(),
        Disposition::Drop
    );
    let valid = question("backup.internal", RecordType::A).to_vec().unwrap();
    for length in 0..valid.len() {
        assert!(
            resolve(&valid[..length], &rules, true).is_err(),
            "accepted truncated DNS packet at {length}"
        );
    }
    let mut trailing = valid.clone();
    trailing.push(0);
    assert!(resolve(&trailing, &rules, true).is_err());
    for range in [4..6, 6..8, 8..10, 10..12] {
        let mut huge_count = valid.clone();
        huge_count[range].copy_from_slice(&u16::MAX.to_be_bytes());
        assert!(resolve(&huge_count, &rules, true).is_err());
    }
    assert!(resolve(&vec![0; MAX_MESSAGE_BYTES + 1], &rules, true).is_err());
    let mut response = valid.clone();
    response[2] |= 0x80;
    assert!(resolve(&response, &rules, true).is_err());
    let mut cycle = valid;
    cycle[12] = 0xc0;
    cycle[13] = 12;
    assert!(resolve(&cycle, &rules, true).is_err());
}

#[test]
fn longest_valid_question_fits_baseline_dns_response() {
    let host = [
        "a".repeat(63),
        "b".repeat(63),
        "c".repeat(63),
        "d".repeat(61),
    ]
    .join(".");
    assert_eq!(host.len(), 253);
    let mut rules = Rules::default();
    grant(&mut rules, HttpTarget::all());
    let query = question(&host, RecordType::AAAA);
    let answer = answer(&query, &rules);
    assert_eq!(&answer.answers[0].name, query.queries[0].name());
}
