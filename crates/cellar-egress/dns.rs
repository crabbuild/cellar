// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! DNS policy for the run's network helper. Forwarding, when permitted, must
//! use a marked socket in the container network namespace, never host DNS.

use crate::{DnsFamily, Host, Rules};
use anyhow::{ensure, Context, Result};
use hickory_proto::{
    op::{Edns, Header, Message, MessageType, OpCode},
    rr::{
        rdata::{A, AAAA},
        DNSClass, RData, Record, RecordType,
    },
    serialize::binary::{BinDecodable, BinDecoder},
};
use std::net::{Ipv4Addr, Ipv6Addr};

/// Bound both UDP messages and length-prefixed TCP messages before decoding.
pub const MAX_MESSAGE_BYTES: usize = 4096;

#[derive(Debug, Eq, PartialEq)]
pub enum Disposition {
    Answer(Vec<u8>),
    Forward,
    /// Offline unmatched queries intentionally time out, matching the selected
    /// container interception contract. The caller must not synthesize NXDOMAIN.
    Drop,
}

/// Decode a single ordinary query, answer only granted A/AAAA names, and leave
/// other queries to the run's explicit internet policy. Synthetic records have
/// zero TTL so a later run or rule change does not inherit a cached grant.
pub fn resolve<T>(packet: &[u8], rules: &Rules<T>, internet: bool) -> Result<Disposition> {
    ensure!(
        packet.len() <= MAX_MESSAGE_BYTES,
        "DNS message exceeds limit"
    );
    let mut decoder = BinDecoder::new(packet);
    let header = Header::read(&mut decoder).context("invalid DNS header")?;
    // Hickory allocates section vectors using header counts. Check the shape
    // before its full decode, even when the actual packet is very short.
    ensure!(
        header.counts.queries == 1
            && header.counts.answers == 0
            && header.counts.authorities == 0
            && header.counts.additionals <= 1,
        "unsupported DNS query shape"
    );
    ensure!(
        header.metadata.message_type == MessageType::Query
            && header.metadata.op_code == OpCode::Query
            && !header.metadata.truncation,
        "unsupported DNS query operation"
    );
    let mut decoder = BinDecoder::new(packet);
    let query = Message::read(&mut decoder).context("invalid DNS query")?;
    ensure!(decoder.is_empty(), "trailing DNS query bytes");
    ensure!(
        query.additionals.is_empty() && query.signature.is_none(),
        "unsupported DNS query records"
    );

    let fallback = if internet {
        Disposition::Forward
    } else {
        Disposition::Drop
    };
    if query.edns.as_ref().is_some_and(|edns| edns.version() != 0) {
        return Ok(fallback);
    }
    let question = &query.queries[0];
    if question.query_class() != DNSClass::IN {
        return Ok(fallback);
    }
    let family = match question.query_type() {
        RecordType::A => DnsFamily::V4,
        RecordType::AAAA => DnsFamily::V6,
        _ => return Ok(fallback),
    };
    let Ok(host) = Host::parse(&question.name().to_ascii()) else {
        return Ok(fallback);
    };
    if !rules.resolves(&host, family) {
        return Ok(fallback);
    }

    let data = match family {
        DnsFamily::V4 => RData::A(A(Ipv4Addr::new(127, 0, 0, 2))),
        DnsFamily::V6 => RData::AAAA(AAAA(Ipv6Addr::LOCALHOST)),
    };
    let mut answer = Message::response(query.metadata.id, OpCode::Query);
    answer.metadata.recursion_desired = query.metadata.recursion_desired;
    answer.queries.push(question.clone());
    answer
        .answers
        .push(Record::from_rdata(question.name().clone(), 0, data));
    if let Some(edns) = query.edns {
        let mut response_edns = Edns::new();
        response_edns.set_max_payload(edns.max_payload().clamp(512, MAX_MESSAGE_BYTES as u16));
        answer.set_edns(response_edns);
    }
    let bytes = answer.to_vec().context("encode intercepted DNS answer")?;
    ensure!(
        bytes.len() <= 512,
        "intercepted DNS answer exceeds baseline payload"
    );
    Ok(Disposition::Answer(bytes))
}

#[cfg(test)]
#[path = "dns_tests.rs"]
mod tests;
