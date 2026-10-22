//! Shared business operations and HTTP normalization for both example hosts.
use anyhow::{ensure, Result};
use cellar::sdk::{consensus::*, Application, Invocation, Output, RaftClass, Registry};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Duration,
};
use subtle::ConstantTimeEq;

pub struct CounterApi {
    pub cells: Mutex<Option<RaftCells>>,
    pub increment: OperationContract,
    pub get: OperationContract,
    secret: String,
    namespace: NamespaceId,
}

pub struct Reply {
    pub status: u16,
    pub body: Vec<u8>,
}

enum Operation {
    Command(i64, CommandOptions),
    Query(QueryOptions),
}

const READ_HEADERS: [&str; 7] = [
    "x-read-consistency",
    "x-min-version",
    "x-read-policy",
    "x-query-timeout-ms",
    "x-query-members",
    "x-query-attempt-ms",
    "x-leader-fallback",
];

fn header<'a>(headers: &'a [(String, String)], key: &str) -> Result<Option<&'a str>> {
    let mut values = headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case(key));
    let value = values.next().map(|(_, value)| value.as_str());
    ensure!(values.next().is_none(), "duplicate header");
    Ok(value)
}

fn read_options(headers: &[(String, String)]) -> Result<QueryOptions> {
    let minimum = header(headers, "x-min-version")?;
    let consistency = match header(headers, "x-read-consistency")?.unwrap_or("stale") {
        "stale" => QueryConsistency::Stale,
        "linearizable" => QueryConsistency::Linearizable,
        "at-least" => QueryConsistency::AtLeast(VersionToken::from_encoded(
            minimum.ok_or_else(|| anyhow::anyhow!("provide x-min-version"))?,
        )?),
        _ => anyhow::bail!("invalid read consistency"),
    };
    ensure!(
        minimum.is_none() || matches!(consistency, QueryConsistency::AtLeast(_)),
        "unexpected minimum version"
    );
    let policy = header(headers, "x-read-policy")?;
    ensure!(
        policy.is_none() || matches!(consistency, QueryConsistency::Stale),
        "read policy requires stale consistency"
    );
    let stale_policy = match policy.unwrap_or("recent-source") {
        "recent-source" => StaleReadPolicy::RecentSource,
        "disconnected-historical" => StaleReadPolicy::DisconnectedHistorical,
        _ => anyhow::bail!("invalid stale read policy"),
    };
    let timeout_ms = header(headers, "x-query-timeout-ms")?
        .unwrap_or("5000")
        .parse::<u64>()?;
    ensure!((1..=5000).contains(&timeout_ms), "invalid query timeout");
    let members = header(headers, "x-query-members")?;
    let attempt = header(headers, "x-query-attempt-ms")?;
    let fallback = header(headers, "x-leader-fallback")?;
    let routing = if let Some(members) = members {
        let mut selected = Vec::new();
        let mut unique = BTreeSet::new();
        for value in members.split(',').take(9) {
            let id = value.parse::<u64>()?;
            ensure!(id.to_string() == value, "noncanonical member ID");
            let member = MemberId::new(id)?;
            ensure!(
                selected.len() < 8 && unique.insert(member),
                "invalid member list"
            );
            selected.push(member);
        }
        let attempt_ms = attempt
            .map(str::parse::<u64>)
            .transpose()?
            .unwrap_or(250.min(timeout_ms));
        ensure!(
            (1..=timeout_ms).contains(&attempt_ms),
            "invalid attempt timeout"
        );
        let leader_fallback = match fallback.unwrap_or("false") {
            "true" => true,
            "false" => false,
            _ => anyhow::bail!("invalid fallback policy"),
        };
        QueryRouting::Replicas {
            members: selected,
            attempt_timeout: Duration::from_millis(attempt_ms),
            leader_fallback,
        }
    } else {
        ensure!(
            attempt.is_none() && fallback.is_none(),
            "routing policy requires members"
        );
        QueryRouting::Local
    };
    Ok(QueryOptions {
        consistency,
        timeout: Duration::from_millis(timeout_ms),
        stale_policy,
        routing,
    })
}

pub fn definition() -> Result<RaftClass> {
    Ok(RaftClass::new("Counter", 1, [21; 32])?
        .command("Increment", [22; 32], |context, input| {
            let delta = i64::from_be_bytes(input.try_into()?);
            context.sql().execute_batch("CREATE TABLE IF NOT EXISTS counter(value INTEGER NOT NULL); INSERT INTO counter SELECT 0 WHERE NOT EXISTS(SELECT 1 FROM counter)")?;
            let value: i64 = context.sql().query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            let value = value.checked_add(delta).ok_or_else(|| anyhow::anyhow!("counter overflow"))?;
            context.sql().execute("UPDATE counter SET value=?1", [value])?;
            Ok(value.to_be_bytes().to_vec())
        })?
        .query("Get", [23; 32], |context, _| {
            let value: i64 = context.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?)
}

impl CounterApi {
    pub fn new(secret: String, namespace: NamespaceId, class: &RaftClass) -> Result<Arc<Self>> {
        ensure!(
            secret.len() >= 16 && !secret.contains(['\r', '\n']),
            "use an HTTP bearer secret of at least 16 bytes"
        );
        Ok(Arc::new(Self {
            cells: Mutex::new(None),
            increment: class.contract("Increment").unwrap(),
            get: class.contract("Get").unwrap(),
            secret,
            namespace,
        }))
    }

    /// The demo bearer grants access to its fixed namespace. Real applications
    /// must bind authenticated principals and allowed cells at their ingress.
    pub async fn request(&self, method: &str, url: &str, headers: &[(String, String)]) -> Reply {
        let text = |status, text: &str| Reply {
            status,
            body: text.as_bytes().to_vec(),
        };
        let authorized = header(headers, "authorization")
            .ok()
            .flatten()
            .is_some_and(|value| {
                value
                    .strip_prefix("Bearer ")
                    .is_some_and(|token| bool::from(token.as_bytes().ct_eq(self.secret.as_bytes())))
            });
        if !authorized {
            return text(401, "unauthorized");
        }
        let service = match self.cells.lock().ok().and_then(|cells| cells.clone()) {
            Some(service) if !service.is_closed() => service,
            _ => return text(503, "Raft bindings are not ready"),
        };
        let parsed = (|| -> Result<_> {
            ensure!(url.len() <= 4096, "URL too large");
            let url = url::Url::parse(url)?;
            let name = url
                .path()
                .strip_prefix("/counters/")
                .filter(|name| !name.is_empty() && !name.contains('/'))
                .ok_or_else(|| anyhow::anyhow!("use /counters/one or /counters/two"))?;
            let cell = service.cell(self.namespace, &format!("Counter:{name}"))?;
            let mut contract = if method == "POST" {
                self.increment.clone()
            } else {
                self.get.clone()
            };
            if let Some(value) = header(headers, "x-contract-digest")? {
                contract.digest = hex::decode(value)?
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("invalid digest"))?;
            }
            if method == "GET" {
                ensure!(
                    url.query().is_none(),
                    "query parameters unsupported for GET"
                );
                return Ok((cell, contract, Operation::Query(read_options(headers)?)));
            }
            ensure!(method == "POST", "use GET or POST");
            for key in READ_HEADERS {
                ensure!(header(headers, key)?.is_none(), "read options require GET");
            }
            let mut query = url.query_pairs();
            let (key, value) = query
                .next()
                .ok_or_else(|| anyhow::anyhow!("provide delta"))?;
            ensure!(
                key == "delta" && query.next().is_none(),
                "provide one delta"
            );
            let delta: i64 = value.parse()?;
            let id = header(headers, "x-command-id")?
                .ok_or_else(|| anyhow::anyhow!("provide x-command-id CLIENT_HEX:SEQUENCE"))?;
            let (client, sequence) = id
                .split_once(':')
                .ok_or_else(|| anyhow::anyhow!("invalid command ID"))?;
            let client: [u8; 16] = hex::decode(client)?
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid client"))?;
            let id = CommandId::new(ClientId::new(client)?, sequence.parse()?)?;
            let retry = header(headers, "x-retry-until-ms")?
                .ok_or_else(|| anyhow::anyhow!("provide x-retry-until-ms"))?
                .parse()?;
            Ok((
                cell,
                contract,
                Operation::Command(delta, CommandOptions::new(id, retry)),
            ))
        })();
        let (cell, contract, operation) = match parsed {
            Ok(value) => value,
            Err(_) => return text(400, "invalid request or unavailable cell"),
        };
        let result = match operation {
            Operation::Command(delta, options) => cell
                .command(contract, delta.to_be_bytes().to_vec(), options)
                .await
                .map(|receipt| (receipt.result, receipt.version, receipt.version_token, None)),
            Operation::Query(options) => {
                cell.query(contract, vec![], options).await.map(|result| {
                    (
                        result.result,
                        result.version,
                        result.version_token,
                        Some(result.served_by.get()),
                    )
                })
            }
        };
        match result {
            Ok((value, version, token, served_by)) => {
                let Ok(value) = value.try_into().map(i64::from_be_bytes) else {
                    return text(500, "invalid counter result");
                };
                Reply { status: 200, body: serde_json::json!({ "value": value, "index": version.position.index.get(), "term": version.position.term.get(), "group": hex::encode(version.scope.group.bytes()), "incarnation": hex::encode(version.scope.incarnation.bytes()), "version_token": token.as_str(), "served_by": served_by }).to_string().into_bytes() }
            }
            Err(error) => {
                let status = match error {
                    CommandFailure::CommandConflict
                    | CommandFailure::CommandExpired
                    | CommandFailure::SequenceGap { .. }
                    | CommandFailure::WrongIncarnation => 409,
                    CommandFailure::UnsupportedCapability
                    | CommandFailure::UnsupportedConsistency => 422,
                    CommandFailure::CorruptState => 500,
                    _ => 503,
                };
                // Preserve Ambiguous plus its original ID; no ingress retries a
                // mutation or turns it into a fresh command identity.
                Reply {
                    status,
                    body: format!("{error:?}").into_bytes(),
                }
            }
        }
    }
}

pub fn application(api: Arc<CounterApi>, class: RaftClass) -> Result<Arc<dyn Application>> {
    Ok(Arc::new(
        Registry::new(move |context, event| {
            let api = api.clone();
            Box::pin(async move {
                let Invocation::Fetch(request) = event else {
                    return Ok(Output::Done);
                };
                let reply = api
                    .request(&request.method, &request.url, &request.headers)
                    .await;
                Ok(Output::Http(
                    cellar::sdk::http::Response::new(
                        reply.status,
                        reply.body,
                        vec![("content-type".into(), "application/json".into())],
                    )
                    .into_http(&context)?,
                ))
            })
        })
        .raft_class(class)?,
    ))
}

pub fn router(api: Arc<CounterApi>) -> axum::Router {
    axum::Router::new().route(
        "/counters/{name}",
        axum::routing::any(move |request: axum::extract::Request| {
            let api = api.clone();
            async move {
                let headers = request
                    .headers()
                    .iter()
                    .map(|(name, value)| {
                        value
                            .to_str()
                            .map(|value| (name.to_string(), value.to_owned()))
                    })
                    .collect::<Result<Vec<_>, _>>();
                let Ok(headers) = headers else {
                    return axum::http::Response::builder()
                        .status(400)
                        .body(axum::body::Body::empty())
                        .unwrap();
                };
                let reply = api
                    .request(
                        request.method().as_str(),
                        &format!("http://counter{}", request.uri()),
                        &headers,
                    )
                    .await;
                axum::http::Response::builder()
                    .status(reply.status)
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(reply.body))
                    .unwrap()
            }
        }),
    )
}
