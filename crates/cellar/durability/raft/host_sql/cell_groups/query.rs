// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable cell queries on the existing node SQL workers.

use super::*;
use crate::raft::cell_groups::{CellImagePin, CellReadAuthority};
use cellar_logic::cell_group::CellVersion;
use std::panic::AssertUnwindSafe;

type VerificationCallback = Box<
    dyn FnOnce(&crate::storage::ImmutableQuery, &CellVersion) -> anyhow::Result<Vec<u8>> + Send,
>;

enum Callback {
    Receipt {
        class: Arc<crate::application::consensus::RaftClass>,
        request: crate::raft::execution::CommandRequest,
        input: Box<[u8]>,
    },
    Verification(VerificationCallback),
    Registered {
        class: Arc<crate::application::consensus::RaftClass>,
        contract: crate::application::consensus::OperationContract,
        input: Box<[u8]>,
    },
}

pub(super) struct Query {
    options: CellSqlOptions,
    authority: CellReadAuthority,
    callback: Callback,
    cancelled: Arc<AtomicBool>,
    closing: Arc<AtomicBool>,
    bytes: Arc<Bytes>,
}

pub struct CellSqlQueryResult {
    result: Vec<u8>,
    observation: crate::raft::version::CellVersionObservation,
    _bytes: Arc<Bytes>,
}

impl CellSqlQueryResult {
    pub fn result(&self) -> &[u8] {
        &self.result
    }

    pub fn observation(&self) -> &crate::raft::version::CellVersionObservation {
        &self.observation
    }

    pub fn version(&self) -> &CellVersion {
        &self.observation.version
    }
}

/// None means the selected image has no retained receipt, never non-commit.
pub struct CellSqlReceiptLookup {
    receipt: Option<crate::raft::execution::StoredReceipt>,
    observation: crate::raft::version::CellVersionObservation,
    _bytes: Arc<Bytes>,
}

impl CellSqlReceiptLookup {
    pub fn receipt(&self) -> Option<&crate::raft::execution::StoredReceipt> {
        self.receipt.as_ref()
    }

    pub fn observation(&self) -> &crate::raft::version::CellVersionObservation {
        &self.observation
    }
}

enum Output {
    Query(Vec<u8>),
    Receipt(Option<crate::raft::execution::StoredReceipt>),
}

impl Output {
    fn retained_bytes(&self) -> usize {
        match self {
            Self::Query(bytes) => bytes.len(),
            Self::Receipt(receipt) => receipt.as_ref().map_or(0, |r| r.result().len()),
        }
    }
}

struct Cancel(Arc<AtomicBool>);

impl Drop for Cancel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

impl CellGroupSqlPool {
    /// Consume an invocation's coherent pin. Queries can coexist with an
    /// unresolved private writer but read only the selected committed image.
    /// The registered dispatcher must enforce operation contracts before here.
    pub async fn query(
        &self,
        options: CellSqlOptions,
        authority: CellReadAuthority,
        callback: impl FnOnce(&crate::storage::ImmutableQuery, &CellVersion) -> anyhow::Result<Vec<u8>>
            + Send
            + 'static,
    ) -> Result<CellSqlQueryResult, ExecutionError> {
        match self
            .query_callback(
                options,
                authority,
                Callback::Verification(Box::new(callback)),
            )
            .await?
        {
            CellSqlOutcome::Queried(output) => Ok(output),
            _ => Err(ExecutionError::Invalid("cell query reply")),
        }
    }

    /// Invoke the namespace's compiled query against the exact selected cell
    /// image and version witness. The existing query owner enforces the original
    /// deadline, cancellation, read-only SQL, output budget and joined cleanup.
    pub async fn query_registered(
        &self,
        options: CellSqlOptions,
        authority: CellReadAuthority,
        class: Arc<crate::application::consensus::RaftClass>,
        mut contract: crate::application::consensus::OperationContract,
        input: Vec<u8>,
    ) -> Result<CellSqlQueryResult, ExecutionError> {
        if contract.operation.len() > 128 {
            return Err(ExecutionError::Registration(
                crate::application::consensus::RegistrationError::Invalid("class/operation name"),
            ));
        }
        contract.operation = contract.operation.into_boxed_str().into_string();
        if input.len() > options.limits().envelope.max_payload_bytes {
            return Err(overloaded());
        }
        match self
            .query_callback(
                options,
                authority,
                Callback::Registered {
                    class,
                    contract,
                    input: input.into_boxed_slice(),
                },
            )
            .await?
        {
            CellSqlOutcome::Queried(output) => Ok(output),
            _ => Err(ExecutionError::Invalid("cell query reply")),
        }
    }

    /// Inspect a committed image without a private writer or command callback.
    /// Contract, payload digest and authenticated command identity are checked
    /// before returning even a duplicate receipt. Missing state is not a proof.
    pub async fn lookup_receipt_registered(
        &self,
        options: CellSqlOptions,
        authority: CellReadAuthority,
        class: Arc<crate::application::consensus::RaftClass>,
        mut request: crate::raft::execution::CommandRequest,
        input: Vec<u8>,
    ) -> Result<CellSqlReceiptLookup, ExecutionError> {
        if authority.barrier().is_none() {
            return Err(ExecutionError::Invalid(
                "receipt lookup requires quorum read",
            ));
        }
        if request.operation.len() > 128
            || input.len() > options.limits().envelope.max_payload_bytes
        {
            return Err(overloaded());
        }
        request.operation = request.operation.into_boxed_str().into_string();
        match self
            .query_callback(
                options,
                authority,
                Callback::Receipt {
                    class,
                    request,
                    input: input.into_boxed_slice(),
                },
            )
            .await?
        {
            CellSqlOutcome::ReceiptLookedUp(output) => Ok(output),
            _ => Err(ExecutionError::Invalid("cell receipt lookup reply")),
        }
    }

    async fn query_callback(
        &self,
        options: CellSqlOptions,
        authority: CellReadAuthority,
        callback: Callback,
    ) -> Result<CellSqlOutcome, ExecutionError> {
        self.admit()?;
        let domain = &self.inner.workers[0].domain;
        if !authority.belongs_to_engine(&self.inner.engine_owner)
            || options.application_digest() != authority.route().schema_digest
        {
            return Err(ExecutionError::Invalid("cell query engine/schema"));
        }
        authority
            .check(domain.mono_ms().map_err(|_| unavailable())?)
            .map_err(crate::raft::execution::authority_failure)?;
        options.limits().validate()?;
        let input_bytes = match &callback {
            Callback::Verification(_) => 0,
            Callback::Registered { input, .. } | Callback::Receipt { input, .. } => input.len(),
        };
        let bytes = self.inner.admission.reserve_read(
            authority.route().history,
            options
                .limits()
                .max_result_bytes
                .checked_add(4096)
                .and_then(|n| n.checked_add(input_bytes))
                .ok_or_else(overloaded)?,
        )?;
        let cell = authority.cell().version.history.clone();
        let deadline = authority.deadline();
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = Cancel(cancelled.clone());
        let query = Query {
            options,
            authority,
            callback,
            cancelled,
            closing: self.inner.closing.clone(),
            bytes,
        };
        let timer = domain.sleep_until(deadline).map_err(|_| unavailable())?;
        crate::asyncrt::select! {
            result = self.call(cell, worker::Action::Query(Box::new(query))) => {
                match result? {
                    output @ (CellSqlOutcome::Queried(_) | CellSqlOutcome::ReceiptLookedUp(_)) => {
                        if domain.mono_ms().map_err(|_| unavailable())? >= deadline {
                            return Err(crate::raft::execution::storage(RuntimeError::DeadlineExceeded));
                        }
                        Ok(output)
                    }
                    _ => Err(ExecutionError::Invalid("cell query reply")),
                }
            }
            _ = timer => Err(crate::raft::execution::storage(RuntimeError::DeadlineExceeded)),
        }
    }
}

// The SQLite wrapper catches progress-hook panics. This wrapper allows the
// domain clock to be captured without claiming that arbitrary clock backends
// themselves implement unwind-safety; a clock failure interrupts the query.
struct QueryClock(AssertUnwindSafe<crate::asyncrt::DomainToken>);

impl QueryClock {
    fn expired(&self, deadline: u64) -> bool {
        self.0.mono_ms().map_or(true, |now| now >= deadline)
    }
}

pub(super) fn run(
    mut query: Query,
    domain: &crate::asyncrt::DomainToken,
) -> Result<CellSqlOutcome, ExecutionError> {
    let check = |query: &Query| {
        if query.cancelled.load(Ordering::Acquire) || query.closing.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        query
            .authority
            .check(domain.mono_ms().map_err(|_| unavailable())?)
            .map_err(crate::raft::execution::authority_failure)
    };
    check(&query)?;
    if let Callback::Registered {
        class,
        contract,
        input,
    } = &query.callback
    {
        crate::raft::execution::validate_registered_query(
            class,
            contract,
            input,
            query.authority.route().schema_digest,
            query.options.limits(),
        )?;
    }
    let database = match (&query.options, query.authority.image()) {
        (CellSqlOptions::Materialized(options), CellImagePin::Materialized(pin)) => {
            crate::storage::open_immutable_generation(
                pin.clone(),
                crate::storage::ImmutableQueryOptions {
                    services: options.query_services.clone(),
                    sqlite_vec: false,
                },
            )
        }
        (CellSqlOptions::Extents(options), CellImagePin::Extents(pin)) => {
            let image = pin
                .view()
                .image(
                    options.cache_directory.clone(),
                    options.cache_pool.clone(),
                    options.cache,
                )
                .map_err(crate::raft::execution::storage)?;
            crate::storage::open_immutable_extent_image(
                image,
                crate::storage::ImmutableQueryOptions {
                    services: options.query_services.clone(),
                    sqlite_vec: false,
                },
            )
        }
        _ => return Err(ExecutionError::Invalid("cell query image storage")),
    }
    .map_err(crate::raft::execution::storage)?;
    let clock = QueryClock(AssertUnwindSafe(domain.clone()));
    let deadline = query.authority.deadline();
    let cancelled = query.cancelled.clone();
    let closing = query.closing.clone();
    database
        .connection()
        .progress_handler(
            1000,
            Some(move || {
                cancelled.load(Ordering::Acquire)
                    || closing.load(Ordering::Acquire)
                    || clock.expired(deadline)
            }),
        )
        .map_err(crate::raft::execution::storage)?;
    check(&query)?;
    let observation = query.authority.cell().observation();
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| match query.callback {
        Callback::Verification(callback) => callback(&database, &observation.version)
            .map(Output::Query)
            .map_err(crate::raft::execution::storage),
        Callback::Registered {
            class,
            contract,
            input,
        } => class
            .invoke_cell_query(&contract, &observation, &database, &input)
            .map(Output::Query)
            .map_err(crate::raft::execution::storage),
        Callback::Receipt {
            class,
            request,
            input,
        } => {
            crate::raft::execution::validate_registered_command(
                query.options.limits(),
                query.authority.route().schema_digest,
                &observation.version.history,
                &request,
                &class,
                &input,
                false,
            )?;
            let receipt = crate::raft::receipts::lookup_cell_identity(
                database.connection(),
                &request,
                &query.options.limits(),
                observation.version.sequence.get(),
            )?;
            Ok(Output::Receipt(receipt))
        }
    }))
    .unwrap_or_else(|panic| {
        Err(crate::raft::execution::storage(
            crate::native_worker::panic_error(panic),
        ))
    })?;
    // The callback has been consumed, so check the remaining owners explicitly.
    if query.cancelled.load(Ordering::Acquire) || query.closing.load(Ordering::Acquire) {
        return Err(unavailable());
    }
    query
        .authority
        .check(domain.mono_ms().map_err(|_| unavailable())?)
        .map_err(crate::raft::execution::authority_failure)?;
    if result.retained_bytes() > query.options.limits().max_result_bytes {
        return Err(overloaded());
    }
    drop(database);
    drop(query.authority);
    Bytes::shrink(&mut query.bytes, result.retained_bytes() + 4096)?;
    Ok(match result {
        Output::Query(result) => CellSqlOutcome::Queried(CellSqlQueryResult {
            result,
            observation,
            _bytes: query.bytes,
        }),
        Output::Receipt(receipt) => CellSqlOutcome::ReceiptLookedUp(CellSqlReceiptLookup {
            receipt,
            observation,
            _bytes: query.bytes,
        }),
    })
}
