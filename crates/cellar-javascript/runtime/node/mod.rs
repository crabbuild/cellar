// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual Node.js process backend. Process and IPC ownership live at bootstrap,
//! outside the native execution domain; SDK effects remain on native invocations.
use super::*;
use anyhow::Context as _;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
    io::Write,
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Instant,
};
mod package;
mod transport;
use transport::Transport;

/// Trusted application process configuration. The module and its node_modules
/// must be deployed together. Node 24 or later is required. Node OS APIs are
/// ordinary external effects, outside Cellar's durability and cancellation proof.
#[derive(Clone, Debug)]
pub struct NodeOptions {
    /// Node executable path, or a command name resolved through PATH.
    pub executable: PathBuf,
    /// Absolute path to the built application module imported by the worker.
    pub entry: PathBuf,
    /// Optional absolute working directory for packages using relative OS paths.
    /// When absent, the process inherits the host's working directory.
    pub working_directory: Option<PathBuf>,
    /// Explicit additions to inherited environment. NODE_OPTIONS and NODE_PATH
    /// are always removed to keep runtime startup and package resolution explicit.
    pub environment: BTreeMap<String, String>,
    /// Maximum time without a responsive Node event loop (not handler duration).
    pub heartbeat_timeout: Duration,
}

impl NodeOptions {
    /// Use `node` from PATH with default supervision and an explicit entry module.
    pub fn new(entry: impl Into<PathBuf>) -> Self {
        Self {
            executable: "node".into(),
            entry: entry.into(),
            working_directory: None,
            environment: BTreeMap::new(),
            heartbeat_timeout: Duration::from_secs(2),
        }
    }
}

/// Starts Node before admitting the host; kills and waits for the owned process
/// on every exit path. A failed worker stops the application, without replay.
pub struct NodeRuntime;

impl NodeRuntime {
    /// Start and authenticate the worker, then call `host` after module validation.
    /// The owned process group is terminated and reaped when the host exits.
    pub fn run<T>(
        options: Options,
        node: NodeOptions,
        registrations: Source,
        host: impl FnOnce(Arc<JavaScriptApplication>) -> Result<T>,
    ) -> Result<T> {
        ensure!(
            cfg!(unix),
            "Node process supervision currently requires Unix"
        );
        ensure!(
            node.heartbeat_timeout >= Duration::from_millis(100)
                && node.heartbeat_timeout <= Duration::from_secs(60),
            "invalid Node heartbeat timeout"
        );
        ensure!(
            node.entry.is_absolute(),
            "Node entry must be an absolute path"
        );
        ensure!(
            node.working_directory
                .as_ref()
                .is_none_or(|path| path.is_absolute()),
            "Node working directory must be absolute"
        );
        run_with(
            options,
            registrations,
            host,
            move |runtime, source, starts, replies, ready| {
                run(runtime, node, source, starts, replies, ready)
            },
        )
    }
}

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            // SAFETY: spawn creates a fresh process group with the child's PID.
            // The unreaped owned child keeps that PID from reuse until wait.
            unsafe {
                libc::kill(-(self.0.id() as i32), libc::SIGKILL);
            }
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct InvocationState {
    start: Start,
    operations: usize,
    pending: HashMap<u64, PendingOperation>,
    response: Option<u64>,
    mutation: bool,
}

struct PendingOperation {
    mutation: bool,
    cancellation: Option<cellar::sdk::web::AbortController>,
}

fn number(value: &Value, field: &str) -> Result<u64> {
    value[field]
        .as_u64()
        .filter(|n| *n <= 9_007_199_254_740_991)
        .context("invalid Node protocol identity")
}

fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .context("invalid Node protocol string")
}

fn reject(transport: &mut Transport, invocation: u64, id: u64, code: &str) -> Result<()> {
    // Only refusals before native admission can promise `not_committed`.
    transport.send(json!({
        "type": "reply",
        "invocation": invocation,
        "id": id,
        "payload": json!({
            "ok": false,
            "code": code,
            "outcome": "not_committed"
        }).to_string()
    }))
}

fn run(
    runtime: Arc<Runtime>,
    node: NodeOptions,
    source: Source,
    starts: mpsc::Receiver<Start>,
    replies: mpsc::Receiver<Reply>,
    ready: mpsc::SyncSender<Result<()>>,
) -> Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    listener.set_nonblocking(true)?;
    let mut secret = [0_u8; 32];
    getrandom::fill(&mut secret)
        .map_err(|_| anyhow::anyhow!("Node IPC authentication initialization failed"))?;
    let secret = secret
        .iter()
        .map(|v| format!("{v:02x}"))
        .collect::<String>();

    // The factory is captured before importing application modules. The channel
    // secret is delivered only over inherited stdin, never argv or the environment.
    let script = format!(
        "const makeBudgets = ({});\nconst makeRunner = ({});\nconst ioSource = {};\n{}",
        include_str!("budget.js"),
        include_str!("../runner.js"),
        serde_json::to_string(include_str!("io.mjs"))?,
        include_str!("worker.mjs")
    );
    let mut command = Command::new(&node.executable);
    if let Some(directory) = &node.working_directory {
        command.current_dir(directory);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let deadline = Instant::now() + runtime.options.startup_timeout;
    let mut process = Process(
        command
            .arg(format!(
                "--max-old-space-size={}",
                runtime.options.heap_bytes / (1024 * 1024)
            ))
            .args(["--input-type=module", "--eval", &script])
            .envs(&node.environment)
            .env_remove("NODE_OPTIONS")
            .env_remove("NODE_PATH")
            .stdin(Stdio::piped())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .context("could not start Node executable")?,
    );

    let config = json!({
        "port": listener.local_addr()?.port(),
        "secret": secret,
        "entry": node.entry,
        "descriptor": source.descriptor().to_string(),
        "maximum": runtime.options.max_objects,
        "lifetimeLimit": runtime.options.max_batch_operations,
        "valueLimit": runtime.options.max_value_bytes,
        "cloneLimit": runtime.options.max_body_bytes.min(runtime.options.max_value_bytes),
        "bridgeLimit": runtime.options.max_bridge_bytes,
        "pendingLimit": runtime.options.max_pending_operations,
        "operationLimit": runtime.options.max_operations,
        "invocationLimit": runtime.options.max_invocations,
        "synchronousTimeout": node.heartbeat_timeout.as_millis()
    });
    let mut stdin = process.0.stdin.take().context("Node stdin missing")?;
    let config = serde_json::to_vec(&config)?;
    ensure!(
        config.len() <= runtime.options.max_value_bytes,
        "Node bootstrap configuration exceeds limit"
    );
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: stdin owns this live pipe descriptor; fcntl only changes its flags.
        let flags = unsafe { libc::fcntl(stdin.as_raw_fd(), libc::F_GETFL) };
        ensure!(flags >= 0, "Node bootstrap pipe flags unavailable");
        // SAFETY: same owned live descriptor, preserving its existing flags.
        ensure!(
            unsafe { libc::fcntl(stdin.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } >= 0,
            "Node bootstrap pipe configuration failed"
        );
    }

    let mut remaining = config.as_slice();
    while !remaining.is_empty() {
        ensure!(Instant::now() < deadline, "Node bootstrap timed out");
        match stdin.write(remaining) {
            Ok(0) => anyhow::bail!("Node bootstrap pipe closed"),
            Ok(n) => remaining = &remaining[n..],
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1))
            }
            Err(error) => return Err(error.into()),
        }
    }
    // EOF completes the bootstrap document; the worker then opens the IPC socket.
    drop(stdin);
    let socket = loop {
        ensure!(Instant::now() < deadline, "Node startup timed out");
        match listener.accept() {
            Ok((socket, _)) => break socket,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1))
            }
            Err(error) => return Err(error.into()),
        }
    };
    drop(listener);

    // JSON string escaping can expand an encoded payload by up to 6x.
    let maximum = runtime
        .options
        .max_value_bytes
        .checked_mul(6)
        .and_then(|n| n.checked_add(4096))
        .context("Node frame limit overflow")?;
    let mut transport = Transport::new(
        socket,
        maximum.min(runtime.options.max_bridge_bytes),
        runtime.options.max_bridge_bytes,
    )?;

    let mut authenticated = false;
    let mut initialized = false;
    let mut heartbeat = Instant::now();
    let mut last_check = heartbeat;
    let mut invocations = HashMap::<u64, InvocationState>::new();
    let mut objects = HashMap::<u64, Life>::new();
    let mut last_operation = 0;
    let mut last_budget = 0;
    let mut budgets = super::budgets::Budgets::default();
    loop {
        if runtime.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        let now = Instant::now();
        let loop_gap = now.saturating_duration_since(last_check);
        let heartbeat_gap = now.saturating_duration_since(heartbeat);
        last_check = now;
        ensure!(
            if initialized {
                heartbeat_gap < node.heartbeat_timeout
            } else {
                now < deadline
            },
            "Node event loop or initialization timed out: initialized={initialized}, heartbeat_gap_ms={}, observer_gap_ms={}, input_ready={:?}, invocations={}, objects={}",
            heartbeat_gap.as_millis(), loop_gap.as_millis(), transport.input_ready().ok(), invocations.len(), objects.len()
        );

        // Retire authority before processing any newly received operations.
        let cancelled: Vec<_> = invocations
            .iter()
            .filter(|(_, state)| {
                !state.start.active.load(Ordering::Acquire)
                    || !state.start.owner.load(Ordering::Acquire)
            })
            .map(|(&id, _)| id)
            .collect();
        for id in cancelled {
            invocations.remove(&id);
            budgets.retire(id);
            transport.send(json!({"type":"cancel","id":id}))?;
        }
        let retired: Vec<_> = objects
            .iter()
            .filter(|(_, life)| !life.load(Ordering::Acquire))
            .map(|(&id, _)| id)
            .collect();
        for id in retired {
            objects.remove(&id);
            transport.send(json!({"type":"retire","id":id}))?;
        }

        // Resolve existing work before admitting more calls into the worker.
        for reply in replies
            .try_iter()
            .take(runtime.options.max_invocations * runtime.options.max_pending_operations)
        {
            if let Some(state) = invocations.get_mut(&reply.invocation) {
                if let Some(operation) = state.pending.remove(&reply.id) {
                    if operation.mutation {
                        state.mutation = false;
                    }
                    transport.send_parts(
                        json!({
                            "type": "reply",
                            "invocation": reply.invocation,
                            "id": reply.id,
                            "payload": reply.payload
                        }),
                        reply.body.as_deref(),
                    )?;
                }
            }
        }

        if initialized {
            for start in starts.try_iter().take(runtime.options.max_invocations) {
                if !start.active.load(Ordering::Acquire) || !start.owner.load(Ordering::Acquire) {
                    continue;
                }
                let payload: Value = serde_json::from_str(&start.payload)?;
                if let Some(object) = payload["object"].as_u64() {
                    if !objects.contains_key(&object)
                        && objects.len() >= runtime.options.max_objects
                    {
                        let _ = start.sender.try_send(Event::Complete {
                            payload: "null".into(),
                            body: None,
                            failed: true,
                            _bytes: runtime.reserve(4)?,
                        });
                        continue;
                    }
                    objects.insert(object, start.owner.clone());
                }
                ensure!(
                    invocations.len() < runtime.options.max_invocations,
                    "Node invocation capacity exhausted"
                );
                transport.send_parts(
                    json!({
                        "type": "start",
                        "id": start.id,
                        "payload": start.payload
                    }),
                    start.body.as_deref(),
                )?;
                invocations.insert(
                    start.id,
                    InvocationState {
                        start,
                        operations: 0,
                        pending: HashMap::new(),
                        response: None,
                        mutation: false,
                    },
                );
            }
        }

        transport.flush()?;
        // Bound each receive pass so a busy peer cannot starve shutdown or replies.
        for _ in 0..256 {
            let Some(frame) = transport.receive()? else {
                break;
            };
            let message = frame.metadata;
            let mut body = frame.body;
            ensure!(
                body.is_none()
                    || (initialized
                        && matches!(message["type"].as_str(), Some("operation" | "complete"))),
                "unexpected Node byte attachment"
            );
            if !authenticated {
                ensure!(
                    message["type"] == "hello" && message["secret"] == secret,
                    "Node authentication failed"
                );
                authenticated = true;
                continue;
            }
            match string(&message, "type")? {
                "ready" => {
                    ensure!(!initialized, "duplicate Node initialization");
                    initialized = true;
                    heartbeat = Instant::now();
                    let _ = ready.send(Ok(()));
                }
                "heartbeat" => heartbeat = Instant::now(),
                "budget_start" if initialized => {
                    let invocation = number(&message, "invocation")?;
                    let id = number(&message, "id")?;
                    ensure!(id > last_budget, "Node budget identity reused");
                    last_budget = id;
                    let result = match invocations.get(&invocation).filter(|state| {
                        state.start.active.load(Ordering::Acquire)
                            && state.start.owner.load(Ordering::Acquire)
                    }) {
                        Some(state) => budgets.start(
                            invocation,
                            id,
                            state.start.budget_source.as_ref(),
                            runtime.options.max_pending_operations,
                        ),
                        None => Err("ContextExpired"),
                    };
                    let reply = match result {
                        Ok(remaining) => {
                            json!({"type":"budget_reply","invocation":invocation,"id":id,"ok":true,"remaining_ms":remaining})
                        }
                        Err(code) => {
                            json!({"type":"budget_reply","invocation":invocation,"id":id,"ok":false,"code":code})
                        }
                    };
                    transport.send(reply)?;
                }
                "budget_end" if initialized => {
                    budgets.release(number(&message, "invocation")?, number(&message, "id")?);
                }
                "cancel_operation" if initialized => {
                    let invocation = number(&message, "invocation")?;
                    let id = number(&message, "id")?;
                    if let Some(state) = invocations.get(&invocation).filter(|state| {
                        state.start.active.load(Ordering::Acquire)
                            && state.start.owner.load(Ordering::Acquire)
                    }) {
                        if let Some(controller) = state
                            .pending
                            .get(&id)
                            .and_then(|operation| operation.cancellation.as_ref())
                        {
                            controller.abort(None)?;
                        }
                    }
                }
                "operation" if initialized => {
                    let invocation = number(&message, "invocation")?;
                    let id = number(&message, "id")?;
                    ensure!(id > last_operation, "Node operation identity reused");
                    last_operation = id;
                    let name = string(&message, "name")?;
                    let payload = string(&message, "payload")?;
                    let callback_scope = message
                        .get("callback_scope")
                        .filter(|value| !value.is_null())
                        .map(|_| number(&message, "callback_scope"))
                        .transpose()?;
                    let synchronous = message
                        .get("synchronous")
                        .map(|v| v.as_bool().context("invalid Node synchronous flag"))
                        .transpose()?
                        .unwrap_or(false);
                    let size = value_size(payload, body.as_deref())?;
                    let Some(state) = invocations.get_mut(&invocation) else {
                        reject(&mut transport, invocation, id, "ContextExpired")?;
                        continue;
                    };
                    // Recheck authority and limits in Rust even though the worker
                    // also enforces them before sending an operation.
                    let mutation = requires_mutation_slot(name);
                    let completion = name == "workers.lifetime.response";
                    let ordinary_pending = state.pending.len()
                        - usize::from(
                            state
                                .response
                                .is_some_and(|id| state.pending.contains_key(&id)),
                        );
                    let refusal = if !state.start.active.load(Ordering::Acquire)
                        || !state.start.owner.load(Ordering::Acquire)
                    {
                        Some("ContextExpired")
                    } else if synchronous && !supports_synchronous(name) {
                        Some("UnsupportedOperation")
                    } else if (name == "cell.call") != body.is_some() {
                        Some("ValidationError")
                    } else if name.len() > 64 || size > runtime.options.max_value_bytes {
                        Some("ValueLimit")
                    } else if (completion && state.response.is_some())
                        || (!completion
                            && (state.operations >= runtime.options.max_operations
                                || ordinary_pending >= runtime.options.max_pending_operations))
                    {
                        Some("OperationLimit")
                    } else if mutation && state.mutation {
                        Some("ConcurrentMutation")
                    } else {
                        None
                    };
                    if let Some(code) = refusal {
                        reject(&mut transport, invocation, id, code)?;
                        continue;
                    }
                    let budget = if message.get("budget_id").is_some() {
                        let limit = message["budget_ms"]
                            .as_u64()
                            .and_then(|value| u32::try_from(value).ok());
                        let selected =
                            match (name == "cell.call", number(&message, "budget_id"), limit) {
                                (true, Ok(budget), Some(limit)) => budgets
                                    .limit(invocation, budget, limit)
                                    .and_then(|()| budgets.get(invocation, budget)),
                                _ => Err("ValidationError"),
                            };
                        match selected {
                            Ok(budget) => Some(budget),
                            Err(code) => {
                                reject(&mut transport, invocation, id, code)?;
                                continue;
                            }
                        }
                    } else if message.get("budget_ms").is_some() {
                        reject(&mut transport, invocation, id, "ValidationError")?;
                        continue;
                    } else {
                        None
                    };
                    let Ok(bytes) = runtime.reserve(size) else {
                        reject(&mut transport, invocation, id, "BridgeLimit")?;
                        continue;
                    };
                    let cancellation = super::supports_operation_cancellation(name)
                        .then(cellar::sdk::web::AbortController::new);
                    if state
                        .start
                        .sender
                        .try_send(Event::Operation {
                            id,
                            name: name.into(),
                            payload: payload.into(),
                            callback_scope,
                            blocking: synchronous,
                            synchronous: None,
                            body,
                            signal: cancellation.as_ref().map(|controller| controller.signal()),
                            budget,
                            _bytes: bytes,
                        })
                        .is_err()
                    {
                        reject(&mut transport, invocation, id, "OperationLimit")?;
                        continue;
                    }
                    if completion {
                        state.response = Some(id);
                    } else {
                        state.operations += 1;
                    }
                    state.pending.insert(
                        id,
                        PendingOperation {
                            mutation,
                            cancellation,
                        },
                    );
                    state.mutation |= mutation;
                }
                "complete" if initialized => {
                    let id = number(&message, "id")?;
                    if let Some(state) = invocations.remove(&id) {
                        state.start.active.store(false, Ordering::Release);
                        // Returning with unawaited SDK work cannot report success.
                        let guest_failed = message["failed"]
                            .as_bool()
                            .context("invalid Node completion")?;
                        let unsettled = !state.pending.is_empty() || budgets.has(id);
                        let failed = guest_failed || unsettled;
                        budgets.retire(id);
                        let payload = if failed {
                            if guest_failed && !unsettled {
                                cellar_guest::rpc_failure::payload(string(&message, "payload")?)
                                    .unwrap_or("null")
                            } else {
                                "null"
                            }
                        } else {
                            string(&message, "payload")?
                        };
                        if failed {
                            body = None;
                        }
                        let bytes = runtime.reserve(value_size(payload, body.as_deref())?)?;
                        let _ = state.start.sender.try_send(Event::Complete {
                            payload: payload.into(),
                            body,
                            failed,
                            _bytes: bytes,
                        });
                    }
                }
                _ => anyhow::bail!("invalid Node protocol message"),
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[cfg(test)]
#[path = "../node_tests.rs"]
mod tests;
