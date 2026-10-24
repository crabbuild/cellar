// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A control reply releases a window slot without waiting for later replies.
use super::*;
use std::collections::VecDeque;

const WINDOW: usize = 16;

pub(super) struct Pending {
    request: SendRequest,
    tag: Option<[u8; frame::TAG]>,
    written: bool,
    outcome: Option<Result<(), TransportError>>,
    deadline: u64,
    exchange: Timer,
}

impl Pending {
    pub(super) fn new(state: &State, mut request: SendRequest) -> Self {
        if let Some(timer) = request.queued.take() {
            timer.finish(true);
        }
        Self {
            request,
            tag: None,
            written: false,
            outcome: None,
            // An unavailable clock expires immediately; it cannot grant more I/O.
            deadline: state
                .domain
                .mono_ms()
                .map_or(0, |now| now.saturating_add(state.options.io_timeout_ms)),
            exchange: state.performance.timer(Operation::ControlExchange),
        }
    }
}

impl State {
    pub(super) async fn send_control(
        self: Arc<Self>,
        pool: Arc<Pool>,
        mut requests: mailbox::Receiver<SendRequest>,
    ) {
        let mut stop = self.stop.subscribe();
        let mut peer_stop = pool.stop.subscribe();
        let mut connection = None;
        loop {
            if *stop.borrow() || *peer_stop.borrow() {
                return;
            }
            let request = crate::asyncrt::select! {
                _ = stop.changed() => return,
                _ = peer_stop.changed() => return,
                request = requests.recv() => match request {
                    Some(request) => request,
                    None => return,
                },
            };
            // Ownership stays outside the cancellable I/O futures. On failure,
            // close the physical stream before releasing these reservations.
            let mut pending = VecDeque::from([Pending::new(&self, request)]);
            let connected = self
                .bounded(Some(&pool), async {
                    if !self.current(&pending[0].request) {
                        return Err(TransportError::StalePeer);
                    }
                    if connection.is_none() {
                        let owner = self.outgoing_connections.acquire(Lane::Control);
                        let tcp = TcpStream::connect(pool.peer.address(Lane::Control)).await?;
                        tcp.set_nodelay(true)?;
                        let tcp = self.observed_socket(tcp, Lane::Control.into());
                        let name =
                            rustls::pki_types::ServerName::try_from(pool.peer.server_name.clone())
                                .map_err(|_| TransportError::Identity)?;
                        let tls = tokio_rustls::TlsConnector::from(self.tls.client.clone())
                            .connect(name, tcp)
                            .await
                            .map_err(|error| TransportError::Tls(error.to_string()))?;
                        let exporter = tls
                            .get_ref()
                            .1
                            .export_keying_material([0; 32], frame::EXPORT_LABEL, None)
                            .map_err(|error| TransportError::Tls(error.to_string()))?;
                        connection = Some((tls, exporter, owner));
                    }
                    Ok(())
                })
                .await;
            let result = match connected {
                Ok(()) => {
                    let (stream, exporter, _owner) =
                        connection.as_mut().expect("connected control stream");
                    self.exchange_control(stream, exporter, &pool, &mut requests, &mut pending)
                        .await
                }
                Err(error) => Err(error),
            };
            if result.is_err() {
                // Partial frames and unread replies are never reused/replayed.
                connection = None;
            }
            for pending in pending {
                let mut outcome = pending.outcome.unwrap_or_else(|| {
                    Err(result
                        .as_ref()
                        .err()
                        .cloned()
                        .unwrap_or(TransportError::Protocol))
                });
                if outcome.is_ok() && !self.current(&pending.request) {
                    outcome = Err(TransportError::StalePeer);
                    connection = None;
                }
                pending.exchange.finish(outcome.is_ok());
                self.finish_delivery(&pool, pending.request, outcome).await;
            }
        }
    }

    pub(super) async fn exchange_control<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
        exporter: &[u8; 32],
        pool: &Pool,
        requests: &mut mailbox::Receiver<SendRequest>,
        pending: &mut VecDeque<Pending>,
    ) -> Result<(), TransportError> {
        let (reader, writer) = tokio::io::split(stream);
        let mut reader = Some(reader);
        let mut writer = Some(writer);
        let mut reading = None;
        let mut writing = None;
        let mut stop = self.stop.subscribe();
        let mut peer_stop = pool.stop.subscribe();
        let mut input_closed = false;
        loop {
            if *stop.borrow() || *peer_stop.borrow() {
                return Err(TransportError::Closed);
            }
            if pending
                .front()
                .is_some_and(|p| p.written && p.outcome.is_some() && !self.current(&p.request))
            {
                return Err(TransportError::StalePeer);
            }
            if let Some(mut completed) = pending.pop_front_if(|p| p.written && p.outcome.is_some())
            {
                let result = completed.outcome.take().expect("observed reply");
                completed.exchange.finish(result.is_ok());
                self.finish_delivery(pool, completed.request, result).await;
            }
            if pending.is_empty() {
                match requests.try_recv() {
                    Ok(request) => pending.push_back(Pending::new(self, request)),
                    Err(_) => return Ok(()),
                }
            }
            if let Some(next) = pending.back_mut().filter(|p| p.tag.is_none()) {
                if !self.current(&next.request) {
                    return Err(TransportError::StalePeer);
                }
                let head = frame::request_head(
                    &self.auth,
                    exporter,
                    &next.request.header,
                    next.request.body.len(),
                    &self.options,
                )?;
                next.tag = Some(head.tag);
                let body = std::mem::take(&mut next.request.body);
                writing = Some(Box::pin(write_control(
                    writer.take().expect("one control writer"),
                    head,
                    body,
                )));
            }
            if reading.is_none() && pending.front().is_some_and(|p| p.outcome.is_none()) {
                reading = Some(Box::pin(read_control(
                    reader.take().expect("one control reader"),
                    &self.options,
                )));
            }
            let deadline = pending.front().expect("active control frame").deadline;
            let mut timeout = self
                .domain
                .sleep_until(deadline)
                .map_err(|_| TransportError::Closed)?;
            // Keep each read/write future alive across every select iteration:
            // dropping a partially completed frame would corrupt the stream.
            crate::asyncrt::select! {
                _ = stop.changed() => return Err(TransportError::Closed),
                _ = peer_stop.changed() => return Err(TransportError::StalePeer),
                _ = &mut timeout => return Err(TransportError::Timeout),
                result = async { writing.as_mut().expect("active writer").await }, if writing.is_some() => {
                    let (next_writer, result) = result;
                    writing = None;
                    writer = Some(next_writer);
                    result?;
                    pending.back_mut().expect("writing frame").written = true;
                },
                result = async { reading.as_mut().expect("active reader").await }, if reading.is_some() => {
                    let (next_reader, response) = result;
                    reading = None;
                    reader = Some(next_reader);
                    let next = pending.front_mut().expect("reply owner");
                    let result = match response?.response(
                        &self.auth, exporter, &next.request.header, &next.tag.expect("sent frame tag"),
                    )? {
                        Disposition::Accepted if self.current(&next.request) => Ok(()),
                        Disposition::Accepted | Disposition::StalePeer => Err(TransportError::StalePeer),
                        Disposition::UnknownGroup => Err(TransportError::UnknownGroup),
                        Disposition::Overloaded => Err(TransportError::Overloaded),
                        Disposition::Refused => Err(TransportError::Refused),
                    };
                    next.outcome = Some(result.clone());
                    result?;
                },
                request = requests.recv(), if writer.is_some() && pending.len() < WINDOW && !input_closed => {
                    match request {
                        Some(request) => pending.push_back(Pending::new(self, request)),
                        None => input_closed = true,
                    }
                },
            }
        }
    }
}

async fn write_control<W: AsyncWrite + Unpin>(
    mut writer: W,
    head: FrameHead,
    body: Vec<u8>,
) -> (W, Result<(), TransportError>) {
    let result = write_frame(&mut writer, &head, &body).await;
    (writer, result)
}

async fn read_control<R: AsyncRead + Unpin>(
    mut reader: R,
    options: &TransportOptions,
) -> (R, Result<FrameHead, TransportError>) {
    let result = read_head(&mut reader, options, Lane::Control, true).await;
    (reader, result)
}
