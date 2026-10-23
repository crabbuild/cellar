//! A control reply releases a window slot without waiting for later replies.
use super::*;
use std::collections::VecDeque;

const WINDOW: usize = 8;

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
            // Several replies can arrive before one gathered write finishes.
            // Drain their completed prefix before sleeping or refilling again.
            while pending
                .front()
                .is_some_and(|p| p.written && p.outcome.is_some())
            {
                if !self.current(&pending.front().expect("completed frame").request) {
                    return Err(TransportError::StalePeer);
                }
                let mut completed = pending.pop_front().expect("completed frame");
                let result = completed.outcome.take().expect("observed reply");
                completed.exchange.finish(result.is_ok());
                self.finish_delivery(pool, completed.request, result).await;
            }
            if writer.is_some() && !input_closed {
                // Amortize TLS/socket work across frames already admitted to
                // this peer. Never wait for a batch or exceed the reply window.
                while pending.len() < WINDOW {
                    match requests.try_recv() {
                        Ok(request) => pending.push_back(Pending::new(self, request)),
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            input_closed = true;
                            break;
                        }
                    }
                }
            }
            if pending.is_empty() {
                return Ok(());
            }
            if writer.is_some() {
                let mut frames = Vec::new();
                for next in pending.iter_mut().filter(|p| p.tag.is_none()) {
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
                    frames.push((head, std::mem::take(&mut next.request.body)));
                }
                if !frames.is_empty() {
                    writing = Some(Box::pin(write_control(
                        writer.take().expect("one control writer"),
                        frames,
                    )));
                }
            }
            // A reply may arrive before the gathered write finishes flushing.
            // Keep reading subsequent FIFO replies so a small duplex buffer
            // cannot deadlock the peer's reply writer against our next frame.
            if reading.is_none()
                && pending
                    .iter()
                    .any(|p| p.tag.is_some() && p.outcome.is_none())
            {
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
                    // No new frame can enter while the writer is owned by this
                    // future. Payloads stay owned until the physical flush ends.
                    for frame in pending.iter_mut().filter(|p| p.tag.is_some()) {
                        frame.written = true;
                    }
                },
                result = async { reading.as_mut().expect("active reader").await }, if reading.is_some() => {
                    let (next_reader, response) = result;
                    reading = None;
                    reader = Some(next_reader);
                    let next = pending.iter_mut().find(|p| p.outcome.is_none()).expect("reply owner");
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
    frames: Vec<(FrameHead, Vec<u8>)>,
) -> (W, Result<(), TransportError>) {
    let result = async {
        // Only slice descriptors are gathered. Each payload moves from its
        // already-charged request into this bounded physical I/O owner.
        let mut slices = frames
            .iter()
            .flat_map(|(head, body)| {
                [
                    IoSlice::new(&head.prefix),
                    IoSlice::new(&head.header),
                    IoSlice::new(&head.tag),
                    IoSlice::new(body),
                ]
            })
            .collect::<Vec<_>>();
        let mut remaining = &mut slices[..];
        while remaining.iter().any(|slice| !slice.is_empty()) {
            let written = writer.write_vectored(remaining).await?;
            if written == 0 {
                return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into());
            }
            IoSlice::advance_slices(&mut remaining, written);
        }
        writer.flush().await?;
        Ok(())
    }
    .await;
    (writer, result)
}

async fn read_control<R: AsyncRead + Unpin>(
    mut reader: R,
    options: &TransportOptions,
) -> (R, Result<FrameHead, TransportError>) {
    let result = read_head(&mut reader, options, Lane::Control, true).await;
    (reader, result)
}
