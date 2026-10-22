//! Bounded control pipelining on the existing authenticated peer connection.
use super::*;

// Drain only already-admitted frames; never wait to fill a window. Every frame
// retains its node/peer byte and item permits. Bulk data remains one at a time.
const CONTROL_WINDOW: usize = 8;

impl State {
    pub(super) async fn send_loop(
        self: Arc<Self>,
        pool: Arc<Pool>,
        lane: Lane,
        mut requests: mailbox::Receiver<SendRequest>,
    ) {
        let mut stop = self.stop.subscribe();
        let mut peer_stop = pool.stop.subscribe();
        let mut connection = None;
        loop {
            if *stop.borrow() || *peer_stop.borrow() {
                return;
            }
            let first = crate::asyncrt::select! {
                _ = stop.changed() => return,
                _ = peer_stop.changed() => return,
                request = requests.recv() => match request { Some(request) => request, None => return },
            };
            let mut window = vec![first];
            if lane == Lane::Control {
                while window.len() < CONTROL_WINDOW {
                    match requests.try_recv() {
                        Ok(request) => window.push(request),
                        Err(_) => break,
                    }
                }
            }
            // Queue residence ends on selection, independently of the later
            // peer exchange. Cancellation of a Delivery does not end either.
            for request in &mut window {
                if let Some(timer) = request.queued.take() {
                    timer.finish(true);
                }
            }
            let exchange = self.performance.timer(match lane {
                Lane::Control => Operation::ControlExchange,
                Lane::Data => Operation::DataExchange,
            });
            let mut outcomes = vec![None; window.len()];
            let result = self
                .bounded(Some(&pool), async {
                    if !window.iter().all(|request| self.current(request)) {
                        return Err(TransportError::StalePeer);
                    }
                    if connection.is_none() {
                        let owner = self.outgoing_connections.acquire(lane);
                        let tcp = TcpStream::connect(pool.peer.address(lane)).await?;
                        tcp.set_nodelay(true)?;
                        let tcp = self.observed_socket(tcp, lane.into());
                        let name =
                            rustls::pki_types::ServerName::try_from(pool.peer.server_name.clone())
                                .map_err(|_| TransportError::Identity)?;
                        let tls = tokio_rustls::TlsConnector::from(self.tls.client.clone())
                            .connect(name, tcp)
                            .await
                            .map_err(|e| TransportError::Tls(e.to_string()))?;
                        let exporter = tls
                            .get_ref()
                            .1
                            .export_keying_material([0; 32], frame::EXPORT_LABEL, None)
                            .map_err(|e| TransportError::Tls(e.to_string()))?;
                        connection = Some((tls, exporter, owner));
                    }
                    let (stream, exporter, _owner) =
                        connection.as_mut().ok_or(TransportError::Closed)?;
                    self.exchange_window(stream, exporter, lane, &window, &mut outcomes)
                        .await
                })
                .await;
            exchange.finish(result.is_ok());
            if result.is_err() {
                // A partially written request or unread reply cannot be reused
                // as the next frame. No frame is blindly replayed on reconnect.
                connection = None;
            }
            for (request, outcome) in window.into_iter().zip(outcomes) {
                let mut outcome = outcome.unwrap_or_else(|| {
                    Err(result
                        .as_ref()
                        .err()
                        .cloned()
                        .unwrap_or(TransportError::Protocol))
                });
                // Earlier replies may precede an await for another frame. A
                // route/process revocation still wins before observer delivery.
                if outcome.is_ok() && !self.current(&request) {
                    outcome = Err(TransportError::StalePeer);
                    connection = None;
                }
                self.finish_delivery(&pool, request, outcome).await;
            }
        }
    }

    pub(super) async fn exchange_window<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
        exporter: &[u8; 32],
        lane: Lane,
        requests: &[SendRequest],
        outcomes: &mut [Option<Result<(), TransportError>>],
    ) -> Result<(), TransportError> {
        let heads = requests
            .iter()
            .map(|request| {
                frame::request_head(
                    &self.auth,
                    exporter,
                    &request.header,
                    request.body.len(),
                    &self.options,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Drive both directions together. Writing the whole window before
        // reading replies can deadlock when either socket/TLS buffer fills.
        let (mut reader, mut writer) = tokio::io::split(stream);
        let write = async {
            for (request, head) in requests.iter().zip(&heads) {
                if !self.current(request) {
                    return Err(TransportError::StalePeer);
                }
                write_frame(&mut writer, head, &request.body).await?;
            }
            Ok(())
        };
        let read = async {
            for ((request, head), outcome) in requests.iter().zip(&heads).zip(outcomes) {
                let response = read_head(&mut reader, &self.options, lane, true).await?;
                let result =
                    match response.response(&self.auth, exporter, &request.header, &head.tag)? {
                        Disposition::Accepted if self.current(request) => Ok(()),
                        Disposition::Accepted | Disposition::StalePeer => {
                            Err(TransportError::StalePeer)
                        }
                        Disposition::UnknownGroup => Err(TransportError::UnknownGroup),
                        Disposition::Overloaded => Err(TransportError::Overloaded),
                        Disposition::Refused => Err(TransportError::Refused),
                    };
                *outcome = Some(result.clone());
                result?;
            }
            Ok(())
        };
        futures_util::try_join!(write, read)?;
        Ok(())
    }

    async fn finish_delivery(
        &self,
        pool: &Pool,
        mut request: SendRequest,
        result: Result<(), TransportError>,
    ) {
        if result.is_err() {
            self.failed.fetch_add(1, Ordering::Relaxed);
            if self.current(&request) {
                // Failure hints never synthesize Raft append/snapshot responses
                // or establish whether a transmitted message reached its peer.
                let _ = self
                    .bounded(Some(pool), async {
                        self.runtime
                            .admit_unreachable(&request.scope, request.peer.replica.member.get())
                            .await?;
                        if request.snapshot {
                            self.runtime
                                .admit_snapshot_status(
                                    &request.scope,
                                    request.peer.replica.member.get(),
                                    false,
                                )
                                .await?;
                        }
                        Ok(())
                    })
                    .await;
            }
        }
        if result.is_ok() && request.snapshot && self.current(&request) {
            // Finish describes transport only; installation still requires the
            // receiver's persisted Raft response.
            let _ = self
                .bounded(Some(pool), async {
                    self.runtime
                        .admit_snapshot_status(
                            &request.scope,
                            request.peer.replica.member.get(),
                            true,
                        )
                        .await?;
                    Ok(())
                })
                .await;
        }
        // Drop payload and both permits before waking the completion observer.
        let reply = request.result.take();
        drop(request);
        if let Some(reply) = reply {
            let _ = reply.send(result);
        }
    }
}
