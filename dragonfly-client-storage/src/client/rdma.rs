/*
 *     Copyright 2026 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::rdma::fabric::{Fabric, PooledBuf, TAG_RANGE_SIZE};
use crate::rdma::rendezvous::{
    read_frame, write_frame, Frame, PieceKind, PieceReady, PieceRequest, RdmaAdvertisement,
    WireCapability, ERROR_CODE_INCOMPATIBLE,
};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use socket2::{SockRef, TcpKeepalive};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, instrument, Span};

/// MAX_CHUNKS caps the number of fabric messages (and thus posted receives) per piece.
pub(crate) const MAX_CHUNKS: u64 = TAG_RANGE_SIZE;

/// RDMAStreamReader exposes completed receive windows as an [`AsyncRead`]. The registered
/// receive ring stays bounded by the negotiated window, while the consumer can write and hash
/// one window concurrently with the fabric receiving the next one.
pub struct RDMAStreamReader {
    receiver: mpsc::Receiver<io::Result<PooledBuf>>,
    current: Option<PooledBuf>,
    position: usize,
}

impl std::fmt::Debug for RDMAStreamReader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RDMAStreamReader")
            .field(
                "current_window_length",
                &self.current.as_ref().map_or(0, PooledBuf::len),
            )
            .field("position", &self.position)
            .finish()
    }
}

impl RDMAStreamReader {
    fn new(receiver: mpsc::Receiver<io::Result<PooledBuf>>) -> Self {
        Self {
            receiver,
            current: None,
            position: 0,
        }
    }
}

impl AsyncRead for RDMAStreamReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if self
                .current
                .as_ref()
                .is_some_and(|window| self.position < window.len())
            {
                let position = self.position;
                let window = self.current.as_mut().expect("checked current window");
                let read_len = (window.len() - position).min(output.remaining());
                // Safety: the fabric task sends a window only after all receive completions
                // have been reaped, and the reader exclusively owns it until it is consumed.
                let available = unsafe { &window.as_mut_slice()[position..position + read_len] };
                output.put_slice(available);
                self.position += read_len;
                return Poll::Ready(Ok(()));
            }

            // Release an exhausted registration before waiting for the producer to acquire the
            // next one. This permits progress even when the memory budget holds one window.
            self.current = None;
            match self.receiver.poll_recv(cx) {
                Poll::Ready(Some(Ok(window))) => {
                    // Dropping the consumed window returns its registration to the pool.
                    self.current = Some(window);
                    self.position = 0;
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// discover asks the parent's already-advertised TCP piece endpoint for its live RDMA
/// capability and rendezvous port. Older and non-RDMA peers simply fail this optional probe.
pub async fn discover(addr: &str, timeout: std::time::Duration) -> ClientResult<RdmaAdvertisement> {
    time::timeout(timeout, async {
        let stream = TcpStream::connect(addr).await?;
        let socket = SockRef::from(&stream);
        socket.set_tcp_nodelay(true)?;
        socket.set_tcp_keepalive(
            &TcpKeepalive::new()
                .with_interval(super::DEFAULT_KEEPALIVE_INTERVAL)
                .with_time(super::DEFAULT_KEEPALIVE_TIME)
                .with_retries(super::DEFAULT_KEEPALIVE_RETRIES),
        )?;
        let (mut reader, mut writer) = stream.into_split();
        write_frame(&mut writer, &Frame::Discover).await?;
        match read_frame(&mut reader).await? {
            Frame::Capability(advertisement) if advertisement.port != 0 => Ok(advertisement),
            Frame::Capability(_) => Err(ClientError::Unsupported(
                "parent advertised an invalid rdma rendezvous port".to_string(),
            )),
            Frame::Error(err) if err.code == ERROR_CODE_INCOMPATIBLE => {
                Err(ClientError::Unsupported(err.message))
            }
            Frame::Error(err) => Err(ClientError::Unknown(format!(
                "rdma discovery error {}: {}",
                err.code, err.message
            ))),
            frame => Err(ClientError::Unknown(format!(
                "unexpected rdma discovery frame: {:?}",
                frame
            ))),
        }
    })
    .await?
}

/// RDMAClient downloads pieces over the libfabric transport: control messages ride a TCP
/// rendezvous connection to the parent's RDMA port, bulk bytes arrive as tagged fabric
/// messages into a pinned buffer. Any error must make the caller fall back to the TCP
/// piece transport; RDMA never has to succeed for a piece to complete.
#[derive(Clone)]
pub struct RDMAClient {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// fabric is the process-shared libfabric endpoint.
    fabric: Arc<Fabric>,

    /// capability is the local side of capability negotiation.
    capability: WireCapability,

    /// addr is the address of the parent's RDMA rendezvous server.
    addr: String,
}

/// RDMAClient implements the libfabric piece download client.
impl RDMAClient {
    /// Creates a new RDMAClient for one parent address.
    pub fn new(
        config: Arc<Config>,
        fabric: Arc<Fabric>,
        capability: WireCapability,
        addr: String,
    ) -> Self {
        Self {
            config,
            fabric,
            capability,
            addr,
        }
    }

    /// fabric_failed reports whether the shared endpoint has been retired and should be
    /// recreated by the downloader before another RDMA attempt.
    pub fn fabric_failed(&self) -> bool {
        self.fabric.is_failed()
    }

    /// Downloads a piece from the parent, returning the piece content reader, offset, and
    /// digest exactly like the TCP and QUIC clients so digest verification upstream is
    /// byte-identical.
    #[instrument(skip_all, fields(parent_addr))]
    pub async fn download_piece(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<(RDMAStreamReader, u64, String)> {
        Span::current().record("parent_addr", self.addr.as_str());
        time::timeout(
            self.config.download.piece_timeout,
            self.handle_download(PieceKind::Piece, number, task_id),
        )
        .await
        .inspect_err(|err| {
            error!("rdma download timeout from {}: {}", self.addr, err);
        })?
    }

    /// Downloads a persistent piece from the parent.
    #[instrument(skip_all, fields(parent_addr))]
    pub async fn download_persistent_piece(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<(RDMAStreamReader, u64, String)> {
        Span::current().record("parent_addr", self.addr.as_str());
        time::timeout(
            self.config.download.piece_timeout,
            self.handle_download(PieceKind::PersistentPiece, number, task_id),
        )
        .await
        .inspect_err(|err| {
            error!("rdma download timeout from {}: {}", self.addr, err);
        })?
    }

    /// Downloads a persistent cache piece from the parent.
    #[instrument(skip_all, fields(parent_addr))]
    pub async fn download_persistent_cache_piece(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<(RDMAStreamReader, u64, String)> {
        Span::current().record("parent_addr", self.addr.as_str());
        time::timeout(
            self.config.download.piece_timeout,
            self.handle_download(PieceKind::PersistentCachePiece, number, task_id),
        )
        .await
        .inspect_err(|err| {
            error!("rdma download timeout from {}: {}", self.addr, err);
        })?
    }

    /// Runs one piece transfer: rendezvous, post receives, signal readiness, await
    /// completions, and hand the landed bytes back as a reader.
    async fn handle_download(
        &self,
        kind: PieceKind,
        number: u32,
        task_id: &str,
    ) -> ClientResult<(RDMAStreamReader, u64, String)> {
        let stream = TcpStream::connect(self.addr.clone()).await?;
        let socket = SockRef::from(&stream);
        socket.set_tcp_nodelay(true)?;
        socket.set_tcp_keepalive(
            &TcpKeepalive::new()
                .with_interval(super::DEFAULT_KEEPALIVE_INTERVAL)
                .with_time(super::DEFAULT_KEEPALIVE_TIME)
                .with_retries(super::DEFAULT_KEEPALIVE_RETRIES),
        )?;
        let (mut reader, mut writer) = stream.into_split();

        let tag = self.fabric.next_tag()?;
        let configured_chunk_size = self.config.storage.server.rdma.chunk_size.as_u64();
        if configured_chunk_size == 0 {
            return Err(ClientError::InvalidParameter);
        }
        let max_inflight_chunks = self.config.storage.server.rdma.max_inflight_chunks;
        if max_inflight_chunks == 0 || u64::from(max_inflight_chunks) > MAX_CHUNKS {
            return Err(ClientError::InvalidParameter);
        }
        let chunk_size = configured_chunk_size.min(self.fabric.max_msg_size() as u64);
        write_frame(
            &mut writer,
            &Frame::Request(PieceRequest {
                kind,
                task_id: task_id.to_string(),
                piece_number: number,
                capability: self.capability.clone(),
                client_endpoint: self.fabric.local_endpoint().to_vec(),
                tag,
                chunk_size,
                max_inflight_chunks,
            }),
        )
        .await?;

        let ready = match read_frame(&mut reader).await? {
            Frame::Ready(ready) => ready,
            Frame::Error(err) if err.code == ERROR_CODE_INCOMPATIBLE => {
                return Err(ClientError::Unsupported(format!(
                    "rdma incompatible with {}: {}",
                    self.addr, err.message
                )));
            }
            Frame::Error(err) => {
                return Err(ClientError::Unknown(format!(
                    "rdma rendezvous error {}: {}",
                    err.code, err.message
                )));
            }
            frame => {
                return Err(ClientError::Unknown(format!(
                    "unexpected rendezvous frame: {:?}",
                    frame
                )));
            }
        };
        debug!(
            "rdma piece ready: offset {}, length {}, chunk size {}, inflight chunks {}",
            ready.offset, ready.length, ready.chunk_size, ready.max_inflight_chunks
        );

        if ready.length == 0
            || ready.chunk_size == 0
            || ready.chunk_size > chunk_size
            || ready.max_inflight_chunks == 0
            || ready.max_inflight_chunks > max_inflight_chunks
        {
            return Err(ClientError::Unknown(format!(
                "invalid rdma piece metadata: length {}, chunk size {}, inflight chunks {}",
                ready.length, ready.chunk_size, ready.max_inflight_chunks
            )));
        }
        let chunk_count = ready.length.div_ceil(ready.chunk_size);
        if chunk_count > MAX_CHUNKS {
            return Err(ClientError::Unknown(format!(
                "piece needs {} rdma chunks, exceeding the {} chunk cap",
                chunk_count, MAX_CHUNKS
            )));
        }
        let window_length = ready.length.min(
            ready
                .chunk_size
                .saturating_mul(u64::from(ready.max_inflight_chunks)),
        );
        let window_length = usize::try_from(window_length).map_err(|_| {
            ClientError::Unknown("rdma receive window exceeds addressable memory".to_string())
        })?;
        let buf = self.fabric.acquire_buffer(window_length).await?;
        let (window_tx, window_rx) = mpsc::channel(2);
        let fabric = self.fabric.clone();
        let transfer_timeout = self.config.storage.server.rdma.transfer_timeout;
        let piece_timeout = self.config.download.piece_timeout;
        let result_offset = ready.offset;
        let result_digest = ready.digest.clone();

        tokio::spawn(async move {
            let transfer = receive_stream(
                fabric,
                buf,
                reader,
                writer,
                ready,
                chunk_count,
                tag,
                transfer_timeout,
                window_tx.clone(),
            );
            let result = time::timeout(piece_timeout, transfer).await;
            let error = match result {
                Ok(Ok(())) => return,
                Ok(Err(err)) => err,
                Err(_) => {
                    ClientError::Unknown("complete rdma piece transfer timed out".to_string())
                }
            };
            let _ = window_tx
                .send(Err(io::Error::other(error.to_string())))
                .await;
        });

        Ok((
            RDMAStreamReader::new(window_rx),
            result_offset,
            result_digest,
        ))
    }
}

#[allow(clippy::too_many_arguments)]
async fn receive_stream(
    fabric: Arc<Fabric>,
    buf: PooledBuf,
    mut reader: OwnedReadHalf,
    mut writer: OwnedWriteHalf,
    ready: PieceReady,
    chunk_count: u64,
    tag: u64,
    transfer_timeout: std::time::Duration,
    window_tx: mpsc::Sender<io::Result<PooledBuf>>,
) -> ClientResult<()> {
    let control = read_frame(&mut reader);
    tokio::pin!(control);
    let mut server_done = false;
    let mut start_chunk = 0;
    let mut next_buf = Some(buf);

    while start_chunk < chunk_count {
        let window_buf = next_buf.take().expect("receive window buffer");
        let window_count = (chunk_count - start_chunk).min(ready.max_inflight_chunks as u64) as u32;
        let final_window = start_chunk + u64::from(window_count) == chunk_count;
        let window_piece_offset = start_chunk * ready.chunk_size;
        let mut window_length = 0usize;
        let mut ops = Vec::with_capacity(window_count as usize);

        for chunk in start_chunk..start_chunk + u64::from(window_count) {
            let piece_offset = chunk * ready.chunk_size;
            let local_offset = usize::try_from(piece_offset - window_piece_offset)
                .map_err(|_| ClientError::InvalidParameter)?;
            let len = ready.chunk_size.min(ready.length - piece_offset);
            let len = usize::try_from(len).map_err(|_| ClientError::InvalidParameter)?;
            window_length = window_length
                .checked_add(len)
                .ok_or(ClientError::InvalidParameter)?;
            ops.push((
                len,
                fabric
                    .post_recv(window_buf.buffer(), local_offset, len, tag + chunk)
                    .await?,
            ));
        }

        write_frame(
            &mut writer,
            &Frame::RecvPosted {
                start_chunk,
                chunk_count: window_count,
            },
        )
        .await?;

        for (expected_len, op) in ops {
            let wait = fabric.wait(op, transfer_timeout);
            tokio::pin!(wait);
            let len = if server_done {
                wait.await?
            } else {
                tokio::select! {
                    result = &mut wait => result?,
                    frame = &mut control => {
                        match frame {
                            Ok(Frame::Error(err)) => {
                                return Err(ClientError::Unknown(format!(
                                    "rdma transfer failed on parent: {}",
                                    err.message
                                )));
                            }
                            Ok(Frame::Done) if final_window => {
                                server_done = true;
                                wait.await?
                            }
                            Ok(frame) => {
                                return Err(ClientError::Unknown(format!(
                                    "unexpected rendezvous frame during transfer: {:?}",
                                    frame
                                )));
                            }
                            Err(err) => return Err(err),
                        }
                    }
                }
            };
            if len != expected_len {
                return Err(ClientError::Unknown(format!(
                    "rdma chunk length mismatch: expected {}, got {}",
                    expected_len, len
                )));
            }
        }

        debug_assert_eq!(window_length, window_buf.len());
        if window_tx.send(Ok(window_buf)).await.is_err() {
            return Err(ClientError::Unknown(
                "rdma stream consumer closed early".to_string(),
            ));
        }
        start_chunk += u64::from(window_count);
        if start_chunk < chunk_count {
            let next_length = ready
                .length
                .saturating_sub(start_chunk.saturating_mul(ready.chunk_size))
                .min(
                    ready
                        .chunk_size
                        .saturating_mul(u64::from(ready.max_inflight_chunks)),
                );
            let next_length =
                usize::try_from(next_length).map_err(|_| ClientError::InvalidParameter)?;
            next_buf = Some(fabric.acquire_buffer(next_length).await?);
        }
    }

    if !server_done {
        match time::timeout(transfer_timeout, &mut control).await? {
            Ok(Frame::Done) => {}
            Ok(Frame::Error(err)) => {
                return Err(ClientError::Unknown(format!(
                    "rdma transfer failed on parent: {}",
                    err.message
                )));
            }
            Ok(frame) => {
                return Err(ClientError::Unknown(format!(
                    "unexpected rendezvous frame: {:?}",
                    frame
                )));
            }
            Err(err) => return Err(err),
        }
    }

    Ok(())
}
