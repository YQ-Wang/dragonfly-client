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

use crate::rdma::fabric::{Fabric, PooledBufReader, TAG_RANGE_SIZE};
use crate::rdma::rendezvous::{
    read_frame, write_frame, Frame, PieceKind, PieceRequest, RdmaAdvertisement, WireCapability,
    ERROR_CODE_INCOMPATIBLE,
};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use socket2::{SockRef, TcpKeepalive};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::time;
use tracing::{debug, error, instrument, Span};

/// MAX_CHUNKS caps the number of fabric messages (and thus posted receives) per piece.
pub(crate) const MAX_CHUNKS: u64 = TAG_RANGE_SIZE;

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
    ) -> ClientResult<(PooledBufReader, u64, String)> {
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
    ) -> ClientResult<(PooledBufReader, u64, String)> {
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
    ) -> ClientResult<(PooledBufReader, u64, String)> {
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
    ) -> ClientResult<(PooledBufReader, u64, String)> {
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
        let transfer_length = usize::try_from(ready.length).map_err(|_| {
            ClientError::Unknown("rdma piece exceeds addressable memory".to_string())
        })?;

        // Keep the destination as one completed lease so upstream can still fall back to TCP
        // before it starts writing the piece. Receives themselves are posted in bounded windows:
        // EFA has limited unexpected-message buffering, while providers also have finite posted
        // receive queues.
        let buf = self.fabric.acquire_buffer(transfer_length).await?;
        let transfer_timeout = self.config.storage.server.rdma.transfer_timeout;
        let control = read_frame(&mut reader);
        tokio::pin!(control);
        let mut server_done = false;
        let mut start_chunk = 0;
        while start_chunk < chunk_count {
            let window_count =
                (chunk_count - start_chunk).min(ready.max_inflight_chunks as u64) as u32;
            let final_window = start_chunk + u64::from(window_count) == chunk_count;
            let mut ops = Vec::with_capacity(window_count as usize);
            for chunk in start_chunk..start_chunk + u64::from(window_count) {
                let offset = chunk * ready.chunk_size;
                let len = ready.chunk_size.min(ready.length - offset);
                ops.push((
                    len as usize,
                    self.fabric
                        .post_recv(buf.buffer(), offset as usize, len as usize, tag + chunk)
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
                let wait = self.fabric.wait(op, transfer_timeout);
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
            start_chunk += u64::from(window_count);
        }

        // Local receive completions prove the bytes landed; Done proves the parent also reaped
        // every send and did not fail while staging a later window.
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

        Ok((buf.into_reader(), ready.offset, ready.digest))
    }
}
