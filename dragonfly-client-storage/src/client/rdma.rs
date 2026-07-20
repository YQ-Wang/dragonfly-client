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

use crate::rdma::fabric::Fabric;
use crate::rdma::rendezvous::{
    read_frame, write_frame, Frame, PieceKind, PieceRequest, RdmaAdvertisement, WireCapability,
    ERROR_CODE_INCOMPATIBLE,
};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use socket2::{SockRef, TcpKeepalive};
use std::io::Cursor;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::time;
use tracing::{debug, error, instrument, Span};

/// DEFAULT_CHUNK_SIZE is the preferred fabric message size; the effective size is the
/// minimum of both peers' limits and the provider's max message size.
pub(crate) const DEFAULT_CHUNK_SIZE: u64 = 4 * 1024 * 1024;

/// MAX_CHUNKS caps the number of fabric messages (and thus posted receives) per piece.
pub(crate) const MAX_CHUNKS: u64 = 4096;

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

    /// Downloads a piece from the parent, returning the piece content reader, offset, and
    /// digest exactly like the TCP and QUIC clients so digest verification upstream is
    /// byte-identical.
    #[instrument(skip_all, fields(parent_addr))]
    pub async fn download_piece(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<(Cursor<Vec<u8>>, u64, String)> {
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
    ) -> ClientResult<(Cursor<Vec<u8>>, u64, String)> {
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
    ) -> ClientResult<(Cursor<Vec<u8>>, u64, String)> {
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
    ) -> ClientResult<(Cursor<Vec<u8>>, u64, String)> {
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

        let tag = self.fabric.next_tag();
        let chunk_size = DEFAULT_CHUNK_SIZE.min(self.fabric.max_msg_size() as u64);
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
            "rdma piece ready: offset {}, length {}, chunk size {}",
            ready.offset, ready.length, ready.chunk_size
        );

        if ready.length == 0 || ready.chunk_size == 0 || ready.chunk_size > chunk_size {
            return Err(ClientError::Unknown(format!(
                "invalid rdma piece metadata: length {}, chunk size {}",
                ready.length, ready.chunk_size
            )));
        }
        let chunk_count = ready.length.div_ceil(ready.chunk_size);
        if chunk_count > MAX_CHUNKS {
            return Err(ClientError::Unknown(format!(
                "piece needs {} rdma chunks, exceeding the {} chunk cap",
                chunk_count, MAX_CHUNKS
            )));
        }

        // Post every receive before telling the parent to send (rendezvous ordering): EFA
        // has limited unexpected-message buffering, so the receiver must be ready first.
        let buf = self.fabric.alloc_buffer(ready.length as usize).await?;
        let mut ops = Vec::with_capacity(chunk_count as usize);
        for chunk in 0..chunk_count {
            let offset = chunk * ready.chunk_size;
            let len = ready.chunk_size.min(ready.length - offset);
            ops.push((
                len as usize,
                self.fabric
                    .post_recv(&buf, offset as usize, len as usize, tag.wrapping_add(chunk))
                    .await?,
            ));
        }
        write_frame(&mut writer, &Frame::RecvPosted).await?;

        let transfer_timeout = self.config.storage.server.rdma.transfer_timeout;
        for (expected_len, op) in ops {
            let len = self.fabric.wait(op, transfer_timeout).await?;
            if len != expected_len {
                return Err(ClientError::Unknown(format!(
                    "rdma chunk length mismatch: expected {}, got {}",
                    expected_len, len
                )));
            }
        }

        // Best-effort read of the parent's Done frame; the local completions above are
        // authoritative for the landed bytes.
        if let Ok(Ok(Frame::Error(err))) =
            time::timeout(transfer_timeout, read_frame(&mut reader)).await
        {
            return Err(ClientError::Unknown(format!(
                "rdma transfer failed on parent: {}",
                err.message
            )));
        }

        let content = buf.into_vec();
        Ok((Cursor::new(content), ready.offset, ready.digest))
    }
}
