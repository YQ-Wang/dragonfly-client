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

use crate::client::rdma::MAX_CHUNKS;
use crate::rdma::fabric::Fabric;
use crate::rdma::rendezvous::{
    read_frame, write_frame, CapabilityRegistry, Frame, PieceKind, PieceReady, PieceRequest,
    RdmaAdvertisement, RendezvousError, WireCapability, ERROR_CODE_INCOMPATIBLE,
    ERROR_CODE_INTERNAL, ERROR_CODE_NOT_FOUND, ERROR_CODE_TOO_LARGE,
};
use crate::Storage;
use dragonfly_client_config::dfdaemon::{Config, RdmaProvider};
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use dragonfly_client_metric::{
    collect_upload_piece_failure_metrics, collect_upload_piece_finished_metrics,
    collect_upload_piece_started_metrics, collect_upload_piece_traffic_metrics,
};
use dragonfly_client_util::{id_generator::IDGenerator, shutdown};
use leaky_bucket::RateLimiter;
use socket2::{Domain, Protocol, Socket, TcpKeepalive, Type};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpListener, TcpStream,
};
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, info, instrument, Span};

/// RDMAServer serves piece content over the libfabric transport. It accepts rendezvous
/// connections on a TCP port, negotiates fabric compatibility fail-closed, and pushes bulk
/// piece bytes as tagged fabric messages. The TCP piece server remains the mandatory
/// fallback; this server failing to start must never take the daemon down.
pub struct RDMAServer {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// addr is the rendezvous listen address.
    addr: SocketAddr,

    /// id_generator generates host ids for tracing spans.
    id_generator: Arc<IDGenerator>,

    /// storage is the local storage.
    storage: Arc<Storage>,

    /// upload_bandwidth_limiter limits upload bandwidth in bytes per second.
    upload_bandwidth_limiter: Arc<RateLimiter>,

    /// shutdown is used to shutdown the RDMA server.
    shutdown: shutdown::Shutdown,

    /// _shutdown_complete is used to notify the RDMA server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,

    /// capability_registry exposes readiness through the normal TCP piece server.
    capability_registry: Option<CapabilityRegistry>,
}

/// PublishedCapability clears a registry entry when its listener exits on any path.
struct PublishedCapability(CapabilityRegistry);

impl Drop for PublishedCapability {
    fn drop(&mut self) {
        self.0.clear();
    }
}

/// RDMAServer implements the rendezvous accept loop over a shared fabric endpoint.
impl RDMAServer {
    /// Creates a new RDMAServer.
    pub fn new(
        config: Arc<Config>,
        addr: SocketAddr,
        id_generator: Arc<IDGenerator>,
        storage: Arc<Storage>,
        upload_bandwidth_limiter: Arc<RateLimiter>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        Self {
            config,
            addr,
            id_generator,
            storage,
            upload_bandwidth_limiter,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
            capability_registry: None,
        }
    }

    /// with_capability_registry publishes the listener only after fabric setup and bind succeed.
    pub fn with_capability_registry(mut self, registry: CapabilityRegistry) -> Self {
        self.capability_registry = Some(registry);
        self
    }

    /// Starts the storage RDMA server. Initialization failures (no usable fabric device,
    /// missing fabric tag) disable the server but keep the daemon running: peers simply use
    /// the TCP piece server.
    pub async fn run(&mut self) -> ClientResult<()> {
        let rdma_config = &self.config.storage.server.rdma;

        let Some(fabric_tag) = rdma_config
            .fabric_tag
            .as_deref()
            .filter(|tag| !tag.is_empty())
        else {
            error!(
                "rdma server disabled: storage.server.rdma.fabricTag is required so peers \
                 only attempt rdma within one reachability domain"
            );
            self.shutdown.recv().await;
            return Ok(());
        };

        let provider = match rdma_config.provider {
            RdmaProvider::Auto => None,
            provider => Some(provider.to_string()),
        };
        let fabric = match Fabric::new(
            provider.as_deref(),
            rdma_config.device.as_deref(),
            rdma_config.max_registered_bytes.as_u64(),
            rdma_config.allow_software_provider,
        ) {
            Ok(fabric) => Arc::new(fabric),
            Err(err) => {
                error!(
                    "rdma server disabled, failed to open fabric endpoint: {}",
                    err
                );
                self.shutdown.recv().await;
                return Ok(());
            }
        };

        let handler = RDMAServerHandler {
            id_generator: self.id_generator.clone(),
            storage: self.storage.clone(),
            upload_bandwidth_limiter: self.upload_bandwidth_limiter.clone(),
            capability: WireCapability {
                provider: fabric.provider().to_string(),
                fabric_tag: fabric_tag.to_string(),
            },
            fabric,
            chunk_size: rdma_config.chunk_size.as_u64(),
            transfer_timeout: rdma_config.transfer_timeout,
        };
        let handler = Arc::new(handler);

        let socket = Socket::new(
            Domain::for_address(self.addr),
            Type::STREAM,
            Some(Protocol::TCP),
        )?;
        socket.set_tcp_nodelay(true)?;
        socket.set_nonblocking(true)?;
        socket.set_tcp_keepalive(
            &TcpKeepalive::new()
                .with_interval(super::DEFAULT_KEEPALIVE_INTERVAL)
                .with_time(super::DEFAULT_KEEPALIVE_TIME)
                .with_retries(super::DEFAULT_KEEPALIVE_RETRIES),
        )?;
        socket.bind(&self.addr.into())?;
        socket.listen(1024)?;
        let std_listener: std::net::TcpListener = socket.into();
        let listener = TcpListener::from_std(std_listener).inspect_err(|err| {
            error!("failed to bind rdma rendezvous server: {}", err);
        })?;
        info!(
            "storage rdma server listening on {}, provider {}",
            self.addr, handler.capability.provider
        );
        let _published_capability = self.capability_registry.as_ref().map(|registry| {
            registry.publish(RdmaAdvertisement {
                capability: handler.capability.clone(),
                port: self.addr.port(),
            });
            PublishedCapability(registry.clone())
        });

        loop {
            tokio::select! {
                tcp_accepted = listener.accept() => {
                    let (tcp, remote_address) = tcp_accepted?;
                    debug!("accepted rdma rendezvous connection from {}", remote_address);

                    let handler = handler.clone();
                    tokio::spawn(async move {
                        if let Err(err) = handler.handle(tcp, remote_address.to_string()).await {
                           error!("failed to serve rdma connection from {}: {}", remote_address, err);
                        }
                    });
                },
                _ = self.shutdown.recv() => {
                    info!("rdma server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }
}

/// RDMAServerHandler handles rendezvous connections and fabric transfers.
struct RDMAServerHandler {
    /// id_generator generates host ids for tracing spans.
    id_generator: Arc<IDGenerator>,

    /// storage is the local storage.
    storage: Arc<Storage>,

    /// upload_bandwidth_limiter limits upload bandwidth in bytes per second.
    upload_bandwidth_limiter: Arc<RateLimiter>,

    /// capability is the local side of capability negotiation.
    capability: WireCapability,

    /// fabric is the shared libfabric endpoint.
    fabric: Arc<Fabric>,

    /// chunk_size is the server's preferred maximum tagged-message size.
    chunk_size: u64,

    /// transfer_timeout bounds each fabric operation and rendezvous wait.
    transfer_timeout: std::time::Duration,
}

/// RDMAServerHandler implements the per-connection transfer flow.
impl RDMAServerHandler {
    /// Handles one rendezvous connection: negotiate, load the piece into a pinned buffer,
    /// wait for the client's receives, send the bytes over the fabric.
    #[instrument(skip_all, fields(host_id, remote_address, task_id, piece_id))]
    async fn handle(&self, stream: TcpStream, remote_address: String) -> ClientResult<()> {
        let (mut reader, mut writer) = stream.into_split();
        let request = match time::timeout(self.transfer_timeout, read_frame(&mut reader)).await? {
            Ok(Frame::Request(request)) => request,
            Ok(frame) => {
                return Err(ClientError::Unknown(format!(
                    "unexpected rendezvous frame: {:?}",
                    frame
                )));
            }
            Err(err) => return Err(err),
        };

        Span::current().record("host_id", self.id_generator.host_id());
        Span::current().record("remote_address", remote_address.as_str());
        Span::current().record("task_id", request.task_id.as_str());
        Span::current().record(
            "piece_id",
            self.storage
                .piece_id(&request.task_id, request.piece_number)
                .as_str(),
        );

        if let Err(reason) = self.capability.compatible(&request.capability) {
            return self
                .abort(&mut writer, ERROR_CODE_INCOMPATIBLE, reason)
                .await;
        }

        collect_upload_piece_started_metrics();
        info!("start upload piece content over rdma");
        match self.handle_piece(&request, &mut reader, &mut writer).await {
            Ok(length) => {
                collect_upload_piece_finished_metrics();
                collect_upload_piece_traffic_metrics(length);
                Ok(())
            }
            Err(err) => {
                collect_upload_piece_failure_metrics();
                Err(err)
            }
        }
    }

    /// Serves one piece over the fabric, returning the piece length for traffic metrics.
    async fn handle_piece(
        &self,
        request: &PieceRequest,
        reader: &mut OwnedReadHalf,
        writer: &mut OwnedWriteHalf,
    ) -> ClientResult<u64> {
        let piece_id = self
            .storage
            .piece_id(&request.task_id, request.piece_number);

        // Fetch the piece metadata for the requested namespace.
        let piece = match request.kind {
            PieceKind::Piece => self.storage.get_piece(&piece_id),
            PieceKind::PersistentPiece => self.storage.get_persistent_piece(&piece_id),
            PieceKind::PersistentCachePiece => self.storage.get_persistent_cache_piece(&piece_id),
        };
        let piece = match piece {
            Ok(Some(piece)) => piece,
            Ok(None) => {
                self.abort(
                    writer,
                    ERROR_CODE_NOT_FOUND,
                    format!("piece {} not found", piece_id),
                )
                .await?;
                return Err(ClientError::PieceNotFound(piece_id));
            }
            Err(err) => {
                self.abort(writer, ERROR_CODE_INTERNAL, err.to_string())
                    .await?;
                return Err(err);
            }
        };

        let chunk_size = request
            .chunk_size
            .min(self.chunk_size)
            .min(self.fabric.max_msg_size() as u64);
        if piece.length == 0 || chunk_size == 0 {
            self.abort(
                writer,
                ERROR_CODE_INTERNAL,
                format!("piece {} has invalid length {}", piece_id, piece.length),
            )
            .await?;
            return Err(ClientError::Unknown("invalid piece length".to_string()));
        }
        let chunk_count = piece.length.div_ceil(chunk_size);
        if chunk_count > MAX_CHUNKS {
            self.abort(
                writer,
                ERROR_CODE_TOO_LARGE,
                format!("piece needs {} chunks, cap is {}", chunk_count, MAX_CHUNKS),
            )
            .await?;
            return Err(ClientError::Unknown("piece too large for rdma".to_string()));
        }

        // Acquire the upload bandwidth limiter, matching the TCP server.
        self.upload_bandwidth_limiter
            .acquire(piece.length as usize)
            .await;

        // Load the piece content into a pinned send buffer.
        let mut buf = match self.fabric.acquire_buffer(piece.length as usize).await {
            Ok(buf) => buf,
            Err(err) => {
                self.abort(writer, ERROR_CODE_TOO_LARGE, err.to_string())
                    .await?;
                return Err(err);
            }
        };
        let content_reader: ClientResult<Box<dyn tokio::io::AsyncRead + Send + Unpin>> =
            match request.kind {
                PieceKind::Piece => self
                    .storage
                    .upload_piece(&piece_id, &request.task_id, None)
                    .await
                    .map(|reader| Box::new(reader) as Box<dyn tokio::io::AsyncRead + Send + Unpin>),
                PieceKind::PersistentPiece => self
                    .storage
                    .upload_persistent_piece(&piece_id, &request.task_id, None)
                    .await
                    .map(|reader| Box::new(reader) as Box<dyn tokio::io::AsyncRead + Send + Unpin>),
                PieceKind::PersistentCachePiece => self
                    .storage
                    .upload_persistent_cache_piece(&piece_id, &request.task_id, None)
                    .await
                    .map(|reader| Box::new(reader) as Box<dyn tokio::io::AsyncRead + Send + Unpin>),
            };
        let mut content_reader = match content_reader {
            Ok(content_reader) => content_reader,
            Err(err) => {
                self.abort(writer, ERROR_CODE_INTERNAL, err.to_string())
                    .await?;
                return Err(err);
            }
        };
        // Safety: no operation over this buffer has been posted yet.
        let content = unsafe { buf.as_mut_slice() };
        if let Err(err) = content_reader.read_exact(content).await {
            self.abort(writer, ERROR_CODE_INTERNAL, err.to_string())
                .await?;
            return Err(err.into());
        }

        // Resolve the downloader's fabric address before promising readiness.
        let dest = match self.fabric.resolve(&request.client_endpoint) {
            Ok(dest) => dest,
            Err(err) => {
                self.abort(writer, ERROR_CODE_INTERNAL, err.to_string())
                    .await?;
                return Err(err);
            }
        };

        write_frame(
            writer,
            &Frame::Ready(PieceReady {
                offset: piece.offset,
                length: piece.length,
                digest: piece.digest.clone(),
                server_endpoint: self.fabric.local_endpoint().to_vec(),
                chunk_size,
            }),
        )
        .await?;

        // The client must post all receives before we send (rendezvous ordering for EFA).
        match time::timeout(self.transfer_timeout, read_frame(reader)).await? {
            Ok(Frame::RecvPosted) => {}
            Ok(frame) => {
                return Err(ClientError::Unknown(format!(
                    "unexpected rendezvous frame: {:?}",
                    frame
                )));
            }
            Err(err) => return Err(err),
        }

        // The pooled lease remains alive until every send completion is reaped.
        let mut ops = Vec::with_capacity(chunk_count as usize);
        for chunk in 0..chunk_count {
            let offset = chunk * chunk_size;
            let len = chunk_size.min(piece.length - offset);
            ops.push(
                self.fabric
                    .post_send(
                        buf.buffer(),
                        offset as usize,
                        len as usize,
                        request.tag.wrapping_add(chunk),
                        dest,
                    )
                    .await?,
            );
        }
        for op in ops {
            self.fabric.wait(op, self.transfer_timeout).await?;
        }

        write_frame(writer, &Frame::Done).await?;
        debug!("finished uploading piece content over rdma");
        Ok(piece.length)
    }

    /// abort reports an error to the client over the rendezvous channel.
    async fn abort(
        &self,
        writer: &mut OwnedWriteHalf,
        code: u32,
        message: String,
    ) -> ClientResult<()> {
        error!("aborting rdma transfer: {}", message);
        write_frame(writer, &Frame::Error(RendezvousError { code, message })).await
    }
}
