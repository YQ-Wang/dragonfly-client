/*
 *     Copyright 2024 The Dragonfly Authors
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

use async_trait::async_trait;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error, Result};
use dragonfly_client_storage::{client::quic::QUICClient, client::tcp::TCPClient};
use dragonfly_client_util::pool::{Builder as PoolBuilder, Entry, Factory, Pool};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncRead;
use tracing::{error, instrument};

/// DEFAULT_DOWNLOADER_CAPACITY is the default capacity of the downloader to store the clients.
const DEFAULT_DOWNLOADER_CAPACITY: usize = 2000;

/// DEFAULT_DOWNLOADER_IDLE_TIMEOUT is the default idle timeout for the downloader.
const DEFAULT_DOWNLOADER_IDLE_TIMEOUT: Duration = Duration::from_secs(420);

/// Downloader is the interface for downloading pieces, which is implemented by different
/// protocols. The downloader is used to download pieces from the other peers.
#[async_trait]
pub trait Downloader: Send + Sync {
    /// download_piece downloads a piece from the other peer by different protocols.
    async fn download_piece(
        &self,
        addr: &str,
        number: u32,
        host_id: &str,
        task_id: &str,
    ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)>;

    /// download_persistent_piece downloads a persistent piece from the other peer by different
    /// protocols.
    async fn download_persistent_piece(
        &self,
        addr: &str,
        number: u32,
        host_id: &str,
        task_id: &str,
    ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)>;

    /// download_persistent_cache_piece downloads a persistent cache piece from the other peer by different
    /// protocols.
    async fn download_persistent_cache_piece(
        &self,
        addr: &str,
        number: u32,
        host_id: &str,
        task_id: &str,
    ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)>;
}

/// DownloaderFactory is the factory for creating different downloaders by different protocols.
pub struct DownloaderFactory {
    /// downloader is the downloader for downloading pieces, which is implemented by different
    /// protocols.
    downloader: Arc<dyn Downloader + Send + Sync>,
}

/// DownloadFactory implements the DownloadFactory trait.
impl DownloaderFactory {
    /// new returns a new DownloadFactory.
    pub fn new(protocol: &str, config: Arc<Config>) -> Result<Self> {
        let downloader: Arc<dyn Downloader> = match protocol {
            "tcp" => Arc::new(TCPDownloader::new(
                config.clone(),
                DEFAULT_DOWNLOADER_CAPACITY,
                DEFAULT_DOWNLOADER_IDLE_TIMEOUT,
            )),
            "quic" => Arc::new(QUICDownloader::new(
                config.clone(),
                DEFAULT_DOWNLOADER_CAPACITY,
                DEFAULT_DOWNLOADER_IDLE_TIMEOUT,
            )),
            #[cfg(feature = "rdma")]
            "rdma" => Arc::new(rdma::RDMADownloader::new(config.clone())),
            _ => {
                error!("unsupported protocol: {}", protocol);
                return Err(Error::InvalidParameter);
            }
        };

        Ok(Self { downloader })
    }

    /// build returns the downloader.
    pub fn build(&self) -> Arc<dyn Downloader> {
        self.downloader.clone()
    }
}

/// QUICDownloader is the downloader for downloading pieces by the QUIC protocol.
/// It will reuse the quic clients to download pieces from the other peers by
/// peer's address.
pub struct QUICDownloader {
    /// client_pool is the pool of the quic clients.
    client_pool: Pool<String, String, QUICClient, QUICClientFactory>,
}

/// Factory for creating QUICClient instances.
struct QUICClientFactory {
    config: Arc<Config>,
}

/// QUICClientFactory implements the Factory trait for creating QUICClient instances.
#[async_trait]
impl Factory<String, QUICClient> for QUICClientFactory {
    type Error = Error;

    /// Creates a new QUICClient for the given address.
    async fn make_client(&self, addr: &String) -> Result<QUICClient> {
        Ok(QUICClient::new(self.config.clone(), addr.clone()))
    }
}

/// QUICDownloader implements the downloader with the QUIC protocol.
impl QUICDownloader {
    /// MAX_CONNECTIONS_PER_ADDRESS is the maximum number of connections per address.
    const MAX_CONNECTIONS_PER_ADDRESS: usize = 32;

    /// new returns a new QUICDownloader.
    pub fn new(config: Arc<Config>, capacity: usize, idle_timeout: Duration) -> Self {
        Self {
            client_pool: PoolBuilder::new(QUICClientFactory {
                config: config.clone(),
            })
            .capacity(capacity)
            .idle_timeout(idle_timeout)
            .build(),
        }
    }

    /// get_client_entry returns a client entry by the address.
    async fn get_client_entry(&self, key: String, addr: String) -> Result<Entry<QUICClient>> {
        self.client_pool.entry(&key, &addr).await
    }

    /// remove_client_entry removes the client if it is idle.
    async fn remove_client_entry(&self, key: String) {
        self.client_pool.remove_entry(&key).await;
    }
    /// get_entry_key generates a semi-random key by combining the client address with
    /// a random number. The randomization helps distribute connections across multiple
    /// slots when the same address attempts to establish multiple concurrent connections.
    fn get_entry_key(&self, addr: &str) -> String {
        format!(
            "{}-{}",
            addr,
            fastrand::usize(..Self::MAX_CONNECTIONS_PER_ADDRESS)
        )
    }
}

/// QUICDownloader implements the Downloader trait.
#[async_trait]
impl Downloader for QUICDownloader {
    /// download_piece downloads a piece from the other peer by the QUIC protocol.
    #[instrument(skip_all)]
    async fn download_piece(
        &self,
        addr: &str,
        number: u32,
        _host_id: &str,
        task_id: &str,
    ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
        let key = self.get_entry_key(addr);
        let entry = self.get_client_entry(key.clone(), addr.to_string()).await?;
        let request_guard = entry.request_guard();

        match entry.client.download_piece(number, task_id).await {
            Ok((reader, offset, digest)) => Ok((Box::new(reader), offset, digest)),
            Err(err) => {
                // If the request fails, it will drop the request guard and remove the client
                // entry to avoid using the invalid client.
                drop(request_guard);
                self.remove_client_entry(key).await;
                Err(err)
            }
        }
    }

    /// download_persistent_piece downloads a persistent piece from the other peer by
    /// the QUIC protocol.
    #[instrument(skip_all)]
    async fn download_persistent_piece(
        &self,
        addr: &str,
        number: u32,
        _host_id: &str,
        task_id: &str,
    ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
        let key = self.get_entry_key(addr);
        let entry = self.get_client_entry(key.clone(), addr.to_string()).await?;
        let request_guard = entry.request_guard();

        match entry
            .client
            .download_persistent_piece(number, task_id)
            .await
        {
            Ok((reader, offset, digest)) => Ok((Box::new(reader), offset, digest)),
            Err(err) => {
                // If the request fails, it will drop the request guard and remove the client
                // entry to avoid using the invalid client.
                drop(request_guard);
                self.remove_client_entry(key).await;
                Err(err)
            }
        }
    }

    /// download_persistent_cache_piece downloads a persistent cache piece from the other peer by
    /// the QUIC protocol.
    #[instrument(skip_all)]
    async fn download_persistent_cache_piece(
        &self,
        addr: &str,
        number: u32,
        _host_id: &str,
        task_id: &str,
    ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
        let key = self.get_entry_key(addr);
        let entry = self.get_client_entry(key.clone(), addr.to_string()).await?;
        let request_guard = entry.request_guard();

        match entry
            .client
            .download_persistent_cache_piece(number, task_id)
            .await
        {
            Ok((reader, offset, digest)) => Ok((Box::new(reader), offset, digest)),
            Err(err) => {
                // If the request fails, it will drop the request guard and remove the client
                // entry to avoid using the invalid client.
                drop(request_guard);
                self.remove_client_entry(key).await;
                Err(err)
            }
        }
    }
}

/// TCPDownloader is the downloader for downloading pieces by the TCP protocol.
/// It will reuse the tcp clients to download pieces from the other peers by
/// peer's address.
pub struct TCPDownloader {
    /// client_pool is the pool of the tcp clients.
    client_pool: Pool<String, String, TCPClient, TCPClientFactory>,
}

/// Factory for creating TCPClient instances.
struct TCPClientFactory {
    config: Arc<Config>,
}

/// TCPClientFactory implements the Factory trait for creating TCPClient instances.
#[async_trait]
impl Factory<String, TCPClient> for TCPClientFactory {
    type Error = Error;

    /// Creates a new TCPClient for the given address.
    async fn make_client(&self, addr: &String) -> Result<TCPClient> {
        Ok(TCPClient::new(self.config.clone(), addr.clone()))
    }
}

/// TCPDownloader implements the downloader with the TCP protocol.
impl TCPDownloader {
    /// MAX_CONNECTIONS_PER_ADDRESS is the maximum number of connections per address.
    const MAX_CONNECTIONS_PER_ADDRESS: usize = 32;

    /// new returns a new TCPDownloader.
    pub fn new(config: Arc<Config>, capacity: usize, idle_timeout: Duration) -> Self {
        Self {
            client_pool: PoolBuilder::new(TCPClientFactory {
                config: config.clone(),
            })
            .capacity(capacity)
            .idle_timeout(idle_timeout)
            .build(),
        }
    }

    /// get_client_entry returns a client entry by the address.
    async fn get_client_entry(&self, key: String, addr: String) -> Result<Entry<TCPClient>> {
        self.client_pool.entry(&key, &addr).await
    }

    /// remove_client_entry removes the client if it is idle.
    async fn remove_client_entry(&self, key: String) {
        self.client_pool.remove_entry(&key).await;
    }

    /// get_entry_key generates a semi-random key by combining the client address with
    /// a random number. The randomization helps distribute connections across multiple
    /// slots when the same address attempts to establish multiple concurrent connections.
    fn get_entry_key(&self, addr: &str) -> String {
        format!(
            "{}-{}",
            addr,
            fastrand::usize(..Self::MAX_CONNECTIONS_PER_ADDRESS)
        )
    }
}

/// TCPDownloader implements the Downloader trait.
#[async_trait]
impl Downloader for TCPDownloader {
    /// download_piece downloads a piece from the other peer by the TCP protocol.
    #[instrument(skip_all)]
    async fn download_piece(
        &self,
        addr: &str,
        number: u32,
        _host_id: &str,
        task_id: &str,
    ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
        let key = self.get_entry_key(addr);
        let entry = self.get_client_entry(key.clone(), addr.to_string()).await?;
        let request_guard = entry.request_guard();

        match entry.client.download_piece(number, task_id).await {
            Ok((reader, offset, digest)) => Ok((Box::new(reader), offset, digest)),
            Err(err) => {
                // If the request fails, it will drop the request guard and remove the client
                // entry to avoid using the invalid client.
                drop(request_guard);
                self.remove_client_entry(key).await;
                Err(err)
            }
        }
    }

    /// download_persistent_piece downloads a persistent piece from the other peer by
    /// the TCP protocol.
    #[instrument(skip_all)]
    async fn download_persistent_piece(
        &self,
        addr: &str,
        number: u32,
        _host_id: &str,
        task_id: &str,
    ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
        let key = self.get_entry_key(addr);
        let entry = self.get_client_entry(key.clone(), addr.to_string()).await?;
        let request_guard = entry.request_guard();

        match entry
            .client
            .download_persistent_piece(number, task_id)
            .await
        {
            Ok((reader, offset, digest)) => Ok((Box::new(reader), offset, digest)),
            Err(err) => {
                // If the request fails, it will drop the request guard and remove the client
                // entry to avoid using the invalid client.
                drop(request_guard);
                self.remove_client_entry(key).await;
                Err(err)
            }
        }
    }

    /// download_persistent_cache_piece downloads a persistent cache piece from the other peer by
    /// the TCP protocol.
    #[instrument(skip_all)]
    async fn download_persistent_cache_piece(
        &self,
        addr: &str,
        number: u32,
        _host_id: &str,
        task_id: &str,
    ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
        let key = self.get_entry_key(addr);
        let entry = self.get_client_entry(key.clone(), addr.to_string()).await?;
        let request_guard = entry.request_guard();

        match entry
            .client
            .download_persistent_cache_piece(number, task_id)
            .await
        {
            Ok((reader, offset, digest)) => Ok((Box::new(reader), offset, digest)),
            Err(err) => {
                // If the request fails, it will drop the request guard and remove the client
                // entry to avoid using the invalid client.
                drop(request_guard);
                self.remove_client_entry(key).await;
                Err(err)
            }
        }
    }
}

/// rdma provides the libfabric piece downloader (AWS EFA and RoCE/InfiniBand). It is an
/// optimization layer: every error surfaces to the caller, which falls back to the TCP
/// downloader for that piece.
#[cfg(feature = "rdma")]
pub mod rdma {
    use super::*;
    use dragonfly_client_config::dfdaemon::RdmaProvider;
    use dragonfly_client_storage::client::rdma::{discover, RDMAClient};
    use dragonfly_client_storage::rdma::fabric::Fabric;
    use dragonfly_client_storage::rdma::rendezvous::{RdmaAdvertisement, WireCapability};
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::time::Instant;
    use tracing::{info, warn};

    /// FABRIC_RETRY_INTERVAL is how long to wait before retrying fabric initialization
    /// after a failure.
    const FABRIC_RETRY_INTERVAL: Duration = Duration::from_secs(300);

    /// INCOMPATIBLE_PARENT_TTL is how long a parent that reported fabric incompatibility
    /// is skipped before RDMA is attempted again.
    const INCOMPATIBLE_PARENT_TTL: Duration = Duration::from_secs(60);

    /// CAPABLE_PARENT_TTL bounds how long a successful discovery result is reused.
    const CAPABLE_PARENT_TTL: Duration = Duration::from_secs(60);

    /// FabricState tracks the lazily initialized process-shared fabric endpoint.
    enum FabricState {
        /// Uninitialized means no initialization has been attempted yet.
        Uninitialized,

        /// Failed records when initialization last failed, for retry backoff.
        Failed(Instant),

        /// Ready holds the shared endpoint and the local negotiation capability.
        Ready(Arc<Fabric>, WireCapability),
    }

    /// RDMADownloader downloads pieces over libfabric with a shared fabric endpoint. The
    /// endpoint is opened lazily on the first download so a misconfigured or unsupported
    /// host degrades to TCP instead of failing at startup.
    pub struct RDMADownloader {
        /// config is the configuration of the dfdaemon.
        config: Arc<Config>,

        /// fabric is the lazily initialized shared endpoint.
        fabric: tokio::sync::Mutex<FabricState>,

        /// incompatible_parents caches parents that reported fabric incompatibility so
        /// every piece does not pay a doomed rendezvous round trip.
        incompatible_parents: std::sync::Mutex<HashMap<String, Instant>>,

        /// capable_parents caches successful discovery so every piece does not add a control
        /// round trip. Transfer failures evict the entry immediately.
        capable_parents: std::sync::Mutex<HashMap<String, (Instant, RdmaAdvertisement)>>,
    }

    /// RDMADownloader implements the downloader over the libfabric transport.
    impl RDMADownloader {
        /// new returns a new RDMADownloader.
        pub fn new(config: Arc<Config>) -> Self {
            Self {
                config,
                fabric: tokio::sync::Mutex::new(FabricState::Uninitialized),
                incompatible_parents: std::sync::Mutex::new(HashMap::new()),
                capable_parents: std::sync::Mutex::new(HashMap::new()),
            }
        }

        /// fabric returns the shared endpoint and local capability, initializing them on
        /// first use and applying retry backoff after failures.
        async fn fabric(&self) -> Result<(Arc<Fabric>, WireCapability)> {
            let mut state = self.fabric.lock().await;
            match &*state {
                FabricState::Ready(fabric, capability) if !fabric.is_failed() => {
                    return Ok((fabric.clone(), capability.clone()))
                }
                FabricState::Ready(_, _) => {
                    // A retired endpoint cannot recover by returning errors forever. Drop it
                    // and let the normal initialization path create a fresh provider endpoint.
                    *state = FabricState::Uninitialized;
                }
                FabricState::Failed(at) if at.elapsed() < FABRIC_RETRY_INTERVAL => {
                    return Err(Error::Unsupported(
                        "rdma fabric initialization failed recently".to_string(),
                    ));
                }
                _ => {}
            }

            let rdma_config = &self.config.storage.server.rdma;
            let Some(fabric_tag) = rdma_config
                .fabric_tag
                .as_deref()
                .filter(|tag| !tag.is_empty())
            else {
                *state = FabricState::Failed(Instant::now());
                return Err(Error::Unsupported(
                    "rdma requires storage.server.rdma.fabricTag".to_string(),
                ));
            };

            let provider = match rdma_config.provider {
                RdmaProvider::Auto => None,
                provider => Some(provider.to_string()),
            };
            match Fabric::new(
                provider.as_deref(),
                rdma_config.device.as_deref(),
                rdma_config.max_registered_bytes.as_u64(),
                rdma_config.allow_software_provider,
            ) {
                Ok(fabric) => {
                    let fabric = Arc::new(fabric);
                    let capability = WireCapability {
                        provider: fabric.provider().to_string(),
                        fabric_tag: fabric_tag.to_string(),
                    };
                    info!(
                        "rdma downloader ready: provider {}, fabric tag {}",
                        capability.provider, capability.fabric_tag
                    );
                    *state = FabricState::Ready(fabric.clone(), capability.clone());
                    Ok((fabric, capability))
                }
                Err(err) => {
                    warn!("rdma fabric initialization failed: {}", err);
                    *state = FabricState::Failed(Instant::now());
                    Err(err)
                }
            }
        }

        /// retire_failed_fabric removes a poisoned shared endpoint after a transfer failure.
        /// Ordinary peer incompatibility leaves the shared endpoint intact.
        async fn retire_failed_fabric(&self) {
            let mut state = self.fabric.lock().await;
            if matches!(&*state, FabricState::Ready(fabric, _) if fabric.is_failed()) {
                *state = FabricState::Uninitialized;
            }
        }

        /// check_parent errors fast for parents recently reported incompatible.
        fn check_parent(&self, addr: &str) -> Result<()> {
            let mut incompatible_parents = self.incompatible_parents.lock().unwrap();
            match incompatible_parents.get(addr) {
                Some(at) if at.elapsed() < INCOMPATIBLE_PARENT_TTL => Err(Error::Unsupported(
                    format!("parent {} is rdma-incompatible", addr),
                )),
                Some(_) => {
                    incompatible_parents.remove(addr);
                    Ok(())
                }
                None => Ok(()),
            }
        }

        /// record_incompatible remembers that a parent reported fabric incompatibility.
        fn record_incompatible(&self, addr: &str, err: &Error) {
            self.capable_parents.lock().unwrap().remove(addr);
            if matches!(err, Error::Unsupported(_)) {
                self.incompatible_parents
                    .lock()
                    .unwrap()
                    .insert(addr.to_string(), Instant::now());
            }
        }

        /// advertisement returns a cached live capability or discovers it through the parent's
        /// advertised TCP piece endpoint.
        async fn advertisement(
            &self,
            addr: &str,
            local: &WireCapability,
        ) -> Result<RdmaAdvertisement> {
            let cached = self.capable_parents.lock().unwrap().get(addr).cloned();
            if let Some((at, advertisement)) = cached {
                if at.elapsed() < CAPABLE_PARENT_TTL {
                    return Ok(advertisement);
                }
                self.capable_parents.lock().unwrap().remove(addr);
            }

            let advertisement = discover(addr, self.config.storage.server.rdma.transfer_timeout)
                .await
                .map_err(|err| {
                    Error::Unsupported(format!("rdma discovery from {} failed: {}", addr, err))
                })?;
            local
                .compatible(&advertisement.capability)
                .map_err(|reason| Error::Unsupported(format!("rdma incompatible: {}", reason)))?;
            self.capable_parents
                .lock()
                .unwrap()
                .insert(addr.to_string(), (Instant::now(), advertisement.clone()));
            Ok(advertisement)
        }

        /// client builds an RDMAClient for one parent address.
        async fn client(&self, addr: &str) -> Result<RDMAClient> {
            self.check_parent(addr)?;
            let (fabric, capability) = self.fabric().await?;
            let advertisement = match self.advertisement(addr, &capability).await {
                Ok(advertisement) => advertisement,
                Err(err) => {
                    self.record_incompatible(addr, &err);
                    return Err(err);
                }
            };
            let mut rendezvous_addr: SocketAddr = addr.parse().map_err(|err| {
                Error::Unsupported(format!("invalid parent piece address {}: {}", addr, err))
            })?;
            rendezvous_addr.set_port(advertisement.port);
            Ok(RDMAClient::new(
                self.config.clone(),
                fabric,
                capability,
                rendezvous_addr.to_string(),
            ))
        }
    }

    /// RDMADownloader implements the Downloader trait.
    #[async_trait]
    impl Downloader for RDMADownloader {
        /// download_piece downloads a piece from the other peer over the fabric.
        #[instrument(skip_all)]
        async fn download_piece(
            &self,
            addr: &str,
            number: u32,
            _host_id: &str,
            task_id: &str,
        ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
            let client = self.client(addr).await?;
            match client.download_piece(number, task_id).await {
                Ok((reader, offset, digest)) => Ok((Box::new(reader), offset, digest)),
                Err(err) => {
                    if client.fabric_failed() {
                        self.retire_failed_fabric().await;
                    }
                    self.record_incompatible(addr, &err);
                    Err(err)
                }
            }
        }

        /// download_persistent_piece downloads a persistent piece from the other peer over
        /// the fabric.
        #[instrument(skip_all)]
        async fn download_persistent_piece(
            &self,
            addr: &str,
            number: u32,
            _host_id: &str,
            task_id: &str,
        ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
            let client = self.client(addr).await?;
            match client.download_persistent_piece(number, task_id).await {
                Ok((reader, offset, digest)) => Ok((Box::new(reader), offset, digest)),
                Err(err) => {
                    if client.fabric_failed() {
                        self.retire_failed_fabric().await;
                    }
                    self.record_incompatible(addr, &err);
                    Err(err)
                }
            }
        }

        /// download_persistent_cache_piece downloads a persistent cache piece from the
        /// other peer over the fabric.
        #[instrument(skip_all)]
        async fn download_persistent_cache_piece(
            &self,
            addr: &str,
            number: u32,
            _host_id: &str,
            task_id: &str,
        ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
            let client = self.client(addr).await?;
            match client
                .download_persistent_cache_piece(number, task_id)
                .await
            {
                Ok((reader, offset, digest)) => Ok((Box::new(reader), offset, digest)),
                Err(err) => {
                    if client.fabric_failed() {
                        self.retire_failed_fabric().await;
                    }
                    self.record_incompatible(addr, &err);
                    Err(err)
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[tokio::test]
        async fn receive_only_config_can_initialize_downloader_fabric() {
            let mut config = Config::default();
            config.download.protocol = "rdma".to_string();
            config.storage.server.rdma.enable = false;
            config.storage.server.rdma.allow_software_provider = true;
            config.storage.server.rdma.fabric_tag = Some("test-fabric".to_string());

            let downloader = RDMADownloader::new(Arc::new(config));
            let (_, capability) = downloader.fabric().await.unwrap();

            assert_eq!(capability.fabric_tag, "test-fabric");
        }
    }
}
