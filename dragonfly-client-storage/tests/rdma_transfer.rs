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

//! End-to-end test of the RDMA piece transport: a real RDMAServer serving a piece from
//! real storage to a real RDMAClient over libfabric. On hosts without RDMA hardware,
//! libfabric selects its tcp/sockets provider, exercising the same application and shim path as
//! the efa and verbs providers while not reproducing hardware-provider behavior.

#![cfg(feature = "rdma")]

use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::Error;
use dragonfly_client_storage::client::rdma::{discover, RDMAClient};
use dragonfly_client_storage::rdma::fabric::Fabric;
use dragonfly_client_storage::rdma::rendezvous::{CapabilityRegistry, WireCapability};
use dragonfly_client_storage::server::{rdma::RDMAServer, tcp::TCPServer};
use dragonfly_client_storage::Storage;
use dragonfly_client_util::{id_generator::IDGenerator, shutdown};
use leaky_bucket::RateLimiter;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;

/// FABRIC_TAG is the shared reachability-domain label for this in-process test pair.
const FABRIC_TAG: &str = "test-fabric";

/// free_port grabs an ephemeral TCP port for the rendezvous listener.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// test_config builds a config with the RDMA transport enabled.
fn test_config() -> Config {
    let mut config = Config::default();
    config.storage.server.rdma.enable = true;
    config.storage.server.rdma.allow_software_provider = true;
    config.storage.server.rdma.fabric_tag = Some(FABRIC_TAG.to_string());
    config.storage.server.rdma.transfer_timeout = Duration::from_secs(10);
    config
}

/// assign_free_ports gives the TCP discovery and RDMA rendezvous listeners distinct ports.
fn assign_free_ports(config: &mut Config) {
    config.storage.server.rdma.port = free_port();
    loop {
        config.storage.server.tcp_port = free_port();
        if config.storage.server.tcp_port != config.storage.server.rdma.port {
            break;
        }
    }
}

/// write_piece stores one finished piece and returns its digest.
async fn write_piece(storage: &Storage, task_id: &str, number: u32, content: &[u8]) -> String {
    storage
        .download_task_started(task_id, content.len() as u64, content.len() as u64, None)
        .await
        .unwrap();
    let piece_id = storage.piece_id(task_id, number);
    storage
        .download_piece_started(&piece_id, number)
        .await
        .unwrap();
    let piece = storage
        .download_piece_from_source_finished(
            &piece_id,
            task_id,
            0,
            content.len() as u64,
            &mut Cursor::new(content.to_vec()),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    piece.digest
}

/// start_server spawns the advertised TCP discovery endpoint and the RDMAServer.
async fn start_server(
    config: Arc<Config>,
    storage: Arc<Storage>,
) -> (
    String,
    String,
    shutdown::Shutdown,
    mpsc::UnboundedReceiver<()>,
) {
    let rdma_addr: SocketAddr = format!("127.0.0.1:{}", config.storage.server.rdma.port)
        .parse()
        .unwrap();
    let tcp_addr: SocketAddr = format!("127.0.0.1:{}", config.storage.server.tcp_port)
        .parse()
        .unwrap();
    let shutdown = shutdown::Shutdown::new();
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::unbounded_channel();
    let id_generator = Arc::new(IDGenerator::new(
        "127.0.0.1".to_string(),
        "localhost".to_string(),
        false,
    ));
    let limiter = Arc::new(
        RateLimiter::builder()
            .initial(1024 * 1024 * 1024)
            .refill(1024 * 1024 * 1024)
            .max(1024 * 1024 * 1024)
            .interval(Duration::from_secs(1))
            .fair(false)
            .build(),
    );
    let capabilities = CapabilityRegistry::default();

    let mut tcp_server = TCPServer::new(
        config.clone(),
        tcp_addr,
        id_generator.clone(),
        storage.clone(),
        limiter.clone(),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    )
    .with_rdma_capabilities(capabilities.clone());
    tokio::spawn(async move {
        tcp_server.run().await.unwrap();
    });

    let mut server = RDMAServer::new(
        config.clone(),
        rdma_addr,
        id_generator,
        storage,
        limiter,
        shutdown.clone(),
        shutdown_complete_tx,
    )
    .with_capability_registry(capabilities);
    tokio::spawn(async move {
        server.run().await.unwrap();
    });

    // Wait for both listeners to come up.
    let rdma_addr = rdma_addr.to_string();
    let tcp_addr = tcp_addr.to_string();
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(&rdma_addr).await.is_ok()
            && tokio::net::TcpStream::connect(&tcp_addr).await.is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (tcp_addr, rdma_addr, shutdown, shutdown_complete_rx)
}

/// client_fabric opens a downloader-side fabric endpoint with the given fabric tag.
fn client_fabric(fabric_tag: &str) -> (Arc<Fabric>, WireCapability) {
    let fabric = Arc::new(Fabric::new(None, None, 512 * 1024 * 1024, true).unwrap());
    let capability = WireCapability {
        provider: fabric.provider().to_string(),
        fabric_tag: fabric_tag.to_string(),
    };
    (fabric, capability)
}

#[tokio::test(flavor = "multi_thread")]
async fn downloads_piece_over_rdma() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut config = test_config();
    assign_free_ports(&mut config);
    let config = Arc::new(config);

    let storage = Arc::new(
        Storage::new(
            config.clone(),
            temp_dir.path(),
            temp_dir.path().to_path_buf(),
        )
        .await
        .unwrap(),
    );

    // 10 MiB piece: the client requests 1 MiB chunks while the server permits the default
    // 4 MiB, exercising lower-value negotiation across ten fabric messages.
    let task_id = "b969ba82f1ba1c1c5eb27f0b7aa051dcaf72e9a8dd574a04e60247f8d0a5f2b4";
    let content: Vec<u8> = (0..10 * 1024 * 1024).map(|i| (i % 249) as u8).collect();
    let digest = write_piece(&storage, task_id, 0, &content).await;

    let (tcp_addr, addr, shutdown, _shutdown_complete_rx) =
        start_server(config.clone(), storage).await;

    let (fabric, capability) = client_fabric(FABRIC_TAG);
    let advertisement = discover(&tcp_addr, Duration::from_secs(5)).await.unwrap();
    assert_eq!(advertisement.port, config.storage.server.rdma.port);
    assert!(capability.compatible(&advertisement.capability).is_ok());
    let mut client_config = config.as_ref().clone();
    client_config.storage.server.rdma.chunk_size = bytesize::ByteSize::mib(1);
    let client = RDMAClient::new(
        Arc::new(client_config),
        fabric.clone(),
        capability,
        addr.clone(),
    );
    for _ in 0..2 {
        let (mut reader, offset, got_digest) = client.download_piece(0, task_id).await.unwrap();
        assert_eq!(offset, 0);
        assert_eq!(got_digest, digest);
        let mut downloaded = Vec::new();
        reader.read_to_end(&mut downloaded).await.unwrap();
        assert_eq!(downloaded, content);
        // Releasing the reader returns its completed registration to the client pool.
        drop(reader);
    }
    let stats = fabric.buffer_pool_stats();
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.cached_buffers, 1);

    shutdown.trigger();
}

#[tokio::test(flavor = "multi_thread")]
async fn falls_back_when_fabric_tags_mismatch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut config = test_config();
    assign_free_ports(&mut config);
    let config = Arc::new(config);

    let storage = Arc::new(
        Storage::new(
            config.clone(),
            temp_dir.path(),
            temp_dir.path().to_path_buf(),
        )
        .await
        .unwrap(),
    );

    let task_id = "c869ba82f1ba1c1c5eb27f0b7aa051dcaf72e9a8dd574a04e60247f8d0a5f2b4";
    write_piece(&storage, task_id, 0, b"content").await;

    let (_tcp_addr, addr, shutdown, _shutdown_complete_rx) =
        start_server(config.clone(), storage).await;

    // A downloader from a different reachability domain must be refused with an
    // incompatibility error (which the downloader maps to TCP fallback).
    let (fabric, capability) = client_fabric("other-fabric");
    let client = RDMAClient::new(config.clone(), fabric, capability, addr.clone());
    let err = client.download_piece(0, task_id).await.unwrap_err();
    assert!(
        matches!(err, Error::Unsupported(_)),
        "expected Unsupported, got: {:?}",
        err
    );

    shutdown.trigger();
}

#[tokio::test(flavor = "multi_thread")]
async fn reports_missing_piece() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut config = test_config();
    assign_free_ports(&mut config);
    let config = Arc::new(config);

    let storage = Arc::new(
        Storage::new(
            config.clone(),
            temp_dir.path(),
            temp_dir.path().to_path_buf(),
        )
        .await
        .unwrap(),
    );

    let (_tcp_addr, addr, shutdown, _shutdown_complete_rx) =
        start_server(config.clone(), storage).await;

    let (fabric, capability) = client_fabric(FABRIC_TAG);
    let client = RDMAClient::new(config.clone(), fabric, capability, addr.clone());
    let err = client
        .download_piece(
            0,
            "d769ba82f1ba1c1c5eb27f0b7aa051dcaf72e9a8dd574a04e60247f8d0a5f2b4",
        )
        .await
        .unwrap_err();
    assert!(
        !matches!(err, Error::Unsupported(_)),
        "a missing piece must not mark the parent incompatible: {:?}",
        err
    );

    shutdown.trigger();
}
