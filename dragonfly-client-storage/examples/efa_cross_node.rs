//! Cross-node EFA RDMA transfer for real files (e.g. a HuggingFace model directory).
//!
//! Server seeds every file under --files-dir as one Dragonfly piece (piece number = index).
//! Client downloads each piece over RDMA and writes the same relative paths under --out-dir.
//!
//! Manifest bootstrap via env:
//!   MODEL_TASK_ID / MODEL_MANIFEST_PIECE  (printed by server as BOOTSTRAP ...)

use bytesize::ByteSize;
use dragonfly_client_config::dfdaemon::{Config, RdmaProvider};
use dragonfly_client_storage::client::rdma::{
    discover, RDMAClient, RDMAStreamReader, ReceivedWindow,
};
use dragonfly_client_storage::client::tcp::TCPClient;
use dragonfly_client_storage::rdma::fabric::Fabric;
use dragonfly_client_storage::rdma::rendezvous::{CapabilityRegistry, WireCapability};
use dragonfly_client_storage::server::{rdma::RDMAServer, tcp::TCPServer};
use dragonfly_client_storage::Storage;
use dragonfly_client_util::digest::{calculate_file_digest, Algorithm, Digest};
use dragonfly_client_util::{id_generator::IDGenerator, shutdown};
use leaky_bucket::RateLimiter;
use sha2::{Digest as Sha2Digest, Sha256};
use std::io::{self, Cursor};
use std::net::SocketAddr;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
struct ManifestEntry {
    piece: u32,
    relative_path: String,
    size: u64,
    digest: String,
}

#[derive(Debug, Clone)]
struct Manifest {
    task_id: String,
    entries: Vec<ManifestEntry>,
}

impl Manifest {
    fn encode(&self) -> Vec<u8> {
        let mut out = format!("task_id={}\n", self.task_id);
        for e in &self.entries {
            out.push_str(&format!(
                "{}\t{}\t{}\t{}\n",
                e.piece, e.size, e.digest, e.relative_path
            ));
        }
        out.into_bytes()
    }

    fn decode(bytes: &[u8]) -> Self {
        let text = String::from_utf8(bytes.to_vec()).expect("utf8 manifest");
        let mut lines = text.lines();
        let first = lines.next().expect("task_id line");
        let task_id = first
            .strip_prefix("task_id=")
            .expect("task_id=")
            .to_string();
        let mut entries = Vec::new();
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let mut parts = line.splitn(4, '\t');
            let piece: u32 = parts.next().unwrap().parse().unwrap();
            let size: u64 = parts.next().unwrap().parse().unwrap();
            let digest = parts.next().unwrap().to_string();
            let relative_path = parts.next().unwrap().to_string();
            entries.push(ManifestEntry {
                piece,
                relative_path,
                size,
                digest,
            });
        }
        Self { task_id, entries }
    }
}

#[derive(Debug)]
struct Args {
    mode: String,
    bind: String,
    parent_host: String,
    tcp_port: u16,
    rdma_port: u16,
    provider: String,
    device: Option<String>,
    fabric_tag: String,
    chunk_mib: u64,
    max_inflight: u32,
    max_registered_mib: u64,
    concurrency: usize,
    digest: DigestAlgorithm,
    sink: Sink,
    transport: Transport,
    files_dir: PathBuf,
    out_dir: PathBuf,
    data_dir: PathBuf,
}

fn parse_args() -> Args {
    let mut args = Args {
        mode: "server".into(),
        bind: "0.0.0.0".into(),
        parent_host: "127.0.0.1".into(),
        tcp_port: 4001,
        rdma_port: 4007,
        provider: "verbs".into(),
        device: None,
        fabric_tag: "roce-test".into(),
        chunk_mib: 4,
        max_inflight: 16,
        // Far above the dfdaemon default, so a measurement is not silently capped by the
        // registration budget unless it is asked to be. Pass --max-registered-mib 512 to
        // reproduce what the default budget does to the receive pipeline.
        max_registered_mib: 64 * 1024,
        concurrency: 2,
        digest: DigestAlgorithm::Sha256,
        sink: Sink::Pwrite,
        transport: Transport::Rdma,
        files_dir: PathBuf::from("/tmp/model"),
        out_dir: PathBuf::from("/tmp/model-out"),
        data_dir: PathBuf::from("/tmp/df-rdma-efa"),
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "server" | "client" => args.mode = arg,
            "--bind" => args.bind = it.next().expect("--bind"),
            "--parent-host" => args.parent_host = it.next().expect("--parent-host"),
            "--tcp-port" => args.tcp_port = it.next().unwrap().parse().unwrap(),
            "--rdma-port" => args.rdma_port = it.next().unwrap().parse().unwrap(),
            "--provider" => args.provider = it.next().expect("--provider"),
            "--device" => args.device = Some(it.next().expect("--device")),
            "--fabric-tag" => args.fabric_tag = it.next().expect("--fabric-tag"),
            "--chunk-mib" => args.chunk_mib = it.next().unwrap().parse().unwrap(),
            "--max-inflight" => args.max_inflight = it.next().unwrap().parse().unwrap(),
            "--max-registered-mib" => args.max_registered_mib = it.next().unwrap().parse().unwrap(),
            "--concurrency" => args.concurrency = it.next().unwrap().parse().unwrap(),
            "--digest" => args.digest = DigestAlgorithm::parse(&it.next().expect("--digest")),
            "--sink" => args.sink = Sink::parse(&it.next().expect("--sink")),
            "--transport" => args.transport = Transport::parse(&it.next().expect("--transport")),
            "--files-dir" => args.files_dir = PathBuf::from(it.next().expect("--files-dir")),
            "--out-dir" => args.out_dir = PathBuf::from(it.next().expect("--out-dir")),
            "--data-dir" => args.data_dir = PathBuf::from(it.next().expect("--data-dir")),
            other => panic!("unknown argument: {other}"),
        }
    }
    args
}

fn provider_from_arg(name: &str) -> RdmaProvider {
    match name {
        "efa" => RdmaProvider::Efa,
        "verbs" => RdmaProvider::Verbs,
        "auto" | "software" => RdmaProvider::Auto,
        other => panic!("unsupported --provider {other} (expected efa|verbs|auto|software)"),
    }
}

fn make_config(args: &Args) -> Config {
    let mut config = Config::default();
    config.storage.server.rdma.enable = true;
    config.storage.server.rdma.provider = provider_from_arg(&args.provider);
    // The software providers are far slower than a NIC and must never be measured by accident,
    // so they are opt-in and only useful for checking that the harness itself works.
    config.storage.server.rdma.allow_software_provider = args.provider == "software";
    config.storage.server.rdma.device = args.device.clone();
    config.storage.server.rdma.fabric_tag = Some(args.fabric_tag.clone());
    config.storage.server.rdma.chunk_size = ByteSize::mib(args.chunk_mib);
    config.storage.server.rdma.max_inflight_chunks = args.max_inflight;
    config.storage.server.rdma.max_registered_bytes = ByteSize::mib(args.max_registered_mib);
    config.storage.server.rdma.max_concurrent_transfers = 64;
    config.storage.server.rdma.transfer_timeout = Duration::from_secs(900);
    config.storage.server.rdma.mmap_content = true;
    config.storage.server.rdma.port = args.rdma_port;
    config.storage.write_buffer_size = 16 * 1024 * 1024;
    config.storage.read_buffer_size = 16 * 1024 * 1024;
    config.storage.server.tcp_port = args.tcp_port;
    config.download.piece_timeout = Duration::from_secs(600);
    config
}

fn task_id_for_dir(dir: &Path) -> String {
    // Stable 64-hex task id derived from the directory path.
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(dir.to_string_lossy().as_bytes());
    let h = hasher.finalize();
    format!("{h:08x}{:056x}", 0u128)
}

fn list_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    fn walk(base: &Path, cur: &Path, out: &mut Vec<PathBuf>) {
        for ent in std::fs::read_dir(cur).unwrap() {
            let ent = ent.unwrap();
            let path = ent.path();
            if path.is_dir() {
                walk(base, &path, out);
            } else {
                out.push(path.strip_prefix(base).unwrap().to_path_buf());
            }
        }
    }
    walk(dir, dir, &mut files);
    files.sort();
    files
}

async fn seed_files(storage: &Storage, task_id: &str, files_dir: &Path) -> Manifest {
    let rels = list_files(files_dir);
    assert!(!rels.is_empty(), "no files under {}", files_dir.display());

    let mut total: u64 = 0;
    for rel in &rels {
        total += std::fs::metadata(files_dir.join(rel)).unwrap().len();
    }
    // Leave headroom for the trailing manifest piece.
    let content_length = total.saturating_add(1024 * 1024);
    storage
        .download_task_started(task_id, content_length, content_length, None)
        .await
        .unwrap();

    let mut entries = Vec::new();
    let mut offset: u64 = 0;
    for (piece, rel) in rels.iter().enumerate() {
        let path = files_dir.join(rel);
        let size = std::fs::metadata(&path).unwrap().len();
        let digest = calculate_file_digest(Algorithm::Sha256, &path)
            .unwrap()
            .to_string();
        // Nodes have enough RAM for ~10GiB shards.
        let bytes = tokio::fs::read(&path).await.expect("read file");
        let piece_no = piece as u32;
        let piece_id = storage.piece_id(task_id, piece_no);
        storage
            .download_piece_started(&piece_id, piece_no)
            .await
            .unwrap();
        storage
            .download_piece_from_source_finished(
                &piece_id,
                task_id,
                offset,
                bytes.len() as u64,
                &mut Cursor::new(bytes),
                Duration::from_secs(600),
            )
            .await
            .unwrap();
        println!(
            "seeded piece={piece_no} path={} offset={offset} size={size} digest={}",
            rel.display(),
            &digest
        );
        entries.push(ManifestEntry {
            piece: piece_no,
            relative_path: rel.to_string_lossy().into_owned(),
            size,
            digest,
        });
        offset = offset.saturating_add(size);
    }

    Manifest {
        task_id: task_id.to_string(),
        entries,
    }
}

async fn run_server(args: Args) {
    let _ = std::fs::remove_dir_all(&args.data_dir);
    std::fs::create_dir_all(&args.data_dir).unwrap();
    let config = Arc::new(make_config(&args));
    let storage = Arc::new(
        Storage::new(
            config.clone(),
            &args.data_dir,
            args.data_dir.join("metadata"),
        )
        .await
        .unwrap(),
    );

    let task_id = task_id_for_dir(&args.files_dir);
    println!(
        "seeding {} into task_id={}",
        args.files_dir.display(),
        task_id
    );
    let t0 = Instant::now();
    let manifest = seed_files(&storage, &task_id, &args.files_dir).await;
    println!(
        "seeded {} files in {:?}",
        manifest.entries.len(),
        t0.elapsed()
    );

    let manifest_bytes = manifest.encode();
    let manifest_piece = manifest.entries.len() as u32;
    let piece_id = storage.piece_id(&task_id, manifest_piece);
    storage
        .download_piece_started(&piece_id, manifest_piece)
        .await
        .unwrap();
    // Manifest is a separate piece at the end of the task content range.
    let manifest_offset: u64 = manifest.entries.iter().map(|e| e.size).sum();
    storage
        .download_piece_from_source_finished(
            &piece_id,
            &task_id,
            manifest_offset,
            manifest_bytes.len() as u64,
            &mut Cursor::new(manifest_bytes),
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    println!("seeded manifest as piece={manifest_piece} offset={manifest_offset}");

    let rdma_addr: SocketAddr = format!("{}:{}", args.bind, args.rdma_port).parse().unwrap();
    let tcp_addr: SocketAddr = format!("{}:{}", args.bind, args.tcp_port).parse().unwrap();
    let shutdown = shutdown::Shutdown::new();
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::unbounded_channel();
    let id_generator = Arc::new(IDGenerator::new(
        args.bind.clone(),
        "efa-model-server".into(),
        false,
    ));
    // Build the upload limiter exactly like dfdaemon does. Hard-coding a smaller bucket here caps
    // the fabric long before any copy or digest does, which silently turns this into a benchmark of
    // the leaky bucket rather than of RDMA.
    let upload_bandwidth_limit = config.upload.bandwidth_limit.as_u64() as usize;
    println!(
        "upload bandwidth limit {} ({:.1} Gbps)",
        config.upload.bandwidth_limit,
        (upload_bandwidth_limit as f64) * 8.0 / 1e9
    );
    let limiter = Arc::new(
        RateLimiter::builder()
            .initial(upload_bandwidth_limit)
            .refill(upload_bandwidth_limit)
            .max(upload_bandwidth_limit)
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
        config,
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

    println!(
        "MODEL_SERVER_READY task_id={} manifest_piece={} tcp={} rdma={} files={}",
        task_id,
        manifest_piece,
        tcp_addr,
        rdma_addr,
        manifest.entries.len()
    );
    println!(
        "BOOTSTRAP task_id={} manifest_piece={} file_count={}",
        task_id,
        manifest_piece,
        manifest.entries.len()
    );
    tokio::signal::ctrl_c().await.ok();
    shutdown.trigger();
    let _ = shutdown_complete_rx.recv().await;
}

async fn run_client(args: Args) {
    std::fs::create_dir_all(&args.out_dir).unwrap();
    let config = Arc::new(make_config(&args));
    let tcp_addr = format!("{}:{}", args.parent_host, args.tcp_port);
    let rdma_addr = format!("{}:{}", args.parent_host, args.rdma_port);

    // The TCP piece server is the transport RDMA falls back to, so it is measured through the
    // same manifest, digest, sink, and concurrency machinery. Only the fabric is skipped.
    let mut client_fabric = None;
    let rdma_client = match args.transport {
        Transport::Tcp => None,
        Transport::Rdma => {
            let provider_hint = match args.provider.as_str() {
                "efa" => Some("efa"),
                "verbs" => Some("verbs"),
                "auto" | "software" => None,
                other => panic!("unsupported provider {other}"),
            };
            let fabric = Arc::new(
                Fabric::new(
                    provider_hint,
                    args.device.as_deref(),
                    64 * 1024 * 1024 * 1024,
                    args.provider == "software",
                )
                .unwrap_or_else(|e| panic!("open {} fabric: {e}", args.provider)),
            );
            let capability = WireCapability {
                provider: fabric.provider().to_string(),
                fabric_tag: args.fabric_tag.clone(),
            };
            let advertisement = discover(&tcp_addr, Duration::from_secs(30))
                .await
                .expect("discover");
            capability
                .compatible(&advertisement.capability)
                .expect("compatible");
            println!(
                "discovered provider={} fabric_tag={} rdma_port={}",
                advertisement.capability.provider,
                advertisement.capability.fabric_tag,
                advertisement.port
            );
            client_fabric = Some(fabric.clone());
            Some(RDMAClient::new(
                config.clone(),
                fabric,
                capability,
                rdma_addr,
            ))
        }
    };
    let tcp_client = TCPClient::new(config.clone(), tcp_addr.clone());

    let task_id = std::env::var("MODEL_TASK_ID").expect("MODEL_TASK_ID");
    let manifest_piece: u32 = std::env::var("MODEL_MANIFEST_PIECE")
        .expect("MODEL_MANIFEST_PIECE")
        .parse()
        .unwrap();

    let mut manifest_buf = Vec::new();
    match rdma_client.as_ref() {
        Some(client) => {
            let (mut reader, _, _) = client
                .download_piece(manifest_piece, &task_id)
                .await
                .expect("download manifest");
            reader.read_to_end(&mut manifest_buf).await.unwrap();
        }
        None => {
            let (reader, _, _) = tcp_client
                .download_piece(manifest_piece, &task_id)
                .await
                .expect("download manifest");
            tokio::pin!(reader);
            reader.read_to_end(&mut manifest_buf).await.unwrap();
        }
    }
    let manifest = Manifest::decode(&manifest_buf);
    let total_bytes: u64 = manifest.entries.iter().map(|e| e.size).sum();
    println!(
        "manifest files={} total_bytes={}",
        manifest.entries.len(),
        total_bytes
    );

    // RDMA lands a whole window in registered memory before the consumer sees it, so the TCP path
    // is batched at the same granularity. Otherwise the two transports would be handing the digest
    // and the sink different sized blocks and the comparison would measure batching, not transport.
    let window_bytes = (args.chunk_mib * 1024 * 1024) as usize * args.max_inflight as usize;

    let t0 = Instant::now();
    let sem = Arc::new(tokio::sync::Semaphore::new(args.concurrency));
    let mut joins = Vec::new();
    for entry in manifest.entries.clone() {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let rdma_client = rdma_client.clone();
        let tcp_client = tcp_client.clone();
        let out_dir = args.out_dir.clone();
        let task_id = task_id.clone();
        let digest_algorithm = args.digest;
        let sink = args.sink;
        joins.push(tokio::spawn(async move {
            let _permit = permit;
            let start = Instant::now();
            let mut reader = match rdma_client.as_ref() {
                Some(client) => PieceReader::Rdma(
                    client
                        .download_piece(entry.piece, &task_id)
                        .await
                        .unwrap_or_else(|e| panic!("download piece {}: {e}", entry.piece))
                        .0,
                ),
                None => PieceReader::Tcp(Box::pin(
                    tcp_client
                        .download_piece(entry.piece, &task_id)
                        .await
                        .unwrap_or_else(|e| panic!("download piece {}: {e}", entry.piece))
                        .0,
                )),
            };

            let path = out_dir.join(&entry.relative_path);
            if sink != Sink::Null {
                if let Some(parent) = path.parent() {
                    tokio::fs::create_dir_all(parent).await.unwrap();
                }
            }
            let mut tokio_file = match sink {
                Sink::Tokio => Some(tokio::fs::File::create(&path).await.unwrap()),
                _ => None,
            };
            let pwrite_file = match sink {
                Sink::Pwrite => Some(Arc::new(std::fs::File::create(&path).unwrap())),
                _ => None,
            };

            // Attribute the wall clock to the three stages that touch every byte, so a slow run
            // can be blamed on the fabric, the digest, or the filesystem rather than guessed at.
            let mut cost = Cost::default();
            let mut hasher = Hasher::new(digest_algorithm);
            let mut written = 0u64;
            let mut spare = None;
            loop {
                let wait = Instant::now();
                let Some(block) = reader.next_block(window_bytes, spare.take()).await.unwrap()
                else {
                    cost.fabric += wait.elapsed();
                    break;
                };
                cost.fabric += wait.elapsed();

                if let Some(file) = pwrite_file.clone() {
                    // Both stages only read the block, so they run on separate blocking threads and
                    // each block reaches the file in one pwrite. tokio::fs::File would instead copy
                    // the block into a buffer of its own first.
                    let position = written;
                    written += block.bytes().len() as u64;
                    let block = Arc::new(block);

                    let digest = {
                        let block = block.clone();
                        tokio::task::spawn_blocking(move || {
                            let mut hasher = hasher;
                            let hash = Instant::now();
                            hasher.update(block.bytes());
                            (hasher, hash.elapsed())
                        })
                    };

                    let write = {
                        let block = block.clone();
                        tokio::task::spawn_blocking(move || {
                            let start = Instant::now();
                            file.write_all_at(block.bytes(), position).unwrap();
                            start.elapsed()
                        })
                    };

                    let (digest, write) = tokio::join!(digest, write);
                    let (returned_hasher, digest_cost) = digest.unwrap();
                    hasher = returned_hasher;
                    cost.digest += digest_cost;
                    cost.write += write.unwrap();
                    spare = Arc::try_unwrap(block).ok().and_then(Block::into_spare);
                    continue;
                }

                let bytes = block.bytes();
                written += bytes.len() as u64;

                let hash = Instant::now();
                hasher.update(bytes);
                cost.digest += hash.elapsed();

                if let Some(file) = tokio_file.as_mut() {
                    let write = Instant::now();
                    file.write_all(bytes).await.unwrap();
                    cost.write += write.elapsed();
                }
                spare = block.into_spare();
            }
            if let Some(mut file) = tokio_file {
                let write = Instant::now();
                file.flush().await.unwrap();
                cost.write += write.elapsed();
            }
            drop(reader);

            assert_eq!(written, entry.size, "{}", entry.relative_path);
            if let Some(digest) = hasher.finalize() {
                assert_eq!(
                    digest, entry.digest,
                    "digest mismatch {}",
                    entry.relative_path
                );
            }

            let elapsed = start.elapsed();
            println!(
                "OK piece={} path={} size={} elapsed={:?} goodput={:.2} Gbps {}",
                entry.piece,
                entry.relative_path,
                entry.size,
                elapsed,
                goodput_gbps(entry.size, elapsed),
                cost.report(entry.size),
            );
            cost
        }));
    }

    let mut total = Cost::default();
    for join in joins {
        total += join.await.unwrap();
    }

    let elapsed = t0.elapsed();
    println!(
        "MODEL_TRANSFER_DONE transport={} files={} bytes={} elapsed={:?} aggregate_goodput={:.2} Gbps digest={} sink={} concurrency={} chunk_mib={} inflight={} fabric_failed={}",
        args.transport.name(),
        manifest.entries.len(),
        total_bytes,
        elapsed,
        goodput_gbps(total_bytes, elapsed),
        args.digest.name(),
        args.sink.name(),
        args.concurrency,
        args.chunk_mib,
        args.max_inflight,
        client_fabric.map(|fabric| fabric.is_failed()).unwrap_or(false),
    );
    println!("MODEL_TRANSFER_COST {}", total.report(total_bytes));
}

fn goodput_gbps(bytes: u64, elapsed: Duration) -> f64 {
    (bytes as f64) * 8.0 / elapsed.as_secs_f64() / 1e9
}

/// Cost splits the per-byte work into the stages that can each cap end-to-end goodput. The stages
/// are summed across concurrent transfers, so with concurrency > 1 they overlap and add up to more
/// than the wall clock.
#[derive(Debug, Default, Clone, Copy)]
struct Cost {
    fabric: Duration,
    digest: Duration,
    write: Duration,
}

impl Cost {
    fn report(&self, bytes: u64) -> String {
        format!(
            "cost[fabric={:?} ({:.1} Gbps) digest={:?} ({:.1} Gbps) write={:?} ({:.1} Gbps)]",
            self.fabric,
            goodput_gbps(bytes, self.fabric),
            self.digest,
            goodput_gbps(bytes, self.digest),
            self.write,
            goodput_gbps(bytes, self.write),
        )
    }
}

impl std::ops::AddAssign for Cost {
    fn add_assign(&mut self, other: Self) {
        self.fabric += other.fabric;
        self.digest += other.digest;
        self.write += other.write;
    }
}

/// DigestAlgorithm selects the receive-side integrity check. `sha256` verifies against the manifest,
/// `crc32` matches what dfdaemon actually computes per piece, and `none` isolates the transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DigestAlgorithm {
    Sha256,
    Crc32,
    None,
}

impl DigestAlgorithm {
    fn parse(name: &str) -> Self {
        match name {
            "sha256" => Self::Sha256,
            "crc32" => Self::Crc32,
            "none" => Self::None,
            other => panic!("unsupported --digest {other} (expected sha256|crc32|none)"),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Sha256 => "sha256",
            Self::Crc32 => "crc32",
            Self::None => "none",
        }
    }
}

enum Hasher {
    Sha256(Sha256),
    Crc32(crc32fast::Hasher),
    None,
}

impl Hasher {
    fn new(algorithm: DigestAlgorithm) -> Self {
        match algorithm {
            DigestAlgorithm::Sha256 => Self::Sha256(Sha256::new()),
            DigestAlgorithm::Crc32 => Self::Crc32(crc32fast::Hasher::new()),
            DigestAlgorithm::None => Self::None,
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Sha256(hasher) => hasher.update(bytes),
            Self::Crc32(hasher) => hasher.update(bytes),
            Self::None => {}
        }
    }

    /// finalize returns a manifest-comparable digest, which only sha256 can produce.
    fn finalize(self) -> Option<String> {
        match self {
            Self::Sha256(hasher) => {
                Some(Digest::new(Algorithm::Sha256, hex::encode(hasher.finalize())).to_string())
            }
            Self::Crc32(hasher) => {
                std::hint::black_box(hasher.finalize());
                None
            }
            Self::None => None,
        }
    }
}

/// PieceReader is one in-flight piece download, over whichever transport the run selected. It
/// exists so the measurement loop below is written once: anything it reports as a difference
/// between the two transports comes from the transport rather than from two hand-written loops
/// that drifted apart.
enum PieceReader {
    Rdma(RDMAStreamReader),
    Tcp(Pin<Box<dyn AsyncRead + Send>>),
}

impl PieceReader {
    /// next_block returns the next block of the piece, or None at end of stream. RDMA hands back
    /// the registered window the NIC wrote into. TCP fills `spare` (a buffer recycled across
    /// blocks, so the socket path is not charged for an allocation per window) up to the same
    /// window size, short only at end of stream.
    async fn next_block(
        &mut self,
        window_bytes: usize,
        spare: Option<Vec<u8>>,
    ) -> io::Result<Option<Block>> {
        match self {
            Self::Rdma(reader) => Ok(reader.next_window().await?.map(Block::Window)),
            Self::Tcp(reader) => {
                let mut buf = spare.unwrap_or_else(|| Vec::with_capacity(window_bytes));
                buf.clear();
                while buf.len() < window_bytes {
                    if reader.read_buf(&mut buf).await? == 0 {
                        break;
                    }
                }

                Ok((!buf.is_empty()).then_some(Block::Owned(buf)))
            }
        }
    }
}

/// Block is one unit of received data handed to the digest and the sink.
enum Block {
    Window(ReceivedWindow),
    Owned(Vec<u8>),
}

impl Block {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Window(window) => window.bytes(),
            Self::Owned(buf) => buf.as_slice(),
        }
    }

    /// into_spare reclaims a TCP block's allocation for the next read. RDMA windows return to the
    /// registered pool on drop instead, which is what lets the fabric reuse them.
    fn into_spare(self) -> Option<Vec<u8>> {
        match self {
            Self::Window(_) => None,
            Self::Owned(buf) => Some(buf),
        }
    }
}

/// Transport selects which parent-side piece server the client downloads from. `rdma` uses the
/// libfabric data plane, `tcp` uses the Vortex piece server that RDMA falls back to. Both are
/// driven through the same digest and sink stages so the difference between them is the transport
/// and nothing else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Transport {
    Rdma,
    Tcp,
}

impl Transport {
    fn parse(name: &str) -> Self {
        match name {
            "rdma" => Self::Rdma,
            "tcp" => Self::Tcp,
            other => panic!("unsupported --transport {other} (expected rdma|tcp)"),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Rdma => "rdma",
            Self::Tcp => "tcp",
        }
    }
}

/// Sink selects where received bytes land. `tokio` writes through tokio::fs, `pwrite` writes each
/// window with one pwrite straight out of registered memory, and `null` drops them to measure the
/// fabric and digest without the filesystem in the way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sink {
    Tokio,
    Pwrite,
    Null,
}

impl Sink {
    fn parse(name: &str) -> Self {
        match name {
            "tokio" => Self::Tokio,
            "pwrite" => Self::Pwrite,
            "null" => Self::Null,
            other => panic!("unsupported --sink {other} (expected tokio|pwrite|null)"),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Tokio => "tokio",
            Self::Pwrite => "pwrite",
            Self::Null => "null",
        }
    }
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    match args.mode.as_str() {
        "server" => run_server(args).await,
        "client" => run_client(args).await,
        other => panic!("mode must be server|client, got {other}"),
    }
}
