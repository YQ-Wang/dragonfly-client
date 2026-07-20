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

//! Safe wrapper around the libfabric C shim (shim.c).
//!
//! One [`Fabric`] wraps one shared FI_EP_RDM endpoint. Transfers multiplex over it with
//! unique tags; per-operation completions are routed by a single progress thread that polls
//! the completion queue.
//!
//! Threading model: the shim requires providers to grant `FI_THREAD_SAFE`, allowing posts,
//! memory registration, and CQ progress to run concurrently. Cancellation and CQ reads retain
//! a narrow lock solely to protect the operation-context lifetime race between those paths.
//!
//! Buffer lifetime invariant: the NIC may DMA into a posted buffer until the completion
//! (success, error, or FI_ECANCELED after fi_cancel) is reaped. Every posted operation
//! therefore holds an `Arc<PinnedBuf>` in the pending-operation map, and the map entry is
//! only removed by the progress thread when the completion arrives. A buffer whose
//! operation never completes is intentionally leaked rather than freed under the NIC.

use dragonfly_client_core::{Error, Result};
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr, CString};
use std::fmt;
use std::hash::{BuildHasher, Hasher, RandomState};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::{oneshot, Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tracing::{error, info, warn};

/// ffi declares the C ABI exported by shim.c.
mod ffi {
    use std::ffi::{c_char, c_int, c_void};

    /// DfrdmaFabric is the opaque fabric handle defined by shim.c.
    #[repr(C)]
    pub struct DfrdmaFabric {
        _private: [u8; 0],
    }

    /// DfrdmaCompletion mirrors the batched CQ result structure in shim.c.
    #[derive(Clone, Copy)]
    #[repr(C)]
    pub struct DfrdmaCompletion {
        pub context: *mut c_void,
        pub flags: u64,
        pub len: usize,
        pub err: i64,
    }

    extern "C" {
        pub fn dfrdma_open(
            prov_name: *const c_char,
            domain_name: *const c_char,
            out: *mut *mut DfrdmaFabric,
        ) -> c_int;
        pub fn dfrdma_close(f: *mut DfrdmaFabric);
        pub fn dfrdma_provider_name(f: *mut DfrdmaFabric) -> *const c_char;
        pub fn dfrdma_max_msg_size(f: *mut DfrdmaFabric) -> usize;
        pub fn dfrdma_mr_required(f: *mut DfrdmaFabric) -> c_int;
        pub fn dfrdma_strerror(err: i64) -> *const c_char;
        pub fn dfrdma_getname(f: *mut DfrdmaFabric, buf: *mut u8, len: *mut usize) -> c_int;
        pub fn dfrdma_av_insert(
            f: *mut DfrdmaFabric,
            addr: *const u8,
            len: usize,
            out: *mut u64,
        ) -> c_int;
        pub fn dfrdma_mr_reg(
            f: *mut DfrdmaFabric,
            buf: *mut c_void,
            len: usize,
            mr_out: *mut *mut c_void,
            desc_out: *mut *mut c_void,
        ) -> c_int;
        pub fn dfrdma_mr_close(mr: *mut c_void) -> c_int;
        pub fn dfrdma_trecv(
            f: *mut DfrdmaFabric,
            buf: *mut c_void,
            len: usize,
            desc: *mut c_void,
            tag: u64,
            context: *mut c_void,
        ) -> i64;
        pub fn dfrdma_tsend(
            f: *mut DfrdmaFabric,
            buf: *const c_void,
            len: usize,
            desc: *mut c_void,
            dest: u64,
            tag: u64,
            context: *mut c_void,
        ) -> i64;
        pub fn dfrdma_cq_read_batch(
            f: *mut DfrdmaFabric,
            out: *mut DfrdmaCompletion,
            capacity: usize,
        ) -> c_int;
        pub fn dfrdma_cancel(f: *mut DfrdmaFabric, context: *mut c_void) -> c_int;
    }
}

/// BUDGET_UNIT is the granularity of the registered-memory budget semaphore.
const BUDGET_UNIT: u64 = 64 * 1024;

/// POST_RETRY_INTERVAL is the pause between retries when a transmit or receive queue is
/// full (FI_EAGAIN).
const POST_RETRY_INTERVAL: Duration = Duration::from_micros(200);

/// POST_RETRY_TIMEOUT bounds how long a single post is retried before giving up.
const POST_RETRY_TIMEOUT: Duration = Duration::from_secs(5);

/// PROGRESS_IDLE_INTERVAL is the sleep between completion-queue polls when idle.
const PROGRESS_IDLE_INTERVAL: Duration = Duration::from_micros(100);

/// PROGRESS_ACTIVE_INTERVAL bounds CPU use after a burst of active-operation yields.
const PROGRESS_ACTIVE_INTERVAL: Duration = Duration::from_micros(10);

/// PROGRESS_ACTIVE_YIELDS is the number of scheduler yields before a short active sleep.
const PROGRESS_ACTIVE_YIELDS: u32 = 64;

/// CQ_BATCH_SIZE amortizes the FFI, cancellation lock, and pending-map lock across completions.
const CQ_BATCH_SIZE: usize = 32;

/// CANCEL_GRACE_TIMEOUT is how long a timed-out operation waits for its cancellation
/// completion before its buffer is intentionally leaked.
const CANCEL_GRACE_TIMEOUT: Duration = Duration::from_secs(5);

/// GETNAME_INITIAL_CAPACITY is the initial buffer size for fi_getname; provider endpoint
/// addresses are typically well under this.
const GETNAME_INITIAL_CAPACITY: usize = 512;

/// fi_error converts a negative fi_errno value into a client error with the libfabric
/// error string.
fn fi_error(op: &str, rc: i64) -> Error {
    // Safety: dfrdma_strerror always returns a valid static string.
    let message = unsafe { CStr::from_ptr(ffi::dfrdma_strerror(rc)) };
    Error::Unknown(format!(
        "libfabric {} failed: {} ({})",
        op,
        message.to_string_lossy(),
        rc
    ))
}

/// Completion is the result of one posted operation.
#[derive(Debug, Clone, Copy)]
struct Completion {
    /// len is the number of bytes transferred.
    len: usize,

    /// err is 0 on success or a positive fi_errno value.
    err: i64,
}

/// CtxBlock is per-operation scratch space handed to libfabric as the operation context.
/// Providers that require the FI_CONTEXT/FI_CONTEXT2 mode bits write into it, so it must
/// stay allocated until the completion is reaped. 128 bytes covers fi_context2 (64 bytes)
/// with slack.
#[repr(C, align(16))]
struct CtxBlock([u8; 128]);

/// PendingOp tracks one posted operation until its completion arrives.
struct PendingOp {
    /// tx delivers the completion to the waiting task.
    tx: oneshot::Sender<Completion>,

    /// _ctx keeps the provider scratch space alive for the duration of the operation.
    _ctx: Box<CtxBlock>,

    /// _buf keeps the posted buffer (and its memory registration) alive until the hardware
    /// is done with it.
    _buf: Arc<PinnedBuf>,
}

/// Handle owns the raw fabric pointer and closes it on drop.
struct Handle(*mut ffi::DfrdmaFabric);

/// Safety: the shim rejects providers that do not grant FI_THREAD_SAFE, and the handle is
/// closed only after the progress thread exits.
unsafe impl Send for Handle {}
unsafe impl Sync for Handle {}

impl Drop for Handle {
    fn drop(&mut self) {
        // Safety: the pointer came from dfrdma_open and is dropped exactly once, after the
        // progress thread has exited and all memory regions were closed (PinnedBuf holds an
        // Arc<FabricInner>, so buffers cannot outlive this handle).
        unsafe { ffi::dfrdma_close(self.0) };
    }
}

/// FabricInner is shared by the fabric API, the progress thread, and pinned buffers.
struct FabricInner {
    /// cancel_progress_lock serializes cancellation with completion reaping so a context
    /// cannot be freed between the pending-map check and fi_cancel. Hot-path posts do not
    /// take this lock.
    cancel_progress_lock: Mutex<()>,

    /// handle is the raw fabric handle; declared after cancel_progress_lock but dropped first among
    /// users because buffers and the progress thread hold Arcs to this struct.
    handle: Handle,

    /// pending maps context addresses to in-flight operations.
    pending: Mutex<HashMap<usize, PendingOp>>,

    /// av maps peer endpoint addresses to fabric addresses (fi_addr_t).
    av: Mutex<HashMap<Vec<u8>, u64>>,

    /// shutdown stops the progress thread.
    shutdown: AtomicBool,

    /// mr_required is true when the provider needs local buffers registered.
    mr_required: bool,
}

impl FabricInner {
    /// mr_close closes a memory region.
    fn mr_close(&self, mr: *mut c_void) {
        if mr.is_null() {
            return;
        }
        // Safety: mr came from dfrdma_mr_reg on this handle and is closed exactly once.
        // The negotiated FI_THREAD_SAFE contract permits concurrent calls on the domain.
        let rc = unsafe { ffi::dfrdma_mr_close(mr) };
        if rc != 0 {
            warn!("failed to close rdma memory region: {}", rc);
        }
    }

    /// cancel_ctx attempts to cancel an operation that is still tracked. CQ progress cannot
    /// remove and free the context between this lookup and fi_cancel.
    fn cancel_ctx(&self, ctx_addr: usize) {
        let _progress_guard = self.cancel_progress_lock.lock().unwrap();
        if !self.pending.lock().unwrap().contains_key(&ctx_addr) {
            return;
        }

        // Safety: the pending entry owns the context block. If the provider has already queued
        // its completion, fi_cancel returns an appropriate not-found/already-complete error.
        let rc = unsafe { ffi::dfrdma_cancel(self.handle.0, ctx_addr as *mut c_void) };
        if rc != 0 {
            warn!("fi_cancel failed: {}", rc);
        }
    }
}

/// MrGuard closes a memory registration when dropped. It is a separate struct from
/// PinnedBuf so that field drop order guarantees the registration is closed before the
/// buffer memory is freed.
struct MrGuard {
    /// mr is the raw memory-region handle, null when the buffer is unregistered.
    mr: *mut c_void,

    /// inner is the owning fabric.
    inner: Arc<FabricInner>,
}

/// Safety: the owning fabric requires FI_THREAD_SAFE and closes the registration before freeing it.
unsafe impl Send for MrGuard {}
unsafe impl Sync for MrGuard {}

impl Drop for MrGuard {
    fn drop(&mut self) {
        self.inner.mr_close(self.mr);
    }
}

/// PinnedBuf is a fixed transfer buffer, optionally registered with the NIC, accounted
/// against the fabric's registered-memory budget.
///
/// The NIC writes into the buffer while operations are in flight, so the data is behind an
/// UnsafeCell and must only be accessed through [`PinnedBuf::as_mut_slice`] before posting
/// or after all completions have been reaped.
pub struct PinnedBuf {
    /// mr_guard closes the registration before data is freed (field order matters).
    mr_guard: MrGuard,

    /// desc is the local descriptor passed to post calls, null when unregistered.
    desc: *mut c_void,

    /// data is the buffer itself; the Vec is never resized so its pointer is stable.
    data: UnsafeCell<Vec<u8>>,

    /// _permit returns the buffer's bytes to the registered-memory budget on drop.
    _permit: OwnedSemaphorePermit,
}

/// Safety: concurrent access is limited to the NIC DMA-ing into disjoint posted ranges;
/// the CPU only touches the data before posting or after completions.
unsafe impl Send for PinnedBuf {}
unsafe impl Sync for PinnedBuf {}

impl PinnedBuf {
    /// len returns the buffer length in bytes.
    pub fn len(&self) -> usize {
        // Safety: the Vec header is only mutated at construction.
        unsafe { (*self.data.get()).len() }
    }

    /// is_empty returns whether the buffer is zero-length.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// as_mut_slice exposes the buffer for filling or reading.
    ///
    /// # Safety
    ///
    /// The caller must guarantee no operation over this buffer is currently posted.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn as_mut_slice(&self) -> &mut [u8] {
        (*self.data.get()).as_mut_slice()
    }

    /// ptr returns a raw pointer to the byte at `offset`.
    fn ptr(&self, offset: usize) -> *mut u8 {
        // Safety: offset is validated by the posting functions.
        unsafe { (*self.data.get()).as_mut_ptr().add(offset) }
    }

    /// into_vec extracts the buffer contents. When this Arc is the last reference (no
    /// operations in flight) the data is moved out without copying.
    pub fn into_vec(self: Arc<Self>) -> Vec<u8> {
        match Arc::try_unwrap(self) {
            Ok(buf) => {
                let PinnedBuf {
                    mr_guard,
                    data,
                    _permit,
                    ..
                } = buf;
                // Close the registration before handing out the memory.
                drop(mr_guard);
                data.into_inner()
            }
            Err(buf) => {
                warn!("rdma buffer still referenced, copying contents");
                // Safety: callers only convert after all completions were reaped.
                unsafe { (*buf.data.get()).clone() }
            }
        }
    }
}

/// BufferPoolStats is a snapshot of registered-buffer cache activity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferPoolStats {
    /// hits is the number of checkouts served by an existing registration.
    pub hits: u64,

    /// misses is the number of checkouts that allocated and registered a new buffer.
    pub misses: u64,

    /// cached_buffers is the number of idle registered buffers.
    pub cached_buffers: usize,

    /// cached_bytes is the total capacity of idle registered buffers.
    pub cached_bytes: usize,
}

/// BufferPool retains completed registered buffers for best-fit reuse. Cached buffers keep
/// their semaphore permits, so active plus idle memory remains bounded by the fabric budget.
struct BufferPool {
    /// idle contains buffers with no in-flight operation or reader.
    idle: Mutex<Vec<Arc<PinnedBuf>>>,

    /// changed wakes checkouts when a buffer is returned to the idle set.
    changed: Notify,

    /// closed prevents buffers from being retained after Fabric shutdown.
    closed: AtomicBool,

    /// hits counts successful idle-buffer reuse.
    hits: AtomicU64,

    /// misses counts new allocations and registrations.
    misses: AtomicU64,
}

impl BufferPool {
    /// new creates an empty registered-buffer pool.
    fn new() -> Self {
        Self {
            idle: Mutex::new(Vec::new()),
            changed: Notify::new(),
            closed: AtomicBool::new(false),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// take_best_fit removes the smallest idle buffer that can contain `len`. If none fits,
    /// all undersized buffers are evicted so their permits can satisfy a larger allocation.
    fn take_best_fit(&self, len: usize) -> Option<Arc<PinnedBuf>> {
        let evicted = {
            let mut idle = self.idle.lock().unwrap();
            let best = idle
                .iter()
                .enumerate()
                .filter(|(_, buf)| buf.len() >= len)
                .min_by_key(|(_, buf)| buf.len())
                .map(|(index, _)| index);
            if let Some(index) = best {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(idle.swap_remove(index));
            }
            std::mem::take(&mut *idle)
        };
        drop(evicted);
        None
    }

    /// recycle retains a completed buffer when this lease owns its only Arc.
    fn recycle(&self, buf: Arc<PinnedBuf>) {
        if self.closed.load(Ordering::Acquire) || Arc::strong_count(&buf) != 1 {
            return;
        }
        let mut idle = self.idle.lock().unwrap();
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        idle.push(buf);
        drop(idle);
        self.changed.notify_one();
    }

    /// close stops future retention and releases every idle registration.
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.changed.notify_waiters();
        self.idle.lock().unwrap().clear();
    }

    /// stats returns a consistent-enough diagnostic snapshot.
    fn stats(&self) -> BufferPoolStats {
        let idle = self.idle.lock().unwrap();
        BufferPoolStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            cached_buffers: idle.len(),
            cached_bytes: idle.iter().map(|buf| buf.len()).sum(),
        }
    }
}

/// PooledBuf is an exclusive lease over a registered buffer. Dropping the lease returns the
/// buffer to its pool only after every operation-owned Arc has been reaped.
pub struct PooledBuf {
    /// buf is taken by Drop and recycled when it has no other owners.
    buf: Option<Arc<PinnedBuf>>,

    /// pool receives the completed buffer.
    pool: Arc<BufferPool>,

    /// logical_len is the transfer-visible prefix of the physical buffer.
    logical_len: usize,
}

impl PooledBuf {
    /// buffer returns the registered allocation for fabric post calls.
    pub(crate) fn buffer(&self) -> &Arc<PinnedBuf> {
        self.buf.as_ref().expect("pooled buffer")
    }

    /// len returns the transfer-visible length.
    pub fn len(&self) -> usize {
        self.logical_len
    }

    /// is_empty returns whether the transfer-visible range is empty.
    pub fn is_empty(&self) -> bool {
        self.logical_len == 0
    }

    /// as_mut_slice exposes only the transfer-visible prefix.
    ///
    /// # Safety
    ///
    /// The caller must guarantee no operation over this buffer is currently posted.
    pub unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buffer().as_mut_slice()[..self.logical_len]
    }

    /// into_reader turns a completed receive lease into an async reader without moving or
    /// copying its registered allocation.
    pub fn into_reader(self) -> PooledBufReader {
        PooledBufReader {
            buffer: self,
            position: 0,
        }
    }
}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        if let Some(buf) = self.buf.take() {
            self.pool.recycle(buf);
        }
    }
}

/// PooledBufReader reads a completed receive directly from registered memory. Its lease returns
/// the registration to the pool when the reader is consumed or dropped.
pub struct PooledBufReader {
    /// buffer owns the registered-memory lease.
    buffer: PooledBuf,

    /// position is the next byte exposed to the downstream storage writer.
    position: usize,
}

impl fmt::Debug for PooledBufReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PooledBufReader")
            .field("length", &self.buffer.len())
            .field("position", &self.position)
            .finish()
    }
}

impl AsyncRead for PooledBufReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let remaining = self.buffer.len().saturating_sub(self.position);
        if remaining == 0 || output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let read_len = remaining.min(output.remaining());
        let start = self.position;
        let end = start + read_len;
        // Safety: PooledBufReader is constructed only after every receive completion was
        // reaped, and it exclusively owns the lease while exposing this range.
        let content = unsafe { &self.buffer.as_mut_slice()[start..end] };
        output.put_slice(content);
        self.position = end;
        Poll::Ready(Ok(()))
    }
}

/// OpHandle is a posted operation whose completion can be awaited exactly once.
pub struct OpHandle {
    /// ctx_addr identifies the operation in the pending map (and to fi_cancel).
    ctx_addr: usize,

    /// rx receives the completion from the progress thread.
    rx: Option<oneshot::Receiver<Completion>>,

    /// inner lets Drop cancel an operation when its owner is abandoned by an early return or
    /// asynchronous task cancellation.
    inner: Arc<FabricInner>,

    /// armed remains true until wait observes a completion or explicitly finishes cancellation.
    armed: bool,
}

impl OpHandle {
    /// cancel requests cancellation while leaving the pending entry responsible for the context
    /// and buffer until the provider reports a completion.
    fn cancel(&self) {
        self.inner.cancel_ctx(self.ctx_addr);
    }
}

impl Drop for OpHandle {
    fn drop(&mut self) {
        if self.armed {
            self.cancel();
        }
    }
}

/// Fabric wraps one shared libfabric RDM endpoint. Share an instance across transfers for one
/// transport role; the downloader and server currently create separate instances. Endpoints are
/// heavyweight, especially on EFA.
pub struct Fabric {
    /// inner is shared with the progress thread and pinned buffers.
    inner: Arc<FabricInner>,

    /// progress is the completion-polling thread, joined on drop.
    progress: Option<std::thread::JoinHandle<()>>,

    /// provider is the concrete provider name selected at runtime (e.g. "efa",
    /// "verbs;ofi_rxm", "tcp").
    provider: String,

    /// local_endpoint is this endpoint's provider-opaque address (fi_getname) to advertise
    /// to peers.
    local_endpoint: Vec<u8>,

    /// max_msg_size is the provider's maximum single-message size.
    max_msg_size: usize,

    /// budget bounds registered/pinned memory, in BUDGET_UNIT permits.
    budget: Arc<Semaphore>,

    /// budget_permits is the total number of permits in the budget.
    budget_permits: u32,

    /// pool retains idle registrations for best-fit reuse.
    pool: Arc<BufferPool>,

    /// tag_counter feeds unique transfer tags.
    tag_counter: AtomicU64,

    /// tag_hasher randomizes transfer tags so concurrent transfers cannot collide by
    /// accident and tags are not guessable across processes.
    tag_hasher: RandomState,
}

impl Fabric {
    /// new opens a fabric endpoint on the given provider ("efa", "verbs", "tcp", ...). When
    /// `provider` is None, hardware providers are tried in preference order; an unrestricted
    /// libfabric lookup is used only when `allow_software_provider` is explicitly enabled.
    /// `device` optionally pins a specific libfabric domain (e.g. "efa_0-rdm" or "rdmap16s27").
    pub fn new(
        provider: Option<&str>,
        device: Option<&str>,
        max_registered_bytes: u64,
        allow_software_provider: bool,
    ) -> Result<Self> {
        let device_cstr = device
            .map(CString::new)
            .transpose()
            .map_err(|_| Error::InvalidParameter)?;

        let mut handle: *mut ffi::DfrdmaFabric = std::ptr::null_mut();
        let candidates: Vec<Option<&str>> = match provider {
            Some(provider) => vec![Some(provider)],
            None if allow_software_provider => vec![Some("efa"), Some("verbs"), None],
            None => vec![Some("efa"), Some("verbs")],
        };
        let mut last_rc = 0;
        for candidate in candidates {
            let provider_cstr = candidate
                .map(CString::new)
                .transpose()
                .map_err(|_| Error::InvalidParameter)?;
            let mut candidate_handle: *mut ffi::DfrdmaFabric = std::ptr::null_mut();
            // Safety: the strings outlive the call; out pointer is valid.
            let rc = unsafe {
                ffi::dfrdma_open(
                    provider_cstr
                        .as_ref()
                        .map_or(std::ptr::null(), |s| s.as_ptr() as *const c_char),
                    device_cstr
                        .as_ref()
                        .map_or(std::ptr::null(), |s| s.as_ptr() as *const c_char),
                    &mut candidate_handle,
                )
            };
            if rc == 0 && !candidate_handle.is_null() {
                handle = candidate_handle;
                break;
            }
            last_rc = rc;
        }
        if handle.is_null() {
            return Err(fi_error("hardware provider discovery", last_rc as i64));
        }

        // Safety: handle is valid; provider name points into fi_info owned by the handle.
        let (provider_name, max_msg_size, mr_required) = unsafe {
            (
                CStr::from_ptr(ffi::dfrdma_provider_name(handle))
                    .to_string_lossy()
                    .into_owned(),
                ffi::dfrdma_max_msg_size(handle),
                ffi::dfrdma_mr_required(handle) != 0,
            )
        };

        let mut endpoint = vec![0u8; GETNAME_INITIAL_CAPACITY];
        let mut endpoint_len = endpoint.len();
        // Safety: buffer and length pointer are valid.
        let mut rc =
            unsafe { ffi::dfrdma_getname(handle, endpoint.as_mut_ptr(), &mut endpoint_len) };
        if rc == 1 {
            endpoint.resize(endpoint_len, 0);
            // Safety: buffer was resized to the length requested by the provider.
            rc = unsafe { ffi::dfrdma_getname(handle, endpoint.as_mut_ptr(), &mut endpoint_len) };
        }
        if rc != 0 {
            // Safety: handle is valid and not yet shared.
            unsafe { ffi::dfrdma_close(handle) };
            return Err(fi_error("fi_getname", rc as i64));
        }
        endpoint.truncate(endpoint_len);

        let budget_permits = (max_registered_bytes / BUDGET_UNIT)
            .max(1)
            .min(Semaphore::MAX_PERMITS as u64)
            .min(u32::MAX as u64) as u32;

        let inner = Arc::new(FabricInner {
            cancel_progress_lock: Mutex::new(()),
            handle: Handle(handle),
            pending: Mutex::new(HashMap::new()),
            av: Mutex::new(HashMap::new()),
            shutdown: AtomicBool::new(false),
            mr_required,
        });

        let progress_inner = inner.clone();
        let progress = std::thread::Builder::new()
            .name("rdma-progress".to_string())
            .spawn(move || progress_loop(progress_inner))
            .map_err(|err| Error::Unknown(format!("failed to spawn rdma progress: {}", err)))?;

        info!(
            "opened rdma fabric: provider {}, max message size {}, mr required {}, endpoint {} bytes",
            provider_name,
            max_msg_size,
            mr_required,
            endpoint.len()
        );

        Ok(Self {
            inner,
            progress: Some(progress),
            provider: provider_name,
            local_endpoint: endpoint,
            max_msg_size,
            budget: Arc::new(Semaphore::new(budget_permits as usize)),
            budget_permits,
            pool: Arc::new(BufferPool::new()),
            tag_counter: AtomicU64::new(0),
            tag_hasher: RandomState::new(),
        })
    }

    /// provider returns the concrete provider name selected at runtime.
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// local_endpoint returns the provider-opaque endpoint address to advertise to peers.
    pub fn local_endpoint(&self) -> &[u8] {
        &self.local_endpoint
    }

    /// max_msg_size returns the provider's maximum single-message size.
    pub fn max_msg_size(&self) -> usize {
        self.max_msg_size
    }

    /// buffer_pool_stats returns registered-buffer reuse and idle-memory counters.
    pub fn buffer_pool_stats(&self) -> BufferPoolStats {
        self.pool.stats()
    }

    /// next_tag derives a pseudo-random transfer base tag from a process-local counter and
    /// randomized hash seed. Each chunk uses a consecutive tag from this base; collisions are
    /// improbable rather than mathematically impossible.
    pub fn next_tag(&self) -> u64 {
        let counter = self.tag_counter.fetch_add(1, Ordering::Relaxed);
        let mut hasher = self.tag_hasher.build_hasher();
        hasher.write_u64(counter);
        hasher.finish()
    }

    /// alloc_buffer allocates a transfer buffer of `len` bytes, waits for registered-memory
    /// budget, and registers the buffer when the provider requires it. Production transfers
    /// should use [`Fabric::acquire_buffer`] so the registration is returned to the pool.
    pub async fn alloc_buffer(&self, len: usize) -> Result<Arc<PinnedBuf>> {
        let mut pooled = self.acquire_buffer(len).await?;
        Ok(pooled.buf.take().expect("pooled buffer"))
    }

    /// acquire_buffer checks out a best-fit registered buffer. It waits for either a returned
    /// registration or fresh budget, evicting undersized idle buffers to avoid permit starvation.
    pub async fn acquire_buffer(&self, len: usize) -> Result<PooledBuf> {
        if len == 0 {
            return Err(Error::InvalidParameter);
        }
        let permits = self.buffer_permits(len)?;

        loop {
            if self.pool.closed.load(Ordering::Acquire) {
                return Err(Error::Unknown("rdma fabric is shut down".to_string()));
            }

            let changed = self.pool.changed.notified();
            if let Some(buf) = self.pool.take_best_fit(len) {
                return Ok(PooledBuf {
                    buf: Some(buf),
                    pool: self.pool.clone(),
                    logical_len: len,
                });
            }

            match self.budget.clone().try_acquire_many_owned(permits) {
                Ok(permit) => {
                    let buf = self.register_buffer(len, permit)?;
                    self.pool.misses.fetch_add(1, Ordering::Relaxed);
                    return Ok(PooledBuf {
                        buf: Some(buf),
                        pool: self.pool.clone(),
                        logical_len: len,
                    });
                }
                Err(TryAcquireError::Closed) => {
                    return Err(Error::Unknown("rdma fabric is shut down".to_string()));
                }
                Err(TryAcquireError::NoPermits) => {}
            }

            let budget = self.budget.clone();
            tokio::select! {
                _ = changed => continue,
                permit = budget.acquire_many_owned(permits) => {
                    let permit = permit.map_err(|_| {
                        Error::Unknown("rdma fabric is shut down".to_string())
                    })?;
                    // A suitable registration may have arrived in parallel with the
                    // semaphore grant. Prefer it and return the redundant permits.
                    if let Some(buf) = self.pool.take_best_fit(len) {
                        drop(permit);
                        return Ok(PooledBuf {
                            buf: Some(buf),
                            pool: self.pool.clone(),
                            logical_len: len,
                        });
                    }
                    let buf = self.register_buffer(len, permit)?;
                    self.pool.misses.fetch_add(1, Ordering::Relaxed);
                    return Ok(PooledBuf {
                        buf: Some(buf),
                        pool: self.pool.clone(),
                        logical_len: len,
                    });
                }
            }
        }
    }

    /// buffer_permits validates a requested buffer and returns its budget units.
    fn buffer_permits(&self, len: usize) -> Result<u32> {
        let permits = len.div_ceil(BUDGET_UNIT as usize).max(1);
        if permits > self.budget_permits as usize {
            return Err(Error::Unknown(format!(
                "buffer of {} bytes exceeds the rdma registered-memory budget",
                len
            )));
        }
        Ok(permits as u32)
    }

    /// register_buffer allocates stable storage and registers it using an already-owned budget.
    fn register_buffer(&self, len: usize, permit: OwnedSemaphorePermit) -> Result<Arc<PinnedBuf>> {
        let mut data = vec![0u8; len];
        let mut mr: *mut c_void = std::ptr::null_mut();
        let mut desc: *mut c_void = std::ptr::null_mut();
        // Safety: data outlives the registration; PinnedBuf's field order guarantees the
        // MrGuard closes the registration before the Vec is freed. FI_THREAD_SAFE permits
        // registration concurrently with endpoint and CQ operations.
        let rc = unsafe {
            ffi::dfrdma_mr_reg(
                self.inner.handle.0,
                data.as_mut_ptr() as *mut c_void,
                len,
                &mut mr,
                &mut desc,
            )
        };
        if rc != 0 {
            if self.inner.mr_required {
                return Err(fi_error("fi_mr_reg", rc as i64));
            }
            warn!(
                "rdma memory registration failed ({}), continuing unregistered",
                rc
            );
            mr = std::ptr::null_mut();
            desc = std::ptr::null_mut();
        }

        Ok(Arc::new(PinnedBuf {
            mr_guard: MrGuard {
                mr,
                inner: self.inner.clone(),
            },
            desc,
            data: UnsafeCell::new(data),
            _permit: permit,
        }))
    }

    /// resolve inserts a peer endpoint address into the address vector, returning its
    /// fabric address. Results are cached.
    pub fn resolve(&self, endpoint: &[u8]) -> Result<u64> {
        // Hold the cache lock through insertion to avoid racing duplicate AV entries.
        let mut av = self.inner.av.lock().unwrap();
        if let Some(addr) = av.get(endpoint) {
            return Ok(*addr);
        }

        let mut addr: u64 = 0;
        // Safety: endpoint bytes are valid for the call and FI_THREAD_SAFE permits
        // concurrent access to the address vector and endpoint.
        let rc = unsafe {
            ffi::dfrdma_av_insert(
                self.inner.handle.0,
                endpoint.as_ptr(),
                endpoint.len(),
                &mut addr,
            )
        };
        if rc != 0 {
            return Err(fi_error("fi_av_insert", rc as i64));
        }

        av.insert(endpoint.to_vec(), addr);
        Ok(addr)
    }

    /// post_recv posts a tagged receive of `len` bytes into `buf` at `offset`.
    pub async fn post_recv(
        &self,
        buf: &Arc<PinnedBuf>,
        offset: usize,
        len: usize,
        tag: u64,
    ) -> Result<OpHandle> {
        self.post(buf, offset, len, tag, None).await
    }

    /// post_send posts a tagged send of `len` bytes from `buf` at `offset` to `dest`.
    pub async fn post_send(
        &self,
        buf: &Arc<PinnedBuf>,
        offset: usize,
        len: usize,
        tag: u64,
        dest: u64,
    ) -> Result<OpHandle> {
        self.post(buf, offset, len, tag, Some(dest)).await
    }

    /// post registers the pending operation, then posts it, retrying while the queue is
    /// full. `dest` selects send (Some) or receive (None).
    async fn post(
        &self,
        buf: &Arc<PinnedBuf>,
        offset: usize,
        len: usize,
        tag: u64,
        dest: Option<u64>,
    ) -> Result<OpHandle> {
        if offset.checked_add(len).is_none_or(|end| end > buf.len()) {
            return Err(Error::InvalidParameter);
        }

        let ctx = Box::new(CtxBlock([0u8; 128]));
        let ctx_addr = &*ctx as *const CtxBlock as usize;
        let (tx, rx) = oneshot::channel();

        // Register the pending operation before posting so a completion arriving
        // immediately after the post always finds it.
        self.inner.pending.lock().unwrap().insert(
            ctx_addr,
            PendingOp {
                tx,
                _ctx: ctx,
                _buf: buf.clone(),
            },
        );

        let deadline = tokio::time::Instant::now() + POST_RETRY_TIMEOUT;
        loop {
            // Safety: buf outlives the operation via the pending map; the range was
            // validated above; ctx_addr points at the boxed context block owned by the
            // pending map. The shim requires FI_THREAD_SAFE, so posts may run concurrently.
            let rc = unsafe {
                match dest {
                    Some(dest) => ffi::dfrdma_tsend(
                        self.inner.handle.0,
                        buf.ptr(offset) as *const c_void,
                        len,
                        buf.desc,
                        dest,
                        tag,
                        ctx_addr as *mut c_void,
                    ),
                    None => ffi::dfrdma_trecv(
                        self.inner.handle.0,
                        buf.ptr(offset) as *mut c_void,
                        len,
                        buf.desc,
                        tag,
                        ctx_addr as *mut c_void,
                    ),
                }
            };

            match rc {
                0 => {
                    return Ok(OpHandle {
                        ctx_addr,
                        rx: Some(rx),
                        inner: self.inner.clone(),
                        armed: true,
                    })
                }
                1 => {
                    if tokio::time::Instant::now() >= deadline {
                        self.inner.pending.lock().unwrap().remove(&ctx_addr);
                        return Err(Error::Unknown(
                            "rdma post retries exhausted, queue stayed full".to_string(),
                        ));
                    }
                    tokio::time::sleep(POST_RETRY_INTERVAL).await;
                }
                rc => {
                    self.inner.pending.lock().unwrap().remove(&ctx_addr);
                    let op = if dest.is_some() {
                        "fi_tsend"
                    } else {
                        "fi_trecv"
                    };
                    return Err(fi_error(op, rc));
                }
            }
        }
    }

    /// wait awaits an operation's completion, returning the transferred length. On timeout
    /// the operation is cancelled; if the cancellation completion does not arrive within a
    /// grace period, the buffer is left pinned (leaked) rather than freed under the NIC.
    pub async fn wait(&self, mut op: OpHandle, timeout: Duration) -> Result<usize> {
        let ctx_addr = op.ctx_addr;
        let rx = op.rx.take().expect("rdma operation receiver");
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(completion)) if completion.err == 0 => {
                op.armed = false;
                Ok(completion.len)
            }
            Ok(Ok(completion)) => {
                op.armed = false;
                Err(fi_error("operation", completion.err))
            }
            Ok(Err(_)) => {
                op.armed = false;
                Err(Error::Unknown("rdma fabric is shut down".to_string()))
            }
            Err(_) => {
                op.cancel();

                // Wait for the cancellation (or late) completion so the pending map entry
                // and its buffer reference are released.
                let deadline = tokio::time::Instant::now() + CANCEL_GRACE_TIMEOUT;
                loop {
                    if !self.inner.pending.lock().unwrap().contains_key(&ctx_addr) {
                        break;
                    }
                    if tokio::time::Instant::now() >= deadline {
                        error!(
                            "rdma operation neither completed nor cancelled; leaking its buffer"
                        );
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                op.armed = false;
                Err(Error::Unknown("rdma operation timed out".to_string()))
            }
        }
    }
}

impl Drop for Fabric {
    fn drop(&mut self) {
        self.inner.shutdown.store(true, Ordering::Relaxed);
        if let Some(progress) = self.progress.take() {
            let _ = progress.join();
        }
        // Release idle registrations before the endpoint handle can close. Leases still held by
        // downstream readers observe the closed pool and release their buffers on drop.
        self.pool.close();
        // Pending operations are NOT cleared here: their buffers must stay alive until the
        // endpoint closes. FabricInner's field order (handle before pending) closes the
        // endpoint before the map releases the buffers.
    }
}

/// progress_loop polls the completion queue and routes completions to waiting tasks. It is
/// the only place pending-map entries (and thus buffer references) are released on the
/// success path.
fn progress_loop(inner: Arc<FabricInner>) {
    let mut active_yields = 0u32;
    while !inner.shutdown.load(Ordering::Relaxed) {
        let mut progressed = false;
        loop {
            let mut entries = [ffi::DfrdmaCompletion {
                context: std::ptr::null_mut(),
                flags: 0,
                len: 0,
                err: 0,
            }; CQ_BATCH_SIZE];
            let mut completed: [Option<(Completion, PendingOp)>; CQ_BATCH_SIZE] =
                std::array::from_fn(|_| None);
            let rc = {
                // Keep cancellation from observing these contexts until every CQ result is
                // removed from pending. Posts and registrations remain concurrent.
                let _guard = inner.cancel_progress_lock.lock().unwrap();
                // Safety: the handle is valid until FabricInner drops, which cannot happen
                // while this thread holds an Arc to it. FI_THREAD_SAFE permits concurrent
                // posts on other threads; entries has the capacity passed to the shim.
                let rc = unsafe {
                    ffi::dfrdma_cq_read_batch(inner.handle.0, entries.as_mut_ptr(), entries.len())
                };
                if rc > 0 {
                    // Batched removal under the cancellation/CQ lock closes the lifetime gap
                    // between cancel_ctx's pending lookup and its fi_cancel call.
                    let mut pending = inner.pending.lock().unwrap();
                    for index in 0..rc as usize {
                        if let Some(op) = pending.remove(&(entries[index].context as usize)) {
                            completed[index] = Some((
                                Completion {
                                    len: entries[index].len,
                                    err: entries[index].err,
                                },
                                op,
                            ));
                        }
                    }
                }
                rc
            };

            match rc {
                count if count > 0 => {
                    progressed = true;
                    // Deliver and drop outside both locks because dropping an operation may
                    // close its memory region.
                    for (index, completion) in
                        completed.into_iter().take(count as usize).enumerate()
                    {
                        if let Some((completion, op)) = completion {
                            // The receiver may have timed out and gone; that is fine, the
                            // buffer reference is released either way.
                            let _ = op.tx.send(completion);
                        } else {
                            let _ = entries[index].flags;
                            warn!("rdma completion for unknown context, dropping");
                        }
                    }
                }
                0 => break,
                rc => {
                    error!("rdma completion queue read failed: {}", rc);
                    return;
                }
            }
        }

        if progressed {
            active_yields = 0;
        } else if inner.pending.lock().unwrap().is_empty() {
            active_yields = 0;
            std::thread::sleep(PROGRESS_IDLE_INTERVAL);
        } else if active_yields < PROGRESS_ACTIVE_YIELDS {
            active_yields += 1;
            std::thread::yield_now();
        } else {
            active_yields = 0;
            std::thread::sleep(PROGRESS_ACTIVE_INTERVAL);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// open_fabric opens a fabric on whatever provider is available (on development hosts
    /// without RDMA hardware libfabric selects its tcp or sockets provider, exercising the
    /// exact same code path as efa/verbs).
    fn open_fabric() -> Fabric {
        Fabric::new(None, None, 64 * 1024 * 1024, true).expect("libfabric endpoint")
    }

    #[test]
    fn automatic_provider_never_silently_selects_software() {
        if let Ok(fabric) = Fabric::new(None, None, 64 * 1024 * 1024, false) {
            assert!(
                fabric.provider() == "efa" || fabric.provider().starts_with("verbs"),
                "unexpected automatic provider: {}",
                fabric.provider()
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn transfers_chunked_messages_between_endpoints() {
        let sender = open_fabric();
        let receiver = open_fabric();
        assert_eq!(sender.provider(), receiver.provider());
        assert!(!receiver.local_endpoint().is_empty());

        // 10 MiB payload in 4 MiB chunks: two full chunks and one partial.
        let length: usize = 10 * 1024 * 1024;
        let chunk_size: usize = (4 * 1024 * 1024).min(sender.max_msg_size());
        let payload: Vec<u8> = (0..length).map(|i| (i % 251) as u8).collect();
        let tag = sender.next_tag();

        let send_buf = sender.alloc_buffer(length).await.unwrap();
        // Safety: nothing is posted over the buffer yet.
        unsafe { send_buf.as_mut_slice() }.copy_from_slice(&payload);

        // Receiver posts every chunk before the sender transmits (rendezvous ordering).
        let recv_buf = receiver.alloc_buffer(length).await.unwrap();
        let mut recv_ops = Vec::new();
        let mut offset = 0;
        let mut chunk = 0u64;
        while offset < length {
            let len = chunk_size.min(length - offset);
            recv_ops.push((
                len,
                receiver
                    .post_recv(&recv_buf, offset, len, tag.wrapping_add(chunk))
                    .await
                    .unwrap(),
            ));
            offset += len;
            chunk += 1;
        }

        let dest = sender.resolve(receiver.local_endpoint()).unwrap();
        let mut send_ops = Vec::new();
        let mut offset = 0;
        let mut chunk = 0u64;
        while offset < length {
            let len = chunk_size.min(length - offset);
            send_ops.push(
                sender
                    .post_send(&send_buf, offset, len, tag.wrapping_add(chunk), dest)
                    .await
                    .unwrap(),
            );
            offset += len;
            chunk += 1;
        }

        let timeout = Duration::from_secs(10);
        for op in send_ops {
            sender.wait(op, timeout).await.unwrap();
        }
        for (expected_len, op) in recv_ops {
            let len = receiver.wait(op, timeout).await.unwrap();
            assert_eq!(len, expected_len);
        }

        assert_eq!(recv_buf.into_vec(), payload);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dropping_an_operation_cancels_and_reaps_it() {
        let fabric = open_fabric();
        let buf = fabric.alloc_buffer(4096).await.unwrap();
        let op = fabric
            .post_recv(&buf, 0, 4096, fabric.next_tag())
            .await
            .unwrap();
        assert_eq!(fabric.inner.pending.lock().unwrap().len(), 1);

        drop(op);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !fabric.inner.pending.lock().unwrap().is_empty() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "cancelled operation was not reaped"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rejects_buffers_over_the_registered_memory_budget() {
        let fabric = Fabric::new(None, None, 1024 * 1024, true).expect("libfabric endpoint");
        assert!(fabric.alloc_buffer(2 * 1024 * 1024).await.is_err());
        assert!(fabric.alloc_buffer(512 * 1024).await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pooled_buffers_reuse_the_best_fit_registration() {
        let fabric = Fabric::new(None, None, 4 * 1024 * 1024, true).expect("libfabric endpoint");
        let first = fabric.acquire_buffer(1024 * 1024).await.unwrap();
        let first_ptr = Arc::as_ptr(first.buffer());
        drop(first);

        let second = fabric.acquire_buffer(512 * 1024).await.unwrap();
        assert_eq!(Arc::as_ptr(second.buffer()), first_ptr);
        assert_eq!(second.len(), 512 * 1024);
        assert_eq!(second.buffer().len(), 1024 * 1024);

        let stats = fabric.buffer_pool_stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pooled_reader_hides_unused_capacity_and_recycles() {
        let fabric = Fabric::new(None, None, 1024 * 1024, true).expect("libfabric endpoint");
        let mut buffer = fabric.acquire_buffer(4096).await.unwrap();
        // Safety: this lease has not been posted.
        unsafe { buffer.as_mut_slice() }.fill(0x5a);
        drop(buffer);

        let smaller = fabric.acquire_buffer(17).await.unwrap();
        let mut reader = smaller.into_reader();
        let mut content = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut content)
            .await
            .unwrap();
        assert_eq!(content, vec![0x5a; 17]);
        drop(reader);

        let stats = fabric.buffer_pool_stats();
        assert_eq!(stats.cached_buffers, 1);
        assert_eq!(stats.cached_bytes, 4096);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pooled_waiter_reuses_returned_full_budget_buffer() {
        let fabric =
            Arc::new(Fabric::new(None, None, 1024 * 1024, true).expect("libfabric endpoint"));
        let first = fabric.acquire_buffer(1024 * 1024).await.unwrap();
        let first_ptr = Arc::as_ptr(first.buffer()) as usize;

        let waiting_fabric = fabric.clone();
        let waiter = tokio::spawn(async move { waiting_fabric.acquire_buffer(1024 * 1024).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("pool waiter timed out")
            .unwrap()
            .unwrap();
        assert_eq!(Arc::as_ptr(second.buffer()) as usize, first_ptr);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn in_flight_buffer_is_not_returned_to_pool() {
        let fabric = open_fabric();
        let buffer = fabric.acquire_buffer(4096).await.unwrap();
        let op = fabric
            .post_recv(buffer.buffer(), 0, 4096, fabric.next_tag())
            .await
            .unwrap();
        drop(buffer);
        assert_eq!(fabric.buffer_pool_stats().cached_buffers, 0);

        drop(op);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !fabric.inner.pending.lock().unwrap().is_empty() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "cancelled operation was not reaped"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(fabric.buffer_pool_stats().cached_buffers, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn closing_pool_releases_idle_registered_budget() {
        let fabric = Fabric::new(None, None, 1024 * 1024, true).expect("libfabric endpoint");
        let buffer = fabric.acquire_buffer(1024 * 1024).await.unwrap();
        drop(buffer);
        assert_eq!(fabric.budget.available_permits(), 0);
        assert_eq!(fabric.buffer_pool_stats().cached_bytes, 1024 * 1024);

        fabric.pool.close();
        assert_eq!(
            fabric.budget.available_permits(),
            fabric.budget_permits as usize
        );
        assert_eq!(fabric.buffer_pool_stats().cached_buffers, 0);
    }
}
