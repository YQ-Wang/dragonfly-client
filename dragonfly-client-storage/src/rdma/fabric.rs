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
//! Threading model: every shim call is serialized by `FabricInner::call_lock`, which makes
//! the wrapper safe regardless of the threading level the provider grants.
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
use std::hash::{BuildHasher, Hasher, RandomState};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
use tracing::{error, info, warn};

/// ffi declares the C ABI exported by shim.c.
mod ffi {
    use std::ffi::{c_char, c_int, c_void};

    /// DfrdmaFabric is the opaque fabric handle defined by shim.c.
    #[repr(C)]
    pub struct DfrdmaFabric {
        _private: [u8; 0],
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
        pub fn dfrdma_cq_read(
            f: *mut DfrdmaFabric,
            context: *mut *mut c_void,
            flags: *mut u64,
            len: *mut usize,
            err: *mut i64,
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

/// Safety: the handle is only used under FabricInner::call_lock.
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
    /// call_lock serializes every shim call on the handle.
    call_lock: Mutex<()>,

    /// handle is the raw fabric handle; declared after call_lock but dropped last among
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
        let _guard = self.call_lock.lock().unwrap();
        // Safety: mr came from dfrdma_mr_reg on this handle and is closed exactly once.
        let rc = unsafe { ffi::dfrdma_mr_close(mr) };
        if rc != 0 {
            warn!("failed to close rdma memory region: {}", rc);
        }
    }

    /// cancel_ctx attempts to cancel an operation that is still tracked. The call lock is taken
    /// before inspecting pending to preserve the same lock order as completion progress.
    fn cancel_ctx(&self, ctx_addr: usize) {
        let _call_guard = self.call_lock.lock().unwrap();
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

/// Safety: the raw handle is only used under the fabric call lock.
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

/// Fabric wraps one shared libfabric RDM endpoint. Create one per process and share it
/// across transfers; endpoints are heavyweight (especially on EFA).
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
            call_lock: Mutex::new(()),
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

    /// next_tag returns a fresh transfer tag. Tags are randomized so concurrent transfers
    /// on the shared endpoint cannot cross-talk.
    pub fn next_tag(&self) -> u64 {
        let counter = self.tag_counter.fetch_add(1, Ordering::Relaxed);
        let mut hasher = self.tag_hasher.build_hasher();
        hasher.write_u64(counter);
        hasher.finish()
    }

    /// alloc_buffer allocates a transfer buffer of `len` bytes, waits for registered-memory
    /// budget, and registers the buffer when the provider requires it.
    pub async fn alloc_buffer(&self, len: usize) -> Result<Arc<PinnedBuf>> {
        let permits = len.div_ceil(BUDGET_UNIT as usize).max(1);
        if permits > self.budget_permits as usize {
            return Err(Error::Unknown(format!(
                "buffer of {} bytes exceeds the rdma registered-memory budget",
                len
            )));
        }

        let permit = self
            .budget
            .clone()
            .acquire_many_owned(permits as u32)
            .await
            .map_err(|_| Error::Unknown("rdma fabric is shut down".to_string()))?;

        let mut data = vec![0u8; len];
        let mut mr: *mut c_void = std::ptr::null_mut();
        let mut desc: *mut c_void = std::ptr::null_mut();
        {
            let _guard = self.inner.call_lock.lock().unwrap();
            // Safety: data outlives the registration; PinnedBuf's field order guarantees
            // the MrGuard closes the registration before the Vec is freed.
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
        if let Some(addr) = self.inner.av.lock().unwrap().get(endpoint) {
            return Ok(*addr);
        }

        let mut addr: u64 = 0;
        {
            let _guard = self.inner.call_lock.lock().unwrap();
            // Safety: endpoint bytes are valid for the call.
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
        }

        self.inner
            .av
            .lock()
            .unwrap()
            .insert(endpoint.to_vec(), addr);
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
            let rc = {
                let _guard = self.inner.call_lock.lock().unwrap();
                // Safety: buf outlives the operation via the pending map; the range was
                // validated above; ctx_addr points at the boxed context block owned by the
                // pending map.
                unsafe {
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
        // Pending operations are NOT cleared here: their buffers must stay alive until the
        // endpoint closes. FabricInner's field order (handle before pending) closes the
        // endpoint before the map releases the buffers.
    }
}

/// progress_loop polls the completion queue and routes completions to waiting tasks. It is
/// the only place pending-map entries (and thus buffer references) are released on the
/// success path.
fn progress_loop(inner: Arc<FabricInner>) {
    while !inner.shutdown.load(Ordering::Relaxed) {
        let mut progressed = false;
        loop {
            let mut context: *mut c_void = std::ptr::null_mut();
            let mut flags: u64 = 0;
            let mut len: usize = 0;
            let mut err: i64 = 0;
            let rc = {
                let _guard = inner.call_lock.lock().unwrap();
                // Safety: the handle is valid until FabricInner drops, which cannot happen
                // while this thread holds an Arc to it.
                unsafe {
                    ffi::dfrdma_cq_read(
                        inner.handle.0,
                        &mut context,
                        &mut flags,
                        &mut len,
                        &mut err,
                    )
                }
            };

            match rc {
                1 => {
                    progressed = true;
                    // Remove under the lock but deliver (and drop the op, which may close a
                    // memory region) outside it, so the pending mutex is never held while
                    // taking the call lock.
                    let op = inner.pending.lock().unwrap().remove(&(context as usize));
                    match op {
                        Some(op) => {
                            // The receiver may have timed out and gone; that is fine, the
                            // buffer reference is released either way.
                            let _ = op.tx.send(Completion { len, err });
                        }
                        None => {
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

        if !progressed {
            std::thread::sleep(PROGRESS_IDLE_INTERVAL);
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
}
