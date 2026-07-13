//! Read-ahead / write-behind byte IO: a [`ReadAhead`] and [`WriteBehind`]
//! that put a dedicated thread on one IO end, with a user-space byte ring buffer
//! in between, so a pipeline stage never blocks on the kernel pipe.
//!
//! A single-threaded reader+worker+writer is coupled to its neighbours only
//! through the small OS pipe buffer (~64 KiB on macOS), so any blip in one stage
//! stalls the others. Pipelines are typically bursty at both ends —
//!
//! ```text
//! producer | this stage | consumer
//! ```
//!
//! — where the producer's per-item cost varies and the consumer periodically
//! flushes a chunk to disk. Putting a dedicated IO thread and a larger buffer
//! between a stage and each pipe decouples the compute from the IO.
//!
//! * [`ReadAhead`] wraps a [`Read`] source. A background thread reads
//!   *ahead* into the ring, so the consumer's `read` / `fill_buf` is served from
//!   user space instead of blocking on the kernel. Implements [`Read`] and
//!   [`BufRead`].
//! * [`WriteBehind`] wraps a [`Write`] sink. The caller's `write` returns as
//!   soon as the bytes land in the ring, and a background thread flushes them
//!   *behind* the caller. Implements [`Write`], plus an explicit
//!   [`finish`](WriteBehind::finish).
//!
//! The buffer between the two threads is a byte ring ([`ringbuf::HeapRb`]). That
//! is an implementation detail — the crate is named for its *behavior*, not the
//! ring — and may change.
//!
//! When *not* to use this crate: streams too small to amortize a thread spawn
//! and a ring allocation (well under a megabyte), seek-heavy access (the
//! adapters are strictly sequential), or async runtimes (these adapters block
//! real threads).
//!
//! # Examples
//!
//! Read-ahead: wrap any [`Read`] and consume it as usual; the kernel reads run
//! on a background thread.
//!
//! ```
//! use std::io::Read;
//! use rawb_io::ReadAhead;
//!
//! let mut reader = ReadAhead::new(std::io::Cursor::new(b"hello, world".to_vec()), 1 << 20);
//! let mut out = String::new();
//! reader.read_to_string(&mut out).unwrap();
//! assert_eq!(out, "hello, world");
//! ```
//!
//! Write-behind: wrap any [`Write`]; `write` returns as soon as the bytes are
//! buffered, and [`finish`](WriteBehind::finish) blocks until the background
//! thread has flushed everything, surfacing any error it hit and handing the
//! sink back.
//!
//! ```
//! use std::io::Write;
//! use rawb_io::WriteBehind;
//!
//! let mut writer = WriteBehind::new(Vec::new(), 1 << 20);
//! writer.write_all(b"hello, world").unwrap();
//! let sink = writer.finish().unwrap();
//! assert_eq!(sink, b"hello, world");
//! ```
//!
//! # Design choices
//!
//! * **A single up-front ring allocation** ([`ringbuf::HeapRb`]) for the bytes —
//!   recycled forever, no per-chunk heap traffic.
//! * **[`thread::park`] / [`unpark`](std::thread::Thread::unpark)** for blocking.
//!   Lock-free fast path when the ring isn't full/empty; only the rare contended
//!   case parks.
//! * **`read` straight into `vacant_slices_mut`** — one memcpy (kernel→ring)
//!   instead of two (kernel→temp + temp→ring).
//! * **Symmetric on write**: the caller pushes bytes into the ring directly; the
//!   IO writer thread drains via `as_slices` + `write_all` + `skip`.
//!
//! # Blocking and teardown
//!
//! * `read` / `fill_buf` block until at least one byte is available (or
//!   EOF/error). `write` blocks only while the ring is full. `flush` blocks
//!   until the ring has drained into the sink *and* the sink's own `flush`
//!   has completed.
//! * Both adapters are `Send` and may be constructed on one thread and used
//!   from another; blocking calls wake correctly wherever they run.
//! * Dropping either adapter joins its IO thread. For the reader this can
//!   block until a pending `read` on the source returns — a blocking read
//!   cannot be cancelled — and any read-ahead still in the ring is
//!   discarded. Dropping a writer drains the ring into the sink first, but
//!   *silently discards* any error from that final drain; call
//!   [`finish`](WriteBehind::finish) when the outcome matters.
//! * The first `Ok(0)` from the source is treated as end-of-stream: the IO
//!   thread stops polling it, and the reader reports EOF from then on.
//!   Sources that transiently return `Ok(0)` and later produce more data
//!   (e.g. tail-followers) are out of scope.
//! * A transient [`ErrorKind::Interrupted`](std::io::ErrorKind::Interrupted)
//!   from the source or sink is retried, not latched.
//!
//! # `unsafe`
//!
//! The crate sets `#![deny(unsafe_code)]`. Its `unsafe` is confined to the read
//! side: the `MaybeUninit<u8>` → `&mut [u8]` cast that lets `read` write straight
//! into the ring, the matching `advance_write_index` that publishes exactly the
//! bytes just read, and the one-time memset that zeroes the ring's storage at
//! construction. Together they mean nothing trusts the wrapped source: the cast
//! never exposes uninitialized memory (std requires the *caller* of
//! [`Read::read`] to pass initialized buffers), and the source's reported byte
//! count is bounds-checked before it is used. The read-loop sites will be
//! removed when [`std::io::BorrowedBuf`] stabilizes (rust-lang/rust#117693). The
//! hard lock-free concurrency `unsafe` lives inside [`ringbuf`], not here.

#![deny(unsafe_code)]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]
#![warn(clippy::undocumented_unsafe_blocks)]

use std::fmt;
use std::io::{self, BufRead, Read, Write};

use crate::sync_shim::atomic::{AtomicBool, AtomicU64, Ordering};
use crate::sync_shim::thread::{self, JoinHandle};
use crate::sync_shim::{Arc, Mutex, MutexGuard};

// Liveness assumption on ringbuf's caching wrappers (`HeapProd`/`HeapCons`,
// what `split()` returns): observing an apparently full/empty ring refreshes
// the cached counterpart index, so the park conditions in this crate always
// act on a fresh boundary state. The round-trip and backpressure tests
// exercise this continuously; revisit if the ringbuf major version changes.
use ringbuf::traits::{Consumer, Observer, Producer, Split};
use ringbuf::{HeapCons, HeapProd, HeapRb};

// ─── Concurrency-primitive routing (std ↔ loom) ──────────────────────────────

/// Single source for every cross-thread primitive the adapters use.
///
/// Normal builds re-export std unchanged, so routing through this module has
/// zero effect on shipped behavior. Under `--cfg loom` (set only by
/// `scripts/loom.sh`) the same names resolve to loom's instrumented replicas,
/// so the model checker can drive and exhaustively check the park/wake/latch
/// protocols. This module must only ever contain re-exports: any logic here
/// would run in one build but not the other.
#[cfg(not(loom))]
mod sync_shim {
    pub(crate) use std::sync::atomic;
    pub(crate) use std::sync::{Arc, Mutex, MutexGuard};
    pub(crate) use std::thread;
}

/// loom's replicas of the primitives; see the `not(loom)` twin above.
#[cfg(loom)]
mod sync_shim {
    pub(crate) use loom::sync::atomic;
    pub(crate) use loom::sync::{Arc, Mutex, MutexGuard};
    pub(crate) use loom::thread;
}

/// Default prefix for the spawned IO threads' names and their panic-fallback
/// error message. [`ReadAhead::new`] and [`WriteBehind::new`] use this;
/// pass your own with [`ReadAhead::with_thread_name`] /
/// [`WriteBehind::with_thread_name`] to label the threads after your own
/// component.
const DEFAULT_THREAD_PREFIX: &str = "rawb-io";

// ─── State shared between an adapter and its IO thread ──────────────────────

/// State shared between a [`ReadAhead`] and its IO thread.
struct ReaderShared {
    /// Set by the IO thread once the source reaches EOF — and on *every*
    /// failure path (error, panic), always strictly AFTER the error latch is
    /// written, so a consumer that observes `eof` under `Acquire` ordering is
    /// guaranteed to also observe the error that caused it. `fill_buf` relies
    /// on this ordering to re-check the latch before reporting a clean EOF.
    eof: AtomicBool,
    /// Set by `Drop` to tell the IO thread to exit even if the source isn't done.
    stop: AtomicBool,
    /// First read error from the IO thread, if any. A write-once / read-many
    /// latch: never drained, surfaced by copying (see [`clone_io_error`]).
    error: Mutex<Option<io::Error>>,
    /// The user-side thread the IO thread wakes; see [`register_waiter`].
    waiter: Mutex<thread::Thread>,
}

/// State shared between a [`WriteBehind`] and its IO thread.
struct WriterShared {
    /// Set by `finish()` or `Drop` to signal the IO thread that no more data
    /// is coming.
    finished: AtomicBool,
    /// Flush-request epoch, incremented by `flush()`, which then parks until
    /// `flush_ack` catches up (or the error latch fires).
    flush_seq: AtomicU64,
    /// Highest flush epoch the IO thread has fully honored: the ring was
    /// drained to the sink and the sink's own `flush` completed. Written only
    /// by the IO thread.
    flush_ack: AtomicU64,
    /// The IO thread's write error, if any. Written once by the IO thread and
    /// never drained, so its presence is the sticky "this writer has failed"
    /// state: every `write`/`flush`/`finish` copies it out (see
    /// [`WriteBehind::peek_error`]) and rejects the operation, and a failed
    /// writer can never accept more bytes into a ring its exited IO thread
    /// would never drain.
    error: Mutex<Option<io::Error>>,
    /// The user-side thread the IO thread wakes; see [`register_waiter`].
    waiter: Mutex<thread::Thread>,
}

/// Record the calling thread as the one the IO thread must wake.
///
/// The adapters are `Send`, so nothing stops a caller constructing on one
/// thread and reading/writing on another (or alternating threads behind a
/// `Mutex`). Wakeups must therefore target whichever thread is about to
/// *park*, not whichever thread happened to run the constructor — unparking
/// the constructor's thread while some other thread parks is a permanent
/// deadlock.
///
/// Every park site follows the same protocol: **register, re-check the park
/// condition, park**. The IO thread's counterpart protocol is: **change
/// state, read the slot, unpark** (see [`unpark_waiter`]). If the IO thread
/// reads the slot after registration it wakes the right thread; if it read
/// the slot before (and woke a stale thread), this mutex's release/acquire
/// edge guarantees the state change is visible to the caller's re-check, so
/// the caller does not park at all. Either way the wakeup cannot be lost.
fn register_waiter(waiter: &Mutex<thread::Thread>) {
    let current = thread::current();
    let mut slot = lock_or_recover(waiter);
    if slot.id() != current.id() {
        *slot = current;
    }
}

/// Wake the most recently registered user-side thread (the counterpart of
/// [`register_waiter`]). Callers must make their state change (push bytes,
/// free space, latch an error, set eof/finished) *before* calling this.
fn unpark_waiter(waiter: &Mutex<thread::Thread>) {
    // Clone out of the lock so the unpark (which may make a syscall) runs
    // without holding it.
    let thread = lock_or_recover(waiter).clone();
    thread.unpark();
}

// ─── Reader ──────────────────────────────────────────────────────────────────

/// `BufRead`-compatible reader fed by an IO thread.
///
/// The reader is `Send`: it may be constructed on one thread and used on
/// another, and each blocking call wakes correctly regardless of which
/// thread makes it (see `register_waiter` in the source).
///
/// Dropping the reader joins the IO thread; if the source is blocked in a
/// `read` at that moment, the drop blocks until that read returns (a
/// blocking read cannot be cancelled).
///
/// The first `Ok(0)` the source returns is treated as end-of-stream: the IO
/// thread stops polling it and the reader reports EOF from then on, even if
/// the source would later produce more bytes.
pub struct ReadAhead {
    /// Consumer side of the ring buffer; the worker reads bytes from here.
    consumer: HeapCons<u8>,
    /// Handle to the IO read thread used to call `unpark` when the ring drains.
    /// The IO thread never migrates, so a fixed handle is correct here.
    io_thread: thread::Thread,
    /// State shared with the IO thread: EOF/stop flags, error latch, waiter.
    shared: Arc<ReaderShared>,
    /// Join handle consumed by `Drop` to reap the IO thread.
    join: Option<JoinHandle<()>>,
}

impl ReadAhead {
    /// Spawn an IO thread that reads from `src` into a ring buffer of
    /// `ring_bytes` capacity (floored to 64 KiB).
    ///
    /// The IO thread is named `"rawb-io-read"`; use [`with_thread_name`] to label
    /// it after your own component.
    ///
    /// # Panics
    ///
    /// Panics if the OS refuses to spawn the IO thread, or if allocating the
    /// ring fails or overflows.
    ///
    /// [`with_thread_name`]: Self::with_thread_name
    pub fn new<R: Read + Send + 'static>(src: R, ring_bytes: usize) -> Self {
        Self::with_thread_name(src, ring_bytes, DEFAULT_THREAD_PREFIX)
    }

    /// Like [`new`](Self::new), but names the spawned IO thread `"{prefix}-read"`
    /// and uses `"{prefix} IO thread panicked"` as the panic-fallback error, so
    /// the thread — and any surfaced IO-thread panic — is labelled after your
    /// component. Behavior is otherwise identical to [`new`](Self::new).
    ///
    /// # Panics
    ///
    /// Panics if the OS refuses to spawn the IO thread, if allocating the
    /// ring fails or overflows, or if `prefix` contains an interior NUL byte
    /// (thread names are C strings).
    pub fn with_thread_name<R: Read + Send + 'static>(
        src: R,
        ring_bytes: usize,
        prefix: &str,
    ) -> Self {
        let (producer, consumer) = zeroed_ring(ring_bytes.max(64 * 1024));
        let shared = Arc::new(ReaderShared {
            eof: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            error: Mutex::new(None),
            waiter: Mutex::new(thread::current()),
        });

        let shared_io = shared.clone();
        let panic_message = format!("{prefix} IO thread panicked");

        let join = thread::Builder::new()
            .name(format!("{prefix}-read"))
            .spawn(move || io_read_thread(src, producer, &shared_io, &panic_message))
            .expect("spawning IO read thread");
        let io_thread = join.thread().clone();

        Self { consumer, io_thread, shared, join: Some(join) }
    }

    /// Number of bytes read ahead and ready to serve without blocking.
    ///
    /// A point-in-time snapshot: the IO thread appends concurrently, so the
    /// value may have grown by the time you act on it; it only shrinks
    /// through this reader's own `read`/`consume`. Useful for tuning
    /// `ring_bytes` — if this is frequently `0` under load, the source (or
    /// the ring size) is the bottleneck.
    pub fn buffered(&self) -> usize {
        self.consumer.occupied_len()
    }

    /// The ring's actual capacity in bytes (`ring_bytes` after the 64 KiB
    /// floor).
    pub fn capacity(&self) -> usize {
        self.consumer.capacity().get()
    }

    /// Copy the stored IO error, if any, leaving it in the slot (see
    /// [`clone_io_error`]). Idempotent: re-reads keep surfacing the failure
    /// rather than masking it as a clean EOF.
    fn peek_error(&self) -> Option<io::Error> {
        lock_or_recover(&self.shared.error).as_ref().map(clone_io_error)
    }
}

impl Read for ReadAhead {
    /// Copy up to `dst.len()` bytes out of the ring, blocking only while the
    /// ring is empty and the stream has not ended. An empty `dst` returns
    /// `Ok(0)` immediately *without consulting the stream state* — it does
    /// not surface a latched error (std leaves empty reads meaningless as
    /// probes, and reporting an error would contradict serving buffered
    /// bytes first).
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        // An empty destination can't make progress; don't block waiting for
        // bytes the caller can't accept.
        if dst.is_empty() {
            return Ok(0);
        }
        let src = self.fill_buf()?;
        let n = src.len().min(dst.len());
        dst[..n].copy_from_slice(&src[..n]);
        self.consume(n);
        Ok(n)
    }
}

impl BufRead for ReadAhead {
    /// Return the buffered bytes, blocking while the ring is empty until the
    /// IO thread delivers data, EOF, or an error. Bytes the source produced
    /// before failing are served before the error is reported.
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        loop {
            // Serve buffered bytes first: everything the source successfully
            // produced before failing (or hitting EOF) is delivered before
            // the terminal state is reported, so the consumer sees the same
            // prefix a plain `BufReader` would have — deterministically.
            // This also keeps the hot path lock-free: the error latch is
            // only consulted once the ring has drained.
            if self.consumer.occupied_len() > 0 {
                let (first, _second) = self.consumer.as_slices();
                return Ok(first);
            }

            if self.shared.eof.load(Ordering::Acquire) {
                // Drain any straggler bytes the producer published just
                // before setting EOF. `Acquire` above pairs with
                // `Release` in the IO thread, so re-checking occupied_len
                // here observes any final push.
                if self.consumer.occupied_len() > 0 {
                    continue;
                }
                // Ring drained and the stream is over: report how it ended.
                // Every IO-thread failure stores its error strictly before
                // setting `eof` (an error always sets `eof` too), so the
                // `Acquire` load above guarantees the error that caused this
                // EOF is visible here — a failure can never masquerade as a
                // clean end-of-stream (silent truncation), and a failed
                // stream never parks (eof is already set).
                if let Some(e) = self.peek_error() {
                    return Err(e);
                }
                return Ok(&[]);
            }

            // Ring is empty and the producer hasn't flagged EOF yet: prepare
            // to park until the IO thread wakes us. Register this thread as
            // the waiter first, then re-check, then park — `register_waiter`
            // explains why that ordering cannot lose a wakeup. Spurious
            // wakeups are harmless because we re-check the loop condition.
            register_waiter(&self.shared.waiter);
            if self.consumer.occupied_len() > 0 || self.shared.eof.load(Ordering::Acquire) {
                continue;
            }
            thread::park();
        }
    }

    fn consume(&mut self, amt: usize) {
        self.consumer.skip(amt);
        // Wake the IO thread in case it parked on a full ring.
        self.io_thread.unpark();
    }
}

/// Stops the IO thread and joins it. If the source is blocked in `read`,
/// this blocks until that read returns — a blocking read cannot be
/// cancelled. Any read-ahead still in the ring is discarded.
impl Drop for ReadAhead {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Release);
        self.io_thread.unpark();
        join_io_thread(self.join.take());
    }
}

impl fmt::Debug for ReadAhead {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadAhead")
            .field("io_thread", &self.io_thread.name())
            .field("buffered", &self.buffered())
            .field("eof", &self.shared.eof.load(Ordering::Relaxed))
            .field("has_error", &lock_or_recover(&self.shared.error).is_some())
            .finish_non_exhaustive()
    }
}

// ─── Writer ──────────────────────────────────────────────────────────────────

/// `Write`-compatible writer that hands bytes off to an IO thread.
///
/// The writer is `Send`: it may be constructed on one thread and used on
/// another, and each blocking call wakes correctly regardless of which
/// thread makes it (see `register_waiter` in the source).
///
/// The IO thread writes whatever the ring holds as soon as it wakes, so a
/// slow trickle of small writes becomes equally small sink writes. If the
/// sink has meaningful per-call overhead (an unbuffered [`File`], a pipe),
/// wrap it in [`std::io::BufWriter`] before handing it in; the final flush
/// at [`finish`](Self::finish) flushes the `BufWriter` through.
///
/// Dropping the writer without calling [`finish`](Self::finish) still drains
/// the ring into the sink, but silently discards any error.
///
/// [`File`]: std::fs::File
pub struct WriteBehind<W> {
    /// Producer side of the ring buffer; the worker pushes bytes here.
    producer: HeapProd<u8>,
    /// Handle to the IO write thread used to call `unpark` when new bytes are
    /// ready. The IO thread never migrates, so a fixed handle is correct here.
    io_thread: thread::Thread,
    /// State shared with the IO thread: finished/flush state, error latch,
    /// waiter.
    shared: Arc<WriterShared>,
    /// Join handle consumed by `finish`/`Drop` to reap the IO thread; carries
    /// the sink back out (`None` if the IO thread panicked).
    join: Option<JoinHandle<Option<W>>>,
}

impl<W: Write + Send + 'static> WriteBehind<W> {
    /// Spawn an IO thread that writes the ring contents to `dst`. Ring
    /// holds `ring_bytes` of pending output (floored to 64 KiB).
    ///
    /// The IO thread is named `"rawb-io-write"`; use [`with_thread_name`] to
    /// label it after your own component.
    ///
    /// # Panics
    ///
    /// Panics if the OS refuses to spawn the IO thread, or if allocating the
    /// ring fails or overflows.
    ///
    /// [`with_thread_name`]: Self::with_thread_name
    pub fn new(dst: W, ring_bytes: usize) -> Self {
        Self::with_thread_name(dst, ring_bytes, DEFAULT_THREAD_PREFIX)
    }

    /// Like [`new`](Self::new), but names the spawned IO thread `"{prefix}-write"`
    /// and uses `"{prefix} IO thread panicked"` as the panic-fallback error, so
    /// the thread — and any surfaced IO-thread panic — is labelled after your
    /// component. Behavior is otherwise identical to [`new`](Self::new).
    ///
    /// # Panics
    ///
    /// Panics if the OS refuses to spawn the IO thread, if allocating the
    /// ring fails or overflows, or if `prefix` contains an interior NUL byte
    /// (thread names are C strings).
    pub fn with_thread_name(dst: W, ring_bytes: usize, prefix: &str) -> Self {
        let rb = HeapRb::<u8>::new(ring_bytes.max(64 * 1024));
        let (producer, consumer) = rb.split();
        let shared = Arc::new(WriterShared {
            finished: AtomicBool::new(false),
            flush_seq: AtomicU64::new(0),
            flush_ack: AtomicU64::new(0),
            error: Mutex::new(None),
            waiter: Mutex::new(thread::current()),
        });

        let shared_io = shared.clone();
        let panic_message = format!("{prefix} IO thread panicked");

        let join = thread::Builder::new()
            .name(format!("{prefix}-write"))
            .spawn(move || io_write_thread(dst, consumer, &shared_io, &panic_message))
            .expect("spawning IO write thread");
        let io_thread = join.thread().clone();

        Self { producer, io_thread, shared, join: Some(join) }
    }
}

impl<W> WriteBehind<W> {
    /// Flush remaining bytes, signal the IO thread to drain, join it, and
    /// hand back the sink.
    ///
    /// This is where a downstream write error surfaces, so prefer it over
    /// relying on `Drop`. On success the sink is returned — so a `File` can
    /// be fsynced before a rename, or a `Vec<u8>` recovered — and on error
    /// the sink is dropped and the latched error returned. The `Drop` that
    /// runs at the end of this call is a no-op (the IO thread has already
    /// been joined).
    pub fn finish(mut self) -> io::Result<W> {
        self.shared.finished.store(true, Ordering::Release);
        self.io_thread.unpark();
        let dst = join_io_thread(self.join.take()).flatten();
        if let Some(e) = self.peek_error() {
            return Err(e);
        }
        // The IO thread returns the sink on every non-panicking exit, and a
        // panic latches an error, which returned above.
        dst.ok_or_else(|| io::Error::other("IO thread exited without returning the sink"))
    }

    /// Number of bytes accepted by `write` but not yet handed to the sink.
    ///
    /// A point-in-time snapshot: the IO thread drains concurrently, so the
    /// value may have shrunk by the time you act on it; it only grows
    /// through this writer's own `write`. After a successful
    /// [`flush`](Write::flush) it is `0`. Useful for tuning `ring_bytes` —
    /// if this hovers near [`capacity`](Self::capacity) under load, the sink
    /// (or the ring size) is the bottleneck.
    pub fn pending(&self) -> usize {
        self.producer.occupied_len()
    }

    /// The ring's actual capacity in bytes (`ring_bytes` after the 64 KiB
    /// floor).
    pub fn capacity(&self) -> usize {
        self.producer.capacity().get()
    }

    /// Copy the stored IO error, if any, leaving it in the slot (see
    /// [`clone_io_error`]). The slot is never drained, so this keeps surfacing
    /// the failure on every call — including the re-entrant writes `Drop` makes.
    fn peek_error(&self) -> Option<io::Error> {
        lock_or_recover(&self.shared.error).as_ref().map(clone_io_error)
    }
}

impl<W> Write for WriteBehind<W> {
    /// Push `buf` into the ring, blocking only while the ring is full; the
    /// bytes reach the sink asynchronously. A previous IO failure is
    /// reported up front — including for zero-length writes — and a failure
    /// after part of `buf` was consumed is reported as `Ok(n)` with the
    /// error surfacing on the next call.
    fn write(&mut self, mut buf: &[u8]) -> io::Result<usize> {
        // Surface any IO-thread error before touching the ring, and only once
        // per call rather than per ring-push iteration (the previous
        // per-iteration check locked the `Mutex` ~150 M times on a 30 GB run).
        // Rejecting a write on a failed writer *up front* is what prevents the
        // deadlock: otherwise we push into a ring the exited IO thread will
        // never drain, fill it, and `park` forever with no one left to `unpark`
        // us. This is exactly the re-entry a block-buffering writer's `Drop`
        // performs (flush buffered blocks + write a stream trailer) after a
        // broken pipe surfaced.
        if let Some(e) = self.peek_error() {
            return Err(e);
        }
        let initial_len = buf.len();
        while !buf.is_empty() {
            let pushed = self.producer.push_slice(buf);
            if pushed > 0 {
                buf = &buf[pushed..];
                self.io_thread.unpark();
                continue;
            }
            // Ring is full — let the IO thread drain, then park until it
            // wakes us. Register this thread as the waiter first, then
            // re-check, then park — `register_waiter` explains why that
            // ordering cannot lose a wakeup.
            register_waiter(&self.shared.waiter);
            if self.producer.vacant_len() > 0 {
                continue;
            }
            if let Some(e) = self.peek_error() {
                return partial_write_result(initial_len - buf.len(), e);
            }
            thread::park();
            // The IO thread also unparks us when it dies on a write
            // error, leaving the ring permanently full. Without this
            // re-check we'd loop forever pushing into a ring nobody
            // drains. Surfacing the error here both reports the failure
            // and breaks the deadlock.
            if let Some(e) = self.peek_error() {
                return partial_write_result(initial_len - buf.len(), e);
            }
        }
        Ok(initial_len)
    }

    /// Block until every byte written so far has been handed to the sink and
    /// the sink's own `flush` has completed — the [`Write::flush`] contract
    /// ("ensure all intermediately buffered contents reach their
    /// destination"; the ring is exactly such a buffer). Costs the caller one
    /// ring drain; write-behind resumes with the next `write`. Surfaces the
    /// latched error instead if the IO thread has failed.
    fn flush(&mut self) -> io::Result<()> {
        // A failed writer can never complete a flush.
        if let Some(e) = self.peek_error() {
            return Err(e);
        }
        // Request a flush epoch and wake the IO thread to honor it. The
        // release half of the fetch_add (paired with the IO thread's acquire
        // load) also publishes every byte pushed before this call, so the IO
        // thread's drain-before-ack sees them all.
        let seq = self.shared.flush_seq.fetch_add(1, Ordering::AcqRel) + 1;
        self.io_thread.unpark();
        loop {
            // Same register -> re-check -> park protocol as `write`; the IO
            // thread acks the epoch (or latches an error) before waking us.
            register_waiter(&self.shared.waiter);
            if self.shared.flush_ack.load(Ordering::Acquire) >= seq {
                return Ok(());
            }
            if let Some(e) = self.peek_error() {
                return Err(e);
            }
            thread::park();
        }
    }
}

/// Signals end-of-stream, drains the ring into the sink, and joins the IO
/// thread — so dropping can block while the sink accepts the remaining
/// bytes. Any error from that final drain is *silently discarded*; call
/// [`finish`](WriteBehind::finish) when the outcome matters.
impl<W> Drop for WriteBehind<W> {
    fn drop(&mut self) {
        // If finish() wasn't called, signal anyway so the IO thread can
        // shut down cleanly. Errors are silently dropped here — explicit
        // finish() is the right path for callers who care.
        self.shared.finished.store(true, Ordering::Release);
        self.io_thread.unpark();
        join_io_thread(self.join.take());
    }
}

impl<W> fmt::Debug for WriteBehind<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteBehind")
            .field("io_thread", &self.io_thread.name())
            .field("pending", &self.pending())
            .field("finished", &self.shared.finished.load(Ordering::Relaxed))
            .field("has_error", &lock_or_recover(&self.shared.error).is_some())
            .finish_non_exhaustive()
    }
}

// ─── IO thread bodies ────────────────────────────────────────────────────────

/// Convert an IO-thread panic payload into the error latched for the user.
///
/// `panic_message` carries the thread-name prefix (see
/// [`ReadAhead::with_thread_name`]) so the surfaced failure names the
/// component whose IO thread died; the payload's text (the argument of the
/// `panic!`) is appended when it is a string, so the actual failure message
/// reaches the caller instead of being discarded with the join result.
fn panic_error(panic_message: &str, payload: Box<dyn std::any::Any + Send>) -> io::Error {
    let detail = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str));
    match detail {
        Some(detail) => io::Error::other(format!("{panic_message}: {detail}")),
        None => io::Error::other(panic_message.to_string()),
    }
}

/// Run the read loop with a panic boundary: a panic anywhere inside it is
/// converted into the latched error (carrying the panic message), the EOF
/// flag, and a final wake — the same guarantees the loop's normal exits
/// provide. Without this, a panic while the consumer is parked on an empty
/// ring would hang it forever, and the panic's text would be lost with the
/// discarded join result.
fn io_read_thread<R: Read>(
    src: R,
    producer: HeapProd<u8>,
    shared: &ReaderShared,
    panic_message: &str,
) {
    // AssertUnwindSafe: the closure owns the producer (a mid-push panic
    // leaves the ring consistent — publication is a single atomic index
    // store), and the shared flags/latch are designed to be observed from
    // the other side at any point.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        io_read_loop(src, producer, shared);
    }));
    if let Err(payload) = result {
        // Build the error BEFORE taking the lock: this recovery path runs
        // outside the catch_unwind boundary, so nothing here may be able to
        // panic while the latch is held (it would skip the eof store and the
        // final wake below).
        let error = panic_error(panic_message, payload);
        // Latch the error BEFORE flagging EOF: `fill_buf` treats an observed
        // `eof` as "the terminal state is fully published" and re-checks the
        // latch after seeing it, so the error must already be in place.
        {
            let mut slot = lock_or_recover(&shared.error);
            if slot.is_none() {
                *slot = Some(error);
            }
        }
        shared.eof.store(true, Ordering::Release);
    }
    // Wake the consumer on any exit so it never parks against a dead
    // producer (on a panic, the loop's own unparks never ran).
    unpark_waiter(&shared.waiter);
}

/// Run the write loop with a panic boundary: a panic anywhere inside it is
/// converted into the latched error (carrying the panic message) and a final
/// wake, so a producer parked on a full ring never hangs against a dead IO
/// thread and the panic's text is not lost.
///
/// Returns the sink so `finish` can hand it back to the caller; `None` if
/// the loop panicked (the sink was consumed by the unwind).
fn io_write_thread<W: Write>(
    dst: W,
    consumer: HeapCons<u8>,
    shared: &WriterShared,
    panic_message: &str,
) -> Option<W> {
    // AssertUnwindSafe: as in `io_read_thread` — the closure owns its ring
    // end and the shared state is made to be observed mid-change.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        io_write_loop(dst, consumer, shared)
    }));
    let dst = match result {
        Ok(dst) => Some(dst),
        Err(payload) => {
            // Built before the lock is taken, for the same reason as in
            // `io_read_thread`: nothing in this recovery path may be able to
            // panic while the latch is held.
            let error = panic_error(panic_message, payload);
            let mut slot = lock_or_recover(&shared.error);
            if slot.is_none() {
                *slot = Some(error);
            }
            None
        }
    };
    unpark_waiter(&shared.waiter);
    dst
}

/// Build the reader's ring with every byte of its storage initialized.
///
/// `io_read_loop` hands the ring's vacant region to an arbitrary `Read` impl
/// as `&mut [u8]`. std places the burden of passing *initialized* memory to
/// [`Read::read`] on the caller — a safe-but-misbehaving impl is allowed to
/// read the buffer it was given — so handing out genuinely uninitialized
/// bytes would make that cast unsound. Zeroing the storage here means every
/// "vacant" byte is merely stale, never uninitialized, and the cast in the
/// read loop is sound no matter what the source does with its buffer.
///
/// The zeroing is a raw `write_bytes` because it is the only O(memset) route:
/// every safe path through the ring's API (`push_slice`, `From<Vec>`,
/// `skip`/`clear`) initializes or retires elements one at a time, which
/// costs ~140 ms for a 16 MiB ring in unoptimized builds versus ~1 ms for a
/// straight memset. Cost: one memset at construction; nothing per read.
#[allow(unsafe_code)]
fn zeroed_ring(capacity: usize) -> (HeapProd<u8>, HeapCons<u8>) {
    let rb = HeapRb::<u8>::new(capacity);
    let (mut producer, consumer) = rb.split();
    let (first, second) = producer.vacant_slices_mut();
    // The zeroing below is only sound if these two slices are the whole
    // allocation. On a fresh ring vacant == capacity by construction; assert
    // it so a change in ringbuf's behavior fails loudly here rather than
    // silently reintroducing uninitialized "vacant" bytes.
    assert_eq!(
        first.len() + second.len(),
        capacity,
        "fresh ring's vacant slices must cover its whole storage"
    );
    // SAFETY: writing zeroes through the vacant `MaybeUninit` slices is the
    // canonical way to initialize such memory, and on a fresh ring the two
    // vacant slices cover exactly the whole allocation. The write index is
    // not advanced, so nothing is published: the consumer still observes an
    // empty ring, and the zeroed bytes are only ever re-exposed through
    // `vacant_slices_mut` in the read loop.
    unsafe {
        std::ptr::write_bytes(first.as_mut_ptr(), 0, first.len());
        std::ptr::write_bytes(second.as_mut_ptr(), 0, second.len());
    }
    (producer, consumer)
}

/// Body of the dedicated read IO thread. Pumps bytes from `src` into the ring
/// buffer, parking when the ring is full, and waking the consumer on each push
/// or at EOF/error. Runs inside [`io_read_thread`]'s panic boundary, which
/// ensures the consumer is always woken even if this loop panics.
#[allow(unsafe_code)]
fn io_read_loop<R: Read>(mut src: R, mut producer: HeapProd<u8>, shared: &ReaderShared) {
    loop {
        if shared.stop.load(Ordering::Acquire) {
            break;
        }

        let (first, _second) = producer.vacant_slices_mut();
        if first.is_empty() {
            // Wake the consumer in case it's waiting (and we've just
            // become full because it's been slow).
            unpark_waiter(&shared.waiter);
            thread::park();
            continue;
        }

        // SAFETY: `vacant_slices_mut` hands back the ring's unwritten region as
        // `&mut [MaybeUninit<u8>]`. We reinterpret it as `&mut [u8]` to pass to
        // `Read::read`. This is sound because:
        //   * `u8` and `MaybeUninit<u8>` share a layout, and the cast changes
        //     neither the pointer nor the length.
        //   * Every byte of the ring's storage was initialized at construction
        //     (see [`zeroed_ring`]), so this region is
        //     initialized-but-stale memory, never uninitialized. std requires
        //     the *caller* of `Read::read` to pass initialized memory — a safe
        //     impl may legally read the buffer it was handed — and that
        //     obligation is met here. The worst a misbehaving source can
        //     observe is stale bytes from an earlier lap of this same ring.
        //   * On `Ok(n)` we publish exactly `n` bytes via `advance_write_index`
        //     below, after checking `n` against the slice length, so a lying
        //     source cannot advance the ring past the region it was given.
        //
        // TODO: replace with std::io::BorrowedBuf once core_io_borrowed_buf
        // (rust-lang/rust#117693) stabilizes; that removes this cast entirely.
        let dst: &mut [u8] =
            unsafe { std::slice::from_raw_parts_mut(first.as_mut_ptr() as *mut u8, first.len()) };
        match src.read(dst) {
            Ok(0) => {
                shared.eof.store(true, Ordering::Release);
                unpark_waiter(&shared.waiter);
                break;
            }
            Ok(n) => {
                // A source that reports more bytes than the buffer holds is
                // buggy; trusting it would publish ring bytes the read never
                // wrote and walk the write index past the vacant region. Fail
                // loudly instead (std's own read loops assert exactly this);
                // `io_read_thread` converts the panic into the latched error.
                assert!(
                    n <= dst.len(),
                    "Read::read reported {n} bytes for a {}-byte buffer",
                    dst.len()
                );
                // SAFETY: `n` is within the vacant slice handed to `read`
                // (asserted above) and the ring storage is fully initialized
                // (see [`zeroed_ring`]), which together are
                // `advance_write_index`'s precondition: it must not publish
                // uninitialized memory or advance past the vacant region.
                unsafe {
                    producer.advance_write_index(n);
                }
                unpark_waiter(&shared.waiter);
            }
            // A read interrupted by a signal (EINTR) is transient by
            // convention — std's own read loops retry it. Latching it as this
            // reader's permanent error would livelock callers like
            // `read_to_end`, which retry `Interrupted` by contract and would
            // spin forever against a latch that re-serves it.
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => {
                // Store the error strictly BEFORE setting `eof`: `fill_buf`
                // re-checks the latch after observing `eof`, and relies on
                // this order (plus the Release/Acquire pairing on `eof`) to
                // never report a failure as a clean end-of-stream.
                *lock_or_recover(&shared.error) = Some(e);
                shared.eof.store(true, Ordering::Release);
                unpark_waiter(&shared.waiter);
                break;
            }
        }
    }
}

/// Body of the dedicated write IO thread. Drains the ring buffer into `dst`,
/// parking when the ring is empty, and waking the producer after each drain to
/// signal available space. Honors `flush()` requests by draining, flushing
/// `dst`, and acking the flush epoch. Returns `dst` on every exit so `finish`
/// can hand it back. Runs inside [`io_write_thread`]'s panic boundary, which
/// ensures the producer is always woken even if this loop panics.
fn io_write_loop<W: Write>(mut dst: W, mut consumer: HeapCons<u8>, shared: &WriterShared) -> W {
    // Record an IO error into the shared slot, then wake the producer (which may
    // be parked on a full ring) so it surfaces the failure instead of blocking
    // forever. The slot is never drained, so this single write latches the
    // failure for every future `write`/`flush`/`finish`.
    let record_error = |e: io::Error| {
        *lock_or_recover(&shared.error) = Some(e);
        unpark_waiter(&shared.waiter);
    };

    loop {
        // Load any pending flush request BEFORE checking the ring: this
        // acquire load pairs with `flush()`'s release increment, so every
        // byte pushed before the flush call is visible to the occupied check
        // below, and the epoch is only acked once the ring has drained past
        // those bytes.
        let flush_requested = shared.flush_seq.load(Ordering::Acquire);
        if consumer.occupied_len() > 0 {
            let (first, _second) = consumer.as_slices();
            // Copy locally because `skip` borrows consumer mutably below.
            let n = first.len();
            if let Err(e) = dst.write_all(first) {
                record_error(e);
                return dst;
            }
            consumer.skip(n);
            unpark_waiter(&shared.waiter);
            continue;
        }
        // Ring is empty: honor any pending flush request before parking.
        // `flush_ack` is written only by this thread, so the relaxed read
        // cannot be stale.
        if flush_requested > shared.flush_ack.load(Ordering::Relaxed) {
            if let Err(e) = flush_retrying(&mut dst) {
                record_error(e);
                return dst;
            }
            shared.flush_ack.store(flush_requested, Ordering::Release);
            unpark_waiter(&shared.waiter);
            continue;
        }
        if shared.finished.load(Ordering::Acquire) {
            // Drain any stragglers — check again under acquire ordering.
            if consumer.occupied_len() > 0 {
                continue;
            }
            // Flush the underlying writer before exit.
            if let Err(e) = flush_retrying(&mut dst) {
                record_error(e);
            }
            return dst;
        }
        thread::park();
    }
}

// ─── Shared IO-error helpers ─────────────────────────────────────────────────

/// Resolve a `write` that hit the error latch after consuming `consumed`
/// bytes into the ring.
///
/// `Write::write`'s contract is that a call which consumed bytes must report
/// them via `Ok(n)`; an error return means "nothing was consumed". The latch
/// is never drained, so deferring the error to the caller's next `write` /
/// `flush` / `finish` loses nothing.
fn partial_write_result(consumed: usize, error: io::Error) -> io::Result<usize> {
    if consumed > 0 { Ok(consumed) } else { Err(error) }
}

/// Flush `dst`, retrying if the flush is interrupted by a signal (EINTR).
///
/// `write_all` already retries [`io::ErrorKind::Interrupted`] internally, but
/// `flush` does not; without this a single EINTR at drain time would latch as
/// the writer's permanent error.
fn flush_retrying<W: Write>(dst: &mut W) -> io::Result<()> {
    loop {
        match dst.flush() {
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

/// Acquire a Mutex even if it's been poisoned by a previous panic.
///
/// Both threads communicate IO errors through the same `Mutex<Option<io::Error>>`,
/// so a panic on one side would otherwise cascade into a panic on the other
/// when it next tries to read or store an error. Treating poison as
/// "no recorded error" lets us surface the original failure instead.
fn lock_or_recover<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Reconstruct an `io::Error` equivalent to `e` without consuming it.
///
/// `io::Error` is `!Clone` (it may wrap an arbitrary boxed payload), so the
/// error slot is surfaced by *copying* rather than draining: every observer
/// gets its own error and the original stays put. This is what makes the slot a
/// write-once, read-many latch — the presence of an error is itself the sticky
/// "this end has failed" state, so no separate flag is needed. OS errors
/// round-trip losslessly (kind + errno + message); for other errors we preserve
/// the kind and the `Display` message, which is the actionable part (any boxed
/// payload is not preserved, so downcasting the surfaced error yields nothing).
///
/// The latched error's `Display` runs on the calling thread while the latch's
/// lock is held; a `Display` impl that panics (pathological but safe) panics
/// that caller, and the poisoned lock is then recovered by [`lock_or_recover`].
fn clone_io_error(e: &io::Error) -> io::Error {
    match e.raw_os_error() {
        Some(code) => io::Error::from_raw_os_error(code),
        None => io::Error::new(e.kind(), e.to_string()),
    }
}

/// Join an IO thread to completion, returning its result (`None` if the
/// handle was already consumed).
///
/// A panic inside the IO loop is *not* lost even though a panicked join is
/// swallowed here: the panic boundary around each loop ([`io_read_thread`] /
/// [`io_write_thread`]) latches the panic's message as an `io::Error` and
/// wakes the counterpart, so the failure still surfaces through the normal
/// error channel (and `finish()` returns `Err`). Joining here reaps the
/// thread, orders its teardown, and carries the writer's sink back out.
fn join_io_thread<T>(join: Option<JoinHandle<T>>) -> Option<T> {
    join.and_then(|handle| handle.join().ok())
}

/// Compile the README's examples as doctests so they cannot rot relative to
/// the API (blocks marked `no_run` are compiled but not executed).
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
mod readme_doctests {}

/// Pin the adapters' auto-derived thread-safety at compile time: both are
/// `Send` (the moved/mutex/hop tests rely on it), and both are deliberately
/// **not** `Sync` — ringbuf's caching wrappers mutate `Cell` state through
/// `&self`, so shared references must never cross threads. If ringbuf ever
/// makes them `Sync`, the `compile_fail` pins below fail and force a
/// conscious decision rather than a silent API change.
///
/// ```
/// fn assert_send<T: Send>() {}
/// assert_send::<rawb_io::ReadAhead>();
/// assert_send::<rawb_io::WriteBehind<Vec<u8>>>();
/// ```
///
/// ```compile_fail
/// fn assert_sync<T: Sync>() {}
/// assert_sync::<rawb_io::ReadAhead>();
/// ```
///
/// ```compile_fail
/// fn assert_sync<T: Sync>() {}
/// assert_sync::<rawb_io::WriteBehind<Vec<u8>>>();
/// ```
#[cfg(doctest)]
mod send_sync_pins {}

// The std test suite exercises the adapters on real OS threads; under
// `--cfg loom` the shim's primitives only function inside `loom::model`, so
// these tests are compiled out and `loom_tests` below takes over.
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Round-trip a payload through a ReadAhead: bytes in == bytes out.
    #[test]
    fn read_ahead_round_trip_small() {
        let payload: Vec<u8> = (0..1000u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut r = ReadAhead::new(Cursor::new(payload.clone()), 64 * 1024);
        let mut out = Vec::new();
        std::io::copy(&mut r, &mut out).unwrap();
        assert_eq!(out, payload);
    }

    /// Payload much larger than the ring buffer — exercises the wrap-around.
    #[test]
    fn read_ahead_round_trip_larger_than_ring() {
        // The constructor floors the ring to 64 KiB, so the payload must
        // exceed THAT to actually wrap. (With a smaller nominal ring this
        // test silently stopped testing wrap-around once — review caught it.)
        let ring = 64 * 1024;
        let payload: Vec<u8> = (0..(ring * 8) as u32).map(|i| i as u8).collect();
        let mut r = ReadAhead::new(Cursor::new(payload.clone()), ring);
        let mut out = Vec::new();
        std::io::copy(&mut r, &mut out).unwrap();
        assert_eq!(out, payload);
    }

    /// Write a payload through WriteBehind and confirm the underlying
    /// sink received every byte after `finish()`.
    #[test]
    fn write_behind_round_trip_with_finish() {
        // WriteBehind takes ownership of `W: Write + Send + 'static`, so
        // we hand it a `Sink` that mirrors bytes into a shared buffer the
        // test can inspect.
        struct Sink(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
        impl Write for Sink {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let payload: Vec<u8> = (0..50_000u32).map(|i| i as u8).collect();
        let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let mut w = WriteBehind::new(Sink(captured.clone()), 4096);
        w.write_all(&payload).unwrap();
        w.finish().unwrap();
        assert_eq!(*captured.lock().unwrap(), payload);
    }

    /// A sink that always fails its writes.
    struct FailingSink;
    impl Write for FailingSink {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "downstream closed"))
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// A sink-write failure must surface as an `Err` rather than hanging the
    /// producer forever against a full, never-draining ring. Regression test
    /// for the missing error re-check after `park()` in `write`.
    #[test]
    fn write_behind_surfaces_sink_error_without_deadlock() {
        // Payload ≫ ring (clamped to a 64 KiB minimum) forces the producer
        // to fill the ring and park while the IO thread dies on its first
        // write.
        let ring = 4096;
        let payload = vec![0u8; 1024 * 1024];
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = thread::spawn(move || {
            let mut w = WriteBehind::new(FailingSink, ring);
            let result = w.write_all(&payload).and_then(|()| w.finish());
            tx.send(result.is_err()).unwrap();
        });
        let errored = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("writer deadlocked on a failing sink");
        worker.join().unwrap();
        assert!(errored, "a failing sink must surface as an error");
    }

    /// Once a `write` has surfaced the rich error, `finish()` must also report
    /// failure rather than falsely returning `Ok`. The error slot is a latch
    /// that is never drained, so both calls observe it.
    #[test]
    fn write_behind_finish_fails_after_error_already_surfaced() {
        let ring = 4096;
        let payload = vec![7u8; 1024 * 1024];
        let mut w = WriteBehind::new(FailingSink, ring);
        // The oversized payload guarantees the producer parks and observes
        // the error, so the first `write_all` returns `Err`.
        assert!(w.write_all(&payload).is_err());
        assert!(w.finish().is_err(), "finish must report the latched error too");
    }

    /// Number of iterations for the re-entrant-write stress test. Kept modest so
    /// the default suite stays fast (each iteration spins up and tears down a
    /// fresh IO thread); the deadlock (pre-fix) reproduces on the very first
    /// iteration, and the fix has been verified locally across hundreds of
    /// thousands of iterations via the `REENTRANT_STRESS_ITERS` override below.
    const REENTRANT_STRESS_ITERS: usize = 2_000;

    /// Re-entrant writes performed after the error is first surfaced. A
    /// block-buffering writer's `Drop` re-enters the sink several times (one
    /// write per buffered block, then a stream trailer, then a flush). The dying
    /// IO thread leaves at most a couple of stray `unpark` tokens, each of which
    /// can rescue one parked write; this count comfortably exceeds them so the
    /// pre-fix hang would reproduce reliably rather than depending on `unpark`
    /// timing.
    const REENTRANT_WRITES: usize = 16;

    /// Run `body` on its own thread and wait up to 10 s for it to finish.
    ///
    /// Returns `Some(value)` if `body` completes, or `None` if it *deadlocks*
    /// (parks forever). A non-deadlocked body finishes in microseconds, so the
    /// 10 s bound only ever elapses on a genuine hang. On deadlock we repeatedly
    /// `unpark` the stuck thread — each nudge lets `WriteBehind::write` fall
    /// through to its post-park error re-check and return, and a body that
    /// re-parks (a loop of writes, like a block-buffering writer's multi-write
    /// `Drop`) needs several — then join it, so the stress test never leaks
    /// threads across iterations.
    fn run_or_detect_deadlock<T, F>(body: F) -> Option<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        use std::sync::mpsc::RecvTimeoutError;
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = thread::spawn(move || {
            let _ = tx.send(body());
        });
        let handle = worker.thread().clone();
        // Fast path: a healthy body reports back near-instantly.
        if let Ok(value) = rx.recv_timeout(std::time::Duration::from_secs(10)) {
            worker.join().unwrap();
            return Some(value);
        }
        // Deadlock path: keep nudging until the body unwinds and reports, then
        // reap it. Bounded so a hang `unpark` can't clear doesn't wedge the
        // suite — we detach and report rather than block the test binary.
        for _ in 0..1000 {
            handle.unpark();
            match rx.recv_timeout(std::time::Duration::from_millis(20)) {
                Ok(_) => {
                    worker.join().unwrap();
                    return None;
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    worker.join().unwrap();
                    return None;
                }
            }
        }
        std::mem::forget(worker);
        None
    }

    /// Drive a `WriteBehind` over a failing sink to the exact deadlock state:
    /// surface the write error, leaving the ring full and the IO thread gone,
    /// then re-enter `write` repeatedly the way a block-buffering writer's `Drop`
    /// does. Returns `true` iff every write surfaced an error. Without the fix one of
    /// the re-entrant writes parks forever, so this never returns and the
    /// watchdog reports a deadlock instead.
    fn reentrant_write_after_error_surfaced(ring: usize) -> bool {
        let mut w = WriteBehind::new(FailingSink, ring);
        // Payload ≥ ring: the producer fills the ring and parks while the IO
        // thread dies on its first write, so this surfaces the error and leaves
        // the ring full with the IO thread already exited.
        let mut all_errored = w.write_all(&vec![0u8; 2 * ring]).is_err();
        for _ in 0..REENTRANT_WRITES {
            all_errored &= w.write_all(b"trailing stream-trailer bytes").is_err();
        }
        all_errored
    }

    /// After a sink write fails and that error has been surfaced once, further
    /// writes must return `Err` rather than park forever.
    ///
    /// This reproduces the re-entry a block-buffering writer's `Drop` performs
    /// while unwinding: it flushes its buffered blocks and writes a stream
    /// trailer back through this writer *after* a broken-pipe error already
    /// propagated out. If a re-entrant write parks on a full ring whose IO
    /// thread has exited, the process deadlocks in `Drop` — which is how a
    /// released binary hung for hours when its downstream sink died on a full
    /// disk (ENOSPC).
    #[test]
    fn write_behind_reentrant_write_after_error_does_not_deadlock() {
        let all_errored =
            run_or_detect_deadlock(|| reentrant_write_after_error_surfaced(64 * 1024)).expect(
                "re-entrant write deadlocked: parked on a full ring whose IO thread had exited",
            );
        assert!(all_errored, "every write after a surfaced error must error, not succeed");
    }

    /// Drive `iters` fresh writers through the surfaced-error + re-entrant
    /// write sequence (one IO-thread lifetime per iteration), failing on any
    /// deadlock. Shared by the default stress test and its soak variant.
    fn reentrant_write_stress(iters: usize) {
        for i in 0..iters {
            let all_errored =
                run_or_detect_deadlock(|| reentrant_write_after_error_surfaced(64 * 1024))
                    .unwrap_or_else(|| panic!("iteration {i}: re-entrant write deadlocked"));
            assert!(all_errored, "iteration {i}: every re-entrant write must surface an error");
        }
    }

    /// Stress the re-entrant-write path across many IO-thread lifetimes to shake
    /// out the timing-dependent hang. Each iteration spins up a fresh writer +
    /// IO thread, kills it via a failing sink, surfaces the error, then
    /// re-enters. The pre-fix deadlock reproduces on the first iteration; the fix
    /// must hold across all of them.
    ///
    /// Override the iteration count with `REENTRANT_STRESS_ITERS=<n>` to grind
    /// on it harder (e.g. hundreds of thousands) when auditing the fix locally,
    /// or run the soak variant.
    #[test]
    fn write_behind_reentrant_write_stress_no_deadlock() {
        let iters = std::env::var("REENTRANT_STRESS_ITERS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(REENTRANT_STRESS_ITERS);
        reentrant_write_stress(iters);
    }

    /// A panic inside the IO write thread must not deadlock the producer and
    /// must surface as a failure (not a silently-successful `finish`).
    #[test]
    fn write_behind_panic_surfaces_without_deadlock() {
        struct PanicSink;
        impl Write for PanicSink {
            fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
                panic!("sink panicked");
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let ring = 4096;
        let payload = vec![1u8; 1024 * 1024];
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = thread::spawn(move || {
            let mut w = WriteBehind::new(PanicSink, ring);
            let result = w.write_all(&payload).and_then(|()| w.finish());
            tx.send(result.is_err()).unwrap();
        });
        let errored = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("writer deadlocked on a panicking IO thread");
        worker.join().unwrap();
        assert!(errored, "an IO-thread panic must surface as an error");
    }

    /// `with_thread_name` labels the spawned IO thread with the caller's prefix,
    /// and reads round-trip identically to `new`.
    #[test]
    fn with_thread_name_labels_the_io_thread() {
        // A `Read` that records the name of the thread its `read` runs on (the
        // spawned IO thread), then serves the payload.
        struct NameCapturingSource {
            payload: Cursor<Vec<u8>>,
            thread_name: std::sync::Arc<std::sync::Mutex<Option<String>>>,
        }
        impl Read for NameCapturingSource {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                let mut slot = self.thread_name.lock().unwrap();
                if slot.is_none() {
                    *slot = Some(thread::current().name().unwrap_or("<unnamed>").to_string());
                }
                drop(slot);
                self.payload.read(buf)
            }
        }

        let payload: Vec<u8> = (0..10_000u32).map(|i| i as u8).collect();
        let thread_name = std::sync::Arc::new(std::sync::Mutex::new(None));
        let src = NameCapturingSource {
            payload: Cursor::new(payload.clone()),
            thread_name: thread_name.clone(),
        };
        let mut r = ReadAhead::with_thread_name(src, 64 * 1024, "my-stage");
        let mut out = Vec::new();
        std::io::copy(&mut r, &mut out).unwrap();
        assert_eq!(out, payload, "read must round-trip identically to new()");
        assert_eq!(thread_name.lock().unwrap().as_deref(), Some("my-stage-read"));
    }

    /// A source whose single read is delayed long enough that a consumer on
    /// another thread parks before any bytes arrive, then serves its payload
    /// in one read, then EOF.
    struct SlowStartSource {
        payload: Option<Vec<u8>>,
        delay: std::time::Duration,
    }
    impl Read for SlowStartSource {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            match self.payload.take() {
                Some(payload) => {
                    thread::sleep(self.delay);
                    assert!(payload.len() <= buf.len(), "test payload must fit one read");
                    buf[..payload.len()].copy_from_slice(&payload);
                    Ok(payload.len())
                }
                None => Ok(0),
            }
        }
    }

    /// A reader constructed on one thread and consumed on another must not
    /// deadlock: wakeups must chase whichever thread is parked in `fill_buf`,
    /// not the thread that happened to run the constructor. Regression test
    /// for the captured-`Thread`-handle deadlock, which parked the consuming
    /// thread forever the moment it saw an empty ring.
    #[test]
    fn read_ahead_moved_across_threads_does_not_deadlock() {
        let payload = vec![7u8; 1024];
        let reader = ReadAhead::new(
            SlowStartSource {
                payload: Some(payload.clone()),
                delay: std::time::Duration::from_millis(100),
            },
            64 * 1024,
        );
        let out = run_or_detect_deadlock(move || {
            let mut reader = reader;
            let mut out = Vec::new();
            reader.read_to_end(&mut out).unwrap();
            out
        })
        .expect("moved reader deadlocked: wakeups went to the constructing thread");
        assert_eq!(out, payload);
    }

    /// A sink that mirrors bytes into a shared buffer but sleeps per write,
    /// forcing the producer to fill the ring and park while the sink drains.
    struct SlowSink {
        received: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
        delay: std::time::Duration,
    }
    impl Write for SlowSink {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            thread::sleep(self.delay);
            self.received.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// A writer constructed on one thread and written from another must not
    /// deadlock once the ring fills. Regression test for the write side of
    /// the captured-`Thread`-handle deadlock.
    #[test]
    fn write_behind_moved_across_threads_does_not_deadlock() {
        let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let payload = vec![42u8; 256 * 1024]; // 4x the (floored) 64 KiB ring
        let writer = WriteBehind::new(
            SlowSink { received: received.clone(), delay: std::time::Duration::from_millis(5) },
            64 * 1024,
        );
        let payload_for_worker = payload.clone();
        run_or_detect_deadlock(move || {
            let mut writer = writer;
            writer.write_all(&payload_for_worker).unwrap();
            writer.finish().unwrap();
        })
        .expect("moved writer deadlocked: wakeups went to the constructing thread");
        assert_eq!(*received.lock().unwrap(), payload);
    }

    /// A panic in the IO read thread must surface as an error rather than
    /// hanging the consumer (or masquerading as a clean EOF).
    #[test]
    fn read_ahead_panic_surfaces_without_deadlock() {
        struct PanicSource;
        impl Read for PanicSource {
            fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
                panic!("source panicked");
            }
        }
        let result = run_or_detect_deadlock(|| {
            let mut r = ReadAhead::new(PanicSource, 64 * 1024);
            let mut out = Vec::new();
            r.read_to_end(&mut out)
        })
        .expect("reader deadlocked on a panicking IO thread");
        assert!(result.is_err(), "an IO-thread panic must surface as an error");
    }

    /// Default iterations for the masked-as-EOF stress tests. Kept modest so
    /// the default suite stays fast; pre-fix the panic-path mask reproduced
    /// within a few thousand iterations on an M-series laptop. The soak
    /// variants run far more.
    const MASK_STRESS_ITERS: usize = 10_000;

    /// What the first `fill_buf` pass of the mask probe observed.
    enum ProbeFirstPass {
        Data,
        Error,
    }

    /// Silence the default panic-hook output for the IO threads the mask
    /// probes kill by the thousand; forward everything else (test assertions
    /// included) to the previous hook. Installed once and never restored: the
    /// filter is transparent for non-probe threads, so leaving it in place is
    /// harmless even when tests share a process.
    fn silence_mask_probe_panics() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            let previous = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                let from_probe =
                    thread::current().name().is_some_and(|n| n.starts_with("mask-probe"));
                if !from_probe {
                    previous(info);
                }
            }));
        });
    }

    /// Drive `iters` fresh readers through a data-then-failure teardown and
    /// assert the failure is never reported as a clean EOF. `make_source`
    /// builds a source that serves one byte and then fails after the given
    /// number of busy-loop spins; sweeping the spin count lands the teardown
    /// at varying points inside the consumer's `fill_buf` window, so each
    /// iteration is one Bernoulli trial against the race window.
    fn assert_failure_never_masks_as_clean_eof<S, F>(make_source: F, iters: usize)
    where
        S: Read + Send + 'static,
        F: Fn(u32) -> S,
    {
        for i in 0..iters {
            let spins = (i % 3000) as u32;
            let mut r = ReadAhead::with_thread_name(make_source(spins), 64 * 1024, "mask-probe");
            // Pass 1: take the byte. A clean EOF is impossible here (the byte
            // sits in the ring until consumed), so the only outcomes are data
            // or an already-latched error.
            let first = match r.fill_buf() {
                Ok(b) if !b.is_empty() => ProbeFirstPass::Data,
                Ok(_) => panic!("iteration {i}: clean EOF before any data from a failing source"),
                Err(_) => ProbeFirstPass::Error,
            };
            match first {
                ProbeFirstPass::Data => r.consume(1),
                ProbeFirstPass::Error => continue,
            }
            // Pass 2 races the teardown; it must never be a clean EOF.
            let masked_as_clean_eof = matches!(r.fill_buf(), Ok(b) if b.is_empty());
            assert!(
                !masked_as_clean_eof,
                "iteration {i}: IO-thread failure surfaced as clean EOF (silent truncation)"
            );
        }
    }

    /// Serves one byte, then panics after a swept spin delay.
    struct OneByteThenSpinPanic {
        served: bool,
        spins: u32,
    }
    impl Read for OneByteThenSpinPanic {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if !self.served {
                self.served = true;
                buf[0] = 42;
                return Ok(1);
            }
            for _ in 0..self.spins {
                std::hint::spin_loop();
            }
            panic!("boom");
        }
    }

    /// Serves one byte, then errors after a swept spin delay.
    struct OneByteThenSpinError {
        served: bool,
        spins: u32,
    }
    impl Read for OneByteThenSpinError {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if !self.served {
                self.served = true;
                buf[0] = 42;
                return Ok(1);
            }
            for _ in 0..self.spins {
                std::hint::spin_loop();
            }
            Err(io::Error::other("boom"))
        }
    }

    /// An IO-thread panic must never surface as a clean EOF. Pre-fix, the
    /// teardown stored `eof` before the error latch, so a `fill_buf` pass
    /// racing the teardown could observe (no error, eof) and report a
    /// panicked source as clean end-of-stream — silent truncation, reproduced
    /// at ~1/2,750 iterations. Override with `PANIC_MASK_STRESS_ITERS=<n>` or
    /// run the soak variant.
    #[test]
    fn io_thread_panic_never_masks_as_clean_eof() {
        silence_mask_probe_panics();
        let iters = std::env::var("PANIC_MASK_STRESS_ITERS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(MASK_STRESS_ITERS);
        assert_failure_never_masks_as_clean_eof(
            |spins| OneByteThenSpinPanic { served: false, spins },
            iters,
        );
    }

    /// A source error must never surface as a clean EOF — the error-path twin
    /// of the panic mask test (the error is stored before `eof`, and
    /// `fill_buf` re-checks the latch after observing `eof`). Override with
    /// `ERROR_MASK_STRESS_ITERS=<n>` or run the soak variant.
    #[test]
    fn io_thread_error_never_masks_as_clean_eof() {
        let iters = std::env::var("ERROR_MASK_STRESS_ITERS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(MASK_STRESS_ITERS);
        assert_failure_never_masks_as_clean_eof(
            |spins| OneByteThenSpinError { served: false, spins },
            iters,
        );
    }

    /// A `Read` impl that reports more bytes than the buffer holds must not
    /// let the reader publish ring bytes the read never wrote (pre-fix the
    /// count was fed unchecked into `advance_write_index`). The IO thread
    /// asserts the count, and the failure surfaces as an error.
    #[test]
    fn lying_source_read_count_surfaces_as_error_not_data() {
        struct LyingSource;
        impl Read for LyingSource {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                Ok(buf.len() + 1) // claims one byte more than the buffer holds
            }
        }
        let result = run_or_detect_deadlock(|| {
            let mut r = ReadAhead::new(LyingSource, 64 * 1024);
            let mut out = Vec::new();
            r.read_to_end(&mut out).map(|_| out.len())
        })
        .expect("reader deadlocked on a lying source");
        assert!(result.is_err(), "a lying read count must surface as an error, not as data");
    }

    /// A transient `Interrupted` (EINTR) from the source must be retried by
    /// the IO thread, not latched as the reader's permanent error. Pre-fix
    /// this livelocked: std's `read_to_string` retries `Interrupted` by
    /// contract, and the latch re-served it forever — losing "hello" too.
    #[test]
    fn read_ahead_retries_interrupted_source() {
        struct InterruptedOnceSource {
            state: u8,
        }
        impl Read for InterruptedOnceSource {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                match self.state {
                    0 => {
                        self.state = 1;
                        Err(io::Error::new(io::ErrorKind::Interrupted, "EINTR"))
                    }
                    1 => {
                        self.state = 2;
                        buf[..5].copy_from_slice(b"hello");
                        Ok(5)
                    }
                    _ => Ok(0),
                }
            }
        }
        let result = run_or_detect_deadlock(|| {
            let mut r = ReadAhead::new(InterruptedOnceSource { state: 0 }, 64 * 1024);
            let mut out = String::new();
            r.read_to_string(&mut out).map(|_| out)
        })
        .expect("reader livelocked on a transient EINTR");
        assert_eq!(result.unwrap(), "hello", "the bytes after the EINTR must still arrive");
    }

    /// A transient `Interrupted` from the sink's `flush` at drain time must be
    /// retried, not latched (`write_all` already retries EINTR internally; the
    /// final flush needs the same treatment).
    #[test]
    fn write_behind_retries_interrupted_flush() {
        struct FlushInterruptedSink {
            received: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
            interrupted_once: bool,
        }
        impl Write for FlushInterruptedSink {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.received.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                if !self.interrupted_once {
                    self.interrupted_once = true;
                    return Err(io::Error::new(io::ErrorKind::Interrupted, "EINTR"));
                }
                Ok(())
            }
        }
        let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut w = WriteBehind::new(
            FlushInterruptedSink { received: received.clone(), interrupted_once: false },
            64 * 1024,
        );
        w.write_all(b"payload").unwrap();
        w.finish().expect("a transient EINTR from flush must not fail finish");
        assert_eq!(*received.lock().unwrap(), b"payload");
    }

    /// `flush` must block until every buffered byte has reached the sink and
    /// the sink's own flush has run — the `Write::flush` contract. Pre-fix it
    /// returned Ok immediately with the whole payload still in the ring.
    #[test]
    fn flush_delivers_all_buffered_bytes_before_returning() {
        let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let payload = vec![9u8; 256 * 1024];
        // Ring larger than the payload so write_all itself never blocks; the
        // slow sink guarantees the bytes are still in flight when flush is
        // called.
        let mut w = WriteBehind::new(
            SlowSink { received: received.clone(), delay: std::time::Duration::from_millis(50) },
            1 << 20,
        );
        w.write_all(&payload).unwrap();
        w.flush().unwrap();
        assert_eq!(
            received.lock().unwrap().len(),
            payload.len(),
            "flush returned Ok with bytes still undelivered"
        );
        w.finish().unwrap();
        assert_eq!(*received.lock().unwrap(), payload);
    }

    /// Accepts every byte and counts how many times its own `flush` runs.
    struct FlushCountingSink {
        flushes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }
    impl Write for FlushCountingSink {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            self.flushes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
    }

    /// `flush` must invoke the underlying sink's `flush`, not just drain the
    /// ring — flush-through, as `BufWriter` does. Also holds on an
    /// already-drained (or never-written) ring.
    #[test]
    fn flush_propagates_to_the_sink() {
        let flushes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut w = WriteBehind::new(FlushCountingSink { flushes: flushes.clone() }, 64 * 1024);
        w.write_all(b"abc").unwrap();
        w.flush().unwrap();
        let after_first = flushes.load(std::sync::atomic::Ordering::Relaxed);
        assert!(after_first >= 1, "flush must reach the sink");
        // A second flush with nothing buffered must still flush the sink.
        w.flush().unwrap();
        assert!(
            flushes.load(std::sync::atomic::Ordering::Relaxed) > after_first,
            "flush on an empty ring must still flush the sink"
        );
    }

    /// Bytes the source successfully produced before failing must be
    /// delivered before the error is reported — the same prefix a plain
    /// `BufReader` would have seen, and deterministically so. Pre-fix, the
    /// buffered read-ahead was discarded and the number of delivered bytes
    /// depended on thread timing.
    #[test]
    fn reader_delivers_buffered_data_before_surfacing_error() {
        struct GoodThenErrorSource {
            payload: Option<Vec<u8>>,
        }
        impl Read for GoodThenErrorSource {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                match self.payload.take() {
                    Some(payload) => {
                        assert!(payload.len() <= buf.len(), "test payload must fit one read");
                        buf[..payload.len()].copy_from_slice(&payload);
                        Ok(payload.len())
                    }
                    None => Err(io::Error::other("device error after the good bytes")),
                }
            }
        }
        let payload: Vec<u8> = (0..100_000u32).map(|i| i as u8).collect();
        let mut r = ReadAhead::new(GoodThenErrorSource { payload: Some(payload.clone()) }, 1 << 20);
        let mut out = Vec::new();
        let result = r.read_to_end(&mut out);
        assert!(result.is_err(), "the source's failure must surface");
        assert_eq!(out, payload, "every byte read before the failure must be delivered");
    }

    /// A failing source's error must surface with its kind and message
    /// preserved, and must keep surfacing on every subsequent read — the
    /// latch is never drained.
    #[test]
    fn reader_error_latch_preserves_kind_and_is_idempotent() {
        struct FailingSource;
        impl Read for FailingSource {
            fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::new(io::ErrorKind::TimedOut, "sensor went away"))
            }
        }
        let mut r = ReadAhead::new(FailingSource, 64 * 1024);
        let mut buf = [0u8; 16];
        for attempt in 0..3 {
            let err = r.read(&mut buf).expect_err("a failing source must error");
            assert_eq!(err.kind(), io::ErrorKind::TimedOut, "attempt {attempt}");
            assert!(err.to_string().contains("sensor went away"), "attempt {attempt}: {err}");
        }
    }

    /// `flush` on a writer whose IO thread has died must surface the latched
    /// error promptly instead of waiting forever for an ack that will never
    /// come.
    #[test]
    fn flush_after_io_thread_death_errors_without_deadlock() {
        let result = run_or_detect_deadlock(|| {
            let mut w = WriteBehind::new(FailingSink, 4096);
            // Oversized payload guarantees the IO thread dies and the error
            // is surfaced by write_all.
            let _ = w.write_all(&vec![0u8; 2 * 64 * 1024]);
            w.flush()
        })
        .expect("flush deadlocked on a dead IO thread");
        assert!(result.is_err(), "flush after IO-thread death must error");
    }

    /// After the IO thread dies mid-call, a `write` that already consumed
    /// bytes must report them via `Ok(n)` — `Write::write`'s contract is that
    /// an error return means nothing was consumed. The latched error then
    /// surfaces on the caller's next operation.
    #[test]
    fn write_reports_partial_consumption_before_surfacing_error() {
        let mut w = WriteBehind::new(FailingSink, 4096); // ring floors to 64 KiB
        let payload = vec![0u8; 128 * 1024]; // 2x the ring
        let n = w.write(&payload).expect("a write that consumed bytes must return Ok(n)");
        assert!(n > 0 && n < payload.len(), "the ring-full write consumed only part: {n}");
        assert!(w.write(&payload).is_err(), "the latched error must surface on the next write");
    }

    /// Zero-length reads and writes return `Ok(0)` immediately — a pre-fix
    /// empty read parked until data arrived even though the caller could not
    /// accept any.
    #[test]
    fn zero_length_reads_and_writes_do_not_block() {
        /// Blocks in `read` until the reader's `Drop` unparks the IO thread.
        struct NeverReadySource;
        impl Read for NeverReadySource {
            fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
                thread::park();
                Ok(0)
            }
        }
        let n = run_or_detect_deadlock(|| {
            let mut r = ReadAhead::new(NeverReadySource, 64 * 1024);
            r.read(&mut []).unwrap()
        })
        .expect("read with an empty destination blocked waiting for data");
        assert_eq!(n, 0);

        let mut w = WriteBehind::new(Vec::new(), 64 * 1024);
        assert_eq!(w.write(&[]).unwrap(), 0);
        w.finish().unwrap();
    }

    /// The text of an IO-thread panic must be preserved in the surfaced error
    /// — pre-fix only the generic "IO thread panicked" label survived, and
    /// the actual failure message was discarded with the join result.
    #[test]
    fn reader_panic_payload_text_is_preserved_in_the_error() {
        struct PanicSource;
        impl Read for PanicSource {
            fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
                panic!("boom-metadata-1234");
            }
        }
        let result = run_or_detect_deadlock(|| {
            let mut r = ReadAhead::with_thread_name(PanicSource, 64 * 1024, "payload-probe");
            let mut out = Vec::new();
            r.read_to_end(&mut out)
        })
        .expect("reader deadlocked on a panicking IO thread");
        let err = result.expect_err("the panic must surface as an error");
        let msg = err.to_string();
        assert!(msg.contains("payload-probe IO thread panicked"), "prefix missing: {msg}");
        assert!(msg.contains("boom-metadata-1234"), "panic payload text missing: {msg}");
    }

    /// Writer-side panics keep their payload text too.
    #[test]
    fn writer_panic_payload_text_is_preserved_in_the_error() {
        struct PanicSink;
        impl Write for PanicSink {
            fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
                panic!("sink exploded spectacularly");
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let result = run_or_detect_deadlock(|| {
            let mut w = WriteBehind::new(PanicSink, 4096);
            w.write_all(&vec![1u8; 128 * 1024]).and_then(|()| w.finish().map(drop))
        })
        .expect("writer deadlocked on a panicking IO thread");
        let err = result.expect_err("the panic must surface as an error");
        assert!(err.to_string().contains("sink exploded spectacularly"), "{err}");
    }

    /// `finish` hands the sink back so callers can keep using it (fsync a
    /// file, reclaim a Vec, ...). The returned sink must hold exactly the
    /// written bytes.
    #[test]
    fn finish_returns_the_sink_with_all_bytes() {
        let payload: Vec<u8> = (0..100_000u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut w = WriteBehind::new(Vec::new(), 64 * 1024);
        w.write_all(&payload).unwrap();
        let sink = w.finish().expect("finish must succeed");
        assert_eq!(sink, payload);
    }

    // ─── Shutdown routes, backpressure, and helper coverage ─────────────────

    /// Dropping a reader mid-stream — with the IO thread parked on a full
    /// ring — must stop the IO thread and join it promptly.
    #[test]
    fn reader_drop_mid_stream_does_not_hang() {
        /// Fills every read completely and never reaches EOF.
        struct EndlessSource;
        impl Read for EndlessSource {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                for b in buf.iter_mut() {
                    *b = 0x5a;
                }
                Ok(buf.len())
            }
        }
        run_or_detect_deadlock(|| {
            let mut r = ReadAhead::new(EndlessSource, 64 * 1024);
            let mut buf = [0u8; 1024];
            r.read_exact(&mut buf).unwrap();
            // Give the IO thread time to refill the ring and park on it.
            thread::sleep(std::time::Duration::from_millis(50));
            drop(r);
        })
        .expect("dropping a mid-stream reader hung");
    }

    /// Dropping a writer without `finish()` must still drain the ring into
    /// the sink — only error *reporting* is forfeited, not the data.
    #[test]
    fn writer_drop_without_finish_flushes_ring_to_sink() {
        let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let payload = vec![3u8; 100_000];
        {
            let mut w = WriteBehind::new(
                SlowSink { received: received.clone(), delay: std::time::Duration::from_millis(1) },
                1 << 20, // ring holds the whole payload: the drop must drain it
            );
            w.write_all(&payload).unwrap();
        } // dropped here without finish()
        assert_eq!(*received.lock().unwrap(), payload, "drop must drain the ring to the sink");
    }

    /// Payload much larger than the ring against a slow sink: the producer
    /// parks for space repeatedly and every byte still arrives in order —
    /// the success analog of the failing-sink deadlock tests.
    #[test]
    fn slow_sink_backpressure_round_trip() {
        let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let payload: Vec<u8> = (0..256 * 1024u32).map(|i| (i * 31) as u8).collect();
        let mut w = WriteBehind::new(
            SlowSink { received: received.clone(), delay: std::time::Duration::from_millis(2) },
            64 * 1024,
        );
        w.write_all(&payload).unwrap();
        w.finish().unwrap();
        assert_eq!(*received.lock().unwrap(), payload);
    }

    /// A source that trickles small delayed chunks: the consumer parks for
    /// data repeatedly and still sees every byte in order.
    #[test]
    fn slow_source_round_trip() {
        struct TricklingSource {
            payload: Vec<u8>,
            pos: usize,
        }
        impl Read for TricklingSource {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if self.pos >= self.payload.len() {
                    return Ok(0);
                }
                thread::sleep(std::time::Duration::from_millis(2));
                let n = buf.len().min(4096).min(self.payload.len() - self.pos);
                buf[..n].copy_from_slice(&self.payload[self.pos..self.pos + n]);
                self.pos += n;
                Ok(n)
            }
        }
        let payload: Vec<u8> =
            (0..64 * 1024u32).map(|i| (i.wrapping_mul(2654435761) >> 7) as u8).collect();
        let mut r = ReadAhead::new(TricklingSource { payload: payload.clone(), pos: 0 }, 64 * 1024);
        let mut out = Vec::new();
        r.read_to_end(&mut out).unwrap();
        assert_eq!(out, payload);
    }

    /// EOF is terminal and stable: after the stream ends, further reads and
    /// fill_bufs keep reporting end-of-stream.
    #[test]
    fn eof_reads_stay_at_eof() {
        let mut r = ReadAhead::new(Cursor::new(b"tail".to_vec()), 64 * 1024);
        let mut out = Vec::new();
        r.read_to_end(&mut out).unwrap();
        assert_eq!(out, b"tail");
        let mut buf = [0u8; 8];
        assert_eq!(r.read(&mut buf).unwrap(), 0);
        assert!(r.fill_buf().unwrap().is_empty());
        assert_eq!(r.read(&mut buf).unwrap(), 0);
    }

    /// Chaining the two adapters (reader → copy → writer) round-trips.
    #[test]
    fn chained_reader_writer_round_trip() {
        let payload: Vec<u8> = (0..500_000u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut r = ReadAhead::new(Cursor::new(payload.clone()), 64 * 1024);
        let mut w = WriteBehind::new(Vec::new(), 64 * 1024);
        std::io::copy(&mut r, &mut w).unwrap();
        let sink = w.finish().unwrap();
        assert_eq!(sink, payload);
    }

    /// `with_thread_name` labels the write-side IO thread (the read side has
    /// its own test above).
    #[test]
    fn with_thread_name_labels_the_write_io_thread() {
        struct NameCapturingSink {
            thread_name: std::sync::Arc<std::sync::Mutex<Option<String>>>,
        }
        impl Write for NameCapturingSink {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                let mut slot = self.thread_name.lock().unwrap();
                if slot.is_none() {
                    *slot = Some(thread::current().name().unwrap_or("<unnamed>").to_string());
                }
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let thread_name = std::sync::Arc::new(std::sync::Mutex::new(None));
        let mut w = WriteBehind::with_thread_name(
            NameCapturingSink { thread_name: thread_name.clone() },
            64 * 1024,
            "my-stage",
        );
        w.write_all(b"name probe").unwrap();
        w.finish().unwrap();
        assert_eq!(thread_name.lock().unwrap().as_deref(), Some("my-stage-write"));
    }

    /// `clone_io_error` round-trips an OS error losslessly (errno + kind).
    #[test]
    fn clone_io_error_round_trips_os_errors() {
        let original = io::Error::from_raw_os_error(2); // ENOENT / ERROR_FILE_NOT_FOUND
        let cloned = clone_io_error(&original);
        assert_eq!(cloned.raw_os_error(), original.raw_os_error());
        assert_eq!(cloned.kind(), original.kind());
    }

    /// `clone_io_error` preserves kind and message for non-OS errors — but,
    /// as documented, not the boxed payload's identity: downcasting the
    /// surfaced error yields nothing.
    #[test]
    fn clone_io_error_preserves_kind_and_message() {
        #[derive(Debug)]
        struct CustomError;
        impl fmt::Display for CustomError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("truncated header")
            }
        }
        impl std::error::Error for CustomError {}

        let original = io::Error::new(io::ErrorKind::InvalidData, CustomError);
        let cloned = clone_io_error(&original);
        assert_eq!(cloned.kind(), io::ErrorKind::InvalidData);
        assert_eq!(cloned.to_string(), original.to_string());
        let downcast_survives =
            cloned.get_ref().is_some_and(|e| e.downcast_ref::<CustomError>().is_some());
        assert!(!downcast_survives, "payload identity is documented as not preserved");
    }

    /// `ring_bytes` below the floor still works — floored to 64 KiB, not
    /// rejected.
    #[test]
    fn tiny_ring_bytes_floors_and_round_trips() {
        let payload: Vec<u8> = (0..100_000u32).map(|i| i as u8).collect();
        let mut r = ReadAhead::new(Cursor::new(payload.clone()), 0);
        let mut out = Vec::new();
        r.read_to_end(&mut out).unwrap();
        assert_eq!(out, payload);

        let mut w = WriteBehind::new(Vec::new(), 1);
        w.write_all(&payload).unwrap();
        assert_eq!(w.finish().unwrap(), payload);
    }

    /// Tiny deterministic PRNG (xorshift32); avoids a dev-dependency.
    struct XorShift(u32);
    impl XorShift {
        fn next_in(&mut self, lo: usize, hi: usize) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 17;
            self.0 ^= self.0 << 5;
            lo + (self.0 as usize) % (hi - lo)
        }
    }

    /// Serves randomly-sized chunks of `payload` per read.
    struct RandomChunkSource {
        payload: Vec<u8>,
        pos: usize,
        rng: XorShift,
    }
    impl Read for RandomChunkSource {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.pos >= self.payload.len() {
                return Ok(0);
            }
            let n = self.rng.next_in(1, 17_000).min(buf.len()).min(self.payload.len() - self.pos);
            buf[..n].copy_from_slice(&self.payload[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
    }

    /// Round-trip `payload_len` seeded-pseudo-random bytes through a reader
    /// (random source chunks, random consumer read sizes) and a writer
    /// (random write sizes), across many wraps of the minimum-size ring.
    /// Fully deterministic per seed, so failures reproduce.
    fn randomized_round_trip(seed: u32, payload_len: usize) {
        let payload: Vec<u8> = (0..payload_len as u32)
            .map(|i| (i.wrapping_mul(2654435761).wrapping_add(seed) >> 9) as u8)
            .collect();

        let mut r = ReadAhead::new(
            RandomChunkSource {
                payload: payload.clone(),
                pos: 0,
                rng: XorShift((seed ^ 0x2545_F491) | 1),
            },
            64 * 1024,
        );
        let mut rng = XorShift((seed ^ 0x9E37_79B9) | 1);
        let mut out = Vec::new();
        let mut buf = vec![0u8; 32 * 1024];
        loop {
            let want = rng.next_in(1, buf.len());
            let n = r.read(&mut buf[..want]).unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        assert_eq!(out, payload, "reader corrupted the stream across ring wraps (seed {seed})");

        let mut w = WriteBehind::new(Vec::new(), 64 * 1024);
        let mut rng = XorShift((seed ^ 0xB529_7A4D) | 1);
        let mut written = 0;
        while written < payload.len() {
            let n = rng.next_in(1, 40_000).min(payload.len() - written);
            w.write_all(&payload[written..written + n]).unwrap();
            written += n;
        }
        assert_eq!(
            w.finish().unwrap(),
            payload,
            "writer corrupted the stream across ring wraps (seed {seed})"
        );
    }

    /// Randomized read/write sizes across many ring wraps, driven by a
    /// deterministic xorshift so failures reproduce.
    #[test]
    fn randomized_chunk_sizes_round_trip() {
        randomized_round_trip(0, 2_000_000);
    }

    // ─── Soundness-assumption pins ───────────────────────────────────────────

    /// The soundness of the read loop's cast rests on `zeroed_ring` having
    /// initialized every byte of storage, which in turn rests on a fresh
    /// ring's vacant slices covering the whole allocation. Pin that
    /// assumption (it is also asserted at construction).
    #[test]
    fn fresh_ring_vacant_slices_cover_the_whole_allocation() {
        let (mut producer, _consumer) = zeroed_ring(64 * 1024);
        let (first, second) = producer.vacant_slices_mut();
        assert_eq!(first.len() + second.len(), 64 * 1024);
    }

    /// A source that READS the buffer it is handed before writing it — legal
    /// for a safe `Read` impl, since std puts the burden of passing
    /// initialized memory on the *caller*. Exercises multiple ring laps so
    /// recycled regions are inspected too. Under Miri (this test is on CI's
    /// curated list) this proves the ring pre-zeroing makes the cast sound;
    /// under a normal run it checks behavior.
    #[test]
    fn source_that_reads_its_buffer_is_sound_and_round_trips() {
        struct NosySource {
            remaining: usize,
        }
        impl Read for NosySource {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if self.remaining == 0 {
                    return Ok(0);
                }
                // Read a prefix of the buffer we were handed (any value is
                // legal; the fold defeats optimizing the reads away).
                let inspected: u64 = buf[..buf.len().min(128)].iter().map(|&b| u64::from(b)).sum();
                let n = buf.len().min(16 * 1024).min(self.remaining);
                self.remaining -= n;
                buf[..n].fill((inspected % 251) as u8);
                Ok(n)
            }
        }
        // 96 000 bytes through the 64 KiB minimum ring = 1.5 laps, so the
        // source inspects both freshly-zeroed and recycled storage.
        let mut r = ReadAhead::new(NosySource { remaining: 96_000 }, 64 * 1024);
        let mut out = Vec::new();
        r.read_to_end(&mut out).unwrap();
        assert_eq!(out.len(), 96_000);
    }

    // ─── Cross-thread usage patterns ─────────────────────────────────────────

    /// The realistic shared-writer shape: several threads take turns writing
    /// through a `Mutex<WriteBehind>`. Every parked write must be woken no
    /// matter which thread parked (repeated waiter re-registration), and each
    /// chunk written under the lock must arrive contiguously.
    #[test]
    fn writer_shared_behind_a_mutex_across_threads_round_trips() {
        let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = std::sync::Arc::new(std::sync::Mutex::new(WriteBehind::new(
            SlowSink { received: received.clone(), delay: std::time::Duration::from_millis(1) },
            64 * 1024,
        )));
        let chunk = 32 * 1024; // half the ring per write: parks happen regularly
        let workers: Vec<_> = (0..4u8)
            .map(|tag| {
                let writer = writer.clone();
                thread::spawn(move || {
                    for _ in 0..8 {
                        let mut w = writer.lock().unwrap();
                        w.write_all(&vec![tag; chunk]).unwrap();
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        let writer =
            std::sync::Arc::into_inner(writer).expect("all workers joined").into_inner().unwrap();
        writer.finish().unwrap();

        let received = received.lock().unwrap();
        assert_eq!(received.len(), 4 * 8 * chunk);
        let mut per_tag = [0usize; 4];
        for c in received.chunks(chunk) {
            let tag = c[0];
            assert!(c.iter().all(|&b| b == tag), "interleaved chunk near tag {tag}");
            per_tag[tag as usize] += 1;
        }
        assert_eq!(per_tag, [8, 8, 8, 8]);
    }

    /// Move one writer across a chain of threads, each writing its own
    /// tagged chunk with the ring under pressure: every hop re-registers the
    /// waiter, and every chunk must arrive in hop order.
    fn writer_hop_chain(hops: usize, chunk: usize) {
        let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink_buffer = received.clone();
        run_or_detect_deadlock(move || {
            let mut writer = WriteBehind::new(
                SlowSink { received: sink_buffer, delay: std::time::Duration::from_millis(1) },
                64 * 1024,
            );
            for hop in 0..hops {
                writer = thread::spawn(move || {
                    let mut w = writer;
                    w.write_all(&vec![(hop % 251) as u8; chunk]).unwrap();
                    w
                })
                .join()
                .unwrap();
            }
            writer.finish().unwrap();
        })
        .expect("writer deadlocked while hopping across threads");

        let received = received.lock().unwrap();
        assert_eq!(received.len(), hops * chunk);
        for (hop, c) in received.chunks(chunk).enumerate() {
            let tag = (hop % 251) as u8;
            assert!(c.iter().all(|&b| b == tag), "hop {hop}: chunk corrupted or out of order");
        }
    }

    /// Fast variant of the hop chain (the soak variant runs hundreds).
    #[test]
    fn writer_hops_across_threads_round_trips() {
        writer_hop_chain(12, 24 * 1024);
    }

    // ─── Composition and flush integrity ─────────────────────────────────────

    /// Composing writers must propagate the inner error out through the outer
    /// `finish()`: the outer IO thread's final flush drives the inner
    /// writer's real flush, which surfaces the inner sink's failure. (With
    /// the pre-fix no-op flush, the inner error was swallowed by the inner
    /// `Drop`.) Also exercises waiter registration from a non-constructing
    /// thread: the inner writer's flush parks the *outer IO thread*.
    #[test]
    fn nested_threaded_writers_propagate_inner_sink_errors() {
        let result = run_or_detect_deadlock(|| {
            let inner = WriteBehind::new(FailingSink, 64 * 1024);
            let mut outer = WriteBehind::new(inner, 64 * 1024);
            outer.write_all(&vec![5u8; 8 * 1024]).and_then(|()| outer.finish().map(drop))
        })
        .expect("nested writers deadlocked");
        assert!(result.is_err(), "the inner sink's failure must surface through the outer finish");
    }

    /// A sink that accepts exactly `remaining` bytes and then fails
    /// permanently.
    struct ByteLimitedSink {
        received: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
        remaining: usize,
    }
    impl Write for ByteLimitedSink {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.remaining == 0 {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "sink capacity exhausted"));
            }
            let n = buf.len().min(self.remaining);
            self.received.lock().unwrap().extend_from_slice(&buf[..n]);
            self.remaining -= n;
            Ok(n)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// `flush()` returning `Ok` must mean every byte written so far has
    /// reached the sink. Sweep the sink's failure point so it lands at every
    /// phase of the write/flush cycle; on each successful flush, the sink
    /// must hold exactly the bytes sent.
    fn flush_integrity_sweep(failure_points: impl Iterator<Item = usize>) {
        for limit in failure_points {
            let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let mut w = WriteBehind::new(
                ByteLimitedSink { received: received.clone(), remaining: limit },
                64 * 1024,
            );
            let chunk = vec![7u8; 997];
            let mut sent = 0;
            for _ in 0..40 {
                if w.write_all(&chunk).is_err() {
                    break;
                }
                sent += chunk.len();
                if w.flush().is_err() {
                    break;
                }
                let received_len = received.lock().unwrap().len();
                assert_eq!(
                    received_len, sent,
                    "flush returned Ok with {received_len} of {sent} bytes at the sink \
                     (sink limit {limit})"
                );
            }
        }
    }

    /// Fast sweep of the flush-integrity invariant (the soak variant sweeps
    /// far more failure points).
    #[test]
    fn flush_ok_means_all_bytes_delivered() {
        flush_integrity_sweep((0..40).map(|i| i * 499));
    }

    /// Debug impls render without deadlocking (they briefly lock the error
    /// slot) and identify the adapter.
    #[test]
    fn debug_impls_render() {
        let r = ReadAhead::new(Cursor::new(vec![1u8; 16]), 64 * 1024);
        assert!(format!("{r:?}").contains("ReadAhead"));
        let w = WriteBehind::new(Vec::new(), 64 * 1024);
        assert!(format!("{w:?}").contains("WriteBehind"));
    }

    // ─── Pins from the independent review round ──────────────────────────────

    /// The first `Ok(0)` from the source is terminal: the IO thread stops
    /// polling it and the reader reports EOF from then on, even though this
    /// source would produce bytes if polled again (documented; tail-following
    /// sources are out of scope).
    #[test]
    fn first_source_ok_zero_is_terminal_eof() {
        struct ZeroThenData {
            calls: u32,
        }
        impl Read for ZeroThenData {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                self.calls += 1;
                match self.calls {
                    1 => Ok(0),
                    _ => {
                        buf[0] = 9;
                        Ok(1)
                    }
                }
            }
        }
        let mut r = ReadAhead::new(ZeroThenData { calls: 0 }, 64 * 1024);
        let mut buf = [0u8; 8];
        assert_eq!(r.read(&mut buf).unwrap(), 0);
        assert_eq!(r.read(&mut buf).unwrap(), 0, "the first Ok(0) must be terminal");
    }

    /// Zero-length operations on an already-failed adapter behave as
    /// documented: an empty read returns `Ok(0)` without consulting the
    /// stream state, while an empty write surfaces the latched error
    /// eagerly.
    #[test]
    fn zero_length_ops_on_failed_adapters_match_documented_semantics() {
        struct AlwaysFailingSource;
        impl Read for AlwaysFailingSource {
            fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::other("source dead"))
            }
        }
        let mut r = ReadAhead::new(AlwaysFailingSource, 64 * 1024);
        assert!(r.read(&mut [0u8; 8]).is_err(), "the failure must latch first");
        assert_eq!(r.read(&mut []).unwrap(), 0, "empty reads do not consult stream state");

        let mut w = WriteBehind::new(FailingSink, 4096);
        let _ = w.write_all(&vec![0u8; 128 * 1024]); // provoke + surface the latch
        assert!(w.write(&[]).is_err(), "empty writes report the latched error eagerly");
    }

    /// A sink that returns `Ok(0)` must surface as a latched `WriteZero`
    /// error — not hang the producer or silently drop bytes.
    #[test]
    fn sink_returning_zero_surfaces_write_zero_error() {
        struct ZeroSink;
        impl Write for ZeroSink {
            fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
                Ok(0)
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let result = run_or_detect_deadlock(|| {
            let mut w = WriteBehind::new(ZeroSink, 4096);
            w.write_all(b"data").and_then(|()| w.finish().map(drop))
        })
        .expect("writer deadlocked on a zero-accepting sink");
        let err = result.expect_err("an Ok(0) sink must surface as an error");
        assert_eq!(err.kind(), io::ErrorKind::WriteZero);
    }

    /// `consume` beyond what `fill_buf` returned saturates instead of
    /// corrupting the ring (matching `BufReader`, which clamps too).
    #[test]
    fn consume_beyond_fill_buf_saturates() {
        let mut r = ReadAhead::new(Cursor::new(b"hello".to_vec()), 64 * 1024);
        let buffered = r.fill_buf().unwrap().len();
        assert!(buffered > 0);
        r.consume(buffered + 1000); // over-consume: must clamp, not corrupt
        let mut out = Vec::new();
        r.read_to_end(&mut out).unwrap();
        assert!(out.is_empty(), "over-consume must clamp at the buffered bytes");
    }

    /// Dropping a writer without `finish()` still flushes the *sink itself*
    /// (the drop path ends with a sink flush), not just drains the ring.
    #[test]
    fn writer_drop_without_finish_flushes_the_sink_itself() {
        let flushes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let mut w = WriteBehind::new(FlushCountingSink { flushes: flushes.clone() }, 4096);
            w.write_all(b"pending").unwrap();
        } // dropped without finish()
        assert!(
            flushes.load(std::sync::atomic::Ordering::Relaxed) >= 1,
            "the drop path must flush the sink"
        );
    }

    /// Several threads take turns reading through a `Mutex<ReadAhead>` —
    /// the read-side analog of the pooled-writer test (repeated waiter
    /// re-registration on the consumer side).
    #[test]
    fn reader_shared_behind_a_mutex_across_threads_round_trips() {
        let payload: Vec<u8> = (0..256 * 1024u32).map(|i| (i * 13) as u8).collect();
        let reader = std::sync::Arc::new(std::sync::Mutex::new(ReadAhead::new(
            Cursor::new(payload.clone()),
            64 * 1024,
        )));
        let out = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let reader = reader.clone();
                let out = out.clone();
                thread::spawn(move || {
                    let mut buf = vec![0u8; 8 * 1024];
                    loop {
                        // Hold the reader lock across the append so the
                        // global byte order matches the read order.
                        let mut r = reader.lock().unwrap();
                        let n = r.read(&mut buf).unwrap();
                        if n == 0 {
                            return;
                        }
                        out.lock().unwrap().extend_from_slice(&buf[..n]);
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(*out.lock().unwrap(), payload);
    }

    /// Fast producer + deliberately slow consumer: the reader's IO thread
    /// parks on a full ring and must be re-woken by `consume` each time
    /// space frees — pins the IO-thread-park half of the reader protocol
    /// under success (Drop-time wake has its own test).
    #[test]
    fn slow_consumer_reader_backpressure_round_trip() {
        let payload: Vec<u8> = (0..256 * 1024u32).map(|i| (i * 7) as u8).collect();
        let mut r = ReadAhead::new(Cursor::new(payload.clone()), 64 * 1024);
        let mut out = Vec::new();
        let mut buf = vec![0u8; 8 * 1024];
        let mut chunks = 0;
        loop {
            let n = r.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
            chunks += 1;
            if chunks % 8 == 0 {
                // Let the IO thread refill the ring and park on it full.
                thread::sleep(std::time::Duration::from_millis(2));
            }
        }
        assert_eq!(out, payload);
    }

    /// `capacity` reports the floored ring size; `buffered` tracks read-ahead
    /// and drains to zero at end-of-stream.
    #[test]
    fn reader_introspection_reports_ring_state() {
        let payload = vec![7u8; 10_000];
        let mut r = ReadAhead::new(Cursor::new(payload.clone()), 0);
        assert_eq!(r.capacity(), 64 * 1024, "capacity is the floored ring size");
        // The IO thread fills the ring concurrently; wait (bounded) until the
        // whole payload — which fits the ring — has been read ahead.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while r.buffered() < payload.len() {
            assert!(std::time::Instant::now() < deadline, "read-ahead never buffered the payload");
            thread::yield_now();
        }
        let mut out = Vec::new();
        r.read_to_end(&mut out).unwrap();
        assert_eq!(out, payload);
        assert_eq!(r.buffered(), 0, "everything buffered was consumed");
    }

    /// `pending` counts bytes not yet handed to the sink — exactly the whole
    /// payload while the sink is blocked in its first write (the IO thread
    /// only retires ring bytes after the sink accepts them) — and is zero
    /// after a successful flush.
    #[test]
    fn writer_introspection_reports_ring_state() {
        /// Blocks each write until the gate sender is dropped.
        struct GatedSink {
            gate: std::sync::mpsc::Receiver<()>,
        }
        impl Write for GatedSink {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                let _ = self.gate.recv(); // blocks until the sender drops
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let (gate_tx, gate_rx) = std::sync::mpsc::channel::<()>();
        let payload = vec![9u8; 10_000];
        let mut w = WriteBehind::new(GatedSink { gate: gate_rx }, 0);
        assert_eq!(w.capacity(), 64 * 1024, "capacity is the floored ring size");
        w.write_all(&payload).unwrap();
        // Deterministic: the IO thread is either not yet draining or blocked
        // inside the sink's first write; ring bytes are only retired after
        // the sink accepts them, so everything written is still pending.
        assert_eq!(w.pending(), payload.len());
        drop(gate_tx); // open the gate: recv() errors and writes proceed
        w.flush().unwrap();
        assert_eq!(w.pending(), 0, "a successful flush leaves nothing pending");
    }

    /// Construct and immediately drop both adapters — before the IO thread
    /// has necessarily run a single loop iteration. The pre-armed unpark
    /// token covers the startup races; nothing may hang or panic.
    #[test]
    fn construct_then_immediate_drop_is_clean() {
        for _ in 0..100 {
            drop(ReadAhead::new(Cursor::new(vec![0u8; 8]), 64 * 1024));
            drop(WriteBehind::new(Vec::new(), 64 * 1024));
        }
    }

    // ─── Soak tests ──────────────────────────────────────────────────────────
    //
    // Long-running stress variants, excluded from the default run. Execute
    // with `cargo ci-soak` (alias for `nextest run --profile soak
    // --run-ignored ignored-only`); CI runs them on a weekly schedule. A hang
    // in a soak test is killed and failed by nextest's terminate-after.

    /// Soak: the re-entrant-write deadlock regression across 50k IO-thread
    /// lifetimes (~1 min).
    #[test]
    #[ignore = "soak — run via cargo ci-soak"]
    fn soak_reentrant_write_stress() {
        reentrant_write_stress(50_000);
    }

    /// Soak: the panic-masked-as-EOF race probe at 150k iterations.
    #[test]
    #[ignore = "soak — run via cargo ci-soak"]
    fn soak_panic_mask_stress() {
        silence_mask_probe_panics();
        assert_failure_never_masks_as_clean_eof(
            |spins| OneByteThenSpinPanic { served: false, spins },
            150_000,
        );
    }

    /// Soak: the error-masked-as-EOF race probe at 150k iterations.
    #[test]
    #[ignore = "soak — run via cargo ci-soak"]
    fn soak_error_mask_stress() {
        assert_failure_never_masks_as_clean_eof(
            |spins| OneByteThenSpinError { served: false, spins },
            150_000,
        );
    }

    /// Soak: randomized round trips across 32 seeds x 4 MB.
    #[test]
    #[ignore = "soak — run via cargo ci-soak"]
    fn soak_randomized_round_trips_across_seeds() {
        for seed in 1..=32 {
            randomized_round_trip(seed, 4_000_000);
        }
    }

    /// Soak: hundreds of writer thread-hops (waiter re-registration under
    /// ring pressure).
    #[test]
    #[ignore = "soak — run via cargo ci-soak"]
    fn soak_writer_hops_across_threads() {
        writer_hop_chain(300, 24 * 1024);
    }

    /// Soak: thousands of construct-on-one-thread / use-on-another cycles
    /// for both adapters. A hang here is killed and failed by nextest's
    /// terminate-after.
    #[test]
    #[ignore = "soak — run via cargo ci-soak"]
    fn soak_cross_thread_construct_use_cycles() {
        let payload = vec![0xa5u8; 4096];
        for _ in 0..8_000 {
            let reader = ReadAhead::new(Cursor::new(payload.clone()), 64 * 1024);
            let out = thread::spawn(move || {
                let mut r = reader;
                let mut out = Vec::new();
                r.read_to_end(&mut out).unwrap();
                out
            })
            .join()
            .unwrap();
            assert_eq!(out, payload);

            let writer = WriteBehind::new(Vec::new(), 64 * 1024);
            let expected = payload.clone();
            let sink = thread::spawn(move || {
                let mut w = writer;
                w.write_all(&expected).unwrap();
                w.finish().unwrap()
            })
            .join()
            .unwrap();
            assert_eq!(sink, payload);
        }
    }

    /// Soak: the flush-integrity sweep across 600 sink failure points.
    #[test]
    #[ignore = "soak — run via cargo ci-soak"]
    fn soak_flush_integrity_sweep() {
        flush_integrity_sweep((0..600).map(|i| i * 131));
    }
}

// ─── loom models ─────────────────────────────────────────────────────────────

/// Model-checked executions of the crate's blocking protocols, run only via
/// `scripts/loom.sh` (never part of a normal `cargo test`). Each test wraps
/// one protocol scenario in `loom::model`, which runs it under every schedule
/// loom's preemption bound admits — including the stale-value reads C11 weak
/// memory allows — and fails on any deadlock (a lost wakeup parks forever),
/// panic, or assertion failure, printing the schedule that triggered it.
/// Sources and sinks are deterministic; the scheduler is the only
/// nondeterminism, as loom requires.
///
/// Ring capacities look huge (the constructors floor `ring_bytes` to 64 KiB)
/// but cost the model nothing: ring bytes move via plain memcpys the checker
/// never interleaves; only the index atomics, mutexes, and park/unpark are
/// modeled operations. "Fill the ring" is therefore one modeled push, not
/// 64 Ki of them.
#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use std::io::Cursor;

    /// A source that serves `data`, then fails with `err_msg`.
    struct FailingSource {
        data: &'static [u8],
        pos: usize,
        err_msg: &'static str,
    }

    impl Read for FailingSource {
        fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
            if self.pos < self.data.len() {
                let n = dst.len().min(self.data.len() - self.pos);
                dst[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
                self.pos += n;
                Ok(n)
            } else {
                Err(io::Error::other(self.err_msg))
            }
        }
    }

    /// A source whose first `read` panics, exercising the catch_unwind
    /// boundary in `io_read_thread`.
    struct PanickingSource;

    impl Read for PanickingSource {
        fn read(&mut self, _dst: &mut [u8]) -> io::Result<usize> {
            panic!("kaboom-probe");
        }
    }

    /// A sink that rejects every write, so the ring never drains — the
    /// setup for the production ENOSPC deadlock the `write` error checks fix.
    struct FailingSink;

    impl Write for FailingSink {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("sink refused"))
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// A sink recording everything it accepts and each `flush` call.
    #[derive(Default)]
    struct RecordingSink {
        data: Vec<u8>,
        flushes: usize,
    }

    impl Write for RecordingSink {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.data.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            self.flushes += 1;
            Ok(())
        }
    }

    /// Silence the default panic-hook output for the probe panics the models
    /// throw once per explored execution; forward everything else (assertion
    /// failures included) to the previous hook. Keyed on the payload rather
    /// than the thread name (as the std suite's filter is) because loom's
    /// threads all report the host test thread's name.
    fn silence_probe_panics() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            let previous = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                let from_probe = info
                    .payload()
                    .downcast_ref::<&str>()
                    .is_some_and(|message| message.contains("kaboom-probe"));
                if !from_probe {
                    previous(info);
                }
            }));
        });
    }

    /// The scenario behind `register_waiter`: construct on one thread, read
    /// on another. In every schedule the migrated reader's parks must be
    /// woken by the IO thread — a wake aimed at the construction thread (the
    /// original cross-thread bug) parks the reader forever, which loom
    /// reports as a deadlock with the schedule that produced it.
    #[test]
    fn reader_migrated_across_threads_drains_to_eof() {
        loom::model(|| {
            let reader = ReadAhead::new(Cursor::new(vec![7u8, 8]), 1);
            thread::spawn(move || {
                let mut reader = reader;
                let mut out = Vec::new();
                reader.read_to_end(&mut out).expect("read_to_end");
                assert_eq!(out, [7, 8]);
            })
            .join()
            .expect("reader thread");
        });
    }

    /// A source failure after data must surface as that error — never as a
    /// clean EOF (silent truncation) — and the bytes produced before the
    /// failure must be served first, under every interleaving of the IO
    /// thread's latch-then-eof publication with the reader's checks.
    #[test]
    fn reader_source_error_never_reads_as_clean_eof() {
        loom::model(|| {
            let mut reader =
                ReadAhead::new(FailingSource { data: b"z", pos: 0, err_msg: "boom-probe" }, 1);
            let mut out = Vec::new();
            let err = reader.read_to_end(&mut out).expect_err("source failed");
            assert_eq!(out, b"z", "bytes before the failure are served first");
            assert!(err.to_string().contains("boom-probe"), "got: {err}");
        });
    }

    /// An IO-thread panic must latch as the error (message preserved), set
    /// EOF after it, and wake the reader — never hang it, never read as a
    /// clean EOF. Exercises the catch_unwind boundary under all schedules.
    #[test]
    fn reader_io_thread_panic_latches_error_and_wakes() {
        silence_probe_panics();
        loom::model(|| {
            let mut reader = ReadAhead::new(PanickingSource, 1);
            let mut buf = [0u8; 4];
            let err = reader.read(&mut buf).expect_err("panic must surface");
            assert!(err.to_string().contains("kaboom-probe"), "got: {err}");
        });
    }

    /// The production ENOSPC deadlock: the sink dies, the ring is full, and
    /// the next `write` must surface the latched error instead of parking
    /// against an IO thread that already exited — whether it observes the
    /// error up front, at the pre-park re-check, or at the post-park
    /// re-check. The trailing drop must join cleanly, not hang.
    #[test]
    fn writer_full_ring_write_surfaces_sink_error_instead_of_hanging() {
        loom::model(|| {
            let mut writer = WriteBehind::new(FailingSink, 1);
            let fill = vec![0u8; writer.capacity()];
            assert_eq!(writer.write(&fill).expect("ring accepts the fill"), fill.len());
            let err = writer.write(&[1]).expect_err("failed writer must reject");
            assert!(err.to_string().contains("sink refused"), "got: {err}");
        });
    }

    /// The same dead-sink/full-ring scenario after migrating the writer to
    /// another thread: the waiter-slot protocol must aim the error wakeup at
    /// the thread that is actually parked in `write`, not the constructor's.
    #[test]
    fn writer_migrated_across_threads_surfaces_sink_error() {
        loom::model(|| {
            let writer = WriteBehind::new(FailingSink, 1);
            thread::spawn(move || {
                let mut writer = writer;
                let fill = vec![0u8; writer.capacity()];
                writer.write_all(&fill).expect("ring accepts the fill");
                let err = writer.write(&[1]).expect_err("failed writer must reject");
                assert!(err.to_string().contains("sink refused"), "got: {err}");
            })
            .join()
            .expect("writer thread");
        });
    }

    /// `flush` must not return until every prior byte reached the sink and
    /// the sink's own `flush` ran (the `flush_seq`/`flush_ack` handshake,
    /// including the IO loop's seq-load-before-empty-check ordering). After
    /// a successful flush nothing is pending, and `finish` hands back the
    /// sink with everything recorded.
    #[test]
    fn writer_flush_drains_ring_and_acks_epoch() {
        loom::model(|| {
            let mut writer = WriteBehind::new(RecordingSink::default(), 1);
            writer.write_all(b"ab").expect("write");
            writer.flush().expect("flush");
            assert_eq!(writer.pending(), 0, "flush returned with bytes still pending");
            let sink = writer.finish().expect("finish");
            assert_eq!(sink.data, b"ab");
            assert!(sink.flushes >= 1, "sink flush must run before flush() returns");
        });
    }
}
