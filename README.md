# rawb-io

**R**ead-**a**head / **w**rite-**b**ehind byte IO for Rust: a `ThreadedReader` and `ThreadedWriter` that put a dedicated thread on one IO end, with a user-space byte ring buffer in between, so a pipeline stage never blocks on the kernel pipe.

## Why

In a shell pipeline — `producer | your_stage | consumer` — the only thing decoupling the stages is the small OS pipe buffer (~64 KiB on macOS). When both ends are bursty (a producer whose per-item cost varies, a consumer that periodically flushes a chunk to disk), any blip in one stage stalls the others: your stage blocks on a `read` that has no bytes yet, or on a `write` to a pipe whose reader is busy.

`rawb-io` moves each IO end onto its own thread with a larger buffer in between:

- **Read-ahead** — `ThreadedReader` wraps a `Read` source. A background thread reads *ahead* into the ring buffer as fast as the source allows, so your `read` / `fill_buf` is served from user space instead of blocking on the kernel. When your stage is busy, the reader keeps filling the buffer; when your stage is hungry, the bytes are already there.
- **Write-behind** — `ThreadedWriter` wraps a `Write` sink. Your `write` returns as soon as the bytes land in the ring, and a background thread flushes them *behind* you. When the downstream sink stalls, your stage keeps working against the buffer instead of blocking on the syscall.

The result is that compute and IO overlap: the worker thread rarely waits on the kernel, and throughput on a bursty pipeline improves without changing your stage's code beyond swapping in the wrapper.

## Usage

Add it to `Cargo.toml`:

```toml
[dependencies]
rawb-io = "0.1"
```

### Read-ahead

`ThreadedReader` implements both `Read` and `BufRead`, so it drops in wherever you already have a reader. The second argument is the ring-buffer size in bytes (floored to 64 KiB).

```rust
use std::io::Read;
use rawb_io::ThreadedReader;

let source = std::fs::File::open("input.dat")?;
// 16 MiB of read-ahead.
let mut reader = ThreadedReader::new(source, 16 * 1024 * 1024);

let mut buf = Vec::new();
reader.read_to_end(&mut buf)?;
```

### Write-behind

`ThreadedWriter` implements `Write`. Call `finish()` when you are done: it flushes any buffered bytes, signals the IO thread to drain, joins it, and hands the sink back — this is where a downstream write error surfaces, so prefer it over relying on `Drop`. Getting the sink back means you can, for example, fsync a `File` before renaming it into place.

```rust
use std::io::Write;
use rawb_io::ThreadedWriter;

let sink = std::fs::File::create("output.dat")?;
// 16 MiB of write-behind.
let mut writer = ThreadedWriter::new(sink, 16 * 1024 * 1024);

writer.write_all(b"...payload...")?;

// Flush, drain the IO thread, and surface any error it hit.
writer.finish()?;
```

If a `ThreadedWriter` is dropped without `finish()`, it still signals the IO thread and joins it, but any final error is discarded — use `finish()` whenever you care about the outcome.

### Naming the IO threads

By default the spawned threads are named `rawb-io-read` / `rawb-io-write`. To label them after your own component (which also labels the fallback error surfaced if the IO thread panics), use `with_thread_name`:

```rust
use rawb_io::ThreadedReader;

let source = std::io::empty();
// Thread is named "decoder-read"; a panic surfaces as "decoder IO thread panicked".
let reader = ThreadedReader::with_thread_name(source, 1 << 20, "decoder");
```

## Error handling

Both adapters surface an IO-thread failure through the normal `io::Result` return values. The failure is *latched*: once the IO thread reports an error (or panics), every subsequent `read` / `write` / `flush` / `finish` returns that error rather than masking it as a clean EOF or a silent success. On the read side the error surfaces only after every successfully-read byte has been delivered, so the consumer sees the same prefix a plain `BufReader` would have produced, then the failure. Transient `ErrorKind::Interrupted` (EINTR) results from the source or sink are the exception: the IO threads retry them, matching std's conventions, instead of latching them as permanent failures. On the write side this is also what prevents a deadlock — a failed writer rejects further writes up front instead of pushing into a ring buffer its already-exited IO thread can never drain. (`io::Error` is `!Clone`, so the latched error is surfaced by reconstruction: OS errors round-trip losslessly by errno; others preserve the kind and message.)

## Implementation notes

The buffer between the two threads is a byte ring buffer ([`ringbuf`](https://crates.io/crates/ringbuf)). **That is an implementation detail** — the crate is named for its *behavior* (read-ahead / write-behind), not its mechanism — and may change. `ringbuf` is the only runtime dependency; blocking uses `thread::park` / `unpark` with a lock-free fast path, so the ring is touched without a mutex except in the rare contended case.

### MSRV

The minimum supported Rust version is **1.89**.

### `unsafe`

The crate sets `#![deny(unsafe_code)]`. Its `unsafe` is confined to the read side: a `MaybeUninit<u8>` → `&mut [u8]` cast that lets `Read::read` write straight into the ring buffer (avoiding a second copy through a temporary), the matching `advance_write_index` that publishes exactly the bytes just read, and a one-time memset that zeroes the ring's storage at construction. Together these mean nothing trusts the wrapped source: the cast never exposes uninitialized memory (std requires the *caller* of `Read::read` to pass initialized buffers), and the source's reported byte count is bounds-checked before it is used. All three sites are documented in full where they occur, and the read-loop pair will be removed once [`std::io::BorrowedBuf`](https://github.com/rust-lang/rust/issues/117693) stabilizes, which expresses "borrowed, partially-initialized buffer" safely and eliminates the cast. The hard lock-free concurrency `unsafe` lives inside `ringbuf`, not here.

## License

MIT — see [LICENSE](LICENSE). Copyright © 2026 Tim Fennell.
