# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial release of `rawb-io` — read-ahead / write-behind byte IO extracted from a battle-tested production implementation.
- `ThreadedReader`: wraps any `Read` source; a background thread reads *ahead* into a byte ring buffer so the consumer is served from user space instead of blocking on the kernel pipe. Implements `Read` and `BufRead`. Construct with `ThreadedReader::new(src, ring_bytes)` or `ThreadedReader::with_thread_name(src, ring_bytes, prefix)` to label the IO thread after your own component.
- `ThreadedWriter`: wraps any `Write` sink; the caller's `write` returns as soon as the bytes land in the ring, and a background thread flushes them *behind* the caller. Implements `Write` plus an explicit `finish()` that flushes, drains the IO thread, and surfaces its final result. Construct with `ThreadedWriter::new(dst, ring_bytes)` or `ThreadedWriter::with_thread_name(dst, ring_bytes, prefix)`.
- A write-once/read-many error latch that surfaces an IO-thread failure (or panic) on every subsequent `read`/`write`/`flush`/`finish` and rejects further work up front, so a downstream sink dying (e.g. a broken pipe or `ENOSPC`) propagates as an error instead of deadlocking a parked worker.
- `Debug` implementations for both adapters, reporting the IO thread's name, buffered/pending byte count, and terminal state.

### Changed

- `ThreadedWriter` is now generic over its sink (`ThreadedWriter<W>`) and `finish()` returns the sink (`io::Result<W>`): the IO thread hands it back at join, so callers can fsync a `File` before renaming it or reclaim a `Vec<u8>`. On error the sink is dropped and the latched error returned.
- `ThreadedWriter::flush` now honors the `Write::flush` contract: it blocks until every buffered byte has been written to the sink and the sink's own `flush` has completed (or returns the latched error if the IO thread has failed). Previously it returned `Ok` immediately while bytes could still be sitting in the ring, breaking write-then-signal patterns and swallowing inner errors when composing writers.
- `ThreadedReader` now delivers every byte the source successfully produced before failing, and surfaces the error once that buffered data is drained — matching `BufReader` semantics and making the delivered prefix deterministic. Previously up to a full ring of read-ahead was discarded and the error surfaced immediately. This also removes the error-latch mutex from the reader's data-serving hot path.
- `ThreadedWriter::write` now reports a partially-consumed call as `Ok(n)` when the IO thread fails mid-call, per the `Write::write` contract (an error return means nothing was consumed); the latched error surfaces on the next operation. `ThreadedReader::read` with an empty destination returns `Ok(0)` immediately instead of blocking until data arrives.
- An IO-thread panic now surfaces with the panic's own message appended to the `"{prefix} IO thread panicked"` label (e.g. the text of a failed assertion), instead of discarding it with the join result.

### Fixed

- `ThreadedReader` and `ThreadedWriter` constructed on one thread and used on another no longer deadlock: IO-thread wakeups now target whichever thread is actually blocked (re-registered before every park) instead of the thread that ran the constructor.
- An IO-thread failure can no longer be reported as a clean EOF: every failure path publishes the error latch strictly before the EOF flag, and `fill_buf` re-checks the latch after observing EOF, closing a race in which a panicked or failed source surfaced as successful end-of-stream (silent truncation).
- The read loop no longer hands uninitialized memory to the wrapped `Read` (the ring is pre-initialized once at construction) and no longer trusts the source's reported byte count (a count larger than the buffer fails loudly instead of publishing unwritten ring bytes) — both were soundness holes reachable from safe-but-misbehaving `Read` impls.
- A transient `ErrorKind::Interrupted` (EINTR) from the wrapped source, or from the sink's final flush, is now retried instead of latched as a permanent failure — previously a single EINTR livelocked std's `read_to_end`/`read_to_string` (which retry `Interrupted` by contract against a latch that re-served it forever) and lost the rest of the stream.

[Unreleased]: https://github.com/tfenne/rawb-io/compare/HEAD
