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

[Unreleased]: https://github.com/tfenne/rawb-io/compare/HEAD
