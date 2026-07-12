# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Initial release.

### Added

- `ReadAhead`: wraps any `Read` source; a dedicated IO thread reads *ahead* into a byte ring buffer so the consumer's `read` / `fill_buf` is served from user space instead of blocking on the kernel. Implements `Read`, `BufRead`, and `Debug`. Construct with `ReadAhead::new(src, ring_bytes)`, or `ReadAhead::with_thread_name(src, ring_bytes, prefix)` to label the IO thread (and any surfaced IO-thread panic) after your own component.
- `WriteBehind<W>`: wraps any `Write` sink; `write` returns as soon as the bytes land in the ring, and the IO thread drains them *behind* the caller. Implements `Write` and `Debug`, plus `finish()`, which drains the ring, flushes the sink, joins the IO thread, and hands the sink back — so a `File` can be fsynced before a rename, or a `Vec<u8>` reclaimed. Same `new` / `with_thread_name` constructors.
- Explicit ring sizing: each adapter takes `ring_bytes` (floored to 64 KiB), allocated once up front and recycled forever.
- `flush()` honors the `Write::flush` contract: it blocks until every buffered byte has reached the sink *and* the sink's own `flush` has completed.
- Robust failure semantics: an IO-thread error or panic (with the panic's own message preserved) is latched write-once/read-many and surfaced by every subsequent `read`/`write`/`flush`/`finish` — never masked as a clean EOF or a silent success, and never able to deadlock a parked caller. Bytes the source produced before failing are delivered first, matching `BufReader` semantics, and transient `ErrorKind::Interrupted` (EINTR) is retried rather than latched.
- Cross-thread friendly: both adapters are `Send` and may be constructed on one thread and used from another (or shared behind a `Mutex`); blocking calls wake correctly wherever they run. Dropping either adapter joins its IO thread; dropping a `WriteBehind` without `finish()` still drains the ring into the sink but discards errors.
- Soundness hardened against misbehaving-but-safe `Read`/`Write` impls: the ring's storage is initialized at construction (the source is never handed uninitialized memory) and reported byte counts are bounds-checked. Verified in CI under Miri (including 32-seed scheduling exploration) and ThreadSanitizer, on Linux/macOS/Windows, with deadlock-regression, backpressure, and weekly soak test tiers.

[Unreleased]: https://github.com/tfenne/rawb-io/compare/HEAD
