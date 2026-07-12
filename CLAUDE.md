# CLAUDE.md — rawb-io

Guidance for Claude (or any coding agent) working in this repo. Human-facing contributor docs live in `CONTRIBUTING.md`; this file captures the things an agent must not get wrong.

## What this is

`rawb-io` — read-ahead / write-behind byte IO: a `ThreadedReader` and `ThreadedWriter` that put a dedicated thread on one IO end, with a `ringbuf` byte ring buffer in between, so a pipeline stage never blocks on the kernel pipe.

## The concurrency code is load-bearing — do not "clean it up"

`src/lib.rs` encodes hard-won fixes for production deadlocks (a downstream sink dying on `ENOSPC` hung a released binary for hours; adapters moved across threads parked forever), for shutdown races (an IO-thread failure surfacing as a clean EOF), and for IO-thread panics. Treat the following as invariants, not style choices, and preserve their exact behavior:

- The **write-once / read-many error latch**: the `Mutex<Option<io::Error>>` slot is *never drained*. Its presence is the sticky "this end has failed" state. `peek_error` copies (`clone_io_error`) rather than takes.
- The **waiter-slot wake protocol**: user-side park sites do register (`register_waiter`) → re-check the park condition → `park`; the IO thread does change state → read the slot → unpark (`unpark_waiter`). The slot mutex's release/acquire edge is what makes a wakeup during cross-thread migration impossible to lose. Never capture a `Thread` handle at construction for user-side wakeups — that deadlocks adapters moved between threads.
- The **error-before-EOF publication order**: every reader failure path (including the panic boundary) stores the error latch strictly *before* setting `eof`, and `fill_buf` re-checks the latch after observing `eof`. This pairing is what prevents a failure surfacing as a clean EOF (silent truncation).
- **Buffered data is served before the terminal state**: `fill_buf` consults `eof`/error only once the ring is empty. Liveness holds because every reader error also sets `eof`.
- The **`catch_unwind` panic boundary** (`io_read_thread` / `io_write_thread`): latches the panic's message as the error (before `eof` on the read side) and wakes the waiter on every exit. Removing or narrowing it reintroduces the hang.
- The **up-front error check and post-park re-check in `ThreadedWriter::write`**: this is what breaks the `ENOSPC` deadlock. Do not move them back inside the push loop. A partially-consumed `write` that hits the latch returns `Ok(n)`, not `Err` (the `Write` contract).
- The **flush epoch handshake** (`flush_seq` / `flush_ack`): the IO thread loads the requested epoch *before* its empty-check and acks only after draining and `dst.flush()`; `flush()` parks until the ack or the error latch fires.
- `ErrorKind::Interrupted` (EINTR) from the source, or from the sink's flush, is retried — never latched.
- `lock_or_recover` poison handling, and the `Drop` / `finish` semantics (drop drains the writer but swallows errors; `finish` returns the sink).

If you think a simplification is warranted, stop and ask — behavior must stay equivalent, and the tests in `src/lib.rs` are the regression suite for these fixes. Run the stress tests with large `REENTRANT_STRESS_ITERS` / `PANIC_MASK_STRESS_ITERS` overrides when touching the write path or the shutdown ordering.

## `unsafe`

The crate is `#![deny(unsafe_code)]` with two narrow `#[allow(unsafe_code)]` sites: `io_read_loop` (the `MaybeUninit<u8>` → `&mut [u8]` cast and the bounds-checked `advance_write_index`) and `zeroed_ring` (the one-time construction memset that keeps the cast sound by guaranteeing the ring's storage is always initialized). Keep `unsafe` confined there, keep the `SAFETY:` write-ups complete, and do not add `unsafe` elsewhere. Never feed the source's reported byte count to `advance_write_index` unchecked, and never build the reader's ring without the zeroing.

## Before calling any change done

Run all five gates and make them pass — these are exactly what CI runs:

```
cargo ci-fmt      # rustfmt --check
cargo ci-lint     # clippy --all-targets -D warnings
cargo ci-test     # nextest, --locked
cargo ci-doctest  # doctests (nextest does not run them)
cargo deny check  # licenses, advisories, bans, sources
```

The `ci-*` aliases live in `.cargo/config.toml`. If `ci-fmt` fails, run `cargo fmt`. CI also runs the test matrix on Linux/macOS/Windows and a curated Miri subset (`.github/workflows/check.yml`).

## Workflow — this repo is PUBLIC

- Work on a **branch + PR onto `main`**. Never commit, push, amend, or force-push `main` directly. The sole exception is the release tooling below, which the **human** runs.
- Keep PRs focused; each PR includes tests for the behavior it changes.
- Add a one-line entry to the `[Unreleased]` section of `CHANGELOG.md` for any user-visible change, in the same PR.

## Releases — NEVER publish

Releases run via `cargo-release` (`release.toml`). The publish is the **human's** job, always. **Never** run `cargo publish` or `cargo release --execute`; dry-run only. `release.toml` templates use `{{version}}`, NOT `{{next_version}}` (the latter renders as a literal in the pre-release commit context).

## More

See `CONTRIBUTING.md` for the dev loop, dependency policy, and code style.
