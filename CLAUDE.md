# CLAUDE.md — rawb-io

Guidance for Claude (or any coding agent) working in this repo. Human-facing contributor docs live in `CONTRIBUTING.md`; this file captures the things an agent must not get wrong.

## What this is

`rawb-io` — read-ahead / write-behind byte IO: a `ThreadedReader` and `ThreadedWriter` that put a dedicated thread on one IO end, with a `ringbuf` byte ring buffer in between, so a pipeline stage never blocks on the kernel pipe.

## The concurrency code is load-bearing — do not "clean it up"

`src/lib.rs` encodes hard-won fixes for a production deadlock (a downstream sink dying on `ENOSPC` hung a released binary for hours) and for IO-thread panics. Treat the following as invariants, not style choices, and preserve their exact behavior:

- The **write-once / read-many error latch**: the `Mutex<Option<io::Error>>` slot is *never drained*. Its presence is the sticky "this end has failed" state. `peek_error` copies (`clone_io_error`) rather than takes.
- The **`PanicGuard`**: on a panicking unwind it wakes the counterpart and records a fallback error (and sets `eof` on the read side). Removing or narrowing it reintroduces the hang.
- The up-front error re-check in `ThreadedWriter::write` (both before the loop and after `park`): this is what breaks the deadlock. Do not move it back inside the push loop.
- `park` / `unpark` blocking, `lock_or_recover` poison handling, and the `Drop` / `finish` semantics.

If you think a simplification is warranted, stop and ask — behavior must stay equivalent, and the tests in `src/lib.rs` are the regression suite for these fixes. Run the stress test with a large `REENTRANT_STRESS_ITERS` when touching anything on the write path.

## `unsafe`

The crate is `#![deny(unsafe_code)]` with a single narrow `#[allow(unsafe_code)]` on `io_read_loop`, covering the `MaybeUninit<u8>` → `&mut [u8]` cast and the matching `advance_write_index`. Keep `unsafe` confined there, keep the `SAFETY:` write-up complete, and do not add `unsafe` elsewhere.

## Before calling any change done

Run all four gates and make them pass — these are exactly what CI runs:

```
cargo ci-fmt     # rustfmt --check
cargo ci-lint    # clippy --all-targets -D warnings
cargo ci-test    # nextest, --locked
cargo deny check # licenses, advisories, bans, sources
```

The `ci-*` aliases live in `.cargo/config.toml`. If `ci-fmt` fails, run `cargo fmt`.

## Workflow — this repo is PUBLIC

- Work on a **branch + PR onto `main`**. Never commit, push, amend, or force-push `main` directly. The sole exception is the release tooling below, which the **human** runs.
- Keep PRs focused; each PR includes tests for the behavior it changes.
- Add a one-line entry to the `[Unreleased]` section of `CHANGELOG.md` for any user-visible change, in the same PR.

## Releases — NEVER publish

Releases run via `cargo-release` (`release.toml`). The publish is the **human's** job, always. **Never** run `cargo publish` or `cargo release --execute`; dry-run only. `release.toml` templates use `{{version}}`, NOT `{{next_version}}` (the latter renders as a literal in the pre-release commit context).

## More

See `CONTRIBUTING.md` for the dev loop, dependency policy, and code style.
