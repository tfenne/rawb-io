# Contributing to rawb-io

Thanks for your interest in rawb-io. This document covers the dev loop, code-style expectations, the release flow, and conventions for contributors and maintainers.

## Getting Started

**Prerequisites:**
- Rust stable, minimum version from `rust-toolchain.toml` / `Cargo.toml`'s `rust-version` field (currently **1.89**).
- [`cargo-nextest`][nextest] for the test runner used in CI.
- [`cargo-deny`][cargo-deny] for the supply-chain check (optional locally; CI runs it on every PR).

The test suite needs no external tools — everything runs in-process.

```sh
cargo build              # debug build
cargo build --release    # release build
```

[nextest]: https://nexte.st/
[cargo-deny]: https://embarkstudios.github.io/cargo-deny/

## Verification Checklist

Run all five before sending a PR. CI runs the same gates (plus a Miri job; see Testing).

```sh
cargo ci-fmt      # rustfmt --check
cargo ci-lint     # clippy --all-targets -D warnings
cargo ci-test     # nextest, --locked
cargo ci-doctest  # doctests (nextest does not run them)
cargo deny check  # licenses, advisories, bans, sources
```

The `ci-*` aliases live in `.cargo/config.toml`. If `cargo ci-fmt` fails, run `cargo fmt` and re-stage.

## Code Style

rawb-io follows the [Rust API Guidelines][rust-api] and a few project-local rules:

- **Idiomatic Rust.** Don't transliterate from C or Python; write Rust.
- **Names matter.** Prefer meaningful names even if longer. Short names are fine in closures and tight loops.
- **Small, focused functions.** Aim for code that makes sense when you come back to it in six months.
- **Doc comments on every public item.** Private items get doc comments when behavior is non-obvious. Comments should explain *why*, not *what*.
- **No premature abstraction.** Solve the problem in front of you.

[rust-api]: https://rust-lang.github.io/api-guidelines/

## The concurrency code is load-bearing

`src/lib.rs` encodes fixes for production deadlocks, shutdown races, and IO-thread panics (see `CLAUDE.md` for the full inventory). In particular: the write-once error latch; the waiter-slot wake protocol (register → re-check → park on the user side; change state → read slot → unpark on the IO side); the error-before-EOF publication order and `fill_buf`'s latch re-check after observing EOF; the `catch_unwind` panic boundary around each IO loop; the flush epoch handshake; and the up-front + post-park error re-checks in `WriteBehind::write`. These are invariants, not style choices. Changes on the IO path must preserve their exact behavior and keep the in-module tests green — those tests are the regression suite for the fixes. When in doubt, open an issue before refactoring.

## `unsafe`

The crate is `#![deny(unsafe_code)]` apart from two narrow `#[allow(unsafe_code)]` sites: the read loop (the `MaybeUninit` → `&mut [u8]` cast and its bounds-checked `advance_write_index`) and `zeroed_ring` (the one-time construction memset that keeps the cast sound by guaranteeing the ring's storage is always initialized). New `unsafe` is not accepted without a strong justification and a complete `SAFETY:` write-up; `clippy::undocumented_unsafe_blocks` is warn-level to keep the discipline. Prefer a safe alternative.

## Testing

- **In-module unit tests** (`#[cfg(test)] mod tests`) cover round-trips, error surfacing, deadlock/panic regressions, backpressure, shutdown routes, and thread naming. README examples compile as doctests via a `#[cfg(doctest)]` include hook.
- Generate test data in code; never commit data files.
- Name tests after the behavior they assert; prefer many small tests over table-driven ones.
- The re-entrant-write stress test honours a `REENTRANT_STRESS_ITERS=<n>` environment override — bump it (hundreds of thousands) when auditing a change to the write path locally. The panic-mask stress honours `PANIC_MASK_STRESS_ITERS=<n>` the same way for changes to the shutdown ordering.
- Long-running stress variants are `#[ignore]`d out of the default run; execute them with `cargo ci-soak`. CI runs them weekly (and on demand) via the Soak workflow, with a nextest terminate-after so a hang fails fast instead of stalling the job.
- CI interprets a curated subset of the suite under Miri — single-seed, plus 32 scheduling seeds on the cheapest concurrency tests so its data-race detector sees different interleavings (Miri interprets every memory access, so the big-payload and timing-sweep tests would take hours). It also runs the suite under ThreadSanitizer with an instrumented std, which watches every atomic including `ringbuf`'s internals. Both commands are in `.github/workflows/check.yml`; note the TSan run requires Linux (it segfaults at startup on macOS aarch64 hosts, even on empty tests).
- The loom models (`#[cfg(all(test, loom))] mod loom_tests` in `src/lib.rs`) model-check the park/wake/latch protocols across every admissible thread interleaving — including the stale-value reads C11 weak memory permits — with the real `ringbuf` index protocol in the loop: `scripts/loom.sh` rebuilds checksum-verified `ringbuf` and `loom` releases with two small patches (documented in the script's header) and points the build at them. CI runs the models exhaustively (no preemption bound) on every PR; run `scripts/loom.sh` locally after touching any protocol code, with `LOOM_MAX_PREEMPTIONS=0` for the exhaustive exploration.

## Adding or upgrading dependencies

rawb-io deliberately has a single runtime dependency (`ringbuf`) and no dev-dependencies. The one carve-out is `loom`, declared under `[target.'cfg(loom)'.dependencies]`: it compiles only when `scripts/loom.sh` sets `RUSTFLAGS="--cfg loom"` for model-checking builds and is never part of a normal build, `cargo test` included. A new direct dependency needs a clear justification in the PR and a `cargo deny check` pass; new licenses get added to `deny.toml`'s allow-list only after deliberate review (no copyleft).

## Pull Requests

- Keep PRs focused. Commit messages explain *why*; the "what" is in the diff.
- Each PR should include tests for the behavior it adds or changes.
- Update `CHANGELOG.md`'s `[Unreleased]` section with a one-line entry in the appropriate subsection (Added / Changed / Fixed / Removed).
- Every CI check must be green before merge.

## Releasing

Releases are cut with [cargo-release]. Configuration lives in `release.toml`. Publishing is the maintainer's job.

```sh
cargo release 0.1.0            # dry run — review what would change
cargo release 0.1.0 --execute  # bump, changelog, commit, tag, push, publish
```

After the push, create the GitHub release object pointing at the new tag (e.g. `gh release create v0.1.0 --notes-file ...`) using that version's CHANGELOG section as the notes.

[cargo-release]: https://github.com/crate-ci/cargo-release
