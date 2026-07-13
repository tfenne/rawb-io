#!/usr/bin/env bash
# Model-check rawb-io's park/wake/latch protocols under loom.
#
# Two crates are rebuilt from checksum-verified crates.io sources (the exact
# releases Cargo.lock pins) with small patches, then supplied to the build via
# command-line cargo `[patch]` overrides:
#
#   * ringbuf — its single `core::sync::atomic` import is swapped for loom's
#     replicas, so the model checker sees the REAL ring index protocol (and
#     the caching wrappers) instead of a hand-written stand-in.
#
#   * loom — its `Notify::wait` (the primitive under `JoinHandle::join`) is
#     patched to tolerate a stray `Thread::unpark` aimed at a joining thread:
#     std's `join` is immune to park tokens, but loom 0.7.2 asserts and kills
#     the model. rawb-io legitimately unparks a thread that may already be
#     joining (an IO thread's exit wake racing a `Drop`). This is
#     tokio-rs/loom#249 (open since 2022, never fixed). The re-block here is
#     fit for these models — no joining thread in them parks again afterward —
#     but it swallows the park token std would preserve, so the std-faithful
#     upstream fix belongs on the unpark side (only unblock threads blocked in
#     `park`; bank the token otherwise, as shuttle does).
#
# Combined with `RUSTFLAGS="--cfg loom"` (which activates the `sync_shim`
# loom re-exports and the `loom_tests` module in src/lib.rs), the whole stack
# — this crate's protocol code AND the real ring index logic — runs under the
# model checker. Normal builds are untouched: no `--cfg loom`, no loom code,
# no patches. The lockfile is snapshotted and restored because applying the
# patches rewrites it for the duration of the run.
#
# Usage:
#   scripts/loom.sh              # run every loom model
#   scripts/loom.sh flush        # filter to matching model names
#   LOOM_MAX_PREEMPTIONS=3 scripts/loom.sh   # widen the schedule bound (default 2)
#   LOOM_MAX_PREEMPTIONS=0 scripts/loom.sh   # no bound: fully exhaustive
set -euo pipefail
cd "$(dirname "$0")/.."

CACHE="${CARGO_HOME:-$HOME/.cargo}/registry/cache"
DEST="target/loom"

# Read one field of one package out of Cargo.lock.
lock_field() { # <crate> <field>
    awk -v crate="$1" -v field="$2" '
        $0 == "name = \"" crate "\"" { in_pkg = 1; next }
        /^\[\[package\]\]/           { in_pkg = 0 }
        in_pkg && $1 == field        { gsub(/"/, "", $3); print $3; exit }' Cargo.lock
}

# Extract <crate>-<version> from the registry cache into $DEST, refusing any
# archive that does not match Cargo.lock's checksum — the patched sources must
# be byte-identical to what production builds compile, minus our patches.
vendor_crate() { # <crate> <version> <sha256>
    local name="$1" version="$2" sha="$3" crate_file
    # `|| true`: on a cold machine (fresh CI runner) the registry cache
    # directory doesn't exist yet, and under `set -euo pipefail` a bare
    # failing `find` would kill the script before the `cargo fetch` below
    # gets a chance to populate it.
    crate_file=$(find "$CACHE" -name "${name}-${version}.crate" 2>/dev/null | head -1 || true)
    if [[ -z "$crate_file" ]]; then
        cargo fetch --quiet
        crate_file=$(find "$CACHE" -name "${name}-${version}.crate" 2>/dev/null | head -1 || true)
    fi
    if [[ -z "$crate_file" ]]; then
        echo "error: ${name}-${version}.crate not in the registry cache even after cargo fetch" >&2
        exit 1
    fi
    if ! echo "${sha}  ${crate_file}" | shasum -a 256 --check --status; then
        echo "error: ${crate_file} does not match Cargo.lock's checksum" >&2
        exit 1
    fi
    tar -xzf "$crate_file" -C "$DEST"
}

RB_VERSION=$(lock_field ringbuf version)
RB_SHA=$(lock_field ringbuf checksum)
LOOM_VERSION=$(lock_field loom version)
LOOM_SHA=$(lock_field loom checksum)
if [[ -z "$RB_VERSION" || -z "$RB_SHA" || -z "$LOOM_VERSION" || -z "$LOOM_SHA" ]]; then
    echo "error: ringbuf/loom version or checksum not found in Cargo.lock" >&2
    exit 1
fi

rm -rf "$DEST" && mkdir -p "$DEST"
vendor_crate ringbuf "$RB_VERSION" "$RB_SHA"
vendor_crate loom "$LOOM_VERSION" "$LOOM_SHA"
PATCHED_RB="$DEST/ringbuf-$RB_VERSION"
PATCHED_LOOM="$DEST/loom-$LOOM_VERSION"

# ── ringbuf: swap its one atomics import for loom's replicas. ────────────────
perl -pi -e 's{^use core::sync::atomic::}{use loom::sync::atomic::}' "$PATCHED_RB/src/rb/shared.rs"

# Post-conditions: fail loudly if a future ringbuf moves its atomics and the
# swap no longer lands — a silently unpatched ring would model nothing.
# src/tests and src/benchmarks aren't compiled as a dependency, so they're
# exempt.
if ! grep -q '^use loom::sync::atomic::' "$PATCHED_RB/src/rb/shared.rs"; then
    echo "error: the atomics-import swap did not apply to src/rb/shared.rs" >&2
    exit 1
fi
if grep -rn --exclude-dir=tests --exclude-dir=benchmarks -E 'core::sync::atomic|std::sync::atomic' "$PATCHED_RB/src"; then
    echo "error: unpatched atomics remain in the ringbuf sources above" >&2
    exit 1
fi
printf '\n[dependencies.loom]\nversion = "%s"\n' "$LOOM_VERSION" >> "$PATCHED_RB/Cargo.toml"

# ── loom: make `Notify::wait` (join's primitive) ignore stray unparks. ───────
python3 - "$PATCHED_LOOM/src/rt/notify.rs" <<'PY'
import pathlib, sys

path = pathlib.Path(sys.argv[1])
src = path.read_text()
anchor = "        // Thread was notified\n        super::execution(|execution| {"
snippet = """        // PATCH(rawb-io scripts/loom.sh): a stray `Thread::unpark` aimed at a
        // thread blocked here (typically in `JoinHandle::join`) resumes it
        // without a pending notification, and the assert below then kills the
        // model. std's `join` is immune to park tokens, so match std: treat
        // that resume as spurious and re-block until actually notified.
        while !super::execution(|execution| self.state.get(&execution.objects).notified) {
            self.state.branch_acquire(true, location);
        }

"""
count = src.count(anchor)
assert count == 1, f"notify.rs anchor found {count} times, expected exactly 1"
path.write_text(src.replace(anchor, snippet + anchor))
PY
if ! grep -q 'PATCH(rawb-io scripts/loom.sh)' "$PATCHED_LOOM/src/rt/notify.rs"; then
    echo "error: the Notify::wait patch did not apply to loom's notify.rs" >&2
    exit 1
fi

# ── Run the models; restore the lockfile the patches rewrite. ────────────────
cp Cargo.lock "$DEST/Cargo.lock.pre-loom"
trap 'mv "$DEST/Cargo.lock.pre-loom" Cargo.lock' EXIT

run_models() {
    cargo --config "patch.crates-io.ringbuf.path=\"$PWD/$PATCHED_RB\"" \
        --config "patch.crates-io.loom.path=\"$PWD/$PATCHED_LOOM\"" \
        test --release --lib "$@"
}
if [[ "${LOOM_MAX_PREEMPTIONS:-2}" == "0" ]]; then
    # No bound: loom explores every schedule its model admits (exhaustive).
    # The variable must not reach loom itself — loom reads a literal 0 as
    # "zero preemptions", the smallest exploration instead of the largest.
    unset LOOM_MAX_PREEMPTIONS
    RUSTFLAGS="--cfg loom ${RUSTFLAGS:-}" run_models "$@"
else
    RUSTFLAGS="--cfg loom ${RUSTFLAGS:-}" \
        LOOM_MAX_PREEMPTIONS="${LOOM_MAX_PREEMPTIONS:-2}" run_models "$@"
fi
