#!/usr/bin/env bash
# Gate 2 of docs/corpus_readiness.md — validate every discovered source.
#
#   scripts/gate2_validate.sh [-j N] [-o OUTDIR]
#
# Runs `validate --full` over every file discovery would ingest, in
# parallel, and writes one report line per file plus the failure list.
# It does not write any lake output, so it is safe against the whole
# corpus before anything is ingested.
#
# The file list comes from `inventory --format paths`, i.e. from
# discovery.rs itself. Do not replace it with a `find | grep` pipeline:
# the skip list is 2 733 files across six reasons, and re-deriving it in
# the shell is how a gate ends up validating a different set than the
# one the run will ingest.
#
# Uses the release binary deliberately. `validate --full` reads one
# 56-byte record per syscall, so it is syscall-bound at ~94 MiB/s in a
# debug build too, but the release binary is still ~10x faster and a
# debug sweep would take hours.
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN="$ROOT/target/release/iri-lake"
INPUT="${INPUT:-$ROOT/data/raw}"
JOBS="${JOBS:-8}"
OUTDIR="${OUTDIR:-/tmp/gate2}"
LIST="$OUTDIR/files.txt"
REPORT="$OUTDIR/reports.txt"
FAILURES="$OUTDIR/failures.txt"

while getopts "j:o:" opt; do
  case "$opt" in
    j) JOBS="$OPTARG" ;;
    o) OUTDIR="$OPTARG" ;;
    *) echo "usage: $0 [-j N] [-o OUTDIR]" >&2; exit 2 ;;
  esac
done

if [ ! -x "$BIN" ]; then
  echo "$BIN not built. Run: cargo build --release" >&2
  exit 2
fi

# Absolute paths throughout, and `--input-root` passed to every command
# so identity parsing can relate a file to its root.
#
# This is not fussiness: `parse_identity` rejects any path that does not
# `starts_with(input_root)`, and `input_root` defaults to the *relative*
# `data/raw`. So `iri-lake validate /abs/path/to/file` fails with "file
# does not match eligible sales filename shape" unless you also pass
# --input-root. The relative-path form works by accident of both sides
# agreeing; this script should not depend on that.
INPUT="$(cd "$INPUT" && pwd)"

mkdir -p "$OUTDIR"
: > "$REPORT"
: > "$FAILURES"

"$BIN" inventory --input-root "$INPUT" --format paths > "$LIST"
n=$(wc -l < "$LIST" | tr -d ' ')
echo "gate2: $n discovered source files, ${JOBS}-way parallel, release binary"
echo "gate2: reports -> $REPORT   failures -> $FAILURES"

export BIN REPORT FAILURES INPUT
one() {
  f="$1"
  if out=$("$BIN" validate --input-root "$INPUT" "$f" --full 2>&1); then
    printf 'PASS %s\n' "$f" >> "$REPORT"
  else
    printf 'FAIL %s\n' "$f" >> "$FAILURES"
    {
      printf 'FAIL %s\n' "$f"
      printf '%s\n' "$out" | grep -v $'\033' || true
      printf '\n'
    } >> "$REPORT"
  fi
}
export -f one

start=$(date +%s)
# BSD xargs has no -a; redirect the list in instead.
xargs -P "$JOBS" -I {} bash -c 'one "$1"' _ {} < "$LIST"
elapsed=$(( $(date +%s) - start ))

passes=$(grep -c '^PASS ' "$REPORT" || true)
fails=$(grep -c '^FAIL ' "$FAILURES" || true)
echo
echo "gate2: ${passes} pass, ${fails} fail, ${elapsed}s"

if [ "$fails" -ne 0 ]; then
  echo
  echo "failures:"
  sed 's/^/  /' "$FAILURES"
  # Non-zero so this can gate a script. An expected failure still needs
  # a human decision, so do not treat a non-zero exit as "just a warning".
  exit 1
fi
