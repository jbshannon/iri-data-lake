#!/usr/bin/env bash
# Measure `ingest-all` wall time across worker counts / scheduling
# strategies on a fixed scope, and emit one CSV row per run.
#
# Usage:
#   scripts/sweep_workers.sh <out.csv> [rounds]
#
# Scope is Year 1 (62 files, 10.31 GiB) unless SCOPE is exported.
# Each run gets a *fresh* output root so the manifest never skips a
# file, and every run is timed with the shell's clock around the
# process, so SHA-256 and discovery are inside the number.
set -euo pipefail

OUT=${1:-/tmp/sweep.csv}
ROUNDS=${2:-2}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN="$ROOT/target/release/iri-lake"
SCRATCH=${SCRATCH:-/tmp/lake-sweep}
SCOPE=${SCOPE:---year 1}
SCOPE_LABEL=${SCOPE_LABEL:-year1}

mkdir -p "$SCRATCH"
echo "run,round,scope,workers,order,wall_s,raw_MiB_s,rows_s,completed,skipped,failed,out_bytes,user_s,sys_s" > "$OUT"

# Configs are "workers order". 1 is the sequential baseline; the rest
# probe the scaling curve. Interleave rounds so thermal drift hits
# every config equally.
CONFIGS=(
  "1 largest-first"
  "2 largest-first"
  "4 largest-first"
  "6 largest-first"
  "8 largest-first"
  "12 largest-first"
  "8 striped"
  "8 smallest-first"
)

n=0
for round in $(seq 1 "$ROUNDS"); do
  for cfg in "${CONFIGS[@]}"; do
    read -r workers order <<<"$cfg"
    n=$((n + 1))
    lake="$SCRATCH/$SCOPE_LABEL-w$workers-$order-r$round"
    rm -rf "$lake"
    log="$SCRATCH/log-$SCOPE_LABEL-w$workers-$order-r$round.txt"
    # `time` on the pipeline gives us user/sys of the binary itself.
    { /usr/bin/time -p "$BIN" ingest-all --input "$ROOT/data/raw" \
        --output-root "$lake" $SCOPE \
        --workers "$workers" --order "$order" ; } > "$log" 2> "$log.time"
    wall=$(awk '/^real/{print $2}' "$log.time")
    user=$(awk '/^user/{print $2}' "$log.time")
    sys=$(awk '/^sys/{print $2}' "$log.time")
    summary=$(grep '^ingest-all done' "$log")
    completed=$(sed -n 's/.*completed=\([0-9]*\).*/\1/p' <<<"$summary")
    skipped=$(sed -n 's/.*skipped=\([0-9]*\).*/\1/p' <<<"$summary")
    failed=$(sed -n 's/.*failed=\([0-9]*\).*/\1/p' <<<"$summary")
    tp=$(grep '^  throughput' "$log" || true)
    raw=$(sed -n 's/.*raw=\([0-9.]*\) MiB\/s.*/\1/p' <<<"$tp")
    rows=$(sed -n 's/.*rows=\([0-9.]*\) M\/s.*/\1/p' <<<"$tp")
    outb=$(sed -n 's/.*bytes=\([0-9]*\) rows=.*/\1/p' <<<"$tp")
    echo "$n,$round,$SCOPE_LABEL,$workers,$order,$wall,${raw:-},${rows:-},${completed:-},${skipped:-},${failed:-},${outb:-},$user,$sys" | tee -a "$OUT"
    rm -rf "$lake"
  done
done
echo "wrote $OUT"
