#!/usr/bin/env bash
# Sweep `ingest-all` across parallelism x mechanism x ordering.
#
#   mechanism: one process with N rayon threads, or N processes each
#              with one thread, byte-balanced shards.
#   order:     how the (largest-first-sorted) file list is dealt out.
#
# Usage: scripts/sweep_matrix.sh [rounds]
set -euo pipefail

ROUNDS=${1:-2}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN="$ROOT/target/release/iri-lake"
SCRATCH=${SCRATCH:-/tmp/lake-matrix}
SCOPE=${SCOPE:---year 1}
SCOPE_LABEL=${SCOPE_LABEL:-year1}
OUT=${OUT:-$SCRATCH/matrix.csv}

mkdir -p "$SCRATCH"
echo "run,round,scope,mechanism,units,order,wall_s,raw_MiB_s,completed,failed" > "$OUT"

BYTES=$(python3 - <<PY
import subprocess,re
out=subprocess.run(["$BIN","ingest-all","--input","$ROOT/data/raw","--output-root","/tmp/x"]+"""$SCOPE""".split()+["--dry-run"],capture_output=True,text=True).stdout
print(re.search(r"\(([0-9.]+) GiB raw",out).group(1))
PY
)
echo "scope=$SCOPE_LABEL selected=${BYTES} GiB"

one_threads() { # units order round
  local units=$1 order=$2 round=$3
  local lake="$SCRATCH/t-$units-$order-$round"
  rm -rf "$lake"
  local s=$(python3 -c 'import time;print(time.time())')
  "$BIN" ingest-all --input "$ROOT/data/raw" --output-root "$lake" $SCOPE \
    --workers "$units" --order "$order" > "$lake.log" 2>&1
  report "threads" "$units" "$order" "$round" "$s" "$(grep -o 'raw=[0-9.]*' "$lake.log" | head -1 | cut -d= -f2)"
  rm -rf "$lake" "$lake.log"
}

one_processes() { # units order round
  local units=$1 order=$2 round=$3
  local lake="$SCRATCH/p-$units-$order-$round"
  rm -rf "$lake"; mkdir -p "$lake"
  local s=$(python3 -c 'import time;print(time.time())')
  for ((i=0;i<units;i++)); do
    "$BIN" ingest-all --input "$ROOT/data/raw" --output-root "$lake" $SCOPE \
      --workers 1 --order "$order" --shard "$i/$units" > "$lake-$i.log" 2>&1 &
  done
  wait
  # Aggregate throughput must use the *makespan*, not the sum of
  # per-process rates: shards finish at different times.
  local mib=$(python3 -c "print(round($BYTES*1024/($(python3 -c 'import time;print(time.time())')-$s),1))")
  report "processes" "$units" "$order" "$round" "$s" "$mib"
  rm -rf "$lake" "$lake"-*.log
}

report() { # mechanism units order round start mib
  local mechanism=$1 units=$2 order=$3 round=$4 start=$5 mib=${6:-}
  local line
  line=$(python3 - "$round" "$mechanism" "$units" "$order" "$start" "$mib" "$SCOPE_LABEL" <<'PY'
import sys,time
round,mech,units,order,start,mib,scope=sys.argv[1:8]
wall=time.time()-float(start)
print(f"{mech},{round},{scope},{mech},{units},{order},{wall:.2f},{mib},{''},{''}")
PY
)
  echo "$line" >> "$OUT"
  echo "$line"
}

run=0
for round in $(seq 1 "$ROUNDS"); do
  for units in 2 4 8; do
    for order in largest-first smallest-first striped; do
      run=$((run+1)); one_threads "$units" "$order" "$round"
    done
  done
  for units in 2 4 8; do
    for order in largest-first smallest-first; do
      run=$((run+1)); one_processes "$units" "$order" "$round"
    done
  done
done
echo "wrote $OUT"
