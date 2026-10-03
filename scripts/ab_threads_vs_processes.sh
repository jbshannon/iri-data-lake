#!/usr/bin/env bash
# A/B one process with N threads against N processes with one thread
# each, over the *same* total work (shards vs in-process workers).
#
# Usage: scripts/ab_threads_vs_processes.sh [rounds]
set -euo pipefail

ROUNDS=${1:-2}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN="$ROOT/target/release/iri-lake"
SCRATCH=${SCRATCH:-/tmp/lake-ab}
SCOPE=${SCOPE:---year 1}
SCOPE_LABEL=${SCOPE_LABEL:-year1}
NPAR=${NPAR:-4}
ORDER=${ORDER:-largest-first}

mkdir -p "$SCRATCH"
echo "run,round,mode,procs,threads,wall_s,total_raw_MiB_s"

run_threads() {
  local procs=$1 w=$2 round=$3
  rm -rf "$SCRATCH/t-$round"
  local start=$(python3 -c 'import time;print(time.time())')
  "$BIN" ingest-all --input "$ROOT/data/raw" --output-root "$SCRATCH/t-$round" \
    $SCOPE --workers "$w" --order "$ORDER" > "$SCRATCH/t-$round.log" 2>&1
  python3 - "$round" threads "$procs" "$w" "$start" <<'PY'
import re,sys,time
round,mode,procs,w,start=sys.argv[1:6]
log=open(f"/tmp/lake-ab/t-{round}.log").read()
m=re.search(r"wall=([0-9.]+)s",log); wall=float(m.group(1))
mi=re.search(r"raw=([0-9.]+) MiB/s",log); mib=float(mi.group(1)) if mi else 0.0
print(f"{mode},{round},{procs},{w},{wall:.2f},{mib:.1f}")
PY
  rm -rf "$SCRATCH/t-$round" "$SCRATCH/t-$round.log"
}

run_processes() {
  local procs=$1 round=$2
  rm -rf "$SCRATCH/p-$round"
  mkdir -p "$SCRATCH/p-$round"
  local start=$(python3 -c 'import time;print(time.time())')
  for ((i=0;i<procs;i++)); do
    "$BIN" ingest-all --input "$ROOT/data/raw" --output-root "$SCRATCH/p-$round" \
      $SCOPE --workers 1 --order "$ORDER" --shard "$i/$procs" \
      > "$SCRATCH/p-$round-$i.log" 2>&1 &
  done
  wait
  python3 - "$round" processes "$procs" "$start" <<'PY'
import re,sys,time
round,mode,procs,start=sys.argv[1:5]
wall=time.time()-float(start)
mib=sum(float(re.search(r"raw=([0-9.]+) MiB/s",open(f"/tmp/lake-ab/p-{round}-{i}.log").read()).group(1)) for i in range(int(procs)))
print(f"{mode},{round},{procs},1,{wall:.2f},{mib:.1f}")
PY
  rm -rf "$SCRATCH/p-$round" "$SCRATCH"/p-$round-*.log
}

for r in $(seq 1 "$ROUNDS"); do
  run_threads "$NPAR" "$NPAR" "$r"
  run_processes "$NPAR" "$r"
done
