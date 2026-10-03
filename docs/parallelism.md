# Parallel ingest (G1) — implementation and measurements

Status: **implemented, measured, one decision still open** (the failure
policy, now that it has a real instance to bite on).

`ingest-all` used to walk the file list in a `for` loop, one file at a
time, with `--workers` parsed and thrown away (`workers: _`). This
document records what replaced it, what was measured on the real
corpus, and which of the plausible designs lost.

Every number below was taken on this machine — Apple M1, 8 cores
(4P + 4E), 16 GiB RAM, APFS NVMe, corpus staged locally at
`~/.julia/dev/IRIData/data/IRI/Raw` (140.85 GiB / 744 files /
2 689 259 921 expected rows). Unless stated otherwise the scope is
**Year 1** (62 files, 10.09 GiB), because that is short enough
(20–75 s per configuration) to run every configuration twice, back to
back, interleaved — the only way to get numbers that survive the
±10–35 % drift `BENCHMARKING.md` warns about on this box.

## 1. What it does now

```bash
iri-lake ingest-all --input data/raw --output-root data/lake \
    --workers 8 --order smallest-first [--shard 2/4]
```

- **File-level rayon pool.** `ingest::ingest_all` runs
  `files.par_iter()` on a dedicated `ThreadPool` of `--workers`
  threads (`available_parallelism`, capped at 16, when unset).
- **One shared manifest.** `manifest::SharedManifest` opens
  `manifest.jsonl` once, loads it into a `HashMap<source_path,
  ManifestRecord>`, and serialises every append behind a short-lived
  mutex. This fixes both halves of what G6 described: appends are
  atomic by construction rather than by `O_APPEND` luck, and the
  O(records²) re-read per lookup is gone (the index is read once).
- **A truncated trailing record is a rejection, not a failure.** A
  source whose last record is incomplete is ingested up to the last
  aligned boundary; the partial record is counted in
  `rejected_rows` and warned about, and the record is a normal
  `success`. A file too short to hold a header is still an error.
- **Failures are recorded, not just logged.** `ingest_all` writes a
  `status: "failed"` manifest record for every source it could not
  ingest, carrying the error message, so `manifest.jsonl` is a
  complete account of what the lake contains *and* what it is
  missing. The run continues past them.
- **`--order`** picks how the work list is dealt out (below — this is
  not a cosmetic knob, it is worth 20 %).
- **`--shard IDX/N`** takes a byte-balanced slice of the corpus, for
  the multi-process shape that was measured and (mostly) rejected.

### Correctness

Not "the row counts matched" — **byte-identical output**:

| check | result |
|---|---|
| Year 1, 1 worker vs 4 workers | 232 Parquet files, 692 669 448 bytes both; every file's SHA-256 identical |
| partition layout | identical set of 62 partition dirs, same file counts per dir |
| manifest, 62 files / 8 workers | 62 lines, all `success`, one per source, no torn lines |
| re-run | 62 skipped, 0 records appended |

and pinned by `tests/parallel_ingest_tests.rs`, which asserts the
parallel lake is byte-identical to the sequential one, that the
manifest holds exactly one intact record per source, that a bad source
is isolated *and recorded*, that a truncated tail is counted rather
than fatal, and that `workers=0` is an error rather than a panic.

Per-file work is otherwise untouched: mmap → header check → batched
parse → Arrow → Parquet → atomic rename, one file at a time on one
thread. No record-level parallelism, as `ARCHITECTURE.md` intends.

## 2. Worker-count scaling (Year 1, 1 worker = 72.8 s)

Median of 2 interleaved rounds, largest-first ordering as shipped at
the time:

| workers | wall | speedup | aggregate raw MiB/s |
|---:|---:|---:|---:|
| 1 | 72.8 s | 1.00× | 141 |
| 2 | 54.0 s | 1.35× | 191 |
| 4 | 36.3 s | 2.01× | 285 |
| 6 | 38.8 s | 1.90× | 266 |
| 8 | 24.9 s | 2.92× | 415 |
| 12 | 28.7 s | 2.55× | 360 |

Six-to-twelve workers is a plateau: a dedicated sweep with the final
ordering (below) measured 6 / 8 / 10 / 12 workers at 21.6 / 22.2 /
23.2 / 22.7 s and again at 23.8 / 22.9 / 22.1 / 22.6 s — inside the
noise, in both rounds. **Oversubscription does not hurt, and it does
not help.** The default (`available_parallelism`) is the right default;
there is no reason for an operator to pass `--workers` at all on this
machine.

## 3. Ordering is worth 20 %, and the intuition is backwards

`ARCHITECTURE.md` and the old code both say "largest files first to
reduce tail latency". **Measured, that is the slowest option.** Rayon's
work-stealing deque hands work out from the *back* of the slice, so a
largest-first list keeps the 936 MB file at index 0 until the very end
of the run, when exactly one worker is left to take it.

Median of 2 rounds, Year 1:

| units | `largest-first` | `smallest-first` | `striped` |
|---|---:|---:|---:|
| 2 threads | 54.0 s | **39.2 s** | 53.9 s |
| 4 threads | 36.3 s | **24.6 s** | 36.5 s |
| 8 threads | 24.9 s | **20.3 s** | 24.7 s |
| 2 processes | **38.1 s** | 39.7 s | — |
| 4 processes | 26.3 s | **22.7 s** | — |
| 8 processes | **19.8 s** | 20.8 s | — |

`--order smallest-first` is now the default; `striped` (deal the N
largest files to N different workers up front) measures the same as
`largest-first` and is kept only because it is the intuitive way to
state what `smallest-first` actually achieves.

Note the asymmetry in the bottom half: for **processes** the ordering
barely matters, because each process owns a static, byte-balanced slice
and never re-balances. For **threads** it is the single biggest lever.
That is the same lesson as §5, one level down.

## 4. Threads vs processes — the shape the corpus plan asked about

`docs/corpus_readiness.md` §7 left this open: "file-level rayon pool
versus one process per machine per shard of years". Measured, on
identical total work (Year 1, 62 files, either all in one pool or
split across N byte-balanced shards writing to one output root):

| units | 1 process, N threads | N processes, 1 thread |
|---:|---:|---:|
| 2 | 39.2 s (smallest-first) | 38.1 s (largest-first) |
| 4 | 24.6 s | 22.7 s |
| 8 | **20.3 s** | 19.8 s |

**A tie at 8 units, within run-to-run noise.** Threads win on every
operational property — one process, one manifest write, one command to
resume, no shard bookkeeping, no "did all four shards finish?" — so
that is the default. `--shard` stays for the case the doc was really
asking about: several *machines*, where a byte-balanced shard is
exactly the right unit of work and there is no shared manifest to
contend for.

**The first version of this experiment said the opposite, and it was
wrong.** Splitting the file list into N equal-*count* contiguous shards
gives shard 0 (4 files, 3.13 GiB) three times the bytes of shard 1 (4
files, 2.04 GiB) on a corpus whose file sizes span 0.02–1.4 GiB. The
makespan was then set by shard 0 and processes "lost" 4-to-1
(54.3 s vs 36.7 s). `take_shard` now cuts on **cumulative bytes**, so
each shard lands within one file of 1/N of the work:

```
shard 0/4: 4 files  3.13 GiB      shard 2/4:  8 files  2.50 GiB
shard 1/4: 4 files  2.04 GiB      shard 3/4: 46 files  2.42 GiB
```

A file larger than 1/N is never split (that would mean splitting one
source across two processes, which the manifest and the output naming
both dislike); it goes whole into the first shard whose boundary it
crosses. That is the 3.13 vs 2.04 above, and it is the irreducible
part of the imbalance.

## 5. Why scaling stops at ~3.5×, and why that is not the pool's fault

The pipeline reaches **3.2–3.7× on 8 cores** depending on the run. The
obvious suspect — a shared lock, or the pool failing to use its
threads — is wrong on both counts:

- The only shared mutable state is one mutex held for a single ~700
  byte JSONL append. Measured cost: 743 appends inside a 271 s run.
- CPU utilisation goes *up* with worker count, not down: 1.0 cores at
  `workers=1`, 2.0 at 4, 3.8 at 8.
- The same machine scales a pure-compute control at **5.4×**: eight
  concurrent `shasum -a 256` over distinct 894 MiB files reaches
  1 544 MiB/s aggregate versus 284 MiB/s for one.

So the box will give 5.4× on a workload that is *only* SHA-256, and
`iri-lake` — which is ~44 % SHA-256 and ~56 % parse + Arrow +
Parquet encode/compress — gets 3.5×. The remaining half of the
pipeline is memory-subsystem bound: Parquet's dictionary/RLE encoding
and the Arrow builders stream far more bytes per row than SHA-256
touches, and the M1's 4P+4E arrangement means the "extra" 4 threads
are efficiency cores. **The parallelism is doing its job; the ceiling
is the pipeline's arithmetic intensity.** Beating it needs less work
per row (the single-pass-hash idea in `BENCHMARKING.md` § What is
left, worth ~10 % of self time), not more threads.

## 6. The full corpus, end to end

The number `docs/corpus_readiness.md` §6 gates the run schedule on.
Two full runs were done, hours apart, and **they disagree by 50 %** —
which is this machine, not the code, so both are reported:

```
$ RUST_LOG=iri_lake=info iri-lake ingest-all --input data/raw \
    --output-root /tmp/lake-full

ingest-all: 744 file(s) selected (140.85 GiB raw, ~2.689B rows) of 744 discovered
ingest-all done: completed=744 skipped=0 failed=0 workers=8 wall=406.12s
  throughput: raw=355.1 MiB/s rows=6.65 M/s out=9.40 GiB bytes=151236520079
              rows=2700651386 rejected_rows=1 slowest_file=25.5s
  note: 1 row(s) rejected as incomplete records (trailing bytes of a truncated source)
```

| | measured |
|---|---|
| wall | **271 s** (first run, 743/744 — the one truncated file still refused at the time) / **406 s** (second run, after ~2 h of continuous benchmarking) |
| aggregate | 530 MiB/s / 355 MiB/s raw; 9.93 / 6.65 M rows/s |
| output | 9.40 GiB of zstd Parquet (3 148 files) |
| rows written | **2 700 651 386**, of which `rejected_rows = 1` |
| failures | 0 — every discovered source is in the lake |
| stray `*.tmp` | 0 |

The 406 s run is not a regression: the same-session control (eight
concurrent `shasum -a 256` over distinct 894 MiB files) had fallen
from **1 544 MiB/s to 977 MiB/s** by the end of the benchmarking day,
and the slowest *single file* in the run went from 14.4 s to 25.5 s.
Normalising by that control gives 406 x (977/1544) ≈ **257 s**, which
agrees with the 271 s measured in a cooler session. **Quote ~270 s
with a ±50 % band, and re-measure on the day of the real run** — the
drift is a property of the hardware, and `BENCHMARKING.md` says so.

Single-threaded the same corpus takes ~17 min (72.8 s per 10.09 GiB
x 14), so the run is **~3.7x** faster than sequential, at the low end
of the 4–6x the extrapolation in `BENCHMARKING.md` guessed at, for the
reason in §5.

### Re-running it (the resume path)

```
ingest-all done: completed=0 skipped=744 failed=0 workers=8 wall=110.29s
```

110 s to skip everything. That is not free: `ingest_file` computes
SHA-256 *before* the skip decision, because the hash is what makes the
decision trustworthy. Restarting an interrupted run therefore costs
~35 % of a full run, spent re-hashing 141 GiB and discarding it. The
alternative (trust size + mtime) would make resume ~10x cheaper and
would weaken the guarantee from "these bytes" to "this inode". Left as
an open option in §8, not done.

## 7. A real corpus defect, and what was decided about it

The corpus contains one genuinely damaged source:

```
WARN ingest failed source=data/raw/Year12/soup/soup_groc_1687_1739
  error=RecordAlignment { size: 637922152, header: 57, record: 56, remainder: 55 }
```

`(size - 57) % 56 == 55`: the file's last record is **one byte short**.
`tail -c 120 | xxd` shows the final line ending `...NONE 0 0` — it
stops inside the last field, with no CRLF. 11 391 465 complete records
followed by a truncated one.

This is what Gate 2 (`validate --full` over every file) exists to
catch, and it is the first hard evidence for the "log and continue"
failure policy: had the run aborted, Gate 4 would have cost 4.5 min
and produced nothing.

**Decided and implemented — a truncated tail is a rejection, not a
failure.** `ingest_file` ingests every complete record, counts the
truncated tail in `rejected_rows`, warns loudly, and records the run
as a `success`. The full run therefore lands **744/744 files,
0 failures, 2 700 651 386 rows** — the inventory's 2 689 259 921 plus
the soup file's 11 391 465 complete rows, minus nothing. A file too
short to hold a header is still an error; that is a different failure
with no records in it at all.

**Decided and implemented — failures are recorded, not just logged.**
A source that genuinely cannot be ingested (a corrupt header, say) now
gets a `status: "failed"` manifest record carrying the error message,
so `manifest.jsonl` is a complete account of what the lake contains
*and* what it is missing. Previously the 743/744 run recorded its one
failure in stdout and nowhere else, and stdout is exactly what a
crashed or re-run job loses. A failed record never matches a skip, so
the source is still retried on the next run.

Both policies cost nothing on a healthy corpus and are pinned by
tests (`a_truncated_trailing_record_is_counted_not_fatal`,
`a_failed_source_is_recorded_in_the_manifest`).

## 8. Still open

1. **Hash before skip.** See §6: a resume costs a full SHA-256 pass
   (110 s on the finished corpus). A `size+mtime` fast path behind a
   `--trust-metadata` flag would make resume ~10x cheaper and weaken
   the idempotence guarantee from "these bytes" to "this inode". Not
   implemented; it is a product decision, not a performance one.
2. **Whether `--shard` earns its keep** now that the default is a
   single pool. It is ~20 lines and it is what a multi-machine run
   needs; keep as-is unless there is a reason to delete it.
3. **`ManifestStatus::InProgress` is still never written** (G5). A run
   killed mid-file leaves a `*.tmp` sibling and no record, which is
   correct-but-silent. Lower value now that failures *are* recorded.

## 9. Reproducing

```bash
scripts/sweep_workers.sh  /tmp/sweep.csv 2      # worker-count curve
scripts/sweep_matrix.sh   2                     # threads x processes x order
scripts/ab_threads_vs_processes.sh 3            # head-to-head, same work

# the headline run
rm -rf /tmp/lake-full
time ./target/release/iri-lake ingest-all \
  --input data/raw --output-root /tmp/lake-full

# the four-process shape
for i in 0 1 2 3; do
  ./target/release/iri-lake ingest-all --input data/raw \
    --output-root data/lake --workers 1 --shard $i/4 &
done; wait
```

Each sweep writes a fresh output root per run (the manifest's
idempotence guard would otherwise skip everything and measure nothing)
and times the process from outside, so SHA-256 and discovery are
inside the number.

**Always take the control.** Absolute throughput on this box drifts by
up to 50 % between sessions (§6), which is enough to make a 4-minute
run look like a regression. Eight concurrent `shasum -a 256` over
distinct files is the cheapest available normaliser:

```bash
FILES=( $(ls -S data/raw/Year1/*/*_groc_1114_1165 | head -8) )
s=$(python3 -c 'import time;print(time.time())')
for f in "${FILES[@]}"; do shasum -a 256 "$f" >/dev/null & done; wait
python3 -c "import time;print(8*936803617/1048576/(time.time()-$s),'MiB/s')"
```

1 544 MiB/s is a cool machine, 977 MiB/s is a machine that has been
benchmarking all afternoon.
