# Benchmarking

The full IRI sales corpus is ~143 GB raw / 2.7 B rows / 744 files,
with the largest individual file at ~1.3 GB / 220 M rows. Optimising
that without measurements is guessing. This document describes a
**disciplined protocol** for profiling `iri-lake` and choosing the
right knobs.

## Goals

The goals are stated in priority order:

1. **Make the correct decision on what to optimise.** Don't optimise
   parsing if the bottleneck is Parquet compression. Don't optimise
   compression if the bottleneck is NVMe write bandwidth.
2. **Reach a stable, repeatable measurement protocol.** Every number
   in this document should be reproducible with the same command on
   the same machine.
3. **Pick sensible defaults for a local NVMe machine** that we can
   recommend to future operators without them having to re-benchmark
   from scratch.

## Methodology

The pipeline decomposes into three phases; **never** measure them
together on the first pass.

| phase | what is timed |
|---|---|
| **parse_only** | mmap + slice + builders, no Arrow finish, no Parquet write |
| **parse_and_arrow** | parse + `RecordBatch::try_new`, no Parquet write |
| **parse_and_parquet** | parse + finish + write + atomic rename |

`iri-lake` ships a Criterion bench under `benches/parse_sales.rs` that
exposes all three as separate `BenchmarkId`s:

```bash
# Tiny in-memory fixture (10k rows, default codec)
cargo bench --bench parse_sales

# Real file (the design point is fzdinent_groc ≈ 1.3 GB / 220 M rows).
BENCH_FILE=data/raw/Year11/fzdinent/fzdinent_groc_1635_1686 \
  cargo bench --bench parse_sales

# Sweep the batch-row knob.
BENCH_FILE=data/raw/Year1/beer/beer_drug_1114_1165 \
  BENCH_BATCH=250000 \
  cargo bench --bench parse_sales

# Sweep the compression codec.
BENCH_FILE=data/raw/Year1/beer/beer_drug_1114_1165 \
  BENCH_COMPRESSION=zstd-3 \
  cargo bench --bench parse_sales
```

`iri-lake benchmark <path>` is the CLI mirror — same idea, fewer
bells and whistles, and writes JSON to stdout for plotting. Each repeat
runs against a fresh scratch output root so the manifest's idempotence
guard can't silently skip repeats 2..N; the report carries a
median/best/worst summary because repeat 0 always pays for a cold page
cache (~3% slower in our measurements).

`cargo run --release --example compression_sweep -- <file>` sweeps the
codec axis specifically. It reports write time, output size and — the
part that matters for a lakehouse — **decode** time for each codec.

## Knobs to sweep

| knob | values to try |
|---|---|
| `batch_rows` | 250 000, 1 000 000, 4 000 000, 8 000 000 |
| compression | `uncompressed`, `snappy`, `lz4`, `lz4_raw`, `zstd-1`, `zstd-3`, `zstd-9` |
| row-group rows | 1 000 000, 4 000 000, 8 000 000, 16 000 000 |

`gzip` was removed as an option. It was accepted by `IngestConfig` but
the `parquet` crate is built without its `flate2` feature, so it failed
at write time with `Disabled feature at compile time: flate2` — a panic
mid-write rather than a usable default. It now falls through to the
unknown-codec path and warns. Do not re-add it without enabling
`flate2` in `Cargo.toml` **and** re-measuring: it is slower than zstd at
every level we tested and has no offsetting advantage.

For a representative corpus the defaults in `IngestConfig::defaults()`
are:

```
batch_rows            = 1_000_000
parquet_row_group_rows= 4_000_000
compression           = zstd
```

These are **not** claimed to be optimal. The codec default has since been
*confirmed* by measurement (see below); `batch_rows` remains a starting
point that a) fits in a typical Arrow batch, b) keeps per-file Arrow
memory under 50 MB, and c) happens to be a good size/throughput
compromise.

## What to measure

For each phase and each knob combination, record:

| metric | how |
|---|---|
| raw MiB / s | `(file_size / 2^20) / wall_seconds` |
| rows / s   | `row_count / wall_seconds` |
| CPU util   | `ps -o %cpu` or `top -b -d 1` sampled during run |
| peak RSS   | `/usr/bin/time -v` peak resident set size |
| output bytes | `stat -c %s` the resulting `.parquet` file |
| compression ratio | `input_bytes / output_bytes` |
| scan latency | DuckDB `SELECT count(*) FROM read_parquet(...)` |

The crate's `IngestStats` already exposes rows/s, raw MiB/s, and
compression ratio. Peak RSS is **not** something we pretend to know;
report the configured batch capacity as a known allocator footprint
instead.

## Reading the numbers

### Measured phase split

Apple M1 (8 cores, 16 GiB, APFS NVMe), `beer_drug_1114_1165`
(35.3 MiB / 660 096 rows), single-threaded, hot cache, Criterion
`benches/parse_sales.rs`:

| phase | time | Mrows/s | raw MiB/s | share |
|---|---:|---:|---:|---:|
| parse_only | 81.4 ms | 8.11 | 433 | 52 % |
| parse_and_arrow | 81.3 ms | 8.12 | 434 | — |
| parse_and_parquet | 157.4 ms | 4.19 | 224 | 48 % |

Read this as: **Arrow `RecordBatch::try_new` is free** (−0.1 %, inside
noise), and Parquet encode+compress is 76.0 ms of a 157.4 ms run. The
bottleneck is split almost exactly in half between parsing and Parquet,
so neither phase alone justifies a rewrite. Optimise Parquet only if you
also raise `worker_threads`; optimise the parser only if you have
already made the writer free.

### Measured codec sweep

`compression_sweep` on `toothpa_groc_1114_1165` (304.0 MiB /
5 691 705 rows), median of 3, production batch/row-group defaults:

| codec | write MiB/s | decode ms | size MiB | B/row |
|---|---:|---:|---:|---:|
| uncompressed | 563.0 | 108.8 | 27.26 | 5.02 |
| snappy | 523.3 | 124.6 | 22.17 | 4.08 |
| lz4 | 519.6 | 115.7 | 22.19 | 4.09 |
| lz4_raw | 520.4 | 117.5 | 22.19 | 4.09 |
| **zstd** (= `zstd-1`) | **500.0** | 138.8 | **18.82** | **3.47** |
| zstd-3 | 460.4 | 142.1 | 17.87 | 3.29 |
| zstd-9 | 283.1 | 141.7 | 17.49 | 3.22 |

Three things this settles:

1. **The codec is nearly free; the encoding is the whole story.** zstd-1
   costs 11 % write throughput versus *uncompressed* (500 vs 563 MiB/s)
   and buys 31 % smaller files. Most of the compression ratio comes from
   Parquet's dictionary + RLE/bit-packing encoding layer, which is always
   on — a truly `UNCOMPRESSED` file still measures 5.02 B/row, verified
   by reading the footer codec. **Never report the headline ratio as
   "compression"; most of it is encoding.**
2. **`lz4` buys nothing over `snappy`** — identical sizes to two decimal
   places, marginally faster write, ~7 % slower decode.
3. **`zstd` (the default) means level 1**, not level 3 — `zstd` and
   `zstd-1` produce byte-identical output. Don't assume otherwise.

### Measured file-size scaling

End-to-end `iri-lake benchmark`, median of 3, warm cache. "wall" is total
process time including SHA-256; "ingest" is the `IngestStats` timer,
which starts after hashing.

| file | MiB | rows | ingest MiB/s | wall MiB/s | wall Mrows/s |
|---|---:|---:|---:|---:|---:|
| beer_drug | 35.3 | 660 k | 222 | 133 | 2.48 |
| shamp_drug | 79.1 | 1.48 M | 220 | 132 | 2.47 |
| toothpa_groc | 304.0 | 5.69 M | 233 | 136 | 2.55 |
| fzdinent_groc | 765.3 | 14.3 M | 227 | 137 | 2.56 |

Throughput is flat from 35 MiB to 765 MiB — this is a **per-row** bound,
not a per-file or per-batch overhead, so there is nothing to gain from
batching fewer, larger files.

### Whole-corpus extrapolation

Corpus is 744 files / 140.85 GiB / 2 689 259 921 rows (`inventory`).

| basis | rate | time |
|---|---:|---:|
| by bytes | 128.4 MiB/s wall | 1 123 s |
| by rows | 4.4 Mrows/s wall | ~1 100 s |

> **≈ 19 minutes single-threaded**, producing ≈ 8.5 GiB of Parquet.

Both bases agree to ~2 %, so the estimate is not sensitive to the
row-weighting of the corpus. It is a **lower bound**: `ingest-all`
defaults to `worker_threads = num_cpus()`, which this sweep never
exercised.

**Update (G1, now implemented and measured).** The inference below was
right, and the measured full-corpus run at `--workers 8
--order smallest-first` came in at **~220 s** — two forced
`--overwrite` runs measured 221.10 s and 220.29 s, agreeing to 0.4 %
with the same-session `shasum` control flat at 1 443–1 475 MiB/s
throughout. (The 271 s / 406 s pair quoted in earlier drafts was a hot
machine: its control had fallen from 1 544 to 977 MiB/s over the same
period, and normalising the second run against it gives 257 s.) The run
lands **744/744 files, 0 failures, 1 rejected row, 2 700 651 386 rows,
9.40 GiB of Parquet from 140.85 GiB raw (15.0x, of which ~3/4 is
Parquet's encoding layer rather than zstd)**. Against a ~17 min
single-threaded baseline that is **~4.6x**. The four-to-six-x range this
section guessed at was optimistic because it assumed the CPU would scale
linearly; it does not, and [`docs/parallelism.md`](docs/parallelism.md)
§5 shows why: eight concurrent `shasum` processes reach 5.4x on this
M1, while the full pipeline reaches 3.5x because Parquet's encoding
layer and the Arrow builders are memory-bound where SHA-256 is not.

| workers | wall (Year-1 scope) | speedup |
|---:|---:|---:|
| 1 | 72.8 s | 1.00x |
| 2 | 39.2 s | 1.86x |
| 4 | 24.6 s | 2.96x |
| 8 | 20.3 s | 3.59x |
| 12 | ~22.7 s | ~3.2x |

Two results from that work are worth carrying into any future
benchmark: **the work-list order is worth 20 %** (smallest-file-first
beats the largest-first order this section's design notes used to
recommend, because rayon hands work out from the back of the deque),
and **a resume costs a full SHA-256 pass** — re-issuing the command
against the finished 744-file lake took 110 s to skip all 744.

Finally: the whole measurement session drifted by up to 50 % in
absolute terms, so **every table here should be read as A/B-within-one-
session only**, and any cross-session comparison should be normalised
against the `shasum` control in
[`docs/parallelism.md`](docs/parallelism.md) §9. That control is
cheaper than a corpus run and caught a "50 % regression" that was
entirely the machine.

**Run the whole sweep in one session.** Absolute throughput drifts
between sessions by up to ~35% on this machine (thermal state, page
cache), enough to move this figure between 18 and 24 minutes for the
*same* code. Only A/B comparisons taken back to back are trustworthy —
which is why the parser optimisation above quotes both sides from the
same session.

`batch_rows` was swept on the 765 MiB file and moves throughput by <1 %
(noise) while moving output size by ~9 %:

| batch_rows | MiB/s | out MiB | ratio |
|---:|---:|---:|---:|
| 250 k | 230.5 | 44.27 | 17.29× |
| 1 M (default) | 233.0 | 46.07 | 16.61× |
| 4 M | 231.2 | 48.37 | 15.82× |
| 8 M | 231.1 | 48.37 | 15.82× |

(4 M and 8 M are identical because `parquet_row_group_rows` defaults to
4 M and caps the row group regardless of batch size.) If storage matters
more than RAM, 250 k buys 9 % smaller files for no throughput cost.

Look for:

- **parse_only ≈ parse_and_arrow ≈ parse_and_parquet** → the
  bottleneck is upstream of Arrow (mmap / parsing).
- **parse_only ≫ parse_and_parquet** → Parquet compression is the
  bottleneck. Try a lower codec level (`zstd-1`) or `snappy`.
- **parse_only fast, parse_and_parquet slow AND high CPU** → CPU is
  compress-bound. Switch to `lz4` or split across cores.
- **All phases slow AND low CPU** → I/O bound. Check NVMe queue depth
  and that the input file is on local storage (not network mount).

## Rejected: writing Arrow IPC (`.arrow` / Feather) instead of Parquet

Worth recording so nobody re-litigates it from first principles.
Streaming the `RecordBatch`es straight to an Arrow IPC file skips the
Parquet encode+compress entirely. Measured on the same parsed batch:

| format | write ms | output | B/row |
|---|---:|---:|---:|
| Arrow IPC | **6.7** | 19.83 MiB | 31.5 |
| Parquet (zstd-3) | 80.3 | 2.43 MiB | 3.86 |
| Parquet (uncompressed) | 65.9 | 3.76 MiB | 5.97 |

*(`beer_drug_1114_1165`, 660 096 rows.)*

The write step really is ~12× faster, and it is *not* a good trade:

- **8.2× the bytes.** 31.5 B/row vs 3.86. Extrapolated to the corpus
  that is **~79 GiB of IPC vs ~8.5 GiB of Parquet** for identical data.
- **The end-to-end win is only 1.82×**, because parsing is unchanged
  and is now the larger half. On cold storage the win shrinks further,
  since IPC becomes I/O-bound pushing 8× more bytes.
- **It breaks the product.** DuckDB cannot read Arrow IPC, so every
  scan-validation query below, hive partitioning, predicate pushdown
  and row-group sizing stop working.

If ingest throughput ever has to be maximised, raising
`worker_threads` is the better lever: it helps both halves and keeps
Parquet.

## Harness traps

Both of these cost real time to diagnose; check here first.

- **`IngestConfig::for_test()` in a benchmark measures the wrong thing.**
  It sets `parquet_row_group_rows = 1024`, so a "benchmark" wrote one
  row group per 1024 rows — 645 tiny row groups for `beer_drug` instead
  of one. It inflated `parse_and_parquet` by **33 %** (236 ms → 157 ms
  once fixed) and made it look as though Parquet cost far more than it
  does. `benches/parse_sales.rs` now starts from `IngestConfig::defaults()`
  and only overrides what it intends to sweep. `for_test()` remains
  correct for unit tests, where tiny row groups are harmless.
- **The manifest's idempotence guard silently eats benchmark repeats.**
  `ingest_file` returns `Skipped` when a prior success matches the
  source hash and config, so `--repeats 3` used to measure once and emit
  two `{"skipped": true}` entries that looked like data. `run_benchmark`
  now gives each repeat a fresh scratch output root and treats `Skipped`
  as a hard error. Any future benchmark that shells out to `ingest_file`
  needs the same isolation.

## Scan validation with DuckDB

A good "did the ingestion preserve semantics?" check:

```sql
-- Schema sanity
DESCRIBE SELECT * FROM read_parquet(
    'data/lake/bronze/iri_sales/year=1/category=beer/channel=drug/part-*.parquet'
);

-- Row count must equal (file_size - 57) / 56 for the source file.
SELECT count(*) FROM read_parquet(
    'data/lake/bronze/iri_sales/year=1/category=beer/channel=drug/part-*.parquet'
);

-- Money: total dollars per category must match the raw total computed
-- offline in Python.
SELECT category, channel, sum(dollars_cents) / 100.0 AS total_dollars
FROM read_parquet('data/lake/bronze/iri_sales/**/*.parquet', hive_partitioning=true)
GROUP BY category, channel
ORDER BY total_dollars DESC
LIMIT 10;

-- Feature-code distribution must be plausibly close to the empirical
-- sample in docs/data_layout.md §3.2 (77.8% NONE, 20.2% B, 2.0% A,
-- 0.03% A+, <0.01% C for beer_drug).
SELECT feature_code, count(*) AS n
FROM read_parquet('data/lake/bronze/iri_sales/year=12/category=beer/channel=drug/part-*.parquet')
GROUP BY feature_code
ORDER BY n DESC;

-- Scan throughput (rows/s on a single DuckDB thread):
.timer on
SELECT count(*), sum(units), sum(dollars_cents)
FROM read_parquet('data/lake/bronze/iri_sales/**/*.parquet', hive_partitioning=true);
```

The scan query gives you the downstream cost of the encoding
decisions. A "slow scan" finding usually points at:

- too many small Parquet files (raise `batch_rows`),
- missing Zstd dictionary encoding on low-cardinality columns
  (currently `category` and `channel` rely on Parquet's automatic
  dictionary — check with `parquet-tools meta` or DuckDB's
  `EXPLAIN`),
- row-group sizing (a row group too small hurts predicate pushdown).

## Profiling notes

### Why `cargo flamegraph --bench parse_sales` does not work

Two independent reasons, both of which cost time to discover:

1. **The binary has no symbols.** `strip = "symbols"` in
   `[profile.release]` is inherited by `[profile.bench]`, so the
   benchmark binary ships with zero DWARF sections and exactly one
   function symbol (`_main`). Anything sampling it produces
   unattributed stacks.
2. **It profiles the wrong program.** `benches/parse_sales.rs` never
   calls `sha256_of_file` and loads input with `fs::read` into a `Vec`,
   not `Mmap` — it does not exercise the path production runs.

Use the `profiling` profile (`Cargo.toml`), which sets `debug = 1` and
`strip = "none"`, and profile the real binary:

```bash
RUSTFLAGS="-C force-frame-pointers=yes" \
  cargo build --profile profiling --bin iri-lake

./target/profiling/iri-lake benchmark <file> --repeats 8 &
sample $(pgrep -x iri-lake) 20 -file /tmp/sample.txt
inferno-collapse-sample /tmp/sample.txt > /tmp/folded.txt
inferno-flamegraph  /tmp/folded.txt > /tmp/fg.svg
```

Two macOS caveats:

- `cargo flamegraph` itself needs `xctrace`, which requires **full
  Xcode** (only Command Line Tools are needed here). `sample` + `inferno`
  is the dependency-light equivalent and needs no `sudo`.
- `force-frame-pointers` is a rustc flag, not a Cargo profile key
  (Cargo warns `unused manifest key` if you add it to `Cargo.toml`).
  It is worth setting: frame pointers make stacks unwind correctly.

### Do not trust `inferno-collapse-sample` percentages

It dropped 43 % of samples on our run (8 882 of 15 646), so every
share computed from its output is inflated by ~1.76×. On our run it
reported `sha2` at 79.8 % when the true figure is 45.3 %. Read the
per-frame weights out of `sample.txt`'s own `Sort by top of stack`
section (weights are trailing, not leading) and divide by the thread
total from the call-graph root. Cross-check against wall clock: if the
profile disagrees with `outer_elapsed - inner_elapsed` by more than a
few points, the profile is wrong, not the timer.

### Measured hot spots (before the parser optimisation below)

`fzdinent_groc_1114_1165` (765 MiB / 14.3 M rows), 8 repeats, 20 s
sample, 15 646 samples, self time:

| region | share |
|---|---:|
| `sha2::sha256::compress256` | 45.3 % |
| parser + Arrow builders (`parse_records_into_builder`, `trim_matches`, `from_utf8`, `parse_uint`) | 26.4 % |
| Parquet encode/write (`arrow_writer::write_primitive`, `RleEncoder`, `LevelInfoBuilder`) | 23.4 % |
| zstd codec (`ZSTD_compressBlock_fast`, `HUF_*`, `FSE_*`) | 2.5 % |
| syscalls (`read`, `write`) | 1.5 % |
| memcpy/memset | 0.3 % |

Call-site totals from the same profile agree with wall clock:
`sha256_of_file` at `ingest.rs:171` accounts for 46.2 % of samples and
the parse/Parquet region at `ingest.rs:290` for 53.8 %, versus 42.7 %
measured from the outer-minus-inner timer delta.

**SHA-256 is the single hottest function in the pipeline** — the
opposite of the old claim that it needed verifying because "we hash
before mmap". `sha256_of_file` runs before the `IngestStats` timer
starts, so no metric this crate reports will ever show it.

**The zstd codec itself is cheap (2.5 %).** Parquet's *encoding* layer —
dictionary/RLE/bit-packing — costs 23.4 %, an order of magnitude more
than the compression that runs on top of it. This is the concrete form
of the finding in § Measured codec sweep: optimising the codec is
optimising the wrong 2.5 %.

**`trim_matches` (8.2 %) + `from_utf8` (5.7 %) + `parse_uint` (4.7 %)
total ~18.6 % of self time** — the third-largest region, and larger
than Parquet's entire encoding layer. This directly contradicted the
long-standing claim in § Things that look slow but are not that the
`str::from_utf8 + trim + parse` chain is "< 5 % of wall time" and not
worth touching. That claim has since been acted on — see below.

### Other tools

- **Cache behaviour.** `perf stat -e cache-misses` is **Linux-only** and
  unavailable here. Use `xctrace` (needs full Xcode) or reason from the
  syscall share instead.
- **I/O queue depth.** `iostat -x 1` during a real run; if `%util` is
  near 100 but `await` is small, you're saturated and adding threads
  helps. If `await` is large, you're queue-bound. Note the measured
  syscall share is only 1.5 %, so I/O is not the constraint on warm
  data — consistent with the 0.06 s re-read noted above.

## Things we *intend* to leave slow

*(Corrected 2026-10: the ASCII claim previously filed here as a
non-issue measured ~18.6 %, not "< 5 %". It was rewritten rather than
dismissed — see § Parser optimisation. The zstd entry remains valid.)*

- **Zstd level 9.** Measured on `toothpa_groc`: 1.8× slower to write
  than `zstd-1` (283 vs 500 MiB/s) for 1.4 % smaller files, and *no*
  faster to decode (141.7 vs 138.8 ms). It loses on both axes. Note
  that `zstd-3` is also a poor trade — 8 % slower write for 5 % smaller
  files. The default `zstd` (level 1) is the sweet spot.
- **The 7-char `str::from_utf8 + trim + parse` chain** (see below).

### SHA-256 is *not* one of these — it is the largest single cost

The older estimate of "~1 GB/s on a single core" was wrong by ~3×.
Measured with `shasum -a 256` on the 765 MiB `fzdinent_groc`:

| stage | time | MiB/s | % of wall |
|---|---:|---:|---:|
| read file (warm) | 0.06 s | 12 700 | 1 % |
| **SHA-256** | **2.48 s** | **308** | **44 %** |
| parse + Arrow + Parquet | 3.37 s | 227 | 55 % |

`IngestStats::elapsed` starts *after* hashing (`ingest.rs`), so the
`raw_MiB_s` / `rows_s` fields it reports **exclude** SHA-256 and manifest
I/O. Comparing them against wall-clock is a reliable way to convince
yourself hashing is free. It is not: it is the single largest item in
the pipeline, and the ingest is still compute-bound long after disk I/O
stops mattering (0.06 s to re-read the file).

A sampling profile independently confirms this: `sha2::sha256::
compress256` is **45.3 %** of self time, and the `sha256_of_file` call
site accounts for 46.2 % of samples, against 42.7 % from the wall-clock
delta. The other stages are ~54 %.

If ingest throughput ever becomes the goal, dropping or parallelising
the hash is the single biggest win available — it would roughly halve
end-to-end time. We keep it because the idempotence guarantee is worth
more than 2× on a job that runs once per corpus, but that is a product
decision, not a performance one, and should be re-litigated if the
guarantee is ever in question.

## Things that look slow but are not

- The Parquet **zstd codec** specifically (2.5 %). Compression is the
  cheap part of the writer; its encoding layer is 23.4 %.

- ~~The 7-char `str::from_utf8 + trim + parse` chain~~ — **no longer
  true.** This used to be listed here, dismissed as "< 5 % of wall
  time ... would buy back < 2 %". It measured ~18.6 %, and the rewrite
  in § Parser optimisation made it roughly twice as fast. The lesson
  generalises: two prior estimates of this path were made without ever
  profiling it, and both were wrong. Do not dismiss a hot path from
  intuition.

## Parser optimisation

`parse_uint` used to run `from_utf8` → `str::trim` → `FromStr` on every
integer field — 8 calls per row. Both steps were wasted work on this
format: `from_utf8` validated UTF-8 on fields that are ASCII digits by
construction, and `str::trim` applied *Unicode* whitespace rules through
`char::is_whitespace` to space-padded (0x20) ASCII.

`parse_int64` now accumulates digits straight from the bytes, skipping
ASCII spaces and checking overflow once rather than twice per digit.
Non-UTF-8 bytes now surface as "non-digit" rather than "non-utf8"; a
fixed-width ASCII format has no legitimate non-UTF-8 field, so the
distinction is not worth a validation pass over every row.

### Result

Criterion `parse_only`, `toothpa_groc_1114_1165` (5.69 M rows):

| | time | throughput |
|---|---:|---:|
| before | 902.60 ms | 6.31 Mrows/s |
| after | 451.26 ms | 12.61 Mrows/s |
| | **−50.0 %** | **+100 %** |

End-to-end `iri-lake benchmark`, `fzdinent_groc_1114_1165`, median of
12 repeats, best of 2 runs each:

| | ingest median | wall median | wall MiB/s |
|---|---:|---:|---:|
| before | 4.045 s | 7.605 s | ~101 |
| after | **2.900 s** | **5.960 s** | **~128** |
| | −28 % | −22 % | +28 % |

Wall improves less than the ingest phase because ~2.4 s of every
repeat is SHA-256, which this change does not touch.

**Correctness was verified by output, not by test count alone:** the
six Parquet files written from `toothpa_groc_1114_1165` (5.69 M rows,
11 columns) are byte-for-byte identical before and after. Unit tests
also cover padding, signs, range checks, interior blanks, non-ASCII
bytes and overflow.

### What is left

Re-profiling after the rewrite (13 305 samples) moves the parser from
26.4 % to 17.8 % of self time and eliminates `trim_matches` and
`is_whitespace` entirely. The new distribution:

| region | share |
|---|---:|
| `sha2::sha256::compress256` | 40.0 % |
| Parquet encode/write | 26.3 % |
| parser (`parse_int64` 9.4 %, `parse_records_into_builder` 6.7 %) | 17.8 % |
| syscalls (`read` 10.2 %, `write` 1.3 %) | 11.5 % |
| zstd codec | 2.8 % |

Two opportunities remain, both larger than anything else left in the
parser:

1. **Single-pass hashing (~10 %).** `ingest_file` streams the file
   through `sha256_of_file` with a 64 KiB `BufReader` and *then* mmaps
   it again to parse, so the data crosses the syscall boundary twice;
   `read` alone is 10.2 % of self time. Mmap first and hash straight
   out of the mapping removes one full pass and nearly all of that
   syscall cost. This is the largest remaining win available and is
   behaviour-preserving.
2. **Parquet's encoding layer (26.3 %).** `arrow_writer::write_primitive`
   (dictionary encoding) is 10.7 % on its own, larger than the entire
   zstd codec. Adjusting `row_group_rows` or enabling dictionary
   fallback is cheaper to try than anything in the parser.
