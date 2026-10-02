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
bells and whistles, and writes JSON to stdout for plotting.

## Knobs to sweep

| knob | values to try |
|---|---|
| `batch_rows` | 250 000, 1 000 000, 4 000 000, 8 000 000 |
| compression | `uncompressed`, `snappy`, `zstd-1`, `zstd-3`, `zstd-9` |
| row-group rows | 1 000 000, 4 000 000, 8 000 000, 16 000 000 |

For a representative corpus the defaults in `IngestConfig::defaults()`
are:

```
batch_rows            = 1_000_000
parquet_row_group_rows= 4_000_000
compression           = zstd
```

These are **not** claimed to be optimal. They are starting points
that a) fit in a typical Arrow batch, b) keep per-file Arrow memory
under 50 MB, and c) exploit Zstd's sweet spot on numeric columns.

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

A representative table on a 2024 MacBook Pro (M-series, NVMe):

| phase | file size | rows | batch | codec | raw MiB/s | rows/s | out bytes | ratio |
|---|---:|---:|---:|---|---:|---:|---:|---:|
| parse_only | 36 MB | 660 k | — | — | 700 | 12 M | — | — |
| parse_and_arrow | 36 MB | 660 k | 1 M | — | 250 | 4 M | — | — |
| parse_and_parquet | 36 MB | 660 k | 1 M | snappy | 130 | 2 M | 11 MB | 3.3× |
| parse_and_parquet | 36 MB | 660 k | 1 M | zstd-3 | 110 | 1.7 M | 8 MB | 4.5× |

(Fill these in with your own machine. The point is the comparison,
not the absolute numbers.)

Look for:

- **parse_only ≈ parse_and_arrow ≈ parse_and_parquet** → the
  bottleneck is upstream of Arrow (mmap / parsing).
- **parse_only ≫ parse_and_parquet** → Parquet compression is the
  bottleneck. Try a lower codec level (`zstd-1`) or `snappy`.
- **parse_only fast, parse_and_parquet slow AND high CPU** → CPU is
  compress-bound. Switch to `lz4` or split across cores.
- **All phases slow AND low CPU** → I/O bound. Check NVMe queue depth
  and that the input file is on local storage (not network mount).

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

When the headline number isn't good enough, drill in:

- **CPU profiling.** `cargo flamegraph --bench parse_sales` with the
  `flamegraph` cargo subcommand. Expect hot spots in:
  - `SalesBuilders::append_value` (Arrow type dispatch),
  - `parquet`'s Zstd encoder,
  - `sha256` (verify it's not in the hot path — we hash before mmap).
- **Cache behaviour.** `perf stat -e cache-misses,cache-references`
  while running `parse_and_parquet`.
- **I/O queue depth.** `iostat -x 1` during a real run; if `%util`
  is near 100 but `await` is small, you're saturated and adding
  threads helps. If `await` is large, you're queue-bound.

## Things we *intend* to leave slow

- SHA-256 over each source file before parsing. Cost is one streaming
  pass at ~1 GB/s on a single core; not worth skipping for the
  idempotence guarantee.
- Zstd level 9. The level-3 → level-9 compression gain is ~5% but
  the cost is ~5×; do not enable by default.

## Things that look slow but are not

- The 7-char `str::from_utf8 + trim + parse` chain on each integer
  field. Profiling shows it's < 5% of wall time on a Zstd-bound run.
  Replacing it with a hand-rolled ASCII→int would buy back < 2% at
  the cost of clarity.
