# `iri-lake` — IRI weekly store-sales to Parquet lakehouse

A high-throughput Rust CLI that discovers IRI store-level weekly sales files,
parses their invariant fixed-width records, and writes typed Apache Parquet
files into a Hive-style partitioned lakehouse.

```
data/raw/    ──►   iri-lake ingest   ──►   data/lake/bronze/iri_sales/
                                                year=12/category=toothpa/channel=drug/
                                                    part-<run>-0001.parquet
                                                …metadata/manifest.jsonl
```

## Scope

`data/raw/` is not one data source. Alongside the store-level sales
files it holds **ten more structurally distinct classes**, and this
tool now ingests all of them:

| dataset | files | rows | source format |
|---|---:|---:|---|
| `iri_sales` | 744 | 2.7 B | fixed-width, 56 B/row |
| `iri_panel` | 1 108 | 16.5 M | delimited, **4 dialects** |
| `iri_product_attr` | 93 | 643 K | fixed-width, 21 B attribute pitch |
| `iri_panel_trips` | 12 | 7.2 M | CSV, 2 dialects |
| `iri_panelist_demos` | 217 (14 canonical) | 1.1 M (75 K written) | CSV, 2 dialects |
| `iri_product_stub` | 124 | 635 K | `.xls` / `.xlsx` |
| `iri_delivery_stores` | 372 (59 canonical) | 763 K (120 K written) | fixed-width, 63 B/row |
| `iri_week_dimension` | 1 | 626 | `.xls` |
| `iri_ads_demos` | 7 | 55 K | CSV |
| `iri_chain_xref`, `iri_manual_store_entry` | 3 | 436 | CSV |

Everything else — 626 `.doc`, 16 `.pdf`, 4 `.zip`, Office lock files,
`.bak`/`.OLD` stragglers — is explicitly skipped at discovery time, and
the *reason* is reported rather than swallowed.

`iri_sales` keeps its own pipeline: it is 143 GB across 744 files with a
measured parallelism sweep and five documented gates behind it. The
other ten share a generic driver — same manifest, same skip decision,
same atomic Parquet write, same failure-is-a-value policy — but their
own discovery and parsers.

The survey behind all of this, including the places where the corpus
contradicts `docs/data_layout.md`, is in
[`docs/other_sources.md`](docs/other_sources.md).

## Why a specialised fixed-width parser

Generic CSV / FWF libraries do roughly the right thing but pay for:

- per-line `String` allocation,
- regex or delimiter state machines,
- row-by-row conversion through a generic DataFrame API.

The IRI sales layout is fully described by **56 bytes per row** (54 of
field content + 2 of CRLF) and is invariant across all 12 years.
`iri-lake` reads the file as bytes, slices each field by its fixed
byte range, and feeds the typed values directly into Arrow column
builders. No row struct is allocated; no per-row String is built. The
parser is engineered to become I/O- or Parquet-compression-bound,
not parsing-bound.

## Data root

The default input root is:

```
data/raw/
```

Resolves to `data/raw/Year<N>/<category>/<category>_(drug|groc)_<w1>_<w2>`
for the sales files. The walker recurses, so the nested
`data/raw/Year12/toothpa/toothpa/<files>` case is handled without
special-casing.

## Output lake

```
data/lake/
├── bronze/
│   └── iri_sales/
│       └── year=<N>/
│           └── category=<category>/
│               └── channel=<drug|groc>/
│                   └── part-<run-id>-<sequence>.parquet
└── metadata/
    └── manifest.jsonl
```

`source_year`, `category`, and `channel` are also written **as physical
columns** inside every Parquet file, so a standalone file is
self-describing and readable from DuckDB / Spark / Polars without
consulting the directory layout.

## Canonical schema

The schema contains only the **raw data columns** parsed out of the
fixed-width record. `source_year`, `category`, and `channel` are
deliberately omitted from the physical columns; they live exclusively
in the Hive-style partition directory layout (`year=N/category=…/
channel=…/`) and are read back as virtual columns by any Hive-aware
reader.

| column | Arrow type | source field |
|---|---|---|
| `iri_key` | `UInt32` | `IRI_KEY` (1–7) |
| `week` | `UInt16` | `WEEK` (9–12) |
| `sy` | `UInt8` | `SY` (14–15) |
| `ge` | `UInt8` | `GE` (17–18) |
| `vend` | `UInt32` | `VEND` (20–24) |
| `item` | `UInt32` | `ITEM` (26–30) |
| `units` | `Int32` | `UNITS` (32–36) |
| `dollars_cents` | `Int64` | `DOLLARS` (38–45), exact integer cents |
| `feature_code` | `UInt8` | `F` (47–50), see below |
| `display` | `UInt8` | `D` (52) |
| `price_reduction` | `Boolean` | `PR` (54) |

`source_year` is derivable from `week` via `fixed_width::week_to_year`
(week 1114-1165 -> year 1, …, week 1687-1739 -> year 12; contiguous,
non-overlapping across the 12 years). `category` and `channel` are
constant per source file and so belong with the file metadata rather
than the row content.

### Reading with Hive partitioning

The directory layout is the modern lakehouse convention (Iceberg, Delta,
DuckDB Hive mode, Spark, Trino, Athena). DuckDB example:

```sql
-- Hive-aware: `year`, `category`, `channel` come back as virtual columns
SELECT year, category, channel, count(*) AS rows, sum(dollars_cents)/100.0 AS total
FROM read_parquet(
    'data/lake/bronze/iri_sales/**/*.parquet',
    hive_partitioning = true
)
GROUP BY year, category, channel
ORDER BY total DESC
LIMIT 20;
```

For a tool that doesn't recognise Hive partitioning, only the eleven
raw data columns above are visible; partition values are accessed by
reading the path. PyArrow and Polars have the same `hive_partitioning`
flag with the same semantics.

### Type-tightening rules

Each numeric Arrow type was chosen from the on-disk **byte width** of
the field — see `fixed_width::max_unsigned`, `max_signed`, `max_cents`.
The rule: W bytes of digits hold at most `10^W − 1`. Add the
constraint that Arrow has no `UInt24`/`Int24`, so widths of 3-4 digits
that overflow `UInt16`/`Int16` are forced up to `UInt32`/`Int32`.

| field | width | max value | Arrow type | reason |
|---|---:|---:|---|---|
| `iri_key` | 7 | 9 999 999 | `UInt32` | no UInt24 |
| `vend`, `item` | 5 | 99 999 | `UInt32` | UInt16 max 65 535 too small |
| `units` | 5 | ±99 999 | `Int32` | Int16 max 32 767 too small |
| `dollars_cents` | 8 | 9 999 999 900 | `Int64` | Int32 max 2.1 B too small (no-decimal worst case) |
| `display` | 1 | 0-9 | `UInt8` | Boolean would need empirical 0/1 confirmation |

### Promotion-feature coding (`F`)

| token | code |
|---|---:|
| (empty / all spaces) | 0 |
| `NONE` | 0 |
| `A` | 1 |
| `A+` | 2 |
| `B` | 3 |
| `C` | 4 |

Anything else fails the file with a typed error identifying the row
and the raw field bytes. The handling of unexpected values will be
made configurable in a later release.

### Money (`DOLLARS` → `dollars_cents`)

No binary float in the canonical schema. `DOLLARS` is parsed exactly
into integer cents (`Int64`). The parser handles 0, 1, or 2
fractional digits, internal spaces, and rejects more than one decimal
point, more than two fractional digits, signs, and non-ASCII bytes.

Downstream views may expose `dollars_cents / 100.0` as a decimal-like
amount.

## CLI

```bash
# Walk the input tree and report eligible files (does NOT parse them).
iri-lake inventory --input data/raw

# List exactly which files would be ingested, one per line. This is the
# supported work list — prefer it to globbing data/raw yourself, since
# discovery skips 2 733 files across six reasons.
iri-lake inventory --input data/raw --format paths

# Validate the header, record alignment, and a 100-row sample.
iri-lake validate data/raw/Year1/beer/beer_drug_1114_1165

# Convert one file into Parquet.
iri-lake ingest data/raw/Year1/beer/beer_drug_1114_1165 --output-root data/lake

# Walk the input tree and report the ten non-sales datasets. Writes
# nothing. `--explain-skips` shows *why* files were skipped, which is
# the part that matters when a class reports zero sources.
iri-lake inventory-other --input data/raw --explain-skips

# The sales pipeline: the original `ingest-all`, renamed and unchanged.
# 744 files, 143 GB. See docs/parallelism.md for the measured
# parallelism and docs/corpus_readiness.md for the gates.
iri-lake ingest-sales --input data/raw --output-root data/lake

# The umbrella: ingest-sales, then every non-sales dataset. Classes run
# sequentially and each is resumable at dataset grain.
#
#   --only panel,stubs   restrict to these classes (repeatable)
#   --skip stubs         exclude classes (repeatable)
#   --no-sales           umbrella over the non-sales classes only
#   --explain-skips      print each class's skip reasons
#   --dry-run            discover and report, write nothing
iri-lake ingest-all --input data/raw --output-root data/lake --workers 8

# Ingest the ten non-sales classes on their own: 2.05 GiB -> 27.0 M
# rows in ~11 s at 8 workers. This is the target to iterate on; the
# sales path is unchanged underneath it.
iri-lake ingest-all --input data/raw --output-root data/lake --no-sales

# Previously: ingest-all meant sales only. That is now `ingest-sales`.
#
# Skipping is the DEFAULT (no flag needed) — this is what lets an
# interrupted full-corpus run simply be re-issued.
#
#   --resume      explicit affirmation of that default
#   --overwrite   discard existing outputs and rewrite them
#
# Passing both is an error: they are opposite intents.
#
#   --workers N   file-level parallelism (default: available
#                 parallelism, capped at 16). 1 = sequential.
#   --order O     work-list order; smallest-first is the default and
#                 beats largest-first by ~20% (see docs/parallelism.md)
#   --shard I/N   take a byte-balanced shard of the corpus, for running
#                 several processes/machines against one output root.
#
# A file that fails is logged, counted, and recorded in the manifest
# as `status: "failed"`; the rest of the run continues. A file whose
# last record is short is NOT a failure: every complete record is
# ingested, the trailing one is counted in `rejected_rows`, and the
# severity (missing terminator vs missing fields) decides whether that
# is an info line or a warning.
iri-lake ingest-sales --input data/raw --output-root data/lake --workers 8

# Run parser / Arrow / Parquet micro-benchmarks on one file.
iri-lake benchmark data/raw/Year1/beer/beer_drug_1114_1165 \
    --batch-rows 1000000 --compression zstd --repeats 3
```

Global flags (also configurable via env: `IRI_LAKE_BATCH_ROWS`,
`IRI_LAKE_COMPRESSION`, `IRI_LAKE_OUTPUT_ROOT`, …):

- `--batch-rows <N>` — rows buffered per flush (default 1 000 000)
- `--row-group-rows <N>` — Parquet row-group target (default 4 000 000)
- `--compression <codec>` — `zstd`, `zstd-1`, `zstd-3`, `zstd-9`,
`snappy`, `lz4`, `lz4_raw`, `uncompressed` (default `zstd`).
  `none` is accepted as a synonym for `uncompressed`. Unrecognised
  codecs warn and fall back to `zstd`.
  Note that `zstd` means **level 1**, not level 3 — `zstd` and
  `zstd-1` produce byte-identical output, and the measured sweep puts
  level 3 and level 9 at 8 %/44 % less write throughput for 5 %/1 %
  smaller files, so neither is a good trade
  ([`BENCHMARKING.md`](BENCHMARKING.md) § *Measured codec sweep*).
- `--worker-threads <N>` — defaults to logical CPU count, capped at 16;
  `ingest-all`'s `--workers` overrides it per run
- `--order <smallest-first|largest-first|striped>` — how `ingest-sales`
  deals its work list to workers. `smallest-first` is the default and is
  the fastest measured (docs/parallelism.md §3)

## Resume semantics

A source is skippable only if a prior `manifest.jsonl` entry satisfies
**all** of:

- `status == "success"`,
- `dataset`, `source_path`, `source_size_bytes`, `source_sha256` all
  match,
- `parser_version`, `output_schema_version`, `compression`,
  `batch_rows`, `row_group_rows` all match,
- the recorded `output_paths` still exist on disk.

A failed prior run remains visible and is retried on the next
`ingest` / `ingest-all` (a failed record never matches the skip check).

Note that the source's SHA-256 is computed **before** the skip
decision, because the hash is what makes the decision trustworthy. A
resume therefore re-hashes every source: re-issuing the command
against the finished 744-file corpus took 110 s to skip all 744.

## Partial and failed sources

A source that is not record-aligned does not fail the file. Every
**complete** record is ingested and the trailing one is counted in
`rejected_rows`, with a normal `success` in the manifest. What the
severity of that trailing defect means depends on whether any *field* is
missing:

| trailing defect | validator | ingest |
|---|---|---|
| last record missing only its CRLF terminator | **passes**, with a warning; all complete records still validated | info; every field of every row written |
| last record missing field bytes | **fails**, reporting how many of its 54 content bytes are present | warning; counted as a real loss |

A row is 54 content bytes plus a 2-byte CRLF terminator, and every
field is read from offsets 0..54 — the terminator is checked as an
invariant, never used as data. So a file that ends one byte into its
final terminator has lost nothing queryable. The staged corpus contains
exactly one such file (`Year12/soup/soup_groc_1687_1739`, 11 391 465
complete records), and it passes Gate 2 with a warning.

A source that genuinely cannot be parsed — a corrupt header, an
unreadable file — *does* fail. It is logged, counted in the summary,
and appended to `manifest.jsonl` as `status: "failed"` with its error
message, so the lake's metadata store records its own gaps instead of
leaving them in stdout. A failed record never matches a skip, so the
source is retried on the next run.

When a prior success *does* match, the flag decides what happens:

| flags | behaviour |
|---|---|
| *(neither)* | skip the source (default) |
| `--resume` | skip the source — explicit affirmation of the default |
| `--overwrite` | re-ingest, replacing the existing Parquet output |
| both | error: mutually exclusive |

Library callers choose explicitly via `IngestConfig::overwrite`
(`SkipIfPresent` / `Overwrite` / `Refuse`; `Refuse` returns an
`IngestError::OutputsExist` rather than silently skipping).

## Build & test

```bash
make fmt       # cargo fmt --all
make clippy    # cargo clippy --all-targets -- -D warnings
make test      # cargo test --all-features
make build     # cargo build --release --locked
make bench     # cargo bench --bench parse_sales
```

The first build downloads and compiles the Arrow and Parquet
crates, which is the dominant cost of the dependency graph.

## Querying the lake (DuckDB)

The analytics layer is a `uv` project next to the Rust crate. It is
strictly downstream: nothing in `cargo build` depends on it, and DuckDB
simply reads the Parquet files the binary writes.

```bash
make gate2                       # validate every discovered sales source (~7 min)
make gate5                       # Gate 5 of docs/corpus_readiness.md
make gate6                       # reconcile the non-sales tables + cross-table joins
make gate6 LAKE=/tmp/some-lake   # ...against any other output root
make sql SQL=sql/my_query.sql    # any file in sql/
```

`sql/gate6_non_sales.sql` checks what a row-count match cannot: that
the manifest and the lake agree per class, that every week referenced
by sales or panel resolves to a calendar date, that the week dimension
covers all twelve years, that every sales store has a roster row, and
that no column is silently empty where the corpus says it is not.

`sql/gate5_reconciliation.sql` holds the reconciliation queries; each
one returns a boolean `gate_5x_ok` column, and `scripts/run_sql.py`
exits non-zero if any is false, so the gate can be scripted rather than
eyeballed. `uv run` syncs `.venv` from `uv.lock` on demand — there is no
install step, and the DuckDB Python package ships no CLI of its own.

Note the DuckDB *shell* (`brew install duckdb`) is a separate thing
from this project and also works; the queries are plain SQL either way.

## Safety notes for the full corpus

- The full corpus is **≈ 143 GB raw / 2.7 B rows / 744 sales files**.
- Always **stage the raw data onto local NVMe** before running. The
  default input path (`data/raw/`) can be a symlink to a fast SSD
  mount — that's what this repo's layout uses.
- Always **test on a single small file first**:

  ```bash
  iri-lake validate data/raw/Year1/beer/beer_drug_1114_1165
  iri-lake ingest   data/raw/Year1/beer/beer_drug_1114_1165 --output-root data/lake
  iri-lake ingest   data/raw/Year1/beer/beer_drug_1114_1165   # should SKIP
  ```

- The staged, gated sequence for the **full 143 GB corpus** — inventory,
  full validation sweep, widening ramp, and the reconciliation queries
  that decide whether the lake is trustworthy — is in
  [`docs/corpus_readiness.md`](docs/corpus_readiness.md). Read it before
  the all-corpus run; it also lists the known gaps that gate that run.

- Then move to **one category** (`--category beer`) and then to a
  full dry-run (`ingest-all --dry-run --max-files 5`) before
  scheduling an all-corpus run.

## Verified format invariants

The implementation was built against these empirically-verified facts:

| | text content | total bytes (incl CRLF) |
|---|---:|---:|
| header | 55 chars | **57** bytes |
| data row | 54 bytes | **56** bytes |

Spec prose quoted `HEADER_LEN = 55` and `RECORD_LEN = 54` "including
CRLF" — those numbers are the *content* portions. The on-disk
constants used by the parser are:

```rust
pub const HEADER_LEN: usize = 57;  // 55 chars + \r\n
pub const RECORD_LEN: usize = 56;  // 54 bytes + \r\n
```

These match the body byte count for every file checked across years
1, 6, 11, and 12. They are central to the parser and are pinned by
unit tests.

## License

Dual-licensed under MIT or Apache 2.0.
