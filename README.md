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

This first version handles **store-level weekly sales** files only:

```
<category>_drug_<week_start>_<week_end>
<category>_groc_<week_start>_<week_end>
```

It does **not** parse PANEL files, product stubs, `Delivery_Stores`,
`DEMOS.CSV`, Excel files, trips files, or documentation. Those file
classes are explicitly skipped at discovery time so they cannot enter
the sales parser by accident.

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
| `category` | `Utf8` | physical, from filename |
| `channel` | `Utf8` | physical, from filename |

`source_year` is **deliberately omitted** from the physical columns. The
IRI `WEEK` field is a single integer counter (range 1114-1739 across the
12 years, contiguous, non-overlapping) that maps to academic year
1-12 via a deterministic function — see `fixed_width::week_to_year`.
Year remains in the partition directory (`year=N/`) but is not duplicated
inside every row. `category` and `channel` are still physical columns
because they aren't derivable from the row content.

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

# Validate the header, record alignment, and a 100-row sample.
iri-lake validate data/raw/Year1/beer/beer_drug_1114_1165

# Convert one file into Parquet.
iri-lake ingest data/raw/Year1/beer/beer_drug_1114_1165 --output-root data/lake

# Walk the input tree, validate every file, and ingest everything,
# largest-first, skipping sources already covered by a successful
# manifest record.
iri-lake ingest-all --input data/raw --output-root data/lake --resume

# Run parser / Arrow / Parquet micro-benchmarks on one file.
iri-lake benchmark data/raw/Year1/beer/beer_drug_1114_1165 \
    --batch-rows 1000000 --compression zstd --repeats 3
```

Global flags (also configurable via env: `IRI_LAKE_BATCH_ROWS`,
`IRI_LAKE_COMPRESSION`, `IRI_LAKE_OUTPUT_ROOT`, …):

- `--batch-rows <N>` — rows buffered per flush (default 1 000 000)
- `--row-group-rows <N>` — Parquet row-group target (default 4 000 000)
- `--compression <codec>` — `zstd`, `zstd-1`, `zstd-3`, `zstd-9`,
  `snappy`, `lz4`, `gzip`, `uncompressed` (default `zstd`)
- `--worker-threads <N>` — defaults to logical CPU count, capped at 16

## Resume semantics

A source is skippable only if a prior `manifest.jsonl` entry satisfies
**all** of:

- `status == "success"`,
- `source_path`, `source_size_bytes`, `source_sha256` all match,
- `parser_version`, `output_schema_version`, `compression`,
  `batch_rows`, `row_group_rows` all match,
- the recorded `output_paths` still exist on disk.

A failed prior run remains visible and is retried on the next
`ingest --resume` / `ingest-all --resume`.

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
