# Architecture

A walk through the design of `iri-lake`. The intent is to make the
system legible to someone who has never seen this code: the byte
layout, the data flow, the safety guarantees, and the planned
extensions.

## High-level data flow

```
┌──────────────┐
│  data/raw/   │   on-disk source files (read-only)
└──────┬───────┘
       │  walkdir → filter → parse filename → infer identity
       ▼
┌──────────────────┐
│ discovery.rs     │  Inventory { files, skipped[] }
└──────┬───────────┘
       │  ingest_file(path, identity, config) per file
       ▼
┌──────────────────┐
│ ingest.rs        │  mmap → validate header → SHA-256
└──────┬───────────┘
       │  body bytes
       ▼
┌──────────────────┐
│ parser.rs        │  parse_records_into_builder(body, builders)
│ fixed_width.rs   │  field offsets, header bytes, stride
│ money.rs         │  dollars → integer cents
│ feature.rs       │  F → u8
└──────┬───────────┘
       │  Arrow RecordBatch
       ▼
┌──────────────────┐
│ arrow_output.rs  │  SalesBuilders (UInt32Builder, …, StringBuilder)
│ parquet_output.rs│  ArrowWriter; tmp → atomic rename
└──────┬───────────┘
       │  Parquet file
       ▼
┌──────────────────┐
│ data/lake/...    │  bronze/iri_sales/year=…/category=…/channel=…/
└──────────────────┘
       │
       ▼
┌──────────────────┐
│ manifest.rs      │  append ManifestRecord → metadata/manifest.jsonl
└──────────────────┘
```

## Discovery & routing

`src/discovery.rs` walks `data/raw/` recursively with `walkdir`,
filtering each directory:

1. Hidden directories are skipped.
2. Top-level directories matching the `IGNORED_DIR_NAMES` list
   (`parsed stub files*`, `demos trips external`, `Academic …`,
   `Pacesetters external`, `TNS advertising data*`) are not descended.
3. Each regular file's name is checked first by `skip_reason_for_filename`
   — lock files (`~$…`), `.OLD` / `.BAK` / `.TMP` backups, and any
   extension matching `.xls`, `.xlsx`, `.doc`, `.docx`, `.csv`,
   `.pdf`, `.zip` are reported as skipped. PANEL files are skipped
   the same way.
4. Surviving names are routed through `parse_identity`:
   - split on `_`,
   - last two segments must be numeric week numbers,
   - third-from-last must be `drug` or `groc`,
   - earlier segments join back as the category (no hard-coded list),
   - the nearest `Year<N>` ancestor decides the year, which must be
     in `1..=12`.

No fixed list of category directories is assumed; the canonical
category mapping (`paptowl` ↔ `paptowls`) is intentionally a later
extension point.

## Fixed-width byte layout

Pinned in `src/fixed_width.rs`. The verified invariants are:

| | text content | total bytes (incl CRLF) |
|---|---:|---:|
| header | 55 chars | 57 bytes |
| data row | 54 bytes | 56 bytes |

Field offsets (zero-based half-open ranges within the 54 content
bytes of each row):

```
IRI_KEY  [ 0..7 ]  7 chars   (+1 separator)
WEEK     [ 8..12]  4 chars   (+1 separator)
SY       [13..15]  2 chars   (+1 separator)
GE       [16..18]  2 chars   (+1 separator)
VEND     [19..24]  5 chars   (+1 separator)
ITEM     [25..30]  5 chars   (+1 separator)
UNITS    [31..36]  5 chars   (+1 separator)
DOLLARS  [37..45]  8 chars   (+1 separator)
F        [46..50]  4 chars   (+1 separator)
D        [51..52]  1 char    (+1 separator)
PR       [53..54]  1 char
CRLF     [54..56]  2 bytes (\r\n)
```

These constants and offsets are pinned by unit tests in
`src/fixed_width.rs` and integration tests that build real binary
fixtures and assert `file_size == HEADER_LEN + n*RECORD_LEN`.

## Per-file ingest pipeline

`iri-lake ingest <path>` runs `ingest::ingest_file`, which performs:

1. **Identity.** `parse_identity` (see Discovery).
2. **Filter check.** Year / category / channel flags from the CLI must
   match; otherwise the file is not processed.
3. **Size & alignment.** `(size - HEADER_LEN) % RECORD_LEN == 0`
   (computed by `fixed_width::expected_rows`).
4. **SHA-256.** Streamed over the file in 64 KiB chunks via
   `metrics::sha256_of_file`.
5. **Plan outputs.** `plan_output_paths` produces
   `<run_id_short>-NNNN.parquet` filenames under the Hive-style
   `bronze/iri_sales/year=<N>/category=<c>/channel=<x>/` directory.
6. **Skip-decision.** `manifest::skip_decision` looks up the prior
   manifest record for this source. If it is a Success with matching
   SHA, parser version, schema version, and config — and the output
   files still exist — the run returns `IngestOutcome::Skipped` and
   stops here.
7. **mmap.** `memmap2::Mmap::map` is the only `unsafe` block in the
   crate; it is bounded by `#[deny(unsafe_code)]` plus
   `#[allow(unsafe_code)]` on `ingest_file` with rationale in
   `lib.rs`.
8. **Header validation.** `fixed_width::validate_header` checks the
   first 57 bytes against the canonical `HEADER_TEXT`.
9. **Per-batch parse + write.** The body is walked in `batch_rows`
   chunks. For each chunk:
   - `parse_records_into_builder` appends one record at a time to
     `SalesBuilders`, slicing by fixed offsets.
   - `builders.finish(schema())` produces a `RecordBatch`.
   - `write_parquet_atomic` writes it to `*.parquet.tmp` then renames
     to the final filename.
10. **Manifest append.** The `ManifestRecord` is appended to
    `data/lake/metadata/manifest.jsonl`.

## Memory budget

For the default `batch_rows = 1_000_000`:

| column | bytes per row | 1 M rows |
|---|---:|---:|
| 7 × `UInt32Builder` | 4 | 28 MB |
| 1 × `Int64Builder` | 8 | 8 MB |
| 2 × `UInt8Builder` | 1 | 2 MB |
| 1 × `BooleanBuilder` | 1 (bit-packed) | ≈ 1 MB |
| 2 × `StringBuilder` | ~8 (low-card, dict-able) | ≈ 8 MB |
| validity bitmaps | — | 0 |
| **total** | | **≈ 47 MB** |

Comfortably bounded for any consumer-side allocator.

## Partitioning strategy

The output layout uses **Hive-style partitioning** by `(year, category,
channel)`. We deliberately exclude `week` from the partition columns:

- there are ~52 weeks per year; adding `week=` would multiply the
  directory count by 50×,
- every Parquet file already contains a physical `week` column, so a
  downstream query can push a `WHERE week = N` predicate into the
  reader without consulting the directory layout,
- and adding `week` would force a write-time decision about whether
  to split files at week boundaries (a sales file spans 52 weeks;
  splitting it across 52 partition directories would defeat the
  point of writing one file per source).

**The Parquet files do NOT contain partition columns.** `source_year`,
`category`, and `channel` are partition-only metadata; they live in the
directory layout (`year=N/category=…/channel=…/`) and any Hive-aware
reader (DuckDB with `hive_partitioning=true`, PyArrow, Polars, Spark,
Trino, Iceberg, Delta) reads them as virtual columns from the
directory names. This is the modern lakehouse convention
(Iceberg/Delta): partition columns are table metadata, not row content.

The trade-off is that a reader without Hive-awareness sees only the
eleven raw data columns and would have to compute year from `week` via
`fixed_width::week_to_year` (or just know which file it is reading).
For analytical workloads the Hive-aware path is the standard.

`source_year` in particular is fully derivable: the IRI `WEEK` field is a
single integer counter (1114-1739 across the 12 years, contiguous,
non-overlapping), so year is a deterministic function of week:

```text
1114-1165 -> 1   1166-1217 -> 2   1218-1269 -> 3   1270-1321 -> 4
1322-1373 -> 5   1374-1426 -> 6   1427-1478 -> 7   1479-1530 -> 8
1531-1582 -> 9   1583-1634 -> 10  1635-1686 -> 11  1687-1739 -> 12
```

The mapping is hard-coded as `fixed_width::week_to_year` rather than
read from a dimension table, because the year ranges are stable across
all 12 years of data and a `match` is faster than a table lookup. The
partition directory still carries the year, so Hive-aware readers can
read it via `hive_partitioning=true`. Standalone files (no directory
context) compute year on demand via `week_to_year`.

### Why this placement: Bronze vs Silver

For `source_year` specifically:

- The function `week -> year` is fully deterministic over the IRI
  week range. It does not need to be stored as a Bronze dimension
  table; a small `match` in code is enough.
- Storing `source_year` as a physical column would be **redundant
  data**: every row would carry a value derivable from another column
  in the same row. The redundancy would let a mis-coded row exist
  (a row where `week` says 1165 but `source_year` says 2), and would
  cost a byte per row plus the encoding/decoding overhead.
- The standard Hive pattern is to encode partition keys in the
  directory layout only, not inside the row. DuckDB, Spark, Polars,
  and PyArrow all support reading the partition columns from the
  path. We follow that pattern.

For the broader `week -> calendar_date` mapping (which IS not a pure
function — the IRI academic week doesn't map cleanly to ISO calendar
dates without the IRI Week Translation file):

- **Bronze**: ingest the IRI Week Translation `.xls` as a small
  reference table `iri_week_dimension`. It is itself data we capture
  from the source — provenance is preserved by storing it raw at
  Bronze rather than derived at Silver.
- **Silver**: join the Bronze dimension to add a typed
  `calendar_start_date` column to the sales fact. Year is *not* added
  at Silver because it's a free function of week — we already have it
  for the asking.

The principle: **derive what is derivable, store what is captured**.
A derived column at Silver is fine if the derivation requires a join
to another Bronze table (e.g. calendar dates). A pure function does
not need a Silver layer at all; it stays as code.

## Metadata & idempotence

`manifest.jsonl` is the authoritative skip-decision store for now.
Each record carries:

- `source_path`, `source_size_bytes`, `source_sha256`,
- `source_year`, `category`, `channel`, `filename_week_start`,
  `filename_week_end`,
- `expected_rows`, `written_rows`, `rejected_rows`,
- `parser_version`, `output_schema_version`, `compression`,
  `batch_rows`, `row_group_rows`,
- `output_paths`, `output_size_bytes`,
- `started_at`, `completed_at`, `duration_ms`,
- `status` (`success` / `failed` / `in_progress`),
- `error_message`.

The trait abstraction (`manifest::ManifestStore`) is the seam where
a Parquet- or SQLite-backed implementation will plug in later. The
on-disk schema is identical so existing manifest files can be
re-ingested cleanly.

## Atomic writes

Every `*.parquet` file is written to a `.parquet.tmp` sibling first;
on writer close and `std::fs::rename`, it becomes the final name. A
consumer that observes a `*.parquet` filename can trust the bytes are
complete. A crashed run leaves at most a `.tmp` sibling that the next
run overwrites.

The renames are scoped inside one source file's output directory, so
they are always on the same filesystem and never cross mount points.

## Error handling

- **Library** uses typed `IngestError` (thiserror) variants that
  carry enough context for a one-line CLI diagnostic — source path,
  byte offset / row number, field name, raw bytes safely escaped.
- **CLI** wraps in `anyhow::Result` for ergonomic `?` propagation
  across the binary's glue code.

Variants are deliberately fine-grained enough that a future
`--reject-invalid-rows` mode can map per-row errors to the
`rejected_rows` Parquet writer without redesigning the error enum.

## `unsafe`

The crate uses `#![deny(unsafe_code)]` with one well-scoped
`#[allow(unsafe_code)]` on `ingest_file`, where the only unsafe
block calls `memmap2::Mmap::map`. The rationale (recorded in `lib.rs`)
is that memmap2 exposes no safe alternative in its current public
API. Everything else in the crate is safe Rust.

## Why no `unsafe` for the parser itself

The hot loop is purely byte slicing and integer parsing — no
pointer arithmetic, no `from_utf8_unchecked`. We use
`str::from_utf8` and `str::parse::<T>` on per-field slices, which
return typed `Result`s and let us surface malformed input as a
structured `IngestError`.

## Planned extensions

1. **PANEL parsing as a separate pipeline.** Years 1–2 are
   tab-delimited, years 3–7 are whitespace-delimited, years 8–12
   are comma-delimited with an extra `MINUTE` column. Year 8 adds
   `PANEL_KK`. The header line is the dispatcher.
2. **Deduplicated `Delivery_Stores`.** Hash the file once, write a
   canonical copy under `data/lake/bronze/delivery_stores/year=<N>/`
   with a symlink or reference back to the canonical from each
   category directory.
3. **Deduplicated `DEMOS.CSV`.** Same pattern — one copy per year.
4. **Product-stub ingestion.** `.xls` (BIFF) for years 1–6,
   `.xlsx` (OOXML) for years 7–12. Two Rust crates needed (or a
   single `.xls`→`.xlsx` pre-pass).
5. **Delta Lake / Apache Iceberg table metadata.** Once the
   partition directory is in place, the upgrade path is to wrap it
   in an Iceberg `Table` and let DuckDB / Spark treat it as a real
   table rather than a directory of files.
6. **DuckDB / dbt analytical layer.** Materialised views on top of
   the Bronze Parquet for the silver / gold layers.
7. **Object-store support.** `object_store` crate behind a `Source`
   trait so a future run can read from S3 / GCS / R2 directly,
   without changing the parser.
8. **Separate raw / bronze / silver models.** Today there is one
   Bronze table. Adding a Silver layer (typed, filtered, with
   `week → date` lookup joined in) and a Gold layer (analytical
   marts) is a natural next step.

## Performance design notes

The intended shape of a full run is:

- one process per machine (initially),
- bounded parallelism across files via a worker pool (rayon with a
  thread count cap, wired through `ingest-all --workers`),
- per file, sequential parsing into Arrow + sequential Parquet write,
- the work list dealt **smallest-file-first**, because rayon's
  work-stealing deque hands work out from the back of the slice — a
  largest-first list leaves the 1.4 GB file to be picked up last.

We deliberately **do not** combine unrestricted file-level and
record-level parallelism, because that oversubscribes CPU, memory
bandwidth, and NVMe queue depth simultaneously. The bench harness
in `benches/parse_sales.rs` measures the per-file pipeline;
parallel ingestion across files is implemented and measured in
[`docs/parallelism.md`](docs/parallelism.md).

## Parallel ingest (implemented)

```
┌──────────────────────────────┐
│ ingest-all (main.rs)         │  discover → filter → largest-first sort
│  → Order → take_shard        │  → --shard IDX/N (optional)
└──────────────┬───────────────┘
               │ Vec<DiscoveredFile>
               ▼
┌──────────────────────────────┐
│ ingest::ingest_all           │  rayon pool, --workers threads
│  files.par_iter()            │  failures collected, not fatal
└──────────────┬───────────────┘
               │ per file, one thread
               ▼
┌──────────────────────────────┐
│ ingest::ingest_file_with     │  unchanged per-file pipeline:
│  → SharedManifest            │  mmap → header → batch parse → Arrow
│    (prior lookup / append)   │  → Parquet → atomic rename
└──────────────────────────────┘
```

Two design points worth stating explicitly:

1. **The manifest is the only shared mutable state.** Each worker owns
   its mmap, its Arrow builders, its Parquet writer and its output
   paths; `SharedManifest` is a mutex over one file handle plus a
   `HashMap` index, held only for the duration of a single JSONL
   append. Measured cost across the 743-file corpus run: unmeasurable.
2. **Failures are values, not panics.** `IngestAllSummary::failures`
   returns them to the caller and the run continues, so one
   unparseable source cannot cost the other 743. This is what makes a
   744-file gate safe to issue unattended.

The measured numbers, the alternatives that were tried (N processes
with byte-balanced shards, three orderings, worker counts 1–12), and
the ceiling analysis are in [`docs/parallelism.md`](docs/parallelism.md).
