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

`source_year`, `category`, and `channel` are also included as
**physical columns** inside the Parquet file for the same reason:
standalone files are self-describing, no directory lookup needed.

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
  thread count cap; not yet wired through `ingest-all`),
- per file, sequential parsing into Arrow + sequential Parquet write,
- largest files processed first to reduce tail latency in a
  multi-hour batch run.

We deliberately **do not** combine unrestricted file-level and
record-level parallelism, because that oversubscribes CPU, memory
bandwidth, and NVMe queue depth simultaneously. The bench harness
in `benches/parse_sales.rs` measures the per-file pipeline; once
that is profiled on a representative file, parallel ingestion across
files is the right next optimisation.
