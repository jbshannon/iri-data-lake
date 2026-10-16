# Excel → Bronze

The corpus's three Excel sources, ingested to Parquet. This document
records what was built, what was measured, and what the parsing had to
get right that a "read the sheet, write the rows" description would
hide.

Status: **implemented and measured.** Every number below came from a
run against the staged corpus, not from a design document.

## 1. Scope

`docs/silver_layer.md` §8 sequences the Silver work and puts this step
first, for a reason worth restating: Silver's highest-value join is
`iri_sales.week → calendar date`, and that join needs a Bronze
dimension table. Until the week workbook is in the lake, every
interesting question about the 2.7 B sales rows is unanswerable,
because `week` is an opaque IRI counter.

| class | source | files | rows | output |
|---|---|---:|---:|---:|
| `iri_week_dimension` | `demos trips external/IRI week translation.xls` | 1 | 626 | 8.0 KiB |
| `iri_fips_market` | `demos trips external/fips by IRI market.xls` | 1 | 3 141 | 64 KiB |
| `iri_product_stub` | four `parsed stub files*/` directories | 124 | 634 824 | 23.2 MiB |
| **total** | | **126** | **638 591** | **23.3 MiB** |

Measured:

```
week_dimension   files=1     completed=1  rows=626      out=7960B    wall=0.04s  (310 skipped-by-discovery)
fips_market      files=1     completed=1  rows=3141     out=64234B   wall=0.01s  (0 skipped-by-discovery)
product_stub     files=124   completed=124 rows=634824  out=23.2MiB  wall=6.07s  (4 skipped-by-discovery)
```

Reproduce:

```bash
cargo run --release -- inventory-other --input data/raw --explain-skips
cargo run --release -- ingest-all --input data/raw --output-root /tmp/lake-excel \
  --no-sales --only week_dimension --only fips_market --only product_stub
```

Three classes, 126 sources, 638 591 rows, 0 failures, 6.12 s. The
whole step is cheap; that is the point of doing it first.

## 2. The reader, and why there is only one

All three sources are `.xls` (BIFF8) or `.xlsx` (OOXML), so they need
one crate: [`calamine`], which auto-detects the container from the
magic bytes. The alternative — `xlrd` for BIFF and `openpyxl` for OOXML,
as the Julia/Python path assumed — is two readers, two sets of type
mappings, and two sets of "what is a date" bugs for a corpus that spans
exactly one format transition.

The dependency is scoped to `src/datasets/excel.rs`. The sales pipeline
and the nine text-format classes do not touch it, so a build that
removes the stubs does not pull Excel support out of the other classes.

### The four things that are easy to get wrong

Each of these produces a table that **loads cleanly and is wrong**,
which is the failure mode this layer is most exposed to.

**1. Excel serial dates.** A date cell is a `f64` counting days from
1899-12-30. Excel believes 1900 was a leap year, so serial 60 is a
phantom 1900-02-29 and everything after it is shifted one day against
a correct calendar. calamine's `as_datetime` applies that correction;
reimplementing the serial arithmetic is how a week table ends up one
day out across 626 rows with no error anywhere.

```rust
// src/datasets/excel.rs — not reimplemented, deliberately.
calamine::ExcelDateTime::new(serial, XlDateType::DateTime, false).as_datetime()
```

The time-of-day component is **truncated, not rounded**: a week
*starts* on a date, and rounding `2001-01-01T18:00` up to the 2nd
would move the whole week a day.

**2. Sheet selection is by content, not position.** Nothing here reads
`sheets[0]`:

| workbook | sheets | data is |
|---|---|---|
| `fips by IRI market.xls` | `Notes`, `fips by IRI market` | the **second** |
| `parsed stub files 2007/prod_beer.xlsx` | 3 sheets | the first, but the other two are not the stub |
| `IRI week translation.xls` | `Sheet1` | the first |

Reading `sheets[0]` of the FIPS workbook yields 10 rows of prose with
one text column. That is a valid Arrow batch and a useless table. Each
class therefore declares how it recognises its sheet — `Key` +
`MARKET NAME` for FIPS, `L1` + `UPC` for stubs, `IRI Week` for the
week table — and a workbook matching nothing is an error that names the
sheets it did find. Integration tests pin this by row count:
`the_fips_documentation_sheet_is_not_ingested_as_data` fails if the
`Notes` sheet is ever read as data.

**3. Type widths chosen from observed values, not guesses.** The rule
is Bronze's existing one (`README.md` § *Type-tightening rules*): the
Arrow type comes from the on-disk width, and the value is checked
against the corpus rather than assumed.

| column | type | why |
|---|---|---|
| `week_dimension.iri_week` | `UInt16` | 1114–1739 |
| `week_dimension.week_start`/`week_end` | `Date32` | not a string, not an Excel serial |
| `fips_market.fips_key` | `UInt32` | observed max **56 045**; census keys are 5 digits |
| `fips_market.iri_market_number` | `UInt16` | observed max 148, 65 distinct |
| `product_stub.UPC` | `Utf8` | `08-01-94922-00101` — the leading zero is significant |
| `product_stub.VOL_EQ` | `Float64` | the only genuinely fractional stub column |
| `product_stub.Level`/`SY`/`GE`/`VEND`/`ITEM` | `UInt16`/`UInt8`/`UInt8`/`UInt32`/`UInt32` | on-disk widths |

`week_start` and `week_end` are `Date32` rather than a string or a
serial because the entire purpose of the table is to attach a calendar
date to `week`. Doing the conversion once, at Bronze, means every
consumer gets the same answer and the conversion cannot drift between
a DuckDB view and a Polars script.

**4. The stub spec column is renamed across editions.** Stub column 13
is named by its `*` prefix and the rest of the name differs per edition
(`*STUBSPEC 1416RC … 00004` vs `*AG C=1+ CATEGORY`). The `*` prefix is
the reliable marker; it is normalised to `stubspec` so all four editions
share one column name. Without this, a `SELECT *` across editions
returns four differently-named copies of the same field.

## 3. `iri_week_dimension`

### Schema

| column | type | notes |
|---|---|---|
| `iri_week` | `UInt16` | natural key, 1114–1739 |
| `week_start` | `Date32` | Monday of the IRI academic week |
| `week_end` | `Date32` | Sunday, 6 days later |
| `source_year` | `UInt8` (nullable) | **only** the authoritative workbook has this column |

`source_year` stays a column here — not, as in `iri_sales`, a pure
function of the row — because it is *captured*, not derived: it comes
from a `Year` column that exists in exactly one of the 312 copies of
this workbook. A Silver view may prefer `fixed_width::week_to_year`,
which is a `match` over stable ranges and costs nothing; the Bronze
column is kept because it is what the source said, and the two agree
on all 626 rows (asserted below).

### Why one workbook and not 312

Discovery ingests **`demos trips external/IRI week translation.xls`**
and reports the 311 per-year copies as skipped
(`subset_of_authoritative`). Three independent reasons:

- The per-year copies are **subsets**. The Year-1 copy starts at week
  1138, not 1114 — it is missing a month of year 1.
- Only the authoritative copy carries the `Year` column.
- Ingesting all 312 would write three partial copies of one table for
  a dimension of 626 rows.

The skips are *reported*, not silent, so the decision is visible:

```
week_dimension  files=1  112128B  skipped=310  editions=full-1114-1739
    skipped by discovery: subset_of_authoritative=310
```

### The year 6–7 gap is not a gap

`docs/data_layout.md` §2.3 records that years 6 and 7 have no week
translation, and `docs/silver_layer.md` §9.1 leaves open whether that
gap is real and whether `demos trips external/` covers it. **It does,
completely.** Measured over the ingested table:

```
year 1  52 weeks  1114-1165      year 7   52 weeks  1427-1478
year 2  52 weeks  1166-1217      year 8   52 weeks  1479-1530
year 3  52 weeks  1218-1269      year 9   52 weeks  1531-1582
year 4  52 weeks  1270-1321      year 10  52 weeks  1583-1634
year 5  52 weeks  1322-1373      year 11  52 weeks  1635-1686
year 6  53 weeks  1374-1426      year 12  53 weeks  1687-1739
```

626 rows, all twelve years, no gaps, contiguous, in order. Years 6 and
12 have **53** weeks — the fact a hard-coded 52-per-year assumption
gets wrong twice.

**Consequence for Silver:** `silver_layer.md` §5.1's three-tier gap
policy (captured → interpolated → missing) reduces to a single tier.
Every week is `captured`; `date_source` is uniformly `'captured'` and
there is nothing to interpolate. The policy is kept in the plan because
it is the right shape for a table whose coverage is incomplete *in
general*, but it does not bind here. This is asserted in
`the_week_dimension_covers_all_626_weeks_including_the_year_6_7_gap`, so
if the authoritative workbook is ever replaced by one that *does* have a
gap, the test fails rather than the gap reaching a consumer as a
silent `NULL`.

### Verified invariants

All asserted in `tests/excel_ingest_tests.rs`:

- **626 rows**, weeks 1114–1739, contiguous and strictly increasing.
- **Every week spans exactly 6 days.** IRI academic weeks run Monday to
  Sunday. This is the single most consequential assertion in the table:
  an off-by-one in the serial conversion still produces plausible dates,
  and the only defence is that they are the right ones.
- **No `NULL`s** in `week_start`, `week_end`, or `source_year`.
- **Week 1114 → 2001-01-01 … 2001-01-07**, the concrete anchor.
- **Year boundaries do not overlap:** week 1165 ends 2001-12-30 and
  week 1166 starts 2001-12-31.
- **`source_year` agrees with `fixed_width::week_to_year`** for all 626
  rows — captured and derived do not disagree.

### Layout

Flat, no partition. 626 rows is smaller than any directory would help
with, and `week` is the partition-by-value anti-pattern that
`ARCHITECTURE.md` § *Partitioning strategy* rejects for `iri_sales`.

```
bronze/iri_week_dimension/part-<run>-0001.parquet
```

## 4. `iri_fips_market`

### Schema

| column | type | source column |
|---|---|---|
| `fips_key` | `UInt32` (non-null) | `Key` |
| `name` | `Utf8` | `Name` |
| `county` | `Utf8` | `NAME_COUNTY` |
| `state` | `Utf8` | `NAME_STATE` |
| `market_name` | `Utf8` | `MARKET NAME` |
| `region_name` | `Utf8` | `REGION NAME` |
| `iri_market_number` | `UInt16` | `IRI_MARKET_NUMBER` |
| `market_name2` | `Utf8` | `MARKET NAME2` |

`Name` is IRI's composite key, `MARKET_NAME | STATE | COUNTY`
(`BIRMINGHAM/MONTGOM | Alabama | Autauga`); it is kept verbatim because
it is what the source published and re-deriving it from three columns
invites a separator mismatch.

`iri_market_number = 0` is a real IRI value (`WHITE SPACE` counties),
not a null sentinel, and is stored as `0`.

### Measured

3 141 rows, **3 141 distinct `fips_key`** — one row per county, no
duplicates, so it joins a store roster without fanning out. Keys
1 001–56 045. 65 distinct IRI markets across 9 regions
(`AL_HI`, `CALIFORNIA`, `GREAT LAKES`, `MID-SOUTH`, `NORTHEAST`,
`PLAINS`, `SOUTH CENTRAL`, `SOUTHEAST`, `WEST`). No empty strings in
any string column.

Flat, no partition, for the same reason as the week table.

## 5. `iri_product_stub`

The only per-file-schema class here. Four editions, four directories,
four filename prefixes, and no schema any two of them fully share:

| directory | years | prefix | format | files |
|---|---|---|---|---:|
| `parsed stub files/` | 1–6 | `prod_` | `.xls` (BIFF8) | 34 |
| `parsed stub files 2007/` | 7 | `prod_` | `.xlsx` | 31 |
| `parsed stub files 2008-2011/` | 8–11 | `prod11_` | `.xlsx` | 31 |
| `parsed stub files 2012/` | 12 | `prod12_` | `.xlsx` | 31 |

124 ingested, 634 824 rows, 23.2 MiB, 6.07 s, 0 failures.

### Per-file schema, and why that is the right call

Columns 1–12 are stable (`L1 L2 L3 L4 L5 L9 Level UPC SY GE VEND
ITEM`). Column 13 is the stub spec (§2.4 above). After that the
attribute columns are **per-category and drift** — `PRODUC_TYPE`,
`TYPE OF BEER/ALE`, `PACKAGE`, `FLAVOR/SCENT` for beer; a different set
for salt snacks. A single crate-wide schema would mean inventing ~69
all-null columns for the sparse categories, or dropping attributes for
the dense ones.

So each stub writes its own schema, and the manifest records a digest
of its column names in `source_schema_fingerprint`. That is separate
from `output_schema_version` for the reason `ARCHITECTURE.md` § *Two
kinds of schema* records: the version answers "has the code changed?"
and is compared *before* the source is parsed; the fingerprint answers
"did this file's columns change?" and is only knowable after reading
the header. Conflating them makes every per-file-schema class
un-skippable.

### Partitioned by edition

```
bronze/iri_product_stub/edition=2001-2006/part-….parquet
bronze/iri_product_stub/edition=2007/…
bronze/iri_product_stub/edition=2008-2011/…
bronze/iri_product_stub/edition=2012/…
```

The stub *directory* encodes the edition and hence the year range; the
file itself carries no year. Edition is the partition key so the four
editions coexist rather than overwriting one another.

### What discovery excludes, and why

```
product_stub  files=124  193.0MiB  skipped=4  editions=2001-2006/2007/2008-2011/2012
    skipped by discovery: lock_file=1, size_variant_excluded=3
```

- **3 × `_sz`** (`prod_beer_sz.xls`, `prod_carbbev_sz.xls`,
  `prod_mustketc_sz.xls`) — alternate pack-size definitions for the
  same product, not separate products. Ingesting them would write
  duplicate UPCs into the same table.
- **1 × `~$prod_factiss.xlsx`** — an Office lock file, 165 bytes.

Both exclusions are asserted in
`stub_discovery_excludes_size_variants_and_lock_files`, which also
fails if an excluded filename reappears in `inv.files`.

### Filename drift is the first problem, not the parsing

Stub filename codes change across the four directories and the mapping
from code to canonical category is not derivable from the filename
alone. The canonical year × category matrix is tracked separately (the
Julia reference pipeline's `reference/prod.csv`). Bronze does **not**
resolve it — that is Silver's job (§5.3 of the silver plan), because it
is a decision about canonical naming rather than about reading bytes.
Bronze preserves the filename and the edition and stays lossless.

## 6. Testing

| where | what |
|---|---|
| `src/datasets/excel.rs` unit tests | serial→date conversion, the 1900 phantom-day correction, time-of-day truncation, ISO-date text cells, stub column normalisation, schema shapes, type widths, batch sizing |
| `tests/excel_ingest_tests.rs` | full pipeline: discover → parse → Parquet → manifest → skip, against the real corpus |

### Why the integration tests read the corpus

Every other integration test in this suite builds a byte-level fixture
in a tempdir. **That trick does not work for Excel.** calamine is a
*reader* — it cannot write a workbook — so a hermetic test would mean
embedding a pre-built `.xls`/`.xlsx` binary in the repository, which is
exactly what the shared fixtures exist to avoid: a binary in git cannot
be reviewed, and silently rots when a parser changes.

Reading the real corpus has a genuine advantage beyond convenience. The
Excel failure modes are almost all *shape* failures — a documentation
sheet read as data, a column at a different position, a value type that
differs from what the schema claims — and only a real workbook has
them. A hand-built fixture would have been designed to match the
parser.

The cost is that the tests need `data/raw/` staged. When it is absent
they **skip with a message**, not fail: a missing corpus is an
environment fact, not a parser regression. Set `IRI_LAKE_TEST_RAW` to
point elsewhere. The pure logic is covered by unit tests that run
unconditionally, so skipping the corpus-bound tests never leaves the
conversion code untested.

Current state — **203 tests pass**, clippy clean under `-D warnings`:

```
running 10 tests   (excel_ingest_tests)
test the_week_dimension_covers_all_626_weeks_including_the_year_6_7_gap ... ok
test week_1114_resolves_to_the_first_calendar_week_of_2001 ... ok
test the_fips_market_table_parses_with_types_that_fit_the_data ... ok
test the_fips_documentation_sheet_is_not_ingested_as_data ... ok
test stub_editions_parse_into_per_file_schemas ... ok
...
test result: ok. 10 passed; 0 failed
```

### Probing a workbook

`examples/probe_excel.rs` dumps sheets, dimensions and the first rows
of any workbook, with the cell variant annotated (`F:` float, `I:`
int, `S:` string, `D:` date):

```bash
cargo run --release --example probe_excel
```

This is how the on-disk shapes in this document were established. It is
a diagnostic, not a gate.

## 7. Resuming

Skip-if-present is the default, and it works through the shared manifest
like every other class: `week_dimension` and `fips_market` were each
ingested, re-run, and confirmed to skip rather than duplicate.

One subtlety applies to all three classes and is worth stating because
it has bitten this codebase before: the **prospective** manifest record
is built *before* the parser exists, so it must carry the **class's**
`schema_version()`, not whatever the parser later reports. While the two
disagreed, no source could ever be skipped and a re-run silently
re-ingested everything into new files. `the_week_dimension_is_skipped_on_a_second_run`
is the regression test.

`--overwrite` replaces a source's output set rather than writing beside
it (`docs/corpus_readiness.md` §7, decision 7).

### The case `--overwrite` does not reach

Bumping `FIPS_MARKET_SCHEMA_VERSION` surfaced a bug that had been live
since the non-sales driver was written. `plan_output_paths` embeds the
current run's short id in every filename, so a re-ingest can never
collide with the prior run's files — which means the superseded set is
only removable by an explicit reap. The driver's reap fired **only**
inside the `OverwriteMode::Overwrite` arm, and that arm is reached only
when `skip_decision` returns a match. A schema-version change is
precisely a *non*-match, so it fell straight through: re-ingest, write
a second file, orphan the first.

Observed: 3 141 rows written, **6 282** read back. Gate 6's 6.1
row-count reconciliation caught it, which is the only reason it was
found before shipping — the ingest summary reported success.

Two defects, both fixed:

1. **No reap on a version change.** Any prior record whose output this
   run supersedes is now stashed and reaped — whether or not
   `--overwrite` was given.
2. **The reap ran before the write, contradicting its own comment.** The
   comment said "Reap only after the replacement is durably recorded"
   while the code deleted first. An interruption between the two would
   have left the source with neither the old output nor the new. The
   reap now runs after the Parquet write *and* the manifest append, and
   skips any path this run just wrote.

`a_schema_version_change_replaces_the_output_rather_than_duplicating_it`
covers it, and fails with both orphaned paths named if the reap is
removed. The sales pipeline already had this right
(`src/ingest.rs`, `prior_outputs_to_remove`); the non-sales driver now
matches it.

## 8. Open questions

1. ~~**Null policy for absent cells.**~~ **Resolved — fixed in schema
   v2.** v1 mapped an absent cell to `""` and declared the string
   columns `NOT NULL`, which conflated "the source did not say" with
   "known to be blank" and made `WHERE county IS NULL` return zero rows
   on a table that might contain unknowns — a coverage audit that could
   not fail. `cell_string` now returns `Option<String>`, `Data::Error`
   (`#REF!`, `#N/A`, …) maps to `NULL` rather than a blank county name,
   and the columns are nullable. This matches [`StubParser`], which
   already wrote `NULL` for `Data::Empty`, so the two Excel parsers in
   this file now agree.

   The corpus has no absent cells in this workbook's data sheet (the
   three `Empty` cells are all in the `Notes` documentation sheet, and
   there are zero `Error` cells anywhere — verified with
   `probe_excel --scan`), so this changes no today's row values. It
   bumps the version anyway, because the schema changed.
2. **`panel complete store list with chains.xlsx`** (219 rows:
   `IRI_Key`, `Outlet`, per-year record counts, `Alias`) is the fourth
   Excel source in the corpus and is **not** ingested. The silver plan
   lists it as "optional enrichment, decide later" (§5.2, item 4) and it
   is not a prerequisite for either the calendar or the store/chain
   join, so building it now would be scope ahead of a decision.
3. **Stub → sales key correspondence.** ~~Unverified.~~ **Resolved.**
   The key is the 4-tuple `(SY, GE, VEND, ITEM)`, not a UPC. See
   [`silver_layer.md`](silver_layer.md) §9.2 for the measurement and
   §5.3 for the two ways to get the join wrong.
4. **Gate coverage.** Closed. `sql/gate6_non_sales.sql` covers both
   new classes: `fips_market` is in the 6.1 manifest-vs-lake row-count
   union, and 6.3b asserts one row per county with no empty strings,
   reporting `county_null` so an unknown-value regression is visible.
   6.3 already asserted the 626-week coverage independently, which is
   what confirmed the year 6–7 finding in §3.
5. **The corpus spells "unknown" three ways, and two of them are not
   `NULL`.** Beyond the FIPS fix above:

   | encoding | where | example |
   |---|---|---|
   | SQL `NULL` | a column absent from an edition's schema | `PACKAGE` (27 741 rows, edition `2001-2006`) |
   | the literal string `MISSING` | stub attribute values | `FLAVOR_SCENT` (129 889 rows, 20.5%) |
   | `NULL` | FIPS, since v2 | none today |

   `WHERE col IS NULL` catches only the first and third. A Silver view
   that reports "known" attributes with `IS NOT NULL` will overcount by
   every `MISSING` value. This is a Silver-layer decision about what
   `MISSING` *means* — it is a value IRI wrote deliberately, not a
   parser artifact — so it is recorded rather than normalised here.
   `probe_excel --scan` reports per-variant cell counts for any
   workbook, which is how the `MISSING` share was measured.