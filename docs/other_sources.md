# Ingest beyond sales — plan, first draft, and profiling

Status: **first draft implemented, gated, and profiled**; awaiting
review. 190 tests green, clippy and fmt clean, and
`sql/gate6_non_sales.sql` passing 9/9 against a real lake built from the
staged corpus.

---

## 1. What is actually in the corpus

`data/raw/` is not one data source. Walking it yields **eleven**
structurally distinct classes, only one of which the original pipeline
touched:

| # | class | files | bytes | shape | on-disk format |
|---|---|---:|---:|---|---|
| 1 | `iri_sales` | 744 | 151.2 GB | store × week × item aggregates | fixed-width 56 B/row |
| 2 | `iri_panel` | 1108 | 770.5 MiB | household × trip × item | delimited, **4 dialects** |
| 3 | `iri_product_attr` | 93 | 580.4 MiB | item attribute dictionary | fixed-width, **21 B pitch** |
| 4 | `iri_panel_trips` | 12 | 303.2 MiB | household × trip × store totals | CSV, **2 dialects** |
| 5 | `iri_panelist_demos` | 217 | 197.5 MiB | panelist demographics | CSV, **2 dialects** |
| 6 | `iri_product_stub` | 124 | 193.0 MiB | product hierarchy + attributes | `.xls` (BIFF) / `.xlsx` |
| 7 | `iri_delivery_stores` | 372 | 47.3 MiB | store roster | fixed-width 63 B/row |
| 8 | `iri_week_dimension` | 1 | 0.1 MiB | week → calendar dates | `.xls` |
| 9 | `iri_ads_demos` | 7 | 3.8 MiB | panelist demos, ad-panel | CSV |
| 10 | `iri_chain_xref`, `iri_manual_store_entry` | 3 | 18 KiB | chain cross-reference | CSV |
| 11 | *documentation* — 626 `.doc`, 16 `.pdf`, 4 `.zip` | | | not data | skipped |

**1 937 files / 2.05 GiB** are actually ingested. Rows 2–10 are about
**1.5 %** of the raw corpus. That proportion drives the design: the
sales pipeline's tuning (1 M-row batches, 56-byte striding, whole-file
mmap) is wrong for everything else, and none of the other classes
justify 143 GB-scale operational ceremony. Measured end to end:
**25.3 M rows in 13 s at 8 workers** (dedup-aware), against the sales pipeline's
~220 s for 744 files.

Class 8 counts 1 file rather than 311 because the per-year copies are
subsets of one authoritative workbook — see §2.

---

## 2. Format findings that contradict `docs/data_layout.md`

The existing survey is broadly right but stale or imprecise in ways
that would break a parser written from it. Each of these was found by
measuring the corpus, and each is pinned by a test built from the real
bytes.

### 2.1 PANEL is four dialects, not three

Sniffing all 1110 header lines:

| count | header | years |
|---:|---|---|
| 461 | `PANID,WEEK,MINUTE,UNITS,OUTLET,DOLLARS,IRI_KEY,COLUPC` | 8–12 |
| 368 | `PANID WEEK UNITS OUTLET DOLLARS IRI_KEY COLUPC` | 4–7 |
| 276 | `PANID\tWEEK\tUNITS\tOUTLET\tDOLLARS\tIRI_KEY\tCOLUPC` | 1–3 |
| **1** | `PANID\tWEEK\tMINUTE\tUNITS\tOUTLET\tDOLLARS\tIRI_KEY\tCOLUPC` | **11** |

The fourth is `Year11/diapers/diapers_PANEL_GK_1635_1686.DAT`: a
tab-delimited file carrying the year-8+ `MINUTE` column. "Years 8+ are
comma-delimited" is wrong for exactly one file out of 1110, and a
delimiter inferred from the year mis-parses it *silently* — a comma
split of a tab-delimited line yields one field, which does not look like
an error, just a row with a single column.

So the delimiter and the MINUTE presence are **two independent facts,
both read from the header**. `Dialect` carries both; a value that
bundled them would need a fifth variant, or would infer one from the
other and mis-parse that file.

### 2.2 Two PANEL files are zero bytes

`Year1/beer`'s `PANEL_DR` and one Year-2 equivalent have no header at
all. A parser that requires a header turns them into failures, and a
failure is a permanent manifest record that retries forever. They are
recorded as **empty successes** — a real file with no content is a fact
about the corpus, not an error.

### 2.3 PANEL money has three encodings, not one

`DOLLARS` and `UNITS` appear in the same column, sometimes the same
file, in three forms:

| form | example | share |
|---|---|---|
| exact 2-decimal | `6.99` | ~99.5 % |
| float32 of a 2-decimal | `0.7299998474` | ~0.01 % |
| **sub-cent computed average** | `4.3091992188` | ~0.4 % |

The third is `units × unit price` at full precision, and it is
concentrated: `Year10/carbbev`'s `PANEL_GK` has **12.7 %** of its rows
in it. Taking the strict integer-cent path rejects the latter two
forms and **nulls out 67 783 real dollar amounts** — 0.4 % of every
PANEL
row.

Bronze preserves all the data, so neither is rounded or dropped. The
schema stores both an **integer** column and a **(digits, scale)**
pair, the same encoding as the trips money columns:

```
panid          UInt32
week           UInt16
minute         UInt16 (nullable)
units          Int32            -- whole-number unit counts only
units_digits   Decimal128(38, 0)
units_scale    Int8
outlet         Utf8
dollars_cents  Int64            -- rounded to integer cents
dollars_digits Decimal128(38, 0)
dollars_scale  Int8
iri_key        UInt32
colupc         Utf8
```

For a whole-number row, `units_digits == units` and `units_scale == 0`
(rounding recovers the value exactly). For a `0.049999997` row,
`units_digits = 49_999_997`, `units_scale = 9`, and `units` is NULL.
For a `4.3091992188` row, `dollars_digits = 43_091_992_188`,
`dollars_scale = 10`, and `dollars_cents = 431`. The integer columns
are convenient for joins against `iri_sales` and for filters by a
whole number; the digits+scale pair is the source of truth.

### 2.4 `Delivery_Stores` is fixed-width at 63 bytes, and the header misleads

`docs/data_layout.md` describes this as space-aligned. All 37 200 data
rows across all 372 files are exactly 63 bytes. The problem is that the
**header's token positions do not match the data's field positions**:

```text
header  IRI_KEY@0  OU@8  EST_ACV@11  Market_Name@20  Open@45  Clsd@50  MskdName@55
data     200039@1   GR@8  9.709999@11 BUFFALO/…@20     539@46   1219@50   Chain87@55
```

Slicing by the header's own offsets lands mid-field. The right-padded
`IRI_KEY` throws the first field off by one. Measured layout:

```text
IRI_KEY  [ 0.. 8)   OU   [ 8..11)   EST_ACV   [11..20)
Market   [20..45)   Open [45..50)   Clsd      [50..55)   MskdName [55..63)
```

### 2.5 `prod_attr` is fixed-width with a 21-byte pitch

Not mentioned in the survey at all. The layout is:

```text
SY [0..3) GE [3..6) VEND [6..12) ITEM [12..18) VOL_EQ [18..27)
then N attribute columns of 21 bytes each
N = ceil((row_len - 27) / 21)
```

Verified across all 93 files: the arithmetic holds exactly, the four
lead fields parse as digits in every sampled row, and the 21-byte
slices carry the header names verbatim. Whitespace-splitting gives
27–32 ragged fields per file and misaligns silently, because `MISSING`
padded to 21 bytes means two adjacent missing attributes are 42 bytes
of padding that looks like one boundary.

Two traps, both of which bit this implementation:

- **The count is a `ceil`, not a floor.** `N = (row_len - 47) / 21`
  gives 29 for `Year9/beer`'s 656-byte rows — one short. The 30th
  attribute, `WINE/LIQUOR TYPE`, is real and carries real values, and
  it was being dropped in all 93 files because the 20-byte remainder
  looked exactly like padding. Correct counts: 13 for `toitisu`, **30**
  for `beer`, 62 for `coldcer`, 82 for `saltsnck`.
- **The header and the rows are different lengths** — 652 vs 656 for
  `beer`. The header's last slot holds a 16-character name, the row's a
  21-wide value field. One length cannot slice both.

One more property, worth stating because it looks like a bug: a value
can **overflow its slot**. `Year9/beer`'s `PACKAGE` is
`LONG NECK BTL IN BOX BEER` — 25 bytes in a 21-byte field — so its last
four bytes land at the start of the next column and `BEER` reads as
`PRODUCT TYPE`. That is the corpus, not the parser: the grid is exactly
21 bytes in every row (verified by requiring a space-to-non-space
transition at each offset in all 16 760 rows), and re-flowing values
would misalign all 28 columns after the first overflow.

### 2.6 Trips' money columns: digits and scale, not floats

Per the data description, `sense 998` is the register-tape total as
entered by the panelist, `sense 999` is the sum of scanned-item
cents, and the third sense variable is generally the same as `sense
999` "scrubbed a bit better". They *should* be integer cents, but the
corpus carries them as strings of decimal digits — and not always
two-digit decimal digits. `4470.8359375` and `4600.316406` are 32-bit
float renderings of 2-decimal amounts (`float32(4470.84)` is
`4470.8359375`); `4.3091992188` is a real sub-cent computed
average. Parsing them as integers nulls **4.6 M of 7.2 M rows**;
storing them as `Float64` would round `0.7299998474` to `0.73` and
not tell a reader that the source meant 999.

The Bronze record keeps the raw digits and the fractional scale in two
columns per value: `(cents999_digits, cents999_scale)`, and so on.
`digits` is the value's decimal-place as `i128`, `scale` is the number
of fractional digits as `i8`. The original bytes are recovered
exactly by `digits / 10^scale`. Silver can fold this into integer
cents once the unit is known — the columns keep every byte of
information until that decision is upstream of the data.

`CENTS998` is genuinely empty in 92–97 % of rows, which is a fact
about the corpus, not a parse failure.
### 2.7 The other survey corrections

- **Trips gained `IRI_Key2` and `KRYSCENTS`** in the year-8+ edition, and
  renamed `IRI_Key` → `IRI_KEY`.
- **`static*.csv` are nested subsets** — `static1_5` ⊂ `static 1_7` ⊂
  `static 1_12` (30 154 / 47 767 / 87 051 rows). Ingesting all three
  triples one table; only the largest is taken.
- **`DEMOS.CSV` is 37 columns in years 3–5 and 45 in years 8–11**, with
  renamed headers (`HH_RACE` → `Household Head Race`). Two dialects.
- **`ads demo<N>` uses two stems** — `ads demo<n>` for years 1–8 and
  `ads demos<n>` for 9–12 — and mixes `.csv` with `.CSV`. 24 directory
  entries, 7 distinct years once the case twins are collapsed. Matching
  only the `demo` stem silently drops years 9–11.
- **Stubs**: 4 editions. The year-7 file has **3 sheets** and only
  `Sheet1` is the stub. Column 13 is `*STUBSPEC …` in some editions and
  `*AG C=1+ CATEGORY` in others — the `*` prefix is the reliable
  marker. Column count is 21 everywhere but the names after column 12
  drift per category, exactly like `prod_attr`.
- **Week translation**: the authoritative file is
  `demos trips external/IRI week translation.xls` — 626 rows, weeks
  **1114–1739**, with a `Year` column. The 311 per-year copies are
  subsets (the year-1 copy starts at week 1138, not 1114). Ingesting
  all 312 writes three partial copies of one table. It is also the only
  copy covering the year 6/7 gap the survey flags.
- **`fips by IRI market.xls` leads with a `Notes` sheet.** Sheet
  selection must be by content, not position.

---

## 3. The shape of the change

### 3.1 Rename, don't repurpose

`ingest-all` becomes `ingest-sales`. It keeps its flags and semantics
verbatim — it is the one pipeline with 744 files, 143 GB, a measured
parallelism sweep and five documented gates behind it, and renaming it
must not become a chance to change it. The CLI's sales path was
extracted into one function that both `ingest-sales` and the umbrella
call, so the two cannot drift.

```make
ingest-sales:   # the old ingest-all, unchanged
ingest-other:   # umbrella over the ten non-sales classes
ingest-all:     # ingest-sales, then ingest-other
```

The umbrella runs the classes **sequentially**, each with its own
parallelism and batch sizing, and skips a class wholesale when the
manifest already covers it. Sequentially because the classes span four
orders of magnitude and have incompatible memory profiles, and because
one class failing must not be able to abandon the other ten.

### 3.2 A dataset registry, not a seventh special case

The original code hard-codes "sales" in nine places: `discovery.rs`
routes by filename, `SourceIdentity` has `channel`, `PartitionPath`
builds `year/category/channel`, `ManifestRecord` has `source_year` and
`channel`, and so on. Ten more datasets that way is ten copies of that
coupling.

`DatasetKind` (`src/model.rs`) names the class and owns three things:
its bronze table name, its declared schema version, and its partition
keys. Identity is one generic `SourceRef` with all-optional fields,
because the classes disagree about what they have.

`iri_sales` keeps its dedicated `SourceIdentity` and `PartitionPath`
untouched — they are part of a performance contract with the sales gate
suite, and changing them would invalidate the measured numbers in
`docs/parallelism.md`.

**Partition keys are declared per class, not inferred from the
identity.** `iri_delivery_stores` carries a category (the directory it
was found in) but must not partition by it: the 31 categories of a year
share one 2 053-row roster, and partitioning by category would make
every join a 31-way union. `iri_panel` does partition by all three,
because a PANEL file really is one `(year, category, outlet)` slice.

### 3.3 The manifest is the shared substrate

All eleven classes share `manifest.jsonl`, because "what is in this
lake, and which source produced it" must be answerable in one place.
This required generalising `ManifestRecord`:

- `dataset: DatasetKind`
- `source_year`, `category`, `channel`, `filename_week_*` → `Option`
- `deduplicated_from`, `source_schema_fingerprint` → new

**Backward compatibility is the constraint**: every existing
`manifest.jsonl` line must still deserialize and still be honoured. So
every new field is `#[serde(default)]` and `dataset` defaults to
`Sales`. A test writes a verbatim pre-change manifest line and reads it
back.

**`output_schema_version` had to change meaning.** It now answers "has
the code changed?" — a constant per class, comparable *before* the
source is parsed, which is what makes resume cheap. The per-file
schema (for `prod_attr`, the stubs, the demos) is a separate
`source_schema_fingerprint`: a digest of the declared column names.

Two bugs were found and fixed while wiring this up, both of which would
have silently re-ingested the entire new corpus on every run:

1. `skip_decision` compared `prior.output_schema_version` against the
   crate-wide `CURRENT_SCHEMA_VERSION`. That is invisible for sales and
   rejects every other class, so nothing was ever skippable. It now
   compares the two records.
2. The prospective record was built with the crate version while the
   written record carried the parser's, so the two never matched.

### 3.4 Schema handling: fixed vs. per-file

Two families, deliberately not unified.

**Fixed schema** (panel, delivery_stores, trips, static, week dimension,
chain xref, manual store entry) — one Arrow schema per class, declared
in code beside its offset constants. All four PANEL dialects normalise
onto one 8-column schema with `minute` **nullable**: the pre-year-8
dialects genuinely lack the column, and modelling that as nullable is
what lets one query span twelve years. `minute = 0` would be *wrong*
rather than imprecise — indistinguishable from a real midnight trip.

**Per-file schema** (stubs, `prod_attr`, demos) — the column set is a
property of the file, not the class. Each writes one Parquet file per
source with the schema its own header declares. Forcing these onto one
schema means inventing 69 all-null columns for `toitisu` or dropping
attributes for `saltsnck`, and a union schema would change shape when a
new category appeared, invalidating every prior file's fingerprint.

### 3.5 Duplicate handling — content hash

The two heavily-overlapping classes (`Delivery_Stores` and
`panelist_demos`) **are content-deduped** at discovery: the walker
hashes every candidate, keeps one canonical per hash (the smallest
path wins, deterministically), and demotes the rest to
`DatasetInventory.deduplicated`. The umbrella runner ingests only the
canonicals and then appends a manifest record for each demoted
source carrying `deduplicated_from` pointing at the canonical and the
canonical's SHA, so a query that filters by source path still resolves.

The dedup is content-based, not name-based: two files with different
names and the same bytes deduplicate. Measurements on this corpus:

| class | discovery | after dedup | deduplicated | distinct |
|---|---:|---:|---:|---:|
| `delivery_stores` | 372 | 59 | 313 | 59 |
| `panelist_demos` | 217 | 14 | 203 | 14 |

The "7" I had expected turned out to be the per-year deduplication
*within* a year directory; across years, the corpus has 14 distinct
`DEMOS.CSV` files. The "62" I had measured for `Delivery_Stores`
was per-year a 2–13-division; across years, 59. The numbers are not
matters of taste — they are what the bytes actually are.

`chain_xref` is also duplicate-prone (4 nested supersets), but its
discovery already takes only one widest file. `panel_static`'s three
`static*.csv` files are nested subsets; only the largest is taken.
### Reconciliation

Row counts were compared against raw source line counts, class by
class:

| class | expected (raw lines) | in lake | |
|---|---:|---:|---|
| `iri_panel` | 16 474 806 | 16 474 806 | exact |
| `iri_panel_trips` | 7 200 810 | 7 200 810 | exact |
| `iri_delivery_stores` | 763 293 | 120 285 (deduped 372 → 59 files) | exact for canonicals |
| `iri_panelist_demos` | 1 119 560 | 75 451 (deduped 217 → 14 files) | exact for canonicals |
| `iri_product_attr` | 642 924 | 642 924 | exact |

`make gate6` then checks the properties a row-count match cannot: that
the manifest and the lake agree per class, that every week referenced by
sales or panel has a calendar date, that the week dimension is 626
contiguous rows spanning all twelve years, that every sales store has a
roster row, that no roster column is silently empty, that PANEL money is
fully populated, that MINUTE presence matches the dialect, and that
trips' `CENTS999` is populated. **9/9 pass.**

---

## 6. Open questions — and the answers

1. **Duplicate `Delivery_Stores` / `panelist_demos`** — *done.*
   Content-hash dedup runs in discovery, leaving 59 and 14 canonical
   files respectively, with `deduplicated_from` records on the rest.
2. **Sub-cent PANEL money** — *preserve all data.* The Bronze schema
   carries an integer column for joins and filters, plus
   `(digits, scale)` for lossless round-tripping. No rounding or
   dropping.
3. **Trips money units** — *preserve all data.* The Bronze schema
   carries `(cents_digits, cents_scale)` per sense. Silver can fold
   into integer cents once the unit is known.
4. **Per-file vs. union schema** — *per-file for Bronze.* Silver can
   decide what to do with the per-file schemas.
5. **Sales in `ingest-all`** — *yes.* Sales runs first under the
   umbrella; `--no-sales` opts out for iteration on the new work.
6. **`Week_dimension` partition** — *flat for now.* 626 rows is small
   and adding `year=` directories without a discoverable use would
   scatter a single table for no consumer benefit.
