# The Silver layer — plan

Status: **plan only**. Nothing in this document is implemented. Every SQL block is a
sketch of the shape a view would take, not a committed gate file.

## Preface

### Why a lake has three layers

The medallion split is not decoration; each layer buys a specific property by giving
something up.

**Bronze** is the *archival* layer. Its contract is deliberately narrow: one source file in,
one Parquet output set out, no interpretation. It preserves the bytes, the provenance, and
the ability to re-derive everything, so a parser bug is always recoverable by re-running.
What Bronze cannot give you is *query shape* — the same fact is 31 times over, in three
schemas, with no stated grain, and nothing in the layout prevents joining two things that
do not actually share a grain. Bronze is auditable and almost unusable.

**Silver** is the *conformity* layer, and it is where the lake becomes queryable. Silver
answers three questions Bronze deliberately refuses to: **what is one row here** (grain),
**what is the key** (identity, when the source gives you the same content twice), and
**what does this value mean in time** (as-of semantics). A consumer reading Silver should
never have to read a source-format document to write a correct query. Everything Silver
adds — a typed column, a normalised dimension, a dedup — is paid for with the ability to
audit it back to a byte range in Bronze.

**Gold** is the *consumption* layer: marts shaped around specific questions. It is not
planned here (§3).

This branch exists because Bronze hit its limit. The sales fact is fully ingested and
reconciled — 744 sources, 2.7006 B rows, Gate 5 green — and it is still the awkward table:
`week` is an opaque IRI counter with no date, `iri_key` resolves to nothing, `vend`/`item`
resolve to nothing. The interesting questions ("what did category X do in the week
starting Nov 3", "which chain drove the price-reduction lift") are unanswerable, and no
amount of Parquet at Bronze fixes that. Silver is where those dimensions get built.

### Why the choices in this document

Each choice below trades something. The trade is the argument.

**Silver is derived from Bronze, never a second ingestion path.** Excel sources are
ingested once, into Bronze (§5.1, §5.3), and Silver reads them like anything else. Two
independent paths to the same data means two sets of bugs and no way to reconcile them.
Corollary: several of the largest items here are *Bronze* prerequisites, and §8 sequences
them first. This is the single biggest scope driver in the plan.

**Grain and identity are stated per table, in writing (§6).** Bronze can carry an
unexamined grain because only one table exists. Add six more and an unstated grain becomes
a silent wrong answer — a trip-level household table joined to a store-week aggregate
produces plausible numbers that are simply not counts of anything. The grain inventory is
the highest-leverage page in this document.

**As-of, not last-write-wins, for anything that changes over time (§5.2).** Store chain
assignment genuinely drifts across 12 years. Collapsing it to current state would make 1998
beer sales join to a 2018 chain name — and that failure is invisible in output, because the
number still adds up. Effective-dating forces the decision into the schema and makes the
as-of join the only join that works.

**Content hash, not filename, is identity (§5.2, §5.4).** `Delivery_Stores` appears 31
times per year with 6 distinct contents; `DEMOS.CSV` is 31 copies of one panelist pool;
three files exist at both the top level and under `demos trips external/`. Dedup on
filename silently loses the fact that the copies *were* copies; dedup on content hash keeps
one canonical row set plus an explicit provenance mapping. If two copies ever diverge,
that becomes a finding instead of a silent overwrite.

**A gap is labelled, not filled (§5.1, §5.3, §5.4).** Years 6–7 have no week translation,
`prod_attr` exists only for years 9–11, `DEMOS.CSV` only for years 3–5 and 8–11. The
temptation is to interpolate or to coalesce to a default so joins stay quiet. Then
`WHERE date_source = 'captured'` returns different answers before and after someone
"improves" the coverage, and the date a consumer sees stops being traceable. `NULL` plus a
`date_source` label keeps the meaning stable as coverage improves.

**Derive what is derivable, store what is captured.** The rule already recorded in
`ARCHITECTURE.md`, and the reason `source_year` stays a view expression over
`week_to_year` rather than a persisted column (§5.1). Storing it would let a row exist
whose `week` and `source_year` disagree, and would cost a byte per row to make that
possible. `calendar_start_date` *is* stored, because it needs a join to another table —
the join is the justification, not the derivability.

**Views first, tables on a stated criterion (§3).** With 2.7 B rows beneath them, a
materialised Silver table is expensive to be wrong about and cheap to delay. Promotion
costs a file and reuses the existing skip/idempotence machinery. The corollary is the
biggest scope decision in the plan: **no Silver fact table over sales is proposed.** Four
joins over a billion-row scan, where every consumer wants a different projection, is a
Gold mart's problem.

**Gates extend to Silver, they do not appear in it.** Validation that only runs on demand
is validation nobody runs. The Sentinel sweep of Gate 5d is the model, and §7 lists the
Silver checks that would follow it — deliberately not written yet, because this layer's
design is not agreed and an executable check becomes a contract.

The honest counterweight to all of this is §9.6: the calendar dimension may be the entire
near-term requirement. The other four domains are drafted so their shape is settled, not
so they all get built.

Two questions were left open when this was drafted; the defaults chosen were:

1. **Deliverable** — this document. No Rust, no Makefile targets, no files under
   `sql/`. Rationale: the Bronze lake's gate discipline (`make gate2`, `make gate5`,
   `sql/gate5_reconciliation.sql`) means anything executable becomes something that can
   fail CI, and the Silver design is not yet agreed. Committed views would be a
   premature contract.
2. **Domains** — all four non-sales domains are covered (calendar, store & chain, product,
   household panel), with depth concentrated on calendar and store, because those are the
   two that the existing sales analysis actually joins against.
3. **Materialization** — views first; promote to a table only on a stated criterion (§3).

If either default is wrong, the change is localized: §5 is per-domain and independent, so
dropping a domain or promoting one early does not disturb the others.

## 1. What Silver is for, here

Bronze's contract is one source file in, one Parquet output set out, with no
interpretation: `manifest.jsonl` records what was written and the directory layout carries
`(year, category, channel)`. That contract is good and should not change.

The non-sales sources break Bronze's assumptions in three specific ways, and Silver is
where they get fixed:

| Problem | Source | Bronze's behaviour today | Silver's fix |
|---|---|---|---|
| **Duplication** | `Delivery_Stores` (6 distinct MD5s/yr), `DEMOS.CSV` (same panelist pool per yr) | 31 near-identical copies per year | content-hash canonicalisation; one row set per *content*, one mapping table from file → canonical |
| **Schema drift mid-series** | PANEL: tab → whitespace → comma; `MINUTE` appears at yr 8; `OUTLET` codes DR/GR/MA → DK/GK/MK/(KK at yr 8 only) | three parsers, three shapes, no unified read path | one unified schema with explicit `channel_code` and a documented `source_format` |
| **Slowly-changing dimensions** | store → chain, item → product attributes, week → date | n/a | effective-dated rows, resolved as-of by the joining fact's week |

A fourth, smaller one: **grain is implicit**. `iri_sales` is one row per
(store, week, vendor, item) weekly aggregate. Nothing in Bronze says so. Silver states
the grain of every table in writing, because a wrong-grain join is the most expensive
mistake available in a lake this size.

The principle already recorded in `ARCHITECTURE.md` holds and is restated here as the
rule Silver is measured against: **derive what is derivable, store what is captured.**
`source_year` stays out of the rows (a pure function of `week`, §Partitioning of
`ARCHITECTURE.md`). `calendar_start_date` *is* a Silver column, because it needs a join
to another table — the join is the justification, not the derivability.

## 2. Layout

```
data/lake/silver/
├── calendar/          # silver_week_dimension
├── store/             # silver_store, silver_chain, silver_market, store<->chain map
├── product/           # silver_product, silver_product_attribute
└── household/         # silver_panel_trip, silver_household, silver_panelist_static
```

- Hive partitioning, one directory per domain, and only where the partition key is
  *truly* non-derivable and high-cardinality in a useful way. `silver_week_dimension`
  carries `year` as a physical column rather than a partition: it is 626 rows.
  `silver_store` carries `valid_from_year`/`valid_to_year` as columns, not partitions —
  the set of stores valid in year *n* is a predicate, not a directory tree.
- Physical partition columns appear in the Parquet **only** where they are also
  partitioning keys, matching the Bronze convention (`ARCHITECTURE.md`,
  "The Parquet files do NOT contain partition columns"). Divergence from Bronze here is
  a deliberate inconsistency to be revisited once two Silver tables exist.
- Every Silver view/table declares its grain, its natural key, and its join keys to
  `iri_sales` in a header comment. Views are not exempt.

## 3. Materialization: views first, with a promotion criterion

Default: a Silver *domain* is a set of DuckDB views over Bronze Parquet.

A view is promoted to a materialized Parquet table when **any** of these holds:

1. A query against it is measured at more than ~60 s and is on a path someone runs
   repeatedly (a gate, a dashboard, a notebook).
2. It must be read by a non-DuckDB consumer (Spark, Polars, a teammate's script).
3. It depends on a Bronze source that is expensive to re-read (the 2.7 B-row sales scan
   behind any enriched fact), rather than only on small dimensions.
4. Its correctness depends on an expensive validation that should not re-run on every
   query (the Sentinel sweep of §4).

Promotion is then mechanical: the view's SQL becomes the materializer's `SELECT`, the
Bronze inputs get their own manifest records with `output_schema_version`, and the
existing skip/idempotence machinery applies unchanged. That is the reason to prefer
views — promotion costs a file, not a rewrite.

**No Silver fact table is planned for the 2.7 B sales rows yet.** The tempting view —
"sales + calendar date + product + store, joined" — is four joins over a billion-row
scan, and every consumer will want a different projection of it. Gold marts are the right
home for that; Silver's job is to make the *dimensions* correct and joinable.

## 4. Conventions

- **Types.** Silver reuses Bronze's type-tightening rules (`README.md`, "Type-tightening
  rules"): no `FLOAT` for money, no `Int32` where `Int64` is needed, `Boolean` only after
  the 0/1 check has actually been run. `dollars_cents` conventions carry over unchanged.
- **Sentinels.** The Gate 5d sweep (`iri_key = 0`, negative dollars, unknown feature) is
  the model. Silver adds its own per-table sentinel list and checks it in a gate, not in
  the view — a view that silently filters bad rows hides the count.
- **NULLs mean "unknown", never "zero".** A missing week translation is `NULL` plus a
  `date_source` label (§5.1), not a fabricated date and not `1970-01-01`.
- **Schema versioning.** `silver_schema_version` per table, recorded in whatever manifest
  record the materialized table produces, following `output_schema_version`'s pattern.
- **Provenance.** Every Silver table carries the Bronze source(s) it was derived from —
  at minimum the set of `source_path`s (after canonicalisation, the canonical path). A
  Silver row that cannot name its origin is not in the lake.
- **Idempotence.** Views inherit it for free. Materialized tables inherit the existing
  SHA + parser-version + config skip decision, unchanged.

## 5. Domains

### 5.1 Calendar — `silver_week_dimension`

**Bronze prerequisite (not yet built).** Ingest the IRI Week Translation `.xls` files:
`IRI week translation.xls` (years 1–5, per category directory) and
`IRI week translation_2008_2017.xls` (years 8–12). This is the `calamine` thread already
open on this branch (`Cargo.toml` + `examples/probe_excel.rs`). Output:
`bronze/iri_week/week=<week>.parquet` or one file per source, keyed by `WEEK`.

**Grain.** One row per IRI `WEEK` (1114–1739, contiguous and non-overlapping; years 6 and
12 have 53 weeks). This is the smallest and highest-leverage table in the lake.

**Schema.**

| column | type | notes |
|---|---|---|
| `week` | `UInt16` | IRI week number; natural key |
| `source_year` | `UInt8` | derivable from `week`; see below |
| `calendar_start_date` | `Date32` | NULL when not captured — see the gap policy |
| `calendar_end_date` | `Date32` | the IRI academic week end, as captured |
| `date_source` | dictionary `UInt8` | `captured` / `interpolated` / `missing` |
| `source_file` | `Utf8` | provenance: which translation `.xls` this came from |

**`source_year` stays a view expression, not a column.** `fixed_width::week_to_year` is a
`match` over hard-coded, stable ranges; storing it would be redundant data that can
disagree with `week` in the same row — the exact failure mode `ARCHITECTURE.md` already
rejected for Bronze. The column is exposed in the view and computed, never persisted.

**The year 6–7 gap.** Both translation files are missing for those years (documented in
`docs/data_layout.md` §2.3), and `demos trips external/IRI week translation.xls` may cover
part of it — this needs to be verified by the calamine probe before the policy is fixed.
Proposed policy, in preference order:

1. **Captured** where a translation file provides the week.
2. **Interpolated** — only if an adjacent year's translation establishes the anchor dates
   and the 53-week year 6 boundary is provable — written to *separate rows or a separate
   table*, flagged `interpolated`, never merged silently into `captured`.
3. Otherwise `calendar_start_date IS NULL`, `date_source = 'missing'`.

A consumer filtering `WHERE date_source = 'captured'` must get the same answer whether or
not interpolation has been done. That is the whole point of the label.

**Joining to sales.** `iri_sales.week = silver_week_dimension.week`. On 2.7 B rows this is
a broadcast hash join on a 626-row build side — effectively free, which is the argument for
doing this in Silver rather than as a materialized sales rewrite.

### 5.2 Store & chain — `silver_store`, `silver_chain`, `silver_market`

The heaviest domain: duplication, masking, and SCD all at once.

**Bronze prerequisites.**

1. `Delivery_Stores` — present in every (year, category); **6 distinct MD5s per year**
   across 31 categories. Ingest all copies, then canonicalise by content hash. The
   expected shape is `bronze/delivery_stores/content=<md5>/year=<N>.parquet` plus a
   mapping from each of the 31 (year, category) paths to its canonical content set.
2. `masked_chain_xref*.csv` — chain cross-reference; note three overlapping windows
   (`1_5`, `1_7`, `1_12` reconstruction) plus a top-level duplicate. Ingest the widest
   (`1_12`) and record that the narrower ones are subsets.
3. `fips by IRI market.xls` — FIPS county → IRI market. Excel, so `calamine`.
4. `panel complete store list with chains.xlsx` — optional enrichment, decide later.

**Grain.** `silver_store`: one row per (`iri_key`, `source_year`) — a store's attributes
as of that year. `Chain87` in year 1 and year 8 are two rows, not one row and not a
conflict. `silver_chain`: one row per (`chain_id`, `source_year`).

**Keys.** `iri_key` (`UInt32`, the sales fact's store id) is the join key. The crosswalk to
sales is `iri_sales.iri_key = silver_store.iri_key` **and** `year`, because store
attributes are year-scoped and sales carries year only as a Hive partition column.

**Why effective-dating rather than last-write-wins.** Chain assignment genuinely changes
over 12 years. A current-state `silver_store` would make 1998 beer sales join to a 2018
chain name — silently, and plausibly enough that nobody would notice in a chart. The
as-of join must be forced by the schema:

```sql
-- sketch: as-of join, not equality join
SELECT s.iri_key, f.dollars_cents, c.chain_name
FROM   silver_sales_view f
JOIN   silver_store s
  ON   s.iri_key = f.iri_key
 AND   s.source_year <= f.source_year
 AND   (s.valid_to_year IS NULL OR s.valid_to_year > f.source_year)
LEFT JOIN silver_chain c USING (chain_id, source_year);
```

**Dedup rule.** Content hash decides canonical identity; the *file* is not identity. Two
`Delivery_Stores` files with the same MD5 in different category directories are one
content set, and the mapping table records all 31 provenance paths. Any future divergence
between two copies that currently share an MD5 becomes a real finding, not a silent
overwrite.

**Open.** `OU` codes (DR/GR/MA → DK/GK/MK, KK at yr 8 only) map to channel; the mapping
belongs in `silver_store.channel_code` as a dictionary column with an explicit
`valid_from_year`/`valid_to_year`, and the year-8-only `KK` needs a decision (real channel
vs one-year anomaly) before it becomes a dimension value rather than a null.

### 5.3 Product master — `silver_product`, `silver_product_attribute`

**Bronze prerequisites.** ~200 MB of stubs in four directories: `parsed stub files/`
(yrs 1–6, `.xls`/BIFF), `parsed stub files 2007/` (yr 7), `parsed stub files 2008-2011/`
(yrs 8–11, `prod11_` prefix), `parsed stub files 2012/` (yr 12, `prod12_` prefix). Skip
the three `_sz` size variants and `~$prod_factiss.xlsx`. `calamine`'s `open_workbook_auto`
covers both BIFF and OOXML in one reader, which is the argument for doing this in Rust
rather than the `xlrd` + `openpyxl` pair the Julia/Python path assumed.

**Filename drift is the first problem, not the parsing.** The stub filename codes drift
across the four directories, and the canonical category mapping is already tracked in git
at `reference/prod.csv` (year × category matrix). Silver must carry the *canonical*
category, derived from that matrix, not from the filename.

**Grain.** `silver_product`: one row per (UPC, source_year). Stub files cover overlapping
years; the union is upserted by UPC with `first_seen_year`/`last_seen_year`, so a UPC
absent in year *n* but present in *n+1* is a gap, not a delete.

**Keys.** `silver_product.upc` (or `item`/`vend` split as the stub provides) joins to
`iri_sales.vend`/`iri_sales.item`. The join is **not** 1:1 — the sales file's `VEND`/`ITEM`
pair is an IRI item code, not a UPC — so the stub→sales key correspondence must be
established empirically before any view claims to join. If it cannot be established, the
stub tables stay dimensions for description and do not pretend to enrich sales.

**`prod_attr`** (years 9–11 only) is a per-UPC attribute text table, not a stub. Treat as
optional enrichment with an explicit year range; its absence in years 1–8 and 12 must
survive into Silver as a labelled gap, not as a missing row that looks like a null value.

### 5.4 Household panel — `silver_panel_trip`, `silver_household`, `silver_panelist_static`

The most work and the most drift. Deprioritise until calendar and store are settled; this
section records the shape so the decision is not re-litigated later.

**Bronze prerequisite.** PANEL files in three formats (yrs 1–2 tab-delimited, yrs 3–7
whitespace, yrs 8–12 comma with `MINUTE`); dispatch on the header line, as
`ARCHITECTURE.md` planned extension 1 already specifies. Plus `demos trips external/`:
`trips<n>*.csv` (12 years, cols `PANID, WEEK, IRI_Key, MINUTE, CENTS998, CENTS999` with
later years adding columns), `static*.csv` (`PANID, Trip_Count, make_static, year`), and
`DEMOS.CSV` (years 3–5, 8–11, ~40 demographic columns, duplicated per category).

**Grain.** `silver_panel_trip`: **one row per (panid, week, outlet/iri_key, upc)** — a
household's purchase at one store in one week. This is the critical distinction: PANEL is
trip-level and household-keyed, sales is store-week-item aggregate. They are different
grains and must never be unioned into one table. The `OUTLET` → `iri_key` mapping comes
from `silver_store`; the PANID is not a store id, ever.

`silver_household`: one row per `panid`, joining `DEMOS.CSV` demographics to trip counts.
`DEMOS.CSV` is deduplicated within a year (all 31 categories share the pool) by content
hash, exactly like `Delivery_Stores`.

**Dedup rule.** Top-level duplicates of `trips12 may13.csv`,
`manual store entry external 8_12.csv`, and
`masked_chains crossreference1_12 RECONSTRUCTION Oct2018.csv` are the same content as their
`demos trips external/` counterparts. Ingest once; record the second path as provenance.
The `.zip` copies of years 8–11 trips are a third copy, not a fourth dataset.

**Long-window files win.** `static 1_12.csv` supersedes `static 1_7.csv` and `static1_5.csv`;
ingest the widest and note the narrower ones as subsets — same rule as the chain xref.

**Privacy note worth stating now.** `silver_household` carries panelist demographics
(income, race, age, education, county) keyed by a stable `panid`. That is person-level
sensitive data. It should be marked as such in the schema comment, access-limited
separately from the sales lake, and not joined to sales in any view that leaves the
machine. Worth an explicit decision before this domain is built.

## 6. Grain & key inventory

The one table worth keeping accurate. Everything else in this document is elaboration of
these five rows.

| Silver table | Grain (one row per…) | Natural key | Joins to `iri_sales` on | Blocking Bronze prerequisite |
|---|---|---|---|---|
| `silver_week_dimension` | IRI week | `week` | `week` | Excel ingest of week translation (.xls) |
| `silver_store` | store per year | (`iri_key`, `source_year`) | `iri_key` + year | `Delivery_Stores` ingest + MD5 canonicalisation |
| `silver_chain` | chain per year | (`chain_id`, `source_year`) | via `silver_store` | `masked_chain_xref` ingest |
| `silver_product` | UPC per year | (`upc`, `source_year`) | `vend`/`item` (**unverified**) | stub ingest (.xls/.xlsx) + filename matrix |
| `silver_panel_trip` | household purchase | (`panid`, `week`, `iri_key`, `upc`) | **none** (different grain) | PANEL 3-format ingest + trips CSV |
| `silver_household` | panelist | `panid` | **none** | `DEMOS.CSV` ingest + dedup |

## 7. What a Silver gate would check

Not written yet, but the checks follow Gate 5's shape (`sql/` + `scripts/run_sql.py`, each
column ending `_ok`, non-zero exit). Candidates, in rough order of value:

- **Coverage.** Every `week` in `iri_sales` has exactly one row in
  `silver_week_dimension`; the count of weeks with `date_source != 'captured'` is reported
  and *expected* for years 6–7 rather than treated as failure.
- **Key integrity.** `count(DISTINCT (iri_key, source_year)) = count(*)` in
  `silver_store`; no duplicate UPC keys in `silver_product`.
- **Dedup.** The number of distinct content hashes per year for `Delivery_Stores` matches
  the documented 6 — a change there is a corpus finding, not noise.
- **SCD sanity.** Every `silver_store` row's `valid_to_year` is `NULL` or greater than
  `source_year`; no overlapping validity windows for a given `iri_key`.
- **Join cost.** The as-of join in §5.2 is measured before it is relied on — if it does not
  broadcast cheaply, §3 promotion criterion 3 applies to the *joined* table, not to the
  sales scan.
- **Null policy.** `calendar_start_date IS NULL` count per year is pinned to a known
  number, so an accidental regression in the Excel parser shows up as a delta.

## 8. Sequencing

Prerequisites before any Silver view is worth writing:

1. **Excel → Bronze** (calamine): week translation first. It is the smallest source, has
   the fewest consumers, and unblocks the highest-value join. Stubs second, fips third.
2. **`Delivery_Stores` → Bronze with MD5 canonicalisation** — needed for any store-keyed
   analysis and for the SCD design to be testable.
3. Then the §5.1 and §5.2 views, then a Silver gate.
4. Product and household only after that, and household last (§5.4).

## 9. Open questions

1. **Years 6–7 week translation.** Verify against `demos trips external/` whether the gap is
   fully or partly covered before fixing the interpolation policy (§5.1).
2. **Stub → sales key correspondence.** Does `iri_sales.vend`/`item` join to a UPC in the
   stubs? Unverified, and §5.3's usefulness depends on it.
3. **`KK` channel (year 8 only).** Real channel for one year, or an anomaly to be folded
   into an existing channel?
4. **Chain xref window.** `1_12` reconstruction is a *reconstruction* — is it authoritative
   for years 1–5, or does it override `1_5`/`1_7` where they disagree?
5. **Panelist data handling.** Confirm the access/segregation policy for
   `silver_household` before any ingestion work starts (§5.4).
6. **Does anything actually need Silver at all yet?** The honest possibility is that the
   calendar join alone is the entire near-term requirement, in which case §5.3–§5.4 are
   parking, not a roadmap.
