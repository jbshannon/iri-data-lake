# The Silver layer — plan

Status: **in progress**. The product-key crosswalk and product-week aggregate have been
profiled, validated, and materialized to Silver Parquet. The full Bronze rerun is complete;
Gates 5 and 6 pass. The remaining Silver objects are still plans.

## Decisions recorded

These decisions were agreed during the Silver kickoff. They supersede conflicting draft
assumptions below. The Bronze rerun is complete and reconciled by Gates 5 and 6.

1. **Calendar view.** Create `silver_week_dimension_v`, one row per IRI week, with the
   required public columns `week`, `calendar_start_date`, and `calendar_end_date`, sourced
   from the Bronze week dimension. The 626-week mapping is captured and complete.
2. **Store identity.** Create an internal surrogate `store_id` for each annual store
   observation. Treat `iri_key` as year-scoped, not as a cross-year store identity; sales
   resolve to the annual store row using year plus `iri_key`. Masked chain names identify
   chain membership, not individual physical stores. Link annual store rows to chains
   separately and do not infer physical-store continuity from a shared chain name.
   Validate that `(source_year, iri_key)` is unique in the roster before using it as the
   annual alternate key; refine it with an additional source discriminator if needed.
3. **Market lookup.** Model markets independently of stores. Use the IRI market number as
   the market key, retain the county-to-market mapping from the FIPS Bronze source, and
   create a market-name crosswalk for matching store-roster labels to market numbers. Link
   annual store IDs to markets through that crosswalk—not by matching a market code to a
   store ID. Keep unmatched or ambiguous labels visible until resolved.
4. **Product-week sales building block.** Create `silver_product_week_v` at grain
   `(upc, week)`, summing `units` and exact `dollars_cents` across stores, categories, and
   sales channels. Preserve `(sy, ge, vend, item) ↔ upc` in a separate
   `silver_product_key_xref_v` so the aggregate retains its UPC-week grain. Full Bronze
   profiling found 324,745 complete mappings, one-to-one in both directions; the 790 stub
   rows without complete keys are excluded from the crosswalk and sales coverage is gated.
   The view evaluation took 66.39 s; Parquet materialization took 68.53 s for the aggregate
   (350,825,041 bytes) and 0.07 s for the crosswalk (3,213,051 bytes).

The market-name matching rules and annual store-key uniqueness still require focused
validation before the store and market views are built.

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

**Preserve time scope for anything that changes (§5.2).** Store and chain attributes vary
across years. Resolve sales to the annual store observation using year plus `iri_key`; use
year-scoped chain relationships rather than carrying a current label backward. Use finer
effective dating only if a source supplies it.

**Content hash, not filename, is identity (§5.2, §5.4).** `Delivery_Stores` and
`DEMOS.CSV` have repeated content across source paths; some files also have differing
content editions. Dedup on filename can lose that distinction. Content-based canonical rows
plus explicit provenance make copies auditable and divergences visible.

**A gap is labelled, not filled (§5.3, §5.4).** `prod_attr` exists only for years 9–11
and `DEMOS.CSV` only for years 3–5 and 8–11. Keep these coverage gaps explicit instead of
interpolating or coalescing unknown values to defaults. The authoritative week workbook,
by contrast, covers all 626 weeks, so the initial calendar view has no date gap to label.

**Derive what is derivable, expose what is captured.** The rule already recorded in
`ARCHITECTURE.md` is why `source_year` can remain a view expression over
`week_to_year` rather than a persisted column (§5.1). The calendar dates, by contrast,
are captured in the Bronze week dimension and exposed directly by the Silver view.

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

The calendar view is the agreed first Silver deliverable. The other domains are planned
separately and should follow only after their keys and joins have been validated.

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
| **Duplication** | `Delivery_Stores` and `DEMOS.CSV` have repeated content across source paths | many near-identical copies | content-hash canonicalisation and provenance back to source paths |
| **Schema drift mid-series** | PANEL: tab → whitespace → comma; `MINUTE` appears at yr 8; `OUTLET` codes DR/GR/MA → DK/GK/MK/(KK at yr 8 only) | several dialects, one Bronze-normalized schema | one unified schema with explicit `channel_code` and a documented `source_format` |
| **Changing relationships** | annual store observations → chain, item → product attributes, week → date | keys and time semantics are not yet consumer-ready | explicit year-scoped keys and relationships; use as-of semantics only where the source supports them |

A fourth, smaller one: **grain is implicit**. `iri_sales` is one row per
(store, week, vendor, item) weekly aggregate. Nothing in Bronze says so. Silver states
the grain of every table in writing, because a wrong-grain join is the most expensive
mistake available in a lake this size.

The principle already recorded in `ARCHITECTURE.md` holds and is restated here as the
rule Silver is measured against: **derive what is derivable, store what is captured.**
The calendar dates are exposed in Silver because they come from joining the captured Bronze
week dimension to sales; the agreed calendar view itself contains only the requested week
and date fields.

## 2. Layout

```
data/lake/silver/
├── calendar/          # silver_week_dimension_v
├── store/             # store, chain, market, county, and validated crosswalk views
├── product/           # silver_product, silver_product_attribute
└── household/         # silver_panel_trip, silver_household, silver_panelist_static
```

- Hive partitioning, one directory per domain, and only where the partition key is
  *truly* non-derivable and high-cardinality in a useful way. The 626-row
  `silver_week_dimension_v` needs no year partition. Annual store observations are
  year-scoped; do not imply continuity with `valid_from_year`/`valid_to_year` unless a
  source crosswalk establishes it.
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

**No row-level Silver copy of the 2.7 B sales fact is planned.** The requested
`silver_product_week_v` is an explicit exception: it is a reduced product-week aggregate,
not an enriched copy of every sales row. Its view definition is in
`sql/silver_product_week.sql`. Its measured full evaluation took 66.39 seconds, so it
qualifies for materialization under §3. `scripts/materialize_product_week.py` writes the
aggregate and key crosswalk as Parquet and reports their write times and sizes.

## 4. Conventions

- **Types.** Silver reuses Bronze's type-tightening rules (`README.md`, "Type-tightening
  rules"): no `FLOAT` for money, no `Int32` where `Int64` is needed, `Boolean` only after
  the 0/1 check has actually been run. `dollars_cents` conventions carry over unchanged.
- **Sentinels.** The Gate 5d sweep (`iri_key = 0`, negative dollars, unknown feature) is
  the model. Silver adds its own per-table sentinel list and checks it in a gate, not in
  the view — a view that silently filters bad rows hides the count.
- **NULLs mean "unknown", never "zero".** Do not fabricate values to make a lookup join
  appear complete. The current calendar source covers all weeks, so the initial calendar
  view needs neither null dates nor a `date_source` label.
- **Schema versioning.** `silver_schema_version` per table, recorded in whatever manifest
  record the materialized table produces, following `output_schema_version`'s pattern.
- **Provenance.** Every Silver table carries the Bronze source(s) it was derived from —
  at minimum the set of `source_path`s (after canonicalisation, the canonical path). A
  Silver row that cannot name its origin is not in the lake.
- **Idempotence.** Views inherit it for free. Materialized tables inherit the existing
  SHA + parser-version + config skip decision, unchanged.

## 5. Domains

### 5.1 Calendar — `silver_week_dimension_v`

**Bronze prerequisite — built.** Ingest the IRI Week Translation `.xls`
files with [`calamine`]: `demos trips external/IRI week
translation.xls`, which covers **all 626 weeks, 1114–1739**, years
1–12. The 311 per-year copies in the category directories are subsets
of it (the Year-1 copy starts at week 1138, not 1114) and are reported
as skipped. Output: `bronze/iri_week_dimension/`, one flat Parquet
file, keyed by `week`. See [`excel_to_bronze.md`](excel_to_bronze.md)
§3.

> **The year 6–7 gap does not exist.** The authoritative Bronze workbook covers all
> 626 weeks, including years 6 and 7. Years 6 and 12 have 53 weeks, the rest have 52, and
> each week spans Monday–Sunday. All dates in the initial view are captured; interpolation
> and a `date_source` column are not needed for this corpus.

**Grain.** One row per IRI `WEEK` (1114–1739, contiguous and non-overlapping; years 6 and
12 have 53 weeks). This is the smallest and highest-leverage table in the lake.

**Agreed view interface.** `silver_week_dimension_v` has one row per IRI week, keyed by
`week`. Its required columns are:

| column | type | Bronze source |
|---|---|---|
| `week` | `UInt16` | `iri_week` |
| `calendar_start_date` | `Date32` | `week_start` |
| `calendar_end_date` | `Date32` | `week_end` |

The initial public interface is limited to these requested calendar fields. Provenance
remains available through Bronze and its manifest; no fabricated dates or interpolation
are needed. The existing measured checks still apply: 626 distinct contiguous weeks,
1114–1739, each spanning six days.

**Joining to sales.** `iri_sales.week = silver_week_dimension_v.week`. On 2.7 B rows this is
a broadcast hash join on a 626-row build side — effectively free, which is the argument for
doing this in Silver rather than as a materialized sales rewrite.

### 5.2 Store, chain & market — Silver views

The heaviest domain: duplicated roster sources, masked chain labels, and market crosswalks.

**Bronze inputs.** The store roster, masked-chain cross-reference, manual-store reference,
and FIPS market lookup are Bronze inputs for this domain. The full-corpus rerun is complete
and Gates 5 and 6 pass; inspect actual coverage and duplicate keys before defining these
views.

**Annual store identity.** `silver_store` represents one roster observation in one source
year and assigns it an internal `store_id`. The candidate alternate key is
(`source_year`, `iri_key`), which must be checked for uniqueness against the completed
roster before it is relied on. Sales resolve to that annual record using the exact year and
`iri_key`; do not use `iri_key` alone across years or an as-of join to carry a store ID
forward.

Masked chain names are chain-level identifiers, not store identifiers. Build a separate
year-aware store-to-chain relationship from the roster and chain cross-reference. A shared
masked chain name can link a store to a chain across years, but does not establish that two
annual store observations are the same physical outlet. Any physical-store continuity would
require a separate, verified store crosswalk.

**Market lookup shape.** The FIPS Bronze table is county-grained (one row per `fips_key`)
and maps counties to `iri_market_number`; it is not a store table. Model the layers
separately:

- `silver_market_v`: one row per IRI market number, with its canonical market and region
  attributes. Preserve market number `0` as the real source value.
- `silver_fips_market_v`: one row per FIPS county, linked to its IRI market. Many counties
  may map to one market.
- `silver_market_name_xref_v`: a validated crosswalk from the store roster's market label
  to `iri_market_number`, retaining the source label, year scope if needed, and match
  status/provenance. Do not assume that similarly named fields match one-to-one.
- `silver_store_market_v`: links an annual `store_id` to a market through that crosswalk.
  Keep unresolved and ambiguous labels visible rather than silently assigning a market.

Thus market codes identify markets, not stores: do not match a market code directly to a
store identifier. Confirm the crosswalk's uniqueness and resulting join cardinality before
using it in sales analysis.

**Sales join.** Join sales to the annual store record on `year` plus `iri_key`, then obtain
chain and market relationships from that year-scoped `store_id`:

```sql
SELECT f.iri_key, f.year, s.store_id, sc.chain_id, sm.iri_market_number
FROM   iri_sales f
JOIN   silver_store_v s
  ON   s.source_year = f.year AND s.iri_key = f.iri_key
LEFT JOIN silver_store_chain_v sc
  ON   sc.store_id = s.store_id AND sc.source_year = f.year
LEFT JOIN silver_store_market_v sm
  ON   sm.store_id = s.store_id AND sm.source_year = f.year;
```

**Dedup rule.** Content hash decides canonical identity; the *file* is not identity. Keep
Bronze provenance for each canonical roster content set, and treat any divergence between
previously identical copies as a data finding rather than a silent overwrite.

**Open.** `OU` codes (DR/GR/MA → DK/GK/MK, KK at yr 8 only) map to channel; the mapping
belongs in `silver_store.channel_code` as a dictionary column with an explicit
`valid_from_year`/`valid_to_year`, and the year-8-only `KK` needs a decision (real channel
vs one-year anomaly) before it becomes a dimension value rather than a null.

### 5.3 Product master — `silver_product`, `silver_product_attribute`

**Bronze prerequisites — built.** ~200 MB of stubs in four directories: `parsed stub files/`
(yrs 1–6, `.xls`/BIFF), `parsed stub files 2007/` (yr 7), `parsed stub files 2008-2011/`
(yrs 8–11, `prod11_` prefix), `parsed stub files 2012/` (yr 12, `prod12_` prefix). 124 files,
634 824 rows, 23.2 MiB, partitioned by `edition`. The three `_sz` size variants and
`~$prod_factiss.xlsx` are excluded by discovery. `calamine`'s `open_workbook_auto`
covers both BIFF and OOXML in one reader, which is the argument for doing this in Rust
rather than the `xlrd` + `openpyxl` pair the Julia/Python path assumed. See
[`excel_to_bronze.md`](excel_to_bronze.md) §5.

**Filename drift is the first problem, not the parsing.** The stub filename codes drift
across the four directories, and the canonical category mapping is already tracked in git
at `reference/prod.csv` (year × category matrix). Silver must carry the *canonical*
category, derived from that matrix, not from the filename.

**Grain.** `silver_product`: one row per (UPC, source_year). Stub files cover overlapping
years; the union is upserted by UPC with `first_seen_year`/`last_seen_year`, so a UPC
absent in year *n* but present in *n+1* is a gap, not a delete.

**Keys.** Sales resolve through the full 4-tuple `(SY, GE, VEND, ITEM)`; the
`VEND`/`ITEM` pair is an IRI item code, not a UPC. The separate
`silver_product_key_xref_v` preserves that tuple alongside UPC. Full Bronze profiling found
324,745 complete distinct tuples and UPCs with a one-to-one mapping in both directions
across editions. The 790 rows whose key and UPC are all null are not product mappings.
The materialized crosswalk is at `data/lake/silver/product_key_xref/`.

### How stubs join to sales

Measured against the real corpus — `beer`, `blades` and `yogurt`, all 12
years, **327 M sales rows**:

```sql
SELECT s.*, st.UPC, st.L9 AS item_desc, st.PRODUCT_TYPE
FROM   iri_sales s
JOIN   iri_product_stub st
  ON   st.edition = <the edition covering s.year>   -- REQUIRED
  AND  st.SY = s.sy AND st.GE = s.ge
  AND  st.VEND = s.vend AND st.ITEM = s.item
```

| | measured |
|---|---|
| key coverage | **100%** — 0 unmatched keys, every year, every category |
| fan-out | **max 1** — many-to-one, no duplicates |
| row-count inflation | **0.000000%** |

Two ways to get this wrong, both of which produce plausible numbers:

**1. Omitting the edition scoping.** The four stub editions cover
overlapping years, so the same item key appears in up to four of them.
Joining all editions at once fans out to **4x** and silently doubles
revenue. Map year → edition explicitly: years 1–6 → `2001-2006`,
7 → `2007`, 8–11 → `2008-2011`, 12 → `2012`.

**2. Narrowing the key.** All four columns are load-bearing — the fact
that dropping one causes fan-out is what proves it is not redundant:

| join key | keys matched | max fan-out |
|---|---:|---:|
| `SY, GE, VEND, ITEM` | 55 523 | **1** |
| `GE, VEND, ITEM` | 55 523 | 3 |
| `VEND, ITEM` | 55 523 | 6 |
| `VEND, ITEM`, all editions | 55 523 | 15 |

**3. Reading the stubs without `union_by_name`.** The stubs are a
per-file-schema class, and DuckDB's default glob read takes one file's
schema:

```sql
read_parquet('.../iri_product_stub/**/*.parquet')                     -- 22 columns
read_parquet('.../iri_product_stub/**/*.parquet', union_by_name=true) -- 94 columns
```

Both return all 634 824 rows. The obvious query silently drops 72
attribute columns, including `PACKAGE` and `TYPE_OF_BEER/ALE`. Any
Silver view over the stubs must pass `union_by_name = true`.

**`MISSING` is not `NULL`.** 20.5% of `FLAVOR_SCENT` values are the
literal string `MISSING`, not a null — IRI wrote it deliberately. A view
that counts known attributes with `IS NOT NULL` will overcount by every
one. See [`excel_to_bronze.md`](excel_to_bronze.md) §8.

**Product-week sales building blocks.** `silver_product_key_xref_v` retains the full IRI
item tuple next to UPC as a reversible mapping. Its profile validates that repeated rows in
overlapping stub editions agree before canonicalizing the mapping. `silver_product_week_v`
then groups all sales by UPC and IRI week, summing units and integer cents across stores,
categories, and channels. The full evaluation and manifest coverage check are scripted in
`sql/silver_product_week.sql`; the measured evaluation took 66.39 seconds. Because the view
repeatedly scans the large sales Bronze table, `scripts/materialize_product_week.py`
persists both outputs: `data/lake/silver/product_week/` (28,314,411 rows,
350,825,041 bytes) and the key crosswalk (324,745 rows, 3,213,051 bytes). Revenue and
quantity sums are stored as exact `BIGINT` values, not floating-point values.

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

This inventory is the contract to keep accurate as the design develops.

| Silver table | Grain (one row per…) | Natural key | Joins to `iri_sales` on | Blocking Bronze prerequisite |
|---|---|---|---|---|
| `silver_week_dimension_v` | IRI week | `week` | `week` | **Bronze input present** — `iri_week_dimension`, 626 weeks |
| `silver_store_v` | annual store observation | internal `store_id`; candidate alternate key (`iri_key`, `source_year`) pending uniqueness check | `iri_key` + year | Completed `iri_delivery_stores` Bronze run |
| `silver_store_chain_v` | annual store-to-chain assignment | (`store_id`, `source_year`, `chain_id`) pending source validation | via annual `store_id` | `iri_chain_xref` plus roster |
| `silver_market_v` | IRI market | `iri_market_number` | via store-market mapping | `iri_fips_market` |
| `silver_fips_market_v` | county | `fips_key` | none directly | `iri_fips_market` |
| `silver_market_name_xref_v` | source market label mapping | source label + scope, to validate | via annual store `store_id` | roster labels plus FIPS market table |
| `silver_store_market_v` | annual store-to-market assignment | (`store_id`, `source_year`) if unique, to validate | via annual `store_id` | validated market-name crosswalk |
| `silver_product` | UPC per year | (`upc`, `source_year`) | `(sy, ge, vend, item)`, year-scoped to the stub edition (**verified**) | **done** — 124 stubs, 634 824 rows |
| `silver_product_key_xref_v` | IRI key tuple ↔ UPC | (`sy`, `ge`, `vend`, `item`) and `upc` | product identity mapping | **Materialized** — 324,745 complete one-to-one mappings |
| `silver_product_week_v` | UPC × IRI week, summed across stores/categories/channels | (`upc`, `week`) | product-week aggregate | **Materialized** — 2,700,651,386 sales rows covered; 66.39-second view evaluation |
| `silver_panel_trip` | household purchase | (`panid`, `week`, `iri_key`, `upc`) | **none** (different grain) | PANEL 3-format ingest + trips CSV |
| `silver_household` | panelist | `panid` | **none** | `DEMOS.CSV` ingest + dedup |

## 7. What a Silver gate would check

Not written yet, but the checks follow Gate 5's shape (`sql/` + `scripts/run_sql.py`, each
column ending `_ok`, non-zero exit). Candidates, in rough order of value:

- **Calendar coverage.** Every sales `week` has exactly one row in
  `silver_week_dimension_v`; its 626 keys are unique and contiguous, and dates are non-null.
- **Store-key integrity.** Verify the annual roster key (`source_year`, `iri_key`) is
  unique before assigning `store_id`; if not, find and document the missing discriminator.
- **Join cardinality.** Check sales-to-store, store-to-chain, and store-to-market joins for
  unmatched keys and fan-out. Market-name mappings must be unambiguous before assignment.
- **Chain semantics.** Test chain links by year and preserve source labels/provenance; do
  not treat a shared chain as proof of the same physical store.
- **Market integrity.** Each FIPS key maps once; market numbers are unique in
  `silver_market_v`; report unmatched/ambiguous store market labels, including rather
  than filtering out market number `0`.
- **Product-week integrity.** The key↔UPC crosswalk is one-to-one, and the product-week
  output accounts for the latest successful sales rows in the manifest without fan-out.
  Record full-evaluation time and materialized Parquet time/size separately.

## 8. Sequencing

**Step 1 is done.** See [`excel_to_bronze.md`](excel_to_bronze.md) for
what was built, measured, and tested.

1. **Bronze ingestion and reconciliation.** Complete. Gates 5 and 6 pass against the
   current lake.
2. Build and gate `silver_week_dimension_v` first: one row per week with the three agreed
   calendar columns.
3. Product-key crosswalk profiling, UPC-week evaluation, and Parquet materialization are
   complete. The materializer is reproducible with `make materialize-product-week`; use
   `--overwrite` when intentionally rebuilding existing outputs.
4. Profile store roster key uniqueness and market-label coverage/cardinality before
   defining store, chain, and market views. Defer household views until its grain and
   privacy handling are agreed.

## 9. Open questions

1. **Years 6–7 week translation.** ~~Verify against `demos trips external/` whether the gap is
   fully or partly covered before fixing the interpolation policy (§5.1).~~ **Resolved:
   fully covered.** The authoritative workbook spans all 626 weeks with no gap, so no
   interpolation policy or `date_source` column is needed. See
   [`excel_to_bronze.md`](excel_to_bronze.md) §3.
2. ~~**Stub → sales key correspondence.** Does `iri_sales.vend`/`item` join to a UPC in the
   stubs?~~ **Resolved.** The key is the 4-tuple `(SY, GE, VEND, ITEM)`, **not** a UPC, and
   all four columns are required. Measured on `beer`/`blades`/`yogurt` across all 12 years
   (327 M sales rows): **100% key coverage, fan-out max 1, row inflation 0.000000%**.
   §5.3 *How stubs join to sales* has the SQL, the year→edition mapping, and the two ways
   to get it wrong.
3. **`KK` channel (year 8 only).** Real channel for one year, or an anomaly to be folded
   into an existing channel?
4. **Chain xref window.** `1_12` reconstruction is a *reconstruction* — is it authoritative
   for years 1–5, or does it override `1_5`/`1_7` where they disagree?
5. **Panelist data handling.** Confirm the access/segregation policy for
   `silver_household` before any ingestion work starts (§5.4).
6. **Physical store continuity.** The agreed `store_id` identifies an annual store
   observation. A verified source that links the same physical outlet across years is still
   needed before claiming cross-year store continuity; masked chain names provide chain,
   not outlet, identity.
7. **Store-market name matching.** Validate whether roster `market_name` values map
   unambiguously to the FIPS table's IRI market names/codes after the Bronze rerun; keep
   unmatched or ambiguous rows unresolved until then.
