# Silver stage specification

**Status:** proposed implementation contract; not an implementation-completion report.

This specification extends [the Silver kickoff plan](silver_layer.md). It preserves its
recorded decisions and existing public product-week interfaces. It incorporates a new,
read-only profile of the smaller Bronze tables and the agreed policy: **preserve source
observations; publish resolved attributes only where observations establish consensus**.
Existing SQL, materializers, and lake files are not changed by this document.

`MUST` denotes a correctness or publication requirement. `SHOULD` denotes a recommended
implementation choice that may change with documented evidence. Unverified semantics are
identified as blockers, not silently converted into defaults.

## 1. Purpose and boundaries

Silver is the lake's **conformed, auditable query contract**. It establishes identity,
grain, time scope, controlled vocabularies, safe join cardinality, and reproducible inputs.
It does not need to physically duplicate every Bronze record to do that.

A successful Silver stage lets a consumer:

- Resolve an IRI week to its captured calendar dates.
- Resolve a sales product tuple to a UPC without fan-out.
- Resolve a sales store to one annual identity, while seeing disagreements in its roster.
- Obtain chain and market assignments only when source evidence supports them.
- Read consistent product attributes without losing edition, category, or missingness.
- Use the existing exact UPC-week sales building block.
- Work with household purchases and trip records without confusing them with sales,
  inventing transaction IDs, or exposing household data to unrestricted consumers.
- Reproduce a release, inspect its validation, and trace results to selected Bronze files.

### 1.1 Included

All current Bronze classes have an explicit disposition: sales, week dimension, delivery
stores, chain cross-reference, manual store entry, FIPS markets, product stubs, product
attributes, panel purchases, panel trips, panelist demographics, panel static membership,
and advertising-panel demographics. Household outputs are a separately enabled,
restricted domain, not a prerequisite for sales/product/calendar publication.

### 1.2 Excluded

- A second raw-data ingestion path or changes to the Rust sales parser.
- A materialized, enriched copy of the 2.7-billion-row sales fact.
- Gold marts, dashboards, causal promotion lift, inferred prices, market shares,
  inflation adjustment, household weighting, or population estimates.
- Imputation of demographics, calendar dates, product attributes, or store continuity.
- Claims that masked chains identify physical stores across years.
- Automatic fuzzy market matching or automatic resolution by majority vote.
- Streaming, distributed execution, mandatory dbt, or an Iceberg/Delta migration in v1.

The existing UPC-week aggregate remains a deliberate, already agreed exception to the
usual distinction between conformed Silver records and aggregated Gold marts. No further
aggregate is implicitly approved by that exception.

## 2. Evidence and current implementation

### 2.1 Already recorded or implemented

The following are recorded in `silver_layer.md` and the product-week implementation; the
full sales evaluation was **not rerun** while preparing this specification:

| Item | Recorded state |
|---|---|
| Bronze sales | 744 sources; 2,700,651,386 rows; Gates 5 and 6 reported passing |
| Calendar | 626 captured weeks, 1114–1739; Monday–Sunday; source years 6 and 12 have 53 weeks |
| Product tuple ↔ UPC | 324,745 complete mappings, bijective on this corpus |
| Incomplete stub rows | 790 all-null key/UPC rows excluded from identity mapping |
| Product-week output | 28,314,411 rows; 350,825,041 Parquet bytes |
| Product crosswalk output | 3,213,051 Parquet bytes |
| Aggregate evaluation | 66.39 seconds; materialization 68.53 seconds |
| Existing definitions | `sql/silver_product_week.sql` |
| Existing writer | `scripts/materialize_product_week.py`; `make materialize-product-week` |

Current output locations are `silver/product_week/` and `silver/product_key_xref/`.
These are legacy flat materializations, not yet release-managed outputs.

### 2.2 Additional read-only profile

These results were measured from the connected local Bronze Parquet during specification
preparation. They describe the physical canonical outputs, before any reconstruction of
semantic scopes from manifest aliases. They are evidence for the design, not universal
constants or proof that future snapshots have the same counts.

The Bronze manifest's SHA-256 at profiling time was:

```text
f8e1f5dba26b5e4bd5a55058effba4b834209312ebd76a29817de081de79e432
```

| Profile | Result | Consequence |
|---|---|---|
| Physical roster records | 120,285 | File-content dedup does not establish row-level uniqueness |
| Distinct `(year, iri_key)` | 24,608 | Candidate annual identity, not unique roster record |
| Distinct annual roster payloads | 39,760 | Preserve an observation relation |
| Annual keys with >1 distinct payload | 11,099 | No arbitrary winning row |
| Annual keys with conflicting ACV | 9,925 | ACV must not be selected with `min`/`max` |
| Conflicting market / chain labels | 11 / 1,208 annual keys | Assignments need conflict states |
| Conflicting open / closed fields | 53 / 64 annual keys | No fabricated operating interval |
| Conflicting outlet code | 0 annual keys | Channel agreement is promising, still gated |
| Repeated `iri_key` within a single roster file | 207 file/key groups | Adding filename alone does not solve identity |
| Roster market labels | 50 | Match against both captured FIPS name columns |
| Labels resolved by uppercase + trim | 35; 15 unmatched; 0 ambiguous | Explicit unresolved mappings are necessary |
| FIPS market numbers | 65, including source value 0 | Market 0 must not disappear as a sentinel |
| Nonzero markets with >1 region | 9 | Region is not functionally dependent on market |
| Chain cross-reference records / chain values | 169 / 141 | `chain` alone is not a raw-record key |
| Unpivoted chain mappings | 1,384; no ambiguous `(year, masked_number)` | Wide rows must be normalized before joining |
| Complete stub rows | 634,034, unique by edition + full tuple | Edition attributes can have a safe many-to-one interface |
| Partial stub keys | 0; the 790 incomplete rows are wholly null | Still distinguish partial keys in future gates |
| Stub UPC format | All complete values have length 17 | UPC is an opaque formatted string, not a numeric barcode |
| PANEL records | 16,474,806 | Preserve record multiplicity |
| Repeated PANEL candidate keys | 421,340 groups at `(panid, week, iri_key, colupc, outlet)` | Draft purchase key is not unique |
| Largest such PANEL group | 410 records | Must not call it a single transaction |
| Trip records | 7,200,810 | Separate from item purchases |
| Repeated trip candidate keys | 4,975 groups at `(year, panid, week, iri_key, minute)` | Minute does not establish a unique trip |
| PANEL/trips `minute` range | 0–10,079 in both | Not a minute-of-day; timestamp interpretation remains unverified |
| PANEL `colupc` lengths | 8, 11, 12, 13 | Do not equate directly with formatted stub UPC |
| Demographic records / annual panelist keys | 75,451 / 36,119 | Preserve annual observations before resolving fields |
| Distinct whole demographic rows | 36,369 | Exact copies exist, but some annual rows differ |
| Static records | 87,051, unique by `(source_year, panid)` | Annual membership, not a timeless household attribute |

### 2.3 Corrections to draft assumptions

This specification supersedes these *draft assumptions*, not the kickoff's agreed public
interfaces:

1. File deduplication has already happened in Bronze; Silver must additionally handle
   repeated and conflicting **records**, rather than deduplicating entire classes again.
2. Annual store identity can be unique while its source observations are not. A source
   discriminator belongs on observations, not automatically on the sales-facing store key.
3. A market can span regions. Preserve county-level region evidence and a market-region
   bridge; do not choose one region for the market.
4. PANEL records are purchase observations, not uniquely identified trips. Trip totals are
   a separate relation and their candidate business key also repeats.
5. Demographics and static membership are time-scoped. A timeless `panid` row cannot carry
   all their attributes without overwriting history.
6. Bronze is parsed source evidence, not a byte-for-byte archive. Exact raw-byte replay
   depends on retained raw inputs; Bronze locators guarantee tracing to parsed records.
7. `reference/prod.csv`, cited by the kickoff plan, is absent from this checkout. Category
   mapping must become an explicit, reviewed dependency rather than an assumed file.
8. Single-file atomic renames and Bronze's per-source skip machinery do not provide
   multi-output Silver transactions or transformation dependency invalidation.

## 3. Architecture

### 3.1 Stack and flow

Retain Rust for ingestion and use **DuckDB SQL with a small Python orchestration layer**
for Silver. SQL defines relations and validation; Python resolves inputs, records hashes,
controls resources, and publishes releases. No additional service is required.

```text
Raw → Rust ingestion → Bronze files + manifest
                              │
                     selected input snapshot
                              │
              source registry + reviewed reference rules
                              │
       ┌──────────────────────┼─────────────────────────┐
       │                      │                         │
  calendar + codes    store/product/market       restricted household
       │              observations + identity    observations + identity
       └──────────────────────┼─────────────────────────┘
                              │
                  public views + product-week Parquet
                              │
                    validation + release descriptor
                              │
                     atomic release publication
                              │
                  DuckDB / Parquet consumers → Gold
```

### 3.2 View-first, not view-only

Small conformed dimensions begin as views over a **selected and retained input set**.
Materialize when repeated measured evaluation exceeds about 60 seconds, a non-DuckDB
consumer requires Parquet, or expensive validation/processing warrants a persisted
boundary. The existing product-week output already meets that criterion.

Every release includes the view definitions and their dependency versions. A view over
an unrestricted live Bronze glob is an exploratory convenience, not a reproducible release.
A persistent DuckDB database is optional; the normative interface is a bootstrap operation
that registers all public views for one release in a session.

A logical `silver_sales_v` makes sales queryable without rewriting its rows. It is not
required to join all dimensions eagerly. Consumers project the fields and joins they need.

### 3.3 Names and compatibility

- Public DuckDB objects use `silver_<name>_v`; retain
  `silver_week_dimension_v`, `silver_product_key_xref_v`, and `silver_product_week_v`.
- Physical object names omit the prefix/suffix: `product_week`, `store_observation`, etc.
- `source_year` means the IRI academic-data year 1–12, **not calendar year**.
- `week` always means the IRI week counter; calendar fields have explicit names.
- `upc` means the source's formatted product identifier. It is `VARCHAR` and retains
  leading zeros, separators, and significant source formatting.
- Public key names are stable even if a table's physical layout changes.
- Partition values are part of the logical schema. If Hive-partitioned, omit them from
  file payloads and recover them from paths; do not store competing physical copies.
  This removes the contradictory partition-column wording in the draft plan.

## 4. Cross-cutting data contracts

### 4.1 Types and precision

Use DuckDB SQL types as the public contract; Parquet/Arrow representations must round-trip.

| Data | Public type and rule |
|---|---|
| Source year | `UTINYINT`, range 1–12 for this corpus |
| IRI week | `USMALLINT`; captured calendar range validated |
| Store key / panelist key | `UINTEGER`, preserving source values |
| Product tuple | `sy`, `ge`: `UTINYINT`; `vend`, `item`: `UINTEGER` |
| UPC and categorical codes | `VARCHAR`; no integer conversion |
| Captured dates | `DATE` / Arrow Date32 |
| Annual/observation/record IDs | Lowercase SHA-256 hex `VARCHAR`, 64 characters |
| Sales money | Exact `BIGINT` integer cents |
| Aggregated units / source-row counts | `BIGINT`, with checked overflow |
| Panel decimal evidence | `DECIMAL(38,0)` coefficient + `TINYINT` scale |
| ACV / volume equivalence | Preserve Bronze `DOUBLE`; do not label as exact cents |
| Statuses | Documented `VARCHAR` vocabulary, constrained by gates |

Aggregate sales into a sufficiently wide exact accumulator (`HUGEINT` in DuckDB), check
that final values fit `BIGINT`, then cast. Overflow fails publication. No binary float is
allowed for sales money or exact conservation comparisons. Consumers may derive decimal
dollars from cents; the canonical stored measure remains cents.

For coefficient `d` and scale `s`, the source decimal is `d × 10^(-s)`. Carry both fields
without conversion through `DOUBLE`. Rounded PANEL cents are explicitly a convenience,
not a replacement for its lossless dollar representation. Never add dollar coefficients
to trips' `CENTS*` coefficients: the latter's business units are not yet verified.

### 4.2 Deterministic keys

Do not use order-dependent `row_number()` or Python's randomized hash for durable IDs.
Define a versioned canonical encoding: UTF-8 JSON arrays, compact separators, fixed field
order, explicit JSON nulls, integers as integers, and no Unicode normalization of raw
identifiers. Hash the encoded bytes with SHA-256, with an entity/version prefix in the
array. Publish encoding test vectors before any surrogate-key release.

- `store_id = H(["store:v1", source_year, iri_key])`: annual identity only.
- `household_id = H(["household:v1", panid])`: internal source identifier, not proof of
  a persistent real-world household; external exposure requires a separate privacy policy.
- `household_year_id = H(["household-year:v1", source_year, panid])`.
- Observation IDs hash entity, semantic scope, and canonical **typed source payload**.
  For doubles, use a specified IEEE-754 bit-string encoding, including null distinction;
  reject non-finite values or handle them in an explicitly versioned rule.
- Record IDs hash entity, canonical source-content identity, and Bronze record locator.
  Purchase/trip records include source-local position so identical records are not lost.

Hash collision checks MUST verify that one ID never represents different encoded keys.
Source observations from different years must not collapse just because their bytes match.
Repacking Bronze may change file-position record IDs; such IDs are snapshot/parse-version
scoped, not asserted to be permanent transaction identifiers. Annual identity IDs remain
stable across file reorderings and rebuilds.

### 4.3 Missingness and resolution

Retain raw values. Each resolved attribute has a status, stored in a companion status
relation where carrying many status columns would be unwieldy.

| Status | Meaning | Resolved value |
|---|---|---|
| `known` | All distinct relevant observations provide the same known value | That value |
| `unknown` | Attribute exists but no observation provides a known value | NULL |
| `not_collected` | Field absent from the applicable source schema | NULL |
| `source_missing` | Source explicitly publishes a documented missing token | NULL |
| `partial` | Known value agrees where present, but other observations lack it | NULL |
| `conflict` | More than one distinct known value, or incompatible evidence | NULL |
| `unmatched` | No supported cross-reference target | NULL |
| `ambiguous` | More than one supported target | NULL |

Resolution is conservative and **attribute-by-attribute**, not whole-row winner selection.
Exact copied records contribute provenance but not extra votes. A mix of missingness
states is retained in evidence; it must not be misleadingly labeled unanimously known.

For stub/product text attributes, normalize outer whitespace for comparison and recognize
the exact trimmed source token `MISSING` as `source_missing`; preserve its raw spelling.
Do not globally treat `NONE`, `0`, empty strings, `999`, or `N/A` as missing. Any additional
sentinel mapping is field-specific, reviewed, versioned, and measured before use. In
particular feature code 0 and IRI market number 0 are retained source values.

Unresolved attributes are valid published information, not reasons to drop their parent
entity. Conflicting identity mappings, multiplicative joins, broken lineage, and lost
facts are publication failures.

### 4.4 Time and source scope

- Resolve sales year from its partition and verify agreement with the week-to-year map.
- Store identity and assignment are annual. No last-observation-carried-forward joins.
- Product editions provide applicability windows, not evidence that attributes were
  measured independently in every covered year.
- Demographics are annual observations; missing years are coverage gaps.
- Preserve `open_week` and `closed_week` as captured source fields. Values can fall outside
  the 626-week analysis window. Do not clamp, fill, or filter sales by them until their
  sentinel and boundary semantics are documented.
- Preserve `minute` as `minute_raw`. Values through 10,079 suggest a weekly counter, but
  range alone does not establish the epoch, timezone, or timestamp calculation.

### 4.5 Provenance without billions of lineage rows

Use three levels:

1. **Release lineage:** exact input manifest records, selected file lists, source hashes,
   parser/schema versions, SQL/config/reference hashes, and validation results.
2. **Entity observation lineage:** observation → canonical source record locator(s), with
   separate aliases for copied source paths and their semantic scopes.
3. **Fact/aggregate lineage:** sales partition input groups and reproducible key predicates.
   `(upc, week)` plus the release's crosswalk identifies contributing sales records; a
   per-sales-row lineage expansion is not required.

`silver_source_v` identifies canonical source content and selected parse outputs.
`silver_source_alias_v` maps all selected paths to that content with source year/category/
edition and dedup pointers. Canonical path selection is operational, not semantic identity.
A global content dedup may span years: restore each legitimate semantic scope from aliases
**once per distinct scope**, without reintroducing one row per copied category file.

Use DuckDB `filename` and zero-based `file_row_number` when supported, or an equivalent
stable, tested file-local ordinal reader. A locator is `(selected file identity, ordinal)`.
It identifies a Bronze record, not an unverified raw line or Excel byte offset. Raw source
paths remain in the source registry, not concatenated into every output row.

## 5. Public relation inventory

All columns explicitly designated as keys are non-null in published identity relations.
Raw observation payload fields retain Bronze nullability. Resolved attributes may be null
only with a status/evidence explanation. `_v` names below are public logical interfaces.
Detailed raw attribute dictionaries retain the original source column names alongside
normalized names; adding a source column does not silently change a fixed public schema.

| Relation (`silver_…_v`) | Grain / primary key | Physical policy |
|---|---|---|
| `week_dimension` | IRI week / `week` | View; optional single-file export |
| `source`, `source_alias` | Selected source parse / source alias+scope | Release metadata views |
| `channel_code` | Code system + raw code + year | Small reference view |
| `sales` | One selected Bronze sales record; no new business PK claim | View only |
| `store` | Annual store identity / `store_id`; unique year+IRI key | View first |
| `store_observation` | Distinct annual typed roster payload / `store_observation_id` | View first |
| `store_attribute_status` | Store + attribute name | View first |
| `chain` | Validated source chain identifier / `chain_id` | View first |
| `chain_year_xref` | Year + masked chain number | View first |
| `store_chain` | Annual store / `store_id` | View first, nullable assignment |
| `market` | Source IRI market number | View first |
| `fips_market` | FIPS county / `fips_key` | View first |
| `market_region` | Market + region | View first; many-to-many bridge |
| `market_name_xref` | Source label + applicability scope | View first, nullable target |
| `store_market` | Annual store / `store_id` | View first, nullable assignment |
| `product_key_xref` | Full tuple; UPC also unique in current corpus | Materialized already |
| `product_edition` | UPC + edition | View first |
| `product` | UPC + source year | View first, edition-backed |
| `product_attribute_observation` | Distinct scoped attribute evidence / observation ID | View first; preserve conflicts |
| `product_attribute` | UPC + year + attribute name | View first, resolved values/status |
| `product_week` | UPC + week | Materialized already |
| `coverage` | Relation + scope + measure | Release metadata view |
| `panel_purchase` | One source purchase record / `purchase_record_id` | Restricted, opt-in |
| `panel_trip_record` | One source trip record / `trip_record_id` | Restricted, opt-in |
| `purchase_trip_link` | Purchase record | Restricted; nullable target/status |
| `household` | Source panelist identity / `household_id` | Restricted, opt-in |
| `household_year` | Panelist + year / `household_year_id` | Restricted, opt-in |
| `household_demographic_observation` | Annual source demographic observation | Restricted, opt-in |
| `household_attribute` | Panelist + year + attribute name | Restricted, opt-in |
| `panelist_static` | Panelist + year | Restricted, opt-in |
| `ads_demographic_observation` | Advertising-panel source observation | Restricted; separate population |

Supporting lineage, attribute-evidence, exception, and reference-rule relations are part of
the release, but need not be loaded into a consumer's default namespace.

## 6. Calendar and codes

### 6.1 `silver_week_dimension_v`

Preserve the kickoff's exact three-column public interface:

| Column | Type | Source |
|---|---|---|
| `week` | `USMALLINT NOT NULL` | Bronze `iri_week` |
| `calendar_start_date` | `DATE NOT NULL` | Bronze `week_start` |
| `calendar_end_date` | `DATE NOT NULL` | Bronze `week_end` |

One row per week, no interpolation. Provenance is release/source metadata rather than
extra public calendar columns. Year derivation belongs in a shared SQL expression or
internal week-year mapping, not an extra persisted calendar contract.

Required checks: exactly 626 unique contiguous keys 1114–1739, six-day difference between
end and start, seven-day difference between consecutive starts, Monday/Sunday endpoints,
no null dates, and agreement with Bronze year ranges. Every referenced fact week must
resolve exactly once.

### 6.2 `silver_channel_code_v`

Columns: `code_system VARCHAR`, `raw_code VARCHAR`, `source_year UTINYINT`,
`channel_code VARCHAR NULL`, `mapping_status VARCHAR`, `rule_id VARCHAR`.
Primary key: `(code_system, raw_code, source_year)`.

Keep sales partition codes, roster `ou`, and panel `outlet` as separate code systems.
Supported conventional mappings are sales `drug`/`groc`, roster `DR`/`GR`, and PANEL
`DR`/`DK` → `drug`, `GR`/`GK` → `groc`, `MA`/`MK` → `mass`, within captured year coverage.
Validate the actual code/year combinations rather than assuming a delimiter or code
system solely from year. PANEL `KK` remains a distinct raw code with unresolved normalized
channel until its documented `combo` interpretation is reviewed. It is never folded into
grocery or dropped. Unexpected codes retain raw evidence and produce explicit exceptions.

## 7. Sales contract

`silver_sales_v` projects the eleven Bronze raw fields plus logical `source_year`,
`category`, and `channel` from the selected partitions. Its row count and all measures
MUST equal the selected Bronze sales input. It may expose derivable annual `store_id`
without joining roster attributes. It does not add a physical file-local ID column to
billions of stored rows.

The **candidate** business key is `(source_year, category, channel, iri_key, week, sy, ge,
vend, item)`. Uniqueness has not been established here. Do not silently `DISTINCT` sales,
assume category overlap is an error, or collapse apparent duplicates. Product-week
currently includes every selected sales record, across categories and channels, by design.
Any future change to overlap semantics requires a new aggregate contract and reconciliation.

Dimension joins are many-to-one and use `LEFT JOIN` until coverage has been verified.
Unmatched facts remain visible. The product-week build additionally requires complete
product mapping; if coverage fails, publication fails rather than omitting those sales.

## 8. Stores, chains, and geography

### 8.1 Store observations and annual identity

`silver_store_observation_v` contains:

- `store_observation_id VARCHAR NOT NULL`, `store_id VARCHAR NOT NULL`;
- `source_year UTINYINT NOT NULL`, `iri_key UINTEGER NOT NULL`;
- raw `ou`, `est_acv DOUBLE`, `market_name`, `open_week USMALLINT`,
  `closed_week USMALLINT`, `masked_name`;
- `source_record_count BIGINT`, the number of selected canonical records represented.

Collapse only identical typed payloads **within the same annual identity**. All source
record locators remain in the observation-lineage relation. Category/file context belongs
in lineage; if analysis reveals genuinely different semantic scopes, expose those scopes
on observations rather than randomly selecting one for a store.

`silver_store_v` contains:

| Column | Contract |
|---|---|
| `store_id` | Deterministic annual surrogate; primary key |
| `source_year`, `iri_key` | Non-null unique alternate key |
| `observation_count BIGINT` | Number of distinct typed observations |
| `source_record_count BIGINT` | Selected canonical records represented |
| `channel_code VARCHAR NULL` | Unanimously known normalized channel |
| `est_acv DOUBLE NULL` | Consensus ACV only; no unit conversion asserted |
| `open_week`, `closed_week` | Consensus source fields, not inferred validity bounds |
| `resolution_status VARCHAR` | `known`, `partial`, `unknown`, or `conflict` summary |

`silver_store_attribute_status_v` has `(store_id, attribute_name)` as its key and includes
`status`, `distinct_known_values BIGINT`, and observation counts. A summary status never
substitutes for per-field status. Compare raw evidence before normalization and document
whether any conflicts disappear under a reviewed, harmless normalization.

No automatic modal value, latest filename, greatest ACV, or longest source window wins.
A later reviewed resolution can reference a `rule_id` and selected evidence, while retaining
the original observations and prior release for audit.

### 8.2 Chains

Unpivot Bronze `year1` … `year12` into source evidence with `chain_id UINTEGER`,
`source_year UTINYINT`, and `masked_chain_number USMALLINT`. Normalize repeated mappings;
gate `(source_year, masked_chain_number) → chain_id` and inspect multiple masks per
chain/year rather than assuming raw `chain` rows are unique.

`silver_chain_v` contains source `chain_id` and documented source coverage, not invented
real chain names. `silver_chain_year_xref_v` retains mask number, year, chain ID, source
record lineage, and mapping status. Source chain identifiers are not inferred from store
IDs and do not establish physical-store identity.

For `silver_store_chain_v`, first resolve the annual roster's masked label by consensus,
then parse only a documented full-match mask syntax (for example `Chain` + digits), then
join the year-specific cross-reference. Output one row for **every** annual store:
`store_id`, `source_year`, nullable `chain_id`, `mapping_status`, and `rule_id`.
Conflicting roster labels yield `conflict`; no target yields `unmatched`; multiple targets
yield `ambiguous` and are a crosswalk integrity failure if a unique assignment is claimed.

The current widest reconstruction is the selected Bronze evidence, not proof that all
narrower historical cross-references agree. Its authority/coverage caveat must be included
in release metadata. Any assertion of agreement with skipped raw sources requires a
Bronze ingestion extension or separately recorded source review, not Silver reading raw CSV.

### 8.3 Manual store entry: restricted interpretation

`iri_manual_store_entry` is ingested with the generic wide chain parser. Its observed
schema includes `year1` … `year13`; that alone is not proof of thirteen valid academic
years or of correctly aligned business fields. Retain it as source evidence only.

**Blocker:** verify each selected file's actual headers, column offsets, year span, and
business meaning against the parser output before using manual entries for assignment.
If the parser has shifted or mislabeled columns, fix/reingest Bronze first. Manual rows
must not override roster or chain links merely because the filename says “manual.”
The same rule applies to the currently uningested panel store/chain workbook: no direct
Excel reads from Silver.

### 8.4 Markets, counties, and regions

`silver_fips_market_v` preserves one row per `fips_key UINTEGER`, plus a zero-padded
five-character `fips_code VARCHAR` for presentation, raw county/state names, market number,
both captured market-name fields, and captured region label. Do not cast county strings
into FIPS keys without a validated dictionary.

`silver_market_v` has `iri_market_number USMALLINT` as the primary key,
`market_name VARCHAR NULL`, `name_status VARCHAR`, and `county_count BIGINT`.
Keep market 0. Conflicting names for a number remain aliases/evidence; the resolved name
is null unless consensus or an approved naming rule establishes one. A market has no
single authoritative `region_name` column.

`silver_market_region_v` has key `(iri_market_number, region_name)` and records the
county-level supporting evidence/count. It is a **bridge**, not a many-to-one dimension.
Joining it to revenue multiplies rows unless the consumer explicitly handles that grain;
Silver does not invent revenue allocation weights.

`silver_market_name_xref_v` contains `source_system`, `source_label`, `normalized_label`,
`source_year`, nullable `iri_market_number`, `mapping_status`, and `rule_id`.
It has one row per source label/year, with candidate targets in a separate evidence table.
Initial matching uses only `upper(trim(label))` against both FIPS market-name fields.
Do not remove punctuation or expand abbreviations without a reviewed alias rule. Two
matching names for the same number count as one candidate; more than one target is
ambiguous. The initial 15 unmatched labels are retained as a work queue, not discarded.

`silver_store_market_v` contains exactly one row per annual store, with nullable market
number and explicit assignment status. Conflicting annual roster market labels remain
unresolved even if a convenient string match exists. Market code is never compared to
`iri_key`. Regions come from the county/market bridge, not from an invented store county.

## 9. Product identity and attributes

### 9.1 `silver_product_key_xref_v`

Retain columns `sy`, `ge`, `vend`, `item`, `upc` and the current bijection contract.
Repeated identical mappings across editions collapse only after validation. Complete
conflicting mappings fail publication; an incomplete mapping is reported separately.

The global identity crosswalk is justified by measured cross-edition agreement. This does
**not** make product attributes timeless. If a future snapshot reuses a tuple for another
UPC or makes UPC non-invertible, stop the build and require a versioned identity redesign;
do not hide disagreement behind `min(upc)` or a filtering `HAVING` clause.

### 9.2 Edition and annual product relations

| Source year | Applicable stub edition |
|---|---|
| 1–6 | `2001-2006` |
| 7 | `2007` |
| 8–11 | `2008-2011` |
| 12 | `2012` |

`silver_product_edition_v` has `(upc, edition)` as its key and includes the tuple,
`l1` … `l5`, `item_description` from `L9`, source `level`, `stubspec`, and `vol_eq DOUBLE`.
Hierarchy labels keep their source meaning; do not relabel `L4` as “brand” without a
validated hierarchy dictionary. Read source files with `union_by_name=true`, but use
explicit public column lists rather than `SELECT *`.

`silver_product_v` exposes `(upc, source_year)` by joining each edition record to its
applicable years. Include `edition` and `attribute_scope = 'edition'` to avoid implying
annual measurement. No forward/backward filling between editions. Presence in an edition
is not proof of sales activity in every year. First/last **sales** observation, if exposed,
is computed from fact evidence and named accordingly, not inferred from stub windows.

A reviewed `stub_category_xref` reference must map `(edition, source workbook identity)`
to the canonical sales category. Preserve raw filename code and mapping status. The
missing `reference/prod.csv` must be replaced or recovered with provenance before category
labels are declared complete. Do not let an unresolved category block the already valid
identity-only UPC mapping. If category membership is multiple, expose a product-category
bridge; never arbitrarily choose a category for a globally unique product.

### 9.3 Attributes

Preserve two evidence families separately:

- Stub attributes, whose scope is an edition and source category/workbook.
- `iri_product_attr` attributes, whose scope is source year/category, currently years 9–11.

Normalize per-file schemas into long-form observations with:
`upc`, full tuple, `source_family`, `edition NULL`, `source_year NULL`, `category NULL`,
`attribute_name`, `source_attribute_name`, `raw_value`, `normalized_value NULL`,
`value_status`, and source observation/lineage ID. Exact scope and source identity are part
of the observation key; if multiple distinct values occur, keep them as separate evidence.
Do not combine similarly normalized column names such as `SIZE` and `SIZE_2` without an
explicit semantic mapping. Keep `vol_eq` separate from exact-money fields.

`silver_product_attribute_v` has key `(upc, source_year, attribute_name)` and resolved
`value VARCHAR NULL`, `status`, `source_families`, and evidence count. Sources resolve by
consensus, not automatic annual-over-edition precedence. If two families disagree, return
`conflict` and retain both values. Optional category-scoped interfaces must include category
in their keys rather than multiplying the product-level interface.

A coverage relation states which families/attributes were collected in each year/category.
Avoid generating a giant Cartesian grid of every possible absent product attribute.
Consumers can distinguish “no annual product-attribute source in year 12” from “source
record exists and explicitly says MISSING” by joining coverage and evidence.

### 9.4 `silver_product_week_v`

Preserve the existing public schema:

| Column | Type / meaning |
|---|---|
| `upc` | `VARCHAR NOT NULL`, source formatted product key |
| `week` | `USMALLINT NOT NULL` |
| `units` | `BIGINT NOT NULL`, exact sum of source units |
| `revenue_cents` | `BIGINT NOT NULL`, exact sum of source `dollars_cents` |
| `sales_rows` | `BIGINT NOT NULL`, number of contributing sales records |

Grain: `(upc, week)`, across **all** selected stores, categories, and sales channels.
There is no category/channel/market breakdown left to recover from this aggregate. A join
to an edition-backed product relation must select the year corresponding to `week` first;
joining all product years would fan out.

Required conservation against the same selected sales inputs:

```text
SUM(product_week.sales_rows)    = COUNT(selected sales records)
SUM(product_week.units)         = SUM(selected sales.units)
SUM(product_week.revenue_cents) = SUM(selected sales.dollars_cents)
```

Check these globally and per IRI source year; check tuple coverage and no mapping fan-out
separately. Matched-row counts alone cannot prove monetary conservation. Output key
uniqueness and final physical types are independent gates. Do not assert the historical
28,314,411 output count as a universal rule for future source snapshots.

## 10. Restricted household domain

This domain can be fully specified without being enabled by default. Its publication is
blocked until access, retention, approved uses, and export policy have an owner/sign-off.
It does not block the unrestricted calendar/store/product release.

### 10.1 Purchase records, not synthetic transactions

`silver_panel_purchase_v` preserves one row per selected canonical PANEL record:

- `purchase_record_id`, `household_id`, `household_year_id`;
- raw `panid`, `source_year`, `week`, `minute_raw NULL`;
- `category`, `outlet_raw`, nullable normalized `channel_code` and mapping status;
- `iri_key`, nullable annual `store_id`, and store mapping status;
- `colupc_raw`, nullable `upc` and product mapping status;
- `units NULL`, exact `units_digits`/`units_scale`;
- `dollars_cents_rounded`, exact `dollars_digits`/`dollars_scale`;
- restricted source-record locator.

Nullability on source fields reflects Bronze; missing/invalid identity fields are retained
with an exception rather than replaced by zero. Do not deduplicate records based on equal
panelist/week/store/item values. An exact duplicate in a source can be a source duplicate
or a real repeat; Silver has insufficient evidence to delete it.

The outlet field is a channel code, not a store ID. PANEL already carries `iri_key`.
An annual sales-store roster may not cover every panel outlet; gate/report its coverage
without asserting sales' stronger roster guarantee applies to PANEL.

**Product mapping blocker:** establish a documented `COLUPC` → formatted stub UPC rule.
The observed 8/11/12/13-character codes do not justify numeric casts, fixed-width padding,
or a standard barcode check-digit assumption. Preserve raw values and unmatched status
until a validated crosswalk covers the intended scope.

### 10.2 Trip records and linkage

`silver_panel_trip_record_v` preserves one source trip record with `trip_record_id`,
`panid`, annual household identity, `source_year`, `week`, both `iri_key` and `iri_key2`,
`minute_raw`, the three original cents coefficient/scale pairs, and lineage. Do not
coalesce the two store fields until their semantics are known. Do not derive total basket
revenue or rounded integer cents until the `CENTS998`, `CENTS999`, and `KRYSCENTS` units
and meanings have been verified.

`silver_purchase_trip_link_v` has one row per purchase with nullable `trip_record_id`,
`link_status`, `candidate_count`, and `rule_id`. Only enable a linkage rule after validating
the fields' semantics. Even when panelist/week/store/minute match, assign a trip only if
exactly one record qualifies. Multiple candidates stay ambiguous; a many-to-many join
must never duplicate purchase measures. Earlier purchases with no minute may remain
unlinked. Do not distribute a week's purchases among trips heuristically.

### 10.3 Household identity, demographics, and static membership

`silver_household_v` has one row per source panelist identifier, with first/last observed
years and coverage metadata, **not demographic attributes**. A stable source key is not
proof of unchanged real-world household composition or absence of identifier reuse.

`silver_household_year_v` has one row per `(panid, source_year)` observed in any enabled
household source, with flags for purchase/trip/demo/static presence. Do not require a
demographic row to retain a household's purchases.

`silver_household_demographic_observation_v` preserves distinct annual source payloads
with lineage and original code strings. A reviewed schema dictionary maps differing source
headers to attributes; parsing a code does not decode its meaning. Preserve income/age/
race/education codes as source codes until their dictionaries are captured. County/FIPS
fields from different editions are not assumed equivalent.

`silver_household_attribute_v` resolves each annual attribute under the same conservative
consensus policy as stores. Do not collapse all years to the last observed value or treat
missing demo years (currently 1–2, 6–7, 12) as zero-valued demographics.

`silver_panelist_static_v` retains `(panid, source_year)`, captured `trip_count`, and raw
`make_static`. Do not cast the flag to boolean without its codebook. `trip_count` is a
captured source statistic, not asserted equal to current observed trip-record count.

`iri_ads_demos` becomes `silver_ads_demographic_observation_v`, preserving its source
population and year evidence. Do not merge advertising-panel and general-panel profiles
by `panid` without verified identifier/population compatibility and approved purpose.
Unknown or malformed source years stay exceptions; they are not assigned from filenames
without an explicit rule.

### 10.4 Security boundary

- Store household outputs, lineage, DuckDB spill, and detailed exception reports under a
  separate configurable restricted root with restrictive filesystem permissions.
- Do not expose household views in the default unrestricted bootstrap or general exports.
- Treat purchase histories and deterministic panelist hashes as sensitive too; hashing is
  pseudonymization, not anonymization or access control.
- Logs and ordinary gate reports contain counts/statuses, not PANIDs, ZIPs, raw demographics,
  purchase histories, or full restricted source paths.
- Record approved retention and deletion handling, including input snapshots, prior
  releases, temporary files, and backups, before production use.
- Aggregate/external household exports need a separately approved disclosure policy;
  this specification does not invent a universally safe minimum cell size.

## 11. Input selection, releases, and publication

### 11.1 Input snapshot

A Silver run MUST select a finite manifest snapshot before opening data:

1. Read a consistent manifest image under coordination with Bronze writers; reject malformed
   or partial trailing records rather than silently accepting an incomplete image.
2. Normalize legacy missing `dataset` to `sales`.
3. Select the **last physical append** per `(dataset, source_path)`, matching the current
   Rust manifest semantics. Preserve append ordinal; timestamp/run-ID sorting is not an
   equivalent total order. Do not filter to success before selecting the latest attempt.
4. Require selected required sources to be successful, or use an explicitly pinned prior
   successful snapshot with a recorded rollback/staleness decision. Never silently hide a
   latest failed attempt by taking an older success.
5. Resolve dedup pointers to canonical selected parse outputs; detect missing targets,
   cycles, mismatched hashes, and lost semantic scopes.
6. Deduplicate the selected **output file list**, validate existence/schema/count metadata,
   and record file digests. Orphan glob matches and superseded outputs are not inputs.
7. Verify intended corpus coverage, not just equality between a partial manifest and a
   partial lake. Use the selected inventory/gate evidence for required classes/scopes.
8. Retain selected inputs for view lifetime and reproduction. Bronze overwrite may delete
   old outputs: pin immutable copies, reflinks, or safe same-filesystem hardlinks of immutable
   files under coordination, or adopt an explicit Bronze retention mechanism. A saved
   manifest pointing to deleted files is not a reproducible snapshot.

No simultaneous uncoordinated Bronze rewrite and Silver read is supported in v1. A local
lake lock or a verified immutable snapshot must prevent missing/mixed inputs. Persist
absolute execution paths only locally; portable descriptors use paths relative to a
recorded input root plus content identities.

### 11.2 Proposed layout

```text
<lake>/silver/
  releases/<release_id>/
    release.json
    views/                         # exact SQL/bootstrap definitions
    inputs.json                    # selected retained input identities
    validation.json
    tables/
      product_key_xref/part-00000.parquet
      product_week/part-00000.parquet
      ...                          # promoted public dimensions only
    reports/                       # non-sensitive counts and coverage
  current.json                     # atomic pointer to a complete release
  .staging/<run_id>/                # never read by consumers
<lake>/metadata/silver_runs.jsonl   # operational attempt log, not sole commit authority
<retained-input-root>/<snapshot_id>/...
<restricted-root>/silver/releases/<release_id>/...
```

A public release points to one coherent unrestricted dependency set. The restricted domain
uses its own access-controlled descriptor/pointer and may depend on a pinned public release;
the unrestricted descriptor must not reveal restricted row-level information.

Do not move or delete existing flat product files merely to implement this layout. Validate
and rebuild/import them into a first managed release, compare schemas/counts/exact totals,
then offer compatibility views through the new bootstrap. Existing direct paths remain
legacy paths until callers explicitly migrate; they are not atomic “current” aliases.

### 11.3 Publication state machine

```text
planned → building → validating → ready → published
                   ↘ failed       ↘ failed
```

1. Acquire the writer/publication lock for the target namespace.
2. Resolve inputs and build fingerprint; skip only a verified matching published result.
3. Write all outputs into a unique staging directory on the publication filesystem.
4. Validate the **persisted outputs**, not merely the query that generated them.
5. Write the complete descriptor, file list/digests, schemas, lineage, and gate results;
   flush/close files and use appropriate fsync barriers for the required crash durability.
6. Atomically rename the staging release directory into its immutable final location.
7. Atomically replace `current.json` using a same-filesystem temporary sibling and rename.
   That pointer replacement is the publication commit point.
8. Append the published attempt record. If logging fails after the commit, recover from
   the valid descriptor/pointer rather than claiming the data was not published.

A reader resolves `current.json` **once**, pins that release ID for its session, and reads
only descriptor-listed files. It must not glob across releases or re-resolve the pointer
between crosswalk and aggregate reads. Failed builds leave the previous pointer untouched.
An orphan final release without a pointer is recoverable/unpublished, not implicitly current.

Two sequential `os.replace` calls for related product files are not a multi-file commit.
The current materializer therefore needs publication integration before it meets this
contract. Object-store publication is out of scope; local rename semantics must not be
claimed for S3 without an appropriate transactional catalog/conditional update design.

### 11.4 Idempotence and schema versions

The build fingerprint covers:

- Selected input content/file identities, schemas, parser versions, and semantic scopes.
- SQL definitions, orchestration semantic version, and schema versions.
- Reviewed category/market/channel/schema dictionaries and all resolution rules.
- Semantic configuration, including enabled domains and aggregate scope.
- DuckDB version and relevant behavior-affecting settings.

Store execution details such as thread count, memory limit, and spill path for observability;
include them in the fingerprint only when they affect the declared output contract.

A matching fingerprint is skippable only when the descriptor is complete, gates passed,
and every required output exists and matches its recorded integrity metadata. Existing
files alone are not sufficient. No-op resume does not reaggregate sales. A forced rebuild
creates a new immutable attempt/release; it does not mutate the published release in place.

Each public relation has an integer `silver_schema_version`, separate from Bronze's parser
and output versions. Changed keys/grain/meaning are breaking changes. Additive nullable
columns still require an explicit contract update. Keep old releases readable; compatibility
views or an announced migration cover renamed/removed fields.

### 11.5 Retention, rollback, and operational recovery

Retain the current release and at least its immediately preceding successful release,
together with every input snapshot needed to evaluate their views. This is a minimum
rollback guarantee, not a maximum retention policy. Additional public retention is a
configured capacity/operational policy; restricted retention requires the approval in §10.

A rollback validates a retained descriptor and its dependencies, then atomically switches
the pointer back. It does not rewrite output files or erase the failed/replaced attempt
history. Record rollback reason, operator, and target release. Back up descriptors,
reference rules, retained inputs, and promoted outputs together; a pointer alone is not
a backup. Exercise restoration before declaring the first managed release operational.

Garbage collection MUST be an explicit, dry-run-capable operation. Delete only unreferenced
staging artifacts or releases/inputs outside retention, after checking current pointers,
rollback protection, dependent restricted releases, and active reader pins. Reader sessions
must hold a lease/pin or operate within a maintenance protocol that prevents collection
of their release. Do not delete inputs solely because a newer Bronze source exists.

Resolve and validate the actual lake root before acquiring locks or planning writes:
`data/lake` in this checkout is a symlink to a shared lake. Locks and safety checks must
coordinate on the resolved storage root, not merely different worktree path spellings.
Refuse cleanup outside configured managed roots, including through unexpected symlinks.
Failed-build diagnostics preserve non-sensitive stage/error details and recovery actions;
restricted temporary artifacts remain access-controlled until safely removed.

## 12. Incremental rebuilds and backfills

V1 correctness path: a whole selected snapshot can be rebuilt locally, with full
product-week recomputation when its dependencies change. Incremental work is an optimization,
not a prerequisite or a reason to relax conservation checks.

Recommended invalidation units:

| Changed input/rule | Minimum invalidation |
|---|---|
| Week translation | Calendar and dependent views/gates |
| Roster observation in one year | That year's stores, consensus, chain/market links |
| Market-name alias | Affected assignments; retain rule version |
| Stub descriptive fields | Applicable edition-backed product years/attributes |
| Tuple ↔ UPC identity | Crosswalk, coverage, and affected aggregate outputs; full rebuild if affected keys are not proven bounded |
| Sales file | Every affected week's UPC-week results, including other channels/categories contributing to those weeks |
| PANEL/trips/demo source | Its restricted semantic scopes and dependent links |
| Missingness/schema dictionary | Every resolved relation depending on the changed rule |

Never append a rerun aggregate to prior totals. Recompute and replace affected logical
partitions in a new release, or maintain explicitly checked exact deltas including old
contributions and deletions. The simpler recompute approach is preferred initially.

A backfill takes an explicit input snapshot and scope, records its intent, validates the
new result, and publishes a new release. Partial scopes cannot masquerade as a full-corpus
product-week release; their descriptor and public namespace must identify that scope.
Reference changes alone invalidate dependent results even when Bronze bytes do not change.

## 13. Storage and execution policy

- Retain current product output paths for legacy callers; managed releases use the layout
  above. Physical table names identify tables, not broad domain directories of mixed grains.
- Small dimensions should remain unpartitioned. Do not create one file per week or UPC.
- Start with the existing compact product-week output; if growth/query measurements warrant
  it, partition by source year, derived from week, and sort within files by `(upc, week)`
  or `(week, upc)` according to measured predicate patterns. Record the selected sort order.
- A reasonable initial large-table file target is 128–512 MiB compressed; it is a tuning
  target, not a correctness gate. Avoid rewriting a 335 MiB aggregate into hundreds of files.
- Use ZSTD Parquet, explicitly recording codec/level and row-group settings. Benchmark
  Silver settings independently; Bronze ingestion measurements do not prove the best
  aggregate write configuration.
- Expose DuckDB thread, memory-limit, and temporary-directory controls. Use bounded
  concurrency rather than launching multiple full sales scans in parallel.
- Enforce resource limits/spill configuration in the runner. Restricted work uses restricted
  spill storage and cleanup. Verify free-space capacity for retained inputs, staging,
  output, and worst observed spill before an expensive build.
- Avoid recomputing the sales aggregate once for “validation” and again for output when
  materializing. Validate source totals/coverage and persisted output as separate checks;
  capture all required totals in as few fact scans as practical.
- Record setup time, query evaluation time, COPY time, validation time, file bytes, and
  total elapsed time separately. Lazy `CREATE VIEW` time is not transformation throughput.

There is no invented performance SLA. Establish a benchmark on the intended machine and
selected input snapshot. Report both improvement/regression against the recorded 66.39 /
68.53-second measurements and hardware/cache/config differences; those historical timings
are not portable pass/fail thresholds.

## 14. Quality gates and exception handling

### 14.1 Gate classes

| Gate | Requirement | Failure behavior |
|---|---|---|
| Input readiness | Complete intended scopes, latest status, valid selected files, no unresolved dedup pointers | Block affected release/domain |
| Schema/precision | Explicit schemas, nullability, exact decimal pairs, checked integer casts | Block publication |
| Calendar | Unique captured dates and exact week coverage | Block public release |
| Annual identity | Unique non-null store/year keys and deterministic ID mapping | Block dependent release |
| Observation conservation | Every canonical scoped record represented, with multiplicity and lineage | Block dependent release |
| Safe joins | Max one row on declared many-to-one dimension joins; facts not dropped | Block publication |
| Product identity | Complete required sales mapping; no tuple or UPC conflicts | Block aggregate publication |
| Product-week conservation | Exact record/unit/cent equality globally and by year; unique UPC/week | Block publication |
| Market integrity | Unique county keys; no fabricated region dependency; assignments match their status | Block invalid assignments; retain unresolved rows |
| Missingness | Missing/source-missing/conflict states consistent with values and coverage | Block invalid relation |
| Household linkage | No lost purchase records or one-to-many implicit trip link | Block restricted publication |
| Lineage/security | Every dependency resolvable; restricted data not in public artifacts/logs | Block affected release |
| Physical output | Reopen Parquet; verify row counts, schemas, keys, checksums | Block pointer switch |

### 14.2 Structural checks versus known data findings

Known store disagreements and unmatched labels are not zero-tolerance “bad row” gates.
They MUST be reported and represented truthfully. Gates assert that disagreement yields
null/status/evidence, not that all evidence agrees. Similar treatment applies to missing
household years and unlinked panel records.

Track coverage counts and rates by year/category/channel where applicable, together with
an approved baseline. New findings or material baseline regressions require review before
publication; exceptions record owner, reason, affected scope, rule/version, and review
status. Never hide a fact-loss or fan-out error behind a coverage waiver. The first managed
release establishes explicit baselines rather than retroactively declaring the current
physical profile a complete semantic truth.

Detailed exceptions include entity/observation keys, issue code, scope, candidate count,
and evidence locators. Store sensitive exceptions under the restricted root. Ordinary
reports show only aggregate counts and safe labels.

### 14.3 Proposed Gate 7 interface

Extend the repository's executable SQL gate convention with `sql/gate7_silver.sql` and
`make gate7`, both **proposed**, not present merely because they are named here.
Use one summary row per assertion and named `*_ok` booleans. Add a machine-readable report
with `check_id`, `severity`, `actual`, `expected`, affected counts, and execution time.

The runner MUST fail on SQL errors, false **or NULL** assertion values, absent expected
assertions, and unexpected zero-row assertion results. Its current `is False` check is
insufficient: a NULL boolean can otherwise appear to pass. Build execution must stop on
failed preflight assertions before constructing or publishing dependent relations.

Fast tests exercise synthetic snapshots in CI. Full-corpus Gates 5/6/7 run locally or in
an appropriately provisioned integration environment and are recorded against an input
snapshot; CI does not need the raw licensed corpus to exercise the contracts.

## 15. Test specification

### 15.1 Unit/SQL fixtures

At minimum, fixtures cover:

1. All week-year boundaries, 53-week years, date endpoints, duplicate/missing weeks.
2. Stable hash encoding, null versus empty string, float encoding, and deliberate ID/key
   collision detection; identical annual keys after input reordering.
3. Identical roster copies, distinct payloads within a file, ACV-only disagreement,
   missing-vs-known evidence, and year reuse of an IRI key.
4. Per-attribute consensus without whole-row winner selection or duplicate-vote bias.
5. Chain unpivot with repeated chain IDs, missing cells, malformed mask labels, and
   ambiguous year/mask mappings; no physical-store continuity inferred.
6. Market number 0, two name aliases for one market, one label for two markets, unresolved
   labels, and one market spanning multiple regions.
7. Stub edition boundaries, all four tuple components, conflicting mappings across
   editions, all-null versus partially null keys, and leading-zero formatted UPCs.
8. Schema drift read with `union_by_name`, explicit `MISSING`, absent attributes, blank
   strings, similarly normalized attribute names, and missing category reference rules.
9. Exact sales sums, zero/signed units, values near integer limits, checked overflow,
   unmatched products, duplicate xref rows, and category/channel overlaps preserved.
10. Identical repeated panel purchases retained; absent minute distinct from zero;
    decimal coefficient/scale round-trip; unmatched/ambiguous trip candidates.
11. Annual demographic conflicts, missing demo years, static membership flags, and no
    accidental merging of advertising and general-panel populations.
12. Manifest reattempt success/failure ordering, legacy missing dataset, hash dedup
    crossing years, duplicate output paths, malformed manifest tails, orphan files,
    and removed selected files.
13. Assertion NULL/false/error/zero rows; missing expected checks must fail.

### 15.2 Publication/recovery tests

Inject a failure after each state-machine step, including after one table is written,
after release rename, after pointer switch, and before run-log append. Assert that readers
see either the old or complete new release, never mixed crosswalk/aggregate files.
Exercise concurrent writers/readers, lock contention, deleted output on resume, changed
SQL/reference with unchanged Bronze, stale-pointer recovery, and crash-left staging cleanup.

### 15.3 Integration acceptance

On the connected corpus, before first managed publication:

- Revalidate Gates 5 and 6 against the selected snapshot; do not rely only on prose status.
- Reproduce the observation/key conflict profile with alias scopes correctly restored.
- Record calendar coverage, product mapping, and sales joins without row inflation.
- Compare managed product-week output with the existing output on key, exact units, cents,
  and contributing record counts, not only file size or execution time.
- Open all promoted outputs with DuckDB and at least one independent Parquet reader.
- Start a fresh session, bootstrap one release, execute representative joins, and verify
  the same schemas and results after a no-op rebuild.
- Verify restricted data cannot be reached through the public bootstrap/artifact tree.

## 16. Proposed implementation organization

Keep the specification independently reviewable. Suggested future files:

```text
sql/silver/
  calendar.sql
  codes.sql
  store_observations.sql
  stores.sql
  chains.sql
  markets.sql
  products.sql
  product_attributes.sql
  household/                       # loaded only by restricted bootstrap
sql/gate7_silver.sql
scripts/silver/
  inputs.py                        # selected manifests, aliases, retention
  registry.py                      # relation contracts and dependency DAG
  build.py                         # orchestration, fingerprint, resources
  publish.py                       # locks, descriptors, commit/recovery
  bootstrap.py                     # release-pinned public/restricted views
reference/silver/
  channels.csv
  stub_categories.csv
  market_aliases.csv
  attribute_dictionary.csv
  source_semantics.md
  ...                              # only reviewed rules; no household data
tests/                             # existing Rust tests remain unchanged
```

Place Python/SQL fixture tests in an appropriate dedicated analytics test directory when
implemented. The illustrative file split is not a requirement to add a framework or use
these exact internal module names. Public relation contracts, input semantics, and
publication guarantees are the requirements.

Proposed operator interface:

```bash
# Future commands/targets; not implemented by this document.
make silver-plan LAKE=...                   # resolve inputs and invalidations; no writes
make silver-build LAKE=...                  # build/validate public staging release
make gate7 LAKE=...                         # validate a pinned managed release
make silver-publish RELEASE=...             # switch only after passing validation
make silver-bootstrap RELEASE=...           # register pinned public views
```

The implementation may combine build and publish in a convenience command, but must
preserve a validate-before-commit boundary. Parameters include selected snapshot, enabled
domains, memory/threads/spill controls, rebuild intent, and output/restricted roots.
`--overwrite` on the existing writer remains its legacy behavior until migrated; it must
not be silently reinterpreted as deleting managed releases.

## 17. Delivery sequence and definition of done

### Phase A — Contract foundation

- Implement input selection/source alias registry and scope reconstruction.
- Capture reviewed key encoding and reference schemas.
- Establish retained input snapshots, release descriptors, and publication/recovery tests.
- Implement calendar and code views; harden assertion handling.

**Done:** a fresh session can load one pinned release's calendar/code/source views, failures
cannot switch its pointer, and all provenance dependencies survive Bronze replacement.

### Phase B — Existing product building block under release control

- Preserve current tuple/UPC and UPC-week schemas.
- Add dependency fingerprints, exact unit/cent conservation, persisted-output checks,
  safe publication, and comparison against legacy materializations.
- Resolve category references separately; identity mapping does not depend on guessed
  category labels.

**Done:** one managed product release reconciles all selected sales, survives failure
injection, and resumes without rescanning unchanged sales.

### Phase C — Annual stores, chains, and markets

- Preserve roster observations; create unique annual identities and per-field statuses.
- Normalize chain mappings; publish assignments with clear reconstruction provenance.
- Publish county/market/region relations and exact-name mapping evidence; retain unresolved
  market labels and conflicting annual store labels.
- Verify manual-source semantics before enabling any use of that evidence.

**Done:** sales-to-store joins are complete and nonmultiplicative; chain/market assignment
coverage is reported honestly; no arbitrary winner or invented cross-year store continuity.

### Phase D — Product descriptive layer

- Publish edition-backed annual products, category mappings, long-form attribute evidence,
  consensus resolution, and coverage for years without annual attributes.

**Done:** consumers can join a product-year without edition fan-out and distinguish known,
source-missing, not-collected, partial, and conflicting attributes.

### Phase E — Restricted household layer

- Obtain access/retention/purpose approval.
- Publish lossless purchase/trip records and annual household observations.
- Enable only verified product/store/trip linkage rules; preserve unresolved cases.

**Done:** no lost purchase multiplicity or decimal precision, no false transaction keys,
no unsafe trip fan-out, and no restricted information in public releases/logs.

### Overall definition of done

A Silver domain is complete when it has an explicit grain/key/schema, documented source
scope, deterministic identity where justified, lossless observation evidence, tested
resolution rules, coverage/exception reporting, release-pinned views, required persisted
outputs, executable passing gates, reproducible lineage, and demonstrated rollback.
“Parquet exists” or “the query ran” is not completion.

## 18. Explicit remaining decisions and blockers

These do not justify inventing data while implementation proceeds:

| Item | Safe specified behavior until resolved | Blocks |
|---|---|---|
| Roster source precedence | Agreed observation preservation + per-field consensus | Only disputed attribute assignment, not annual identity |
| Physical-store continuity | No cross-year outlet link | Cross-year physical-store analyses |
| Mask reconstruction authority | Expose selected-source provenance/caveat; no claim of agreement with skipped versions | Stronger historical chain-authority claims |
| Manual-entry schema/year meaning | Preserve evidence; no assignment use | Manual enrichment |
| Market label aliases | Exact uppercase/trim matching only; unresolved status | Complete market assignment, not publication of truthful partial coverage |
| `KK` interpretation | Preserve raw code; unresolved normalized channel | A fully normalized panel channel distribution |
| Missing category matrix | Reviewed replacement with source provenance | Complete canonical product-category labeling |
| PANEL `COLUPC` correspondence | Preserve raw code; no guessed normalization | UPC-linked household analyses |
| `minute` epoch/timezone | Keep raw integer | Derived trip timestamps |
| Trips monetary fields / second store key | Keep exact pairs and both source keys | Basket money and stronger trip linkage |
| Demographic codebooks/aliases | Preserve source codes and annual observations | Decoded demographic interpretations |
| Household policy | Restricted domain disabled by default | Household publication/export |

The resulting architecture is intentionally conservative about **meaning** and strict about
**accounting**: uncertainty remains visible, while lost facts, fan-out, stale dependencies,
and mixed releases are prohibited.

## References

- [Silver kickoff and measured product-week work](silver_layer.md)
- [Architecture and Bronze conventions](../ARCHITECTURE.md)
- [Corpus readiness and gates](corpus_readiness.md)
- [Non-sales source findings](other_sources.md)
- [Excel-to-Bronze findings and missing-value encodings](excel_to_bronze.md)
- [Product-week SQL](../sql/silver_product_week.sql)
- [Current product-week materializer](../scripts/materialize_product_week.py)
- [Current SQL runner](../scripts/run_sql.py)
- [Current non-sales reconciliation](../sql/gate6_non_sales.sql)
