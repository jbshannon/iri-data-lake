# `data/IRI/Raw/` — Layout Reference

A survey of the raw IRI scanner-data drop. Use this when sketching an ingestion
strategy; it captures what's on disk, the regularities, and the irregularities.

Totals: **12 years × 31 categories ≈ 145 GB** of raw files, ~2.7 B sales rows.

> **Corrected by measurement.** This survey was written from IRI's
> documentation. Sniffing all 1110 PANEL headers, all 93 `prod_attr`
> files and all 372 `Delivery_Stores` files found several places where
> the data disagrees with what is written below. The corrections are
> marked inline with **⚠**; the full evidence, and the layout
> constants that replace each wrong claim, are in
> [`other_sources.md`](other_sources.md).
>
> The most consequential: PANEL has **four** dialects, not three;
> `prod_attr` has a **21-byte attribute pitch** that whitespace-splitting
> silently misaligns; and `CENTS998`/`CENTS999` in the trips files are
> **floats**, not integers. A parser written from this file alone would
> produce a lake that loads cleanly and is wrong.

---

## 1. Top-level layout

```
data/IRI/Raw/
├── Academic data set file and field description/   # 4 IRI PDFs + How to unzip.docx
├── Year1/ … Year12/                                # scanner data per year (12 dirs)
├── parsed stub files/                              # product stubs for years 1-6 (.xls)
├── parsed stub files 2007/                         # product stubs for year 7 (.xlsx)
├── parsed stub files 2008-2011/                    # product stubs for years 8-11 (.xlsx)
├── parsed stub files 2012/                         # product stubs for year 12 (.xlsx)
├── demos trips external/                           # household-panel + lookup tables
├── Pacesetters external/                           # new-product analytics PDFs/Excels
├── TNS advertising data2/                          # ad-spend xls files
├── static 1_12.csv                                 # top-level copies of common files
├── trips12 may13.csv
├── manual store entry external 8_12.csv
├── masked_chains crossreference1_12 RECONSTRUCTION Oct2018.csv
└── panel complete store list with chains.xlsx
```

Two files are duplicated at the top level (`trips12 may13.csv`,
`masked_chains crossreference1_12 RECONSTRUCTION Oct2018.csv`,
`manual store entry external 8_12.csv`) and again inside
`demos trips external/`. Treat them as the same content.

---

## 2. Per-year scanner data (`Year<n>/`)

Each `Year<n>` directory contains **31 category subdirectories** (the
`CATEGORIES` vector in `src/IRIData.jl`). Two of those 31 are renamed in the
middle of the series:

| Years | `paptowl` dir | `paptowls` dir |
|---|---|---|
| 1–7 | ✅ | — |
| 8–11 | — | ✅ |
| 12 | ✅ | — |

There is also one nested-level oddity: **`Year12/toothpa/toothpa/`** wraps an
extra directory level. The existing pipeline handles this in `salesdir`.

### 2.1 File inventory per (year, category)

For every (year, category) you generally find:

| File pattern | Count per (y, c) | Description |
|---|---|---|
| `<cat>_drug_<w1>_<w2>` | 1 | Drug-channel sales (fixed-width `.DAT` body, no ext) |
| `<cat>_groc_<w1>_<w2>` | 1 | Grocery-channel sales (same shape) |
| `<cat>_PANEL_<DR\|GR\|MA>_<w1>_<w2>.dat` | 3 | Household-panel transactions (years 1–7) |
| `<cat>_PANEL_<DK\|GK\|MK>_<w1>_<w2>.DAT` | 3 | Household-panel transactions (years 8–12) |
| `<cat>_prod_attr` | 1 (only years 9, 10, 11) | Per-UPC product-attribute text table |
| `Delivery_Stores` | 1 | Store roster for that (year, channel grouping) |
| `DEMOS.CSV` | 1 (only years 3, 4, 5, 8, 9, 10, 11) | Panelist demographics for that year |
| `IRI week translation.xls` | 1 (years 1–5) / `IRI week translation_2008_2017.xls` (years 8–12) | Week→date lookup |
| `ADB Measure Definitions.doc` | 1 (most years) | documentation |
| `panel_measure_definition.doc` | 1 (most years) | documentation |

#### Quirks

- **Year 8** is the only year that ships a 4th panel file: `<cat>_PANEL_KK_<w>_<w>.DAT`
  (drugstore "KK" channel). Years 1–7 and 9–12 have only DR/GR/MA or DK/GK/MK.
- **Year 6** and **year 7** are missing `IRI week translation*.xls` and the two
  `.doc` files in every category folder; some categories also lack `PANEL_DR`.
  Treat these as lean years.
- **Years 1 and 2** use lowercase `.dat` for panel files; **years 3+** use
  uppercase `.DAT`. PANEL file delimiter and column set also shifts at year 8
  (see §3.3).
- A handful of stragglers exist (Office lock files, `.OLD`, `.bak`):
  - `parsed stub files 2007/~$prod_factiss.xlsx` (Excel lock, 165 B)
  - `Year2/margbutr/margbutr_PANEL_GR_1166_1217.OLD`
  - `Year3/coldcer/coldcer_PANEL_GR_1218_1269.DAT.bak`
  - `Year3/coldcer/Edit1.bak`, `Edit2.bak`, `Edit3.bak`
  Filter these out in any directory walker.
- `parsed stub files/` (years 1–6) includes 3 `_sz` size-variant files that
  the existing pipeline excludes via the `r"^prod\d?\d?_[a-z]+.xls"` regex:
  `prod_beer_sz.xls`, `prod_carbbev_sz.xls`, `prod_mustketc_sz.xls`.

### 2.2 Size & row counts

Per-year totals across all 31 categories for the sales `*_drug_*` + `*_groc_*`
files (FWF, 54 bytes/row):

| Year | Drug rows | Groc rows | Total rows | Total GB |
|---|---:|---:|---:|---:|
| 1  | 12.9 M | 180.5 M | 193.4 M | 10.17 |
| 2  | 13.0 M | 184.6 M | 197.6 M | 10.41 |
| 3  | 13.3 M | 197.5 M | 210.8 M | 11.12 |
| 4  | 13.0 M | 197.8 M | 210.8 M | 11.09 |
| 5  | 13.5 M | 198.3 M | 211.8 M | 11.15 |
| 6  | 13.8 M | 204.8 M | 218.6 M | 11.46 |
| 7  | 16.4 M | 228.6 M | 244.9 M | 12.83 |
| 8  | 17.8 M | 234.6 M | 252.4 M | 13.27 |
| 9  | 16.8 M | 232.6 M | 249.4 M | 13.29 |
| 10 | 16.7 M | 223.9 M | 240.6 M | 12.83 |
| 11 | 16.8 M | 217.7 M | 234.5 M | 12.50 |
| 12 | 17.2 M | 218.7 M | 235.9 M | 12.36 |
| **All** | **181 M** | **2 520 M** | **2 701 M** | **143.1** |

Drug rows are ~7% of the volume; grocery rows dominate by a ~14× ratio because
grocery covers many more SKUs/categories. Largest single file:
`fzdinent_groc` (frozen dinners, grocery) at ~1.3 GB across most years; smallest
is `hotdog_drug` at ~0.5 MB.

PANEL files are an order of magnitude smaller (~1–2 M rows per year across all
categories). Stubs total ~200 MB across all four year-groups.

### 2.3 IRI week numbering

The week ranges encoded in the sales filenames are continuous across years
(IRI's `WEEK` field is a single integer counter, not "year-relative"):

| Year | Drug/Groc weeks | Weeks/yr |
|---|---|---|
| 1  | 1114–1165 | 52 |
| 2  | 1166–1217 | 52 |
| 3  | 1218–1269 | 52 |
| 4  | 1270–1321 | 52 |
| 5  | 1322–1373 | 52 |
| 6  | 1374–1426 | 53 |
| 7  | 1427–1478 | 52 |
| 8  | 1479–1530 | 52 |
| 9  | 1531–1582 | 52 |
| 10 | 1583–1634 | 52 |
| 11 | 1635–1686 | 52 |
| 12 | 1687–1739 | 53 |

Years 6 and 12 are 53-week years. The week→date lookup is in
`IRI week translation.xls` (years 1–5) or `IRI week translation_2008_2017.xls`
(years 8–12); it's missing entirely for years 6 and 7. The lookup maps each
`WEEK` (1114, 1115, …) to a calendar week-end date and is needed if you want
calendar dates.

---

## 3. Sales data (the bulk of the corpus)

### 3.1 File format

Every `*_drug_*` and `*_groc_*` file is a fixed-width `.DAT` with a single
ASCII header line followed by CRLF-terminated data rows. **The data rows are
exactly 54 bytes** (verified across all 12 years).

Header (55 bytes incl. CRLF):
```
IRI_KEY WEEK SY GE VEND  ITEM  UNITS DOLLARS  F    D PR
```

Column schema (1-based inclusive ranges, matches the existing `readfwf` schema):

| Column | Range | Type | Notes |
|---|---|---|---|
| IRI_KEY | 1–7 | Int64 | store id |
| WEEK | 9–12 | Int64 | IRI week number (see §2.3) |
| SY | 14–15 | Int64 | system/year flag |
| GE | 17–18 | Int64 | geographic region |
| VEND | 20–24 | Int64 | vendor code |
| ITEM | 26–30 | Int64 | item code (UPC components) |
| UNITS | 32–36 | Int64 | units sold |
| DOLLARS | 38–45 | Float64 | dollar sales |
| F | 47–50 | String | promo feature flag (NONE, A, A+, B, C, …) |
| D | 52 | Int64 | display flag |
| PR | 54 | Bool (0/1) | price-reduction flag |

Spacing varies (extra spaces around `VEND`, `ITEM`, `DOLLARS`, `F`) — the
existing `readfwf` parses by range, so whitespace handling is unnecessary.

### 3.2 Promo `F` distribution (sample)

From `Year12/beer_drug` (737 K rows):

| Value | Count | % |
|---|---:|---:|
| NONE | 573 108 | 77.8 |
| B | 148 661 | 20.2 |
| A | 15 018 | 2.0 |
| A+ | 249 | 0.03 |
| C | 34 | <0.01 |

### 3.3 PANEL files (NOT to be parsed with the same schema)

PANEL files are a **different format** than the drug/groc files:

| Years | Delimiter | Columns (header) |
|---|---|---|
| 1–3 | tab (`\t`) | `PANID WEEK UNITS OUTLET DOLLARS IRI_KEY COLUPC` |
| 4–7 | whitespace | same as 1–3 |
| 8 | comma | `PANID,WEEK,MINUTE,UNITS,OUTLET,DOLLARS,IRI_KEY,COLUPC` (note new `MINUTE` col) |
| 9–12 | comma | same as 8 |

> **⚠ Corrected.** Years 1–3 are tab, not 1–2 — year 3 ships tab
> files too. And there is a **fourth** dialect: year 11's
> `diapers_PANEL_GK_1635_1686.DAT` is **tab-delimited *with* the
> year-8+ `MINUTE` column**. "Years 8+ are comma-delimited" is wrong for
> exactly one file out of 1110, and a delimiter inferred from the year
> mis-parses it silently — a comma split of a tab line yields one
> field, which is not an error, just a row with no columns.
>
> **Treat the delimiter and the MINUTE presence as two independent
> facts, both read from the header line.** A year-based lookup cannot
> get this right.

> **⚠ Also: `UNITS` and `DOLLARS` have three encodings.** Exact
> 2-decimal (`6.99`), a 32-bit float rendering of one
> (`0.7299998474`), and a genuinely sub-cent computed average
> (`4.3091992188`). The third is 0.4 % of all PANEL rows and is
> concentrated — 12.7 % of `Year10/carbbev`'s `PANEL_GK`. A strict
> integer-cent parser nulls out **67 783 real dollar amounts**.

> **⚠ And: two PANEL files are zero bytes** — `Year1/beer`'s `PANEL_DR`
> and one Year-2 equivalent. They have no header at all, so a parser
> that requires one turns them into permanent failures.

The `OUTLET` codes also change across years:

| Years | OUTLET codes | Channel |
|---|---|---|
| 1–7 | DR, GR, MA | drug, grocery, mass |
| 8 | DK, GK, KK, MK | drug, grocery, combo, mass (year 8 adds KK) |
| 9–12 | DK, GK, MK | drug, grocery, mass |

Year 1 has zero rows in `PANEL_DR` for some categories (e.g. beer: 0 rows).
Year 8 has the extra `PANEL_KK` file per category.

PANID is the **household panelist id**, not a store id. The PANEL files are
the trip-level "this household bought X" data; the drug/groc files are the
store-level weekly aggregates.

---

## 4. `Delivery_Stores` — store roster

> **⚠ Corrected.** This file is **fixed-width at 63 bytes**, not
> whitespace-aligned — all 37 200 data rows across all 372 files are
> exactly 63 bytes. And the header's own token positions do **not**
> match the data's field positions, so slicing by them lands mid-field:
>
> ```text
> header  IRI_KEY@0  OU@8  EST_ACV@11  Market_Name@20  Open@45  Clsd@50  MskdName@55
> data     200039@1   GR@8  9.709999@11 BUFFALO/…@20     539@46   1219@50   Chain87@55
> ```
>
> The right-padded `IRI_KEY` is what throws the first field off by
> one. The measured layout is
> `IRI_KEY [0..8) OU [8..11) EST_ACV [11..20) Market [20..45) Open
> [45..50) Clsd [50..55) MskdName [55..63)`.
>
> `Open`/`Clsd` are **IRI week numbers** in the same 1114–1739 space as
> `iri_sales.week`, with `9998` as the "still delivering" sentinel — a
> value, not a null.

Found in every (year, category) directory. ~2 000 stores per year, ~2 MB. The
`Y1` year shows **6 distinct MD5s** across 31 categories — meaning there are 6
different store-channel editions per year (one per channel grouping: e.g.
grocery-mass stores differ from drugstore stores). The same MD5 can be shared
across many categories because they all see the same store set.

Header:
```
IRI_KEY OU EST_ACV  Market_Name              Open Clsd MskdName
```

`MskdName` is the masked chain name. Example row:
```
 200039 GR 9.709999 BUFFALO/ROCHESTER         539 1219 Chain87
```

`OU` is the outlet code (GR, MA, DR, GK, MK, DK, …) and matches the OUTLET
codes used in the PANEL files.

---

## 5. `DEMOS.CSV` — panelist demographics

Present in **years 3, 4, 5, 8, 9, 10, 11** — i.e. all categories get a copy.
~4 000–6 500 rows per (year, category) (panelist sizes shrink over time as
panels roll). Header uses quoted CSV, ~40 demographic columns:

```
"Panelist ID","Panelist Type","Combined Pre-Tax Income of HH","Family Size","HH_RACE","Type of Residential Possession","COUNTY","HH_AGE","HH_EDU","HH_OCC",…
```

Likely the same panelist pool as `trips<n>.csv`, just joined with demographic
attributes.

---

## 5.1 `<category>_prod_attr` — item attribute dictionary

> **Not in the original survey.** Years 9–11 only, 93 files, 608 MB.
> Fixed-width, but **not** at the header's token positions: a 27-byte
> key prefix (`SY GE VEND ITEM VOL_EQ`) followed by **N attribute
> columns of exactly 21 bytes**, where `N = ceil((row_len - 27) / 21)`.
>
> Three traps, all of which produce a lake that loads cleanly and is
> wrong:
>
> - Whitespace-splitting misaligns, because `MISSING` padded to 21 bytes
>   makes two adjacent missing attributes look like one 42-byte field.
> - **The count is a `ceil`, not a floor.** `(row_len - 47) / 21` gives
>   29 for `Year9/beer`'s 656-byte rows, one short — it drops the 30th
>   attribute (`WINE/LIQUOR TYPE`, with real values) in all 93 files,
>   because the 20-byte remainder looks exactly like padding.
> - **The header is 4 bytes shorter than the rows** (652 vs 656): its
>   last slot holds a 16-character name, the row's a 21-wide value
>   field. One length cannot slice both.
>
> A value can also **overflow its slot**: `Year9/beer`'s `PACKAGE` is
> `LONG NECK BTL IN BOX BEER`, 25 bytes in a 21-byte field, so its last
> four bytes land at the start of the next column and `BEER` reads as
> `PRODUCT TYPE`. The grid is still exactly 21 bytes in every row.

## 6. Product stubs

Four directories cover the 12 years with drifting filename codes:

| Dir | Years covered | Files | Format | Total MB |
|---|---|---:|---|---:|
| `parsed stub files/` | 1–6 | 34 (incl. 3 `_sz` to skip) | .xls (BIFF) | ~62 |
| `parsed stub files 2007/` | 7 | 31 + 1 lockfile | .xlsx | ~19 |
| `parsed stub files 2008-2011/` | 8–11 | 31 (prefix `prod11_`) | .xlsx | ~25 |
| `parsed stub files 2012/` | 12 | 31 (prefix `prod12_`) | .xlsx | ~97 |

Filename codes change over time — `reference/prod.csv` (tracked in git) is
the year × category matrix that maps each code to a canonical category.

---

## 7. External / aggregate data (`demos trips external/`, top-level)

| File | Rows | Description |
|---|---:|---|
| `trips1 jul08.csv` … `trips7 jul08.csv` | 500 K–730 K/yr | Household-trip records (years 1–7) |
| `trips8 may13.csv` … `trips12 may13.csv` | 395 K–315 K/yr | Household-trip records (years 8–12) |
| `trips8/9/10/11 may13.zip` | — | Compressed duplicates of years 8–11 trips CSVs |

> **⚠ Corrected: `CENTS998` / `CENTS999` / `KRYSCENTS` are floats,
> not integers.** The corpus holds `4470.8359375` and `4600.316406` —
> 32-bit float renderings of 2-decimal amounts (`float32(4470.84)` is
> `4470.8359375`). Parsing them as integers nulls **4.6 M of 7.2 M
> rows**, silently. They are also not comparable to
> `iri_sales.dollars_cents`: the unit is IRI's own scaled unit, the
> corpus ships no codebook for it, and the values are plainly dollars
> (`4470.84` is a plausible trip total; `$44.71` is not).
>
> `CENTS998` is genuinely empty in 92–97 % of rows, which is a fact
> about the corpus, not a parse failure.
>
> **⚠ And: the ad-panel files use two stems.** `ads demo<n>` for years
> 1–8 and `ads demos<n>` for years 9–12, with `.csv` and `.CSV` mixed
> — 24 directory entries, not 12. Matching only the `demo` stem silently
> drops years 9–11.
| `static 1_12.csv` | 87 051 | Panelist + Trip_Count + make_static flag by year |
| `static 1_7.csv` | 47 767 | Same, years 1–7 only |
| `static1_5.csv` | 30 154 | Same, years 1–5 only |
| `ads demo1.csv` … `ads demos12.csv` (note case-mixed) | — | Ad/demo data per year |
| `manual store entry external.csv` | — | Manual store-chain codes (early years) |
| `manual store entry external 8_11.csv` | — | Manual store-chain codes (years 8–11) |
| `masked_chain_xref.csv` | — | Early masked-chain cross-ref |
| `masked_chain_xref1_7.csv` | — | Masked-chain xref, years 1–7 |
| `masked_chain_xref1_12.csv` | — | Masked-chain xref, years 1–12 (reconstruction) |
| `IRI week translation.xls` | — | Week→date lookup |
| `fips by IRI market.xls` | — | FIPS-county → IRI-market mapping |
| `panel complete store list with chains.xlsx` | — | Full panel store list |
| `ADB Measure Definitions.doc` | — | Variable definitions |
| `panel_measure_definition.doc` | — | Panel measure definitions |

Two top-level files duplicate content already in `demos trips external/`:
`trips12 may13.csv` and `manual store entry external 8_12.csv`. (Top-level
`masked_chains crossreference1_12 RECONSTRUCTION Oct2018.csv` duplicates
`demos trips external/masked_chain_xref1_12.csv`.)

Trips files have ~5 000–8 800 unique PANIDs per year (decreasing over time);
cols are `PANID, WEEK, IRI_Key, MINUTE, CENTS998, CENTS999` with later years
adding columns.

`static*` files have cols `PANID, Trip_Count, make_static, year` — each
row is one (panelist, year) with how many trips they took and whether they
were made static.

---

## 8. Documentation files (read-only references)

`Academic data set file and field description/` contains four IRI PDFs:

- `Academic data set file and field description 1_5.pdf` (covers years 1–5)
- `… 2_00.pdf` (2.00 rev)
- `… 2_3.pdf`
- `… 2_31.pdf` (latest, years up to 2018)

Plus `How to unzip.docx`. The schema in §3.1 is documented in these PDFs but
the schema in `src/sales.jl` (and here) is the canonical version.

`Pacesetters external/` and `TNS advertising data2/` are unrelated analytics
supplements (PDFs of new-product reports and ad-spend Excels). Probably not
needed for sales/stub ingestion.

---

## 9. Ingestion-strategy notes

1. **Walk, don't glob by year.** The directory walker must handle
   `Year12/toothpa/toothpa/<files>` (one extra nesting level) and the
   `paptowl` ↔ `paptowls` rename in years 8–11. Year-group keys work better
   than filename regexes for routing.

2. **Filter aggressively.** Skip the 4 known stragglers (lockfiles,
   `.OLD`, `.bak`, `_sz` stub variants) and any file not matching one of:
   `<cat>_(drug|groc)_\d+_\d+`, `<cat>_PANEL_(DR|GR|MA|DK|GK|MK|KK)_\d+_\d+.(dat|DAT)`,
   `<cat>_prod_attr`, `Delivery_Stores`, `DEMOS.CSV`.

3. **Use the FWF schema.** Sales data is fixed-width with explicit byte
   ranges — already validated across all 12 years. Don't CSV-parse it.
   The 54-byte row length is invariant.

4. **Three different PANEL formats.** Years 1–2 are tab-delimited without
   `MINUTE`; years 3–7 are whitespace-delimited without `MINUTE`; years 8–12
   are comma-delimited with `MINUTE`. Year 8 adds `PANEL_KK`. Sniff the
   header line to choose the right parser.

5. **`Delivery_Stores` is deduplicatable.** Six distinct MD5s per year cover
   all 31 categories. Hash once, read once, broadcast to all categories that
   share the MD5.

6. **`DEMOS.CSV` is also deduplicatable within a year** (all 31 categories
   share the same panelist pool). Quick win for storage.

7. **`prod_attr` is year-9–11 only.** Not present for years 1–8 or 12.
   Treat as optional enrichment.

8. **Stubs need two parsers.** `.xls` (BIFF) for years 1–6 → `xlrd`;
   `.xlsx` (OOXML) for years 7–12 → `openpyxl`. Both already declared in
   `pyproject.toml` and used via `PythonCall.jl`.

9. **Trips & static overlap.** `static 1_12.csv` covers all years; the
   smaller `static 1_7.csv` and `static1_5.csv` are supersets of the same
   data over smaller windows — only need the largest one.

10. **Top-level duplicates of external files** can be deleted or skipped —
    they add ~30 MB of redundancy.

11. **Watch memory.** ~2.7 B sales rows total. A single `Year<n>/<cat>_groc_*`
    file is ~1.3 GB / ~220 M rows at the upper end (frozen dinners). The
    existing `readfwf` uses `Threads.@spawn` per row and is single-machine
    memory-safe; do not load a whole year into RAM without chunking.

12. **Week lookup is missing for years 6 and 7** — *in the per-year
    copies*. `demos trips external/IRI week translation.xls` covers the
    full **1114–1739** range with a `Year` column and is the only copy
    that includes years 6 and 7. The 311 per-year files are subsets of
    it (the year-1 copy starts at week 1138, not 1114), so ingest the
    external copy and skip the rest.

---

## 11. Corrections index

Every claim above marked **⚠**, with its evidence in
[`other_sources.md`](other_sources.md):

| § | claim | reality |
|---|---|---|
| 3.3 | PANEL has 3 dialects, years 1–2 tab / 3–7 whitespace | **4** dialects; years 1–3 tab; one year-11 file is tab **with** MINUTE |
| 3.3 | PANEL `DOLLARS` is a 2-decimal decimal | **three** encodings; 0.4 % sub-cent, concentrated in `carbbev` |
| 3.3 | — | two PANEL files are **zero bytes** |
| 4 | `Delivery_Stores` is space-aligned | fixed-width **63 B**; header offsets are off by one |
| 4 | `Open`/`Clsd` are dates | IRI **week numbers**, `9998` = open-ended |
| 5.1 | *(not covered)* | `prod_attr` exists: 93 files, 608 MB, 21-byte attribute pitch |
| 7 | trips money are cents | **floats**; 4.6 M of 7.2 M rows nulled if parsed as ints |
| 7 | 12 `ads demo*.csv` | **two stems**, two extension cases, 24 entries |
| 12 | week lookup missing for years 6–7 | true of the per-year copies; the external copy has them |
