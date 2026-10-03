# Corpus readiness — running the full IRI corpus

Status: **mostly plan.** This document changes no behaviour, but it did
surface two bugs, both fixed in the same branch: G2 (`--resume` and
`--overwrite` were inert) and G8 (a broken `make fixtures` target), plus
the G5 leftover-cleanup gap, now automatic. The rest are records and
proposals. The point is
that the first all-corpus ingest is a *gated* operation rather than an
exploratory one.

The design point is the full corpus: **140.85 GiB raw / 2.69 B rows / 744 sales
files**, largest file ≈1.3 GB / 220 M rows (measured via `inventory`; see
§2 — `README.md` rounds this to "≈143 GB / 2.7 B", conflating GB with GiB).
`ARCHITECTURE.md` describes the
per-file pipeline; this document describes how to get from "works on one
fixture" to "the whole corpus is in the lake and reconciled".

The performance knobs (`batch_rows`, `row_group_rows`, `compression`,
`worker_threads`) are **owned by the `benchmarking` worktree** and are
deliberately not re-decided here. What this document fixes is the
*sequencing*: which gates must pass, in what order, and what "done" means.

## 0. Known gaps that gate the full run

These were found while writing this plan. Each is either a dependency on
another worktree or a decision that has not been made yet.

| # | gap | impact on a full run | owner |
|---|---|---|---|
| G1 | **Closed.** `ingest-all` ran files sequentially in a `for` loop; `rayon` was a declared dependency and `--workers` was destructured as `workers: _`. Now: a bounded rayon pool over the file list, one shared lock-serialised manifest, `--order` / `--shard` scheduling flags, and per-file output verified **byte-identical** to the sequential run. Full corpus measured at **~220 s** (two forced runs at 221.10 s and 220.29 s, agreeing to 0.4 % with the machine-state control flat; an earlier 271 s / 406 s pair was a hot machine — see `parallelism.md` §6), was ~17 min sequential. Implementation, measurements and the rejected designs are in [`docs/parallelism.md`](parallelism.md). | — | closed |
| G2 | **Fixed on `planning`.** `--resume`/`--overwrite` were parsed but never reached the ingest path: `Cli::overwrite_mode` was never called and `config.overwrite` was never read, so `OverwriteMode` was dead code and `skip_decision` alone drove the skip. `--overwrite` silently skipped; `--resume --overwrite` resolved silently. Now wired through and covered by tests. | — | closed |
| G3 | `UnknownFeaturePolicy::Fail` is hard-coded (`Cli::unknown_feature_policy`), and the config field is not overridable from the CLI. | Any unexpected `F` token fails its whole file. **Decision: keep as-is** — failing loudly on an unexpected `F` token is the right default for a bronze layer, and the manifest records the failure so the run stays resumable. Revisit only if the real corpus turns out to contain such tokens. | accepted |
| G4 | `week_range_strict` defaults to `false` and is not settable from the CLI. | **Decision: defer.** Assume filename week ranges are recorded correctly for now; add a catch later once testing gives evidence about whether that assumption holds. §5's per-year week-range query stays as a cheap corpus-wide backstop. | deferred |
| G5 | **Closed.** Two independent consequences of `ManifestStatus::InProgress` never being written are now handled. **(a) Failures visible only in stdout:** `ingest_all` writes a `status: "failed"` record carrying the error message for any source it could not ingest, so `manifest.jsonl` records its own gaps rather than leaving them in the log — which is exactly what a crashed or re-run job loses. A failed record never matches the skip check, so the source is still retried next run. **(b) Unattended `*.tmp` leftovers:** an interrupted run leaves a `.tmp` sibling with no manifest line, so `src/cleanup.rs` sweeps `*.tmp` under the output root at the start of every `ingest` / `ingest-all`, before anything is written, deleting only those older than 24 h (`--tmp-max-age-hours`, `IRI_LAKE_TMP_MAX_AGE_HOURS`). The age gate means a concurrent run's in-flight `.tmp` is never a candidate, and failures are logged, never fatal. `make clean-tmp` runs the same sweep by hand. (b) landed on `main` first, as `c24e59f`. | — | closed |
| G6 | **Closed as a side effect of G1.** `manifest.jsonl` was append-only and `last_for_path` re-read the whole file per lookup. | O(records²) per run was never actually a problem at 744 records. `manifest::SharedManifest` now loads the file once into an index and serialises appends behind a mutex, so appends are atomic by construction rather than by `O_APPEND` luck. | closed |
| G7 | **False alarm — corrected.** `data/raw` is a symlink to `~/.julia/dev/IRIData/data/IRI/Raw` and resolves fine; the corpus is staged and measures 140.85 GiB / 744 files / 2.69 B rows. An earlier draft of this table claimed the symlink was "dangling" on the strength of `du -sh data/raw` reporting 0 B. That command reports the *symlink itself*; it does not follow the link. Use `du -shL` or a trailing slash. | none | closed |
| G8 | **Closed upstream in `c24e59f`** (merged via PR #3), which drops the `make fixtures` target. Investigated here before acting on it, and confirmed it was genuinely vestigial rather than merely broken: the test target was deleted after the initial scaffold (`d04540b`), there is no `tests/fixtures/` directory, `.gitignore` never mentions it, no test reads a fixture off disk, and `tests/common/mod.rs` builds every fixture into a fresh tempdir at test time. Re-adding an emitter would have written a directory nothing reads. | — | closed |

### The one that actually matters

G1 is the difference between a one-night run and a one-week run.
**Closed and measured**: the full corpus ingests in **~220 s** at
`--workers 8`, landing **744/744 files, 0 failures, 1 rejected row,
2 700 651 386 rows**. Two forced runs measured 221.10 s and 220.29 s —
agreeing to 0.4 % with the machine-state control flat at
1 443–1 475 MiB/s — so on a cool box the figure is repeatable rather
than a band. (An earlier pair of 271 s / 406 s runs was a hot machine;
see `parallelism.md` §6.) The schedule for the full ingest can now be
committed. Two things moved the number and are now the default:
byte-balanced work distribution and **smallest-first** dealing, worth
20 % over the largest-first ordering `ARCHITECTURE.md` originally
recommended. See [`docs/parallelism.md`](parallelism.md).

## 1. Verify the staged corpus

The corpus is expected to already be staged on local NVMe (it is, on this
machine). The runbook assumes a symlink so the repo layout is unchanged.

```bash
# Confirm the staged root is real and local before anything else.
readlink data/raw                       # where it currently points
[ -e data/raw ] && echo "resolves"      # -e FOLLOWS the symlink
ls -L data/raw | head                   # -L lists the target, not the link
df -h "$(readlink data/raw)"            # must be local NVMe, not network
du -shL data/raw                        # expect ~141 GiB (note the -L)
```

> Beware: `du -sh data/raw` reports the **symlink itself** and prints `0B`
> even when the corpus is fully present. Use `du -shL data/raw` or
> `du -sh data/raw/` (trailing slash), or `test -e`, which follows links.

Record the staging source and destination in the run log. If the corpus is
ever re-staged, every `source_sha256` in `manifest.jsonl` still matches only
if the bytes are identical — which is exactly what the SHA-256 in the resume
check is for.

## 2. Gate 1 — inventory

No parsing happens here. This is the cheapest way to prove discovery sees
what we think it sees.

```bash
cargo run --release -- inventory --input data/raw --format json > /tmp/inventory.json
```

Check, before going further (measured baseline, from an actual run on the
staged corpus):

- file count ≈ 744 — **confirmed exactly 744**
- no `skipped[]` entry is a file we expected to ingest (PANEL, stubs,
  `Delivery_Stores`, `DEMOS.CSV` **should** appear here — that is the
  filter working)
- the `year=` spread covers 1..=12 with no year gaps — **confirmed, all 12
  years present**
- category names look like real IRI categories, including the known
  `paptowl` / `paptowls` spelling split

Measured inventory:

```
 year  channel          files    GiB           rows
    1 drug             31   0.67       12904698
    1 groc             31   9.41      180491970
   ...
   12 groc             31  11.41      207350429

TOTAL files=744 bytes=140.85 GiB expected_rows=2689259921
skipped=2733 (top reasons: backup_extension=5, not_sales_filename=468,
panel_file=1108, stub_or_excel=1152)
```

The shape is strikingly regular: **31 files per year per channel**,
12 years × 2 channels × 31 = 744. That uniformity is itself a check — if a
future inventory shows any year/channel not equal to 31, something is being
skipped that should not be. The 2733 skips break down as PANEL (1108),
stub/Excel (1152), non-sales filenames (468) and backups (5), which is the
expected behaviour of the discovery filter.

**2.69 B rows**, not the 2.7 B quoted in `README.md`; `du` reports 140.85 GiB
against the "≈143 GB" quoted there (GB vs GiB).

If discovery is wrong, stop. Everything downstream inherits the mistake.

## 3. Gate 2 — full validation sweep

Validate **every** file before ingesting any of them. `validate --full`
checks header bytes and every record's alignment; it does not write output,
so it is safe to run against the whole corpus.

> **One file in the staged corpus is imperfect**, so this is not
> hypothetical: `Year12/soup/soup_groc_1687_1739` is 637 922 152
> bytes and `(size - 57) % 56 == 55`. All eleven content fields of its
> final record are intact; what is missing is the `LF` of the final
> CRLF:
>
> ```
> complete record : ' 252154 1724  0  1 51000 13459    11    26.29 NONE 0 0\r\n'
> failing record  : ' 252154 1724  0  1 51000 18064     4     9.56 NONE 0 0\r'
> ```
>
> A row is 54 content bytes plus a 2-byte terminator, and every field is
> read from offsets 0..54 — the terminator is never a source of data. So
> this file loses nothing queryable, and it **passes** the gate with a
> warning. All 11 391 465 of its complete records are validated (the
> old code scored it `expected_rows = 0` and therefore checked *none*
> of them). Ingest writes those 11 391 465 rows, logs at info, and
> records `rejected_rows = 1` for the unterminated row. See decision 6
> in §7.

```bash
# One file:
./target/release/iri-lake validate data/raw/Year1/beer/beer_drug_1114_1165 --full

# The whole gate:
make gate2                      # ~7 min on the staged corpus
scripts/gate2_validate.sh -j 8  # same thing, explicit
```

Results land in `/tmp/gate2/`: `reports.txt` (one line per file) and
`failures.txt` (the non-zero exits). The gate exits 1 if anything fails,
so it is usable from a script.

Do not re-derive the file list in the shell. The sweep used to be a
`find | grep -Ev ...` pipeline, which is a *different* implementation of
discovery's filter rather than the same one — discovery skips 2 733
files across six reasons, and PANEL/stub files are not reliably
filterable from the shell. `inventory --format paths` emits the list
from `discovery.rs` itself, and the gate and the ingest run therefore
cannot drift apart.

### Result on the staged corpus (2026-10-03)

```
744 discovered source files, 8-way parallel, release binary
744 pass, 0 fail, 385s
```

**Every file in the corpus passes `--full`.** For all 744 that means a
correct header, correct alignment, and every record's CRLF verified —
2.69 B records' worth. That is a stronger statement than Gate 1 (which
only counted files) or Gate 4 (which only proves what was written
parses); this checks the input.

One of the 744 carries the terminator-only defect named above, and it
passes with a warning rather than failing. A file whose final record is
missing *field* bytes still fails — that is real data loss and the gate
reports it as such. The two are separated by
`fixed_width::classify_trailing`, and `tests/trailing_defect_tests.rs`
pins both directions.

Do not re-derive the file list in the shell. The sweep used to be a
`find | grep -Ev ...` pipeline, which is a *different* implementation of
discovery's filter rather than the same one — discovery skips 2 733
files across six reasons, and PANEL/stub files are not reliably
filterable from the shell. `inventory --format paths` emits the list
from `discovery.rs` itself, and the gate and the ingest run therefore
cannot drift apart.

Because `week_range_strict` is off (G4), this sweep does *not* catch
filename/row week disagreements; those are caught in Gate 5.

## 4. Gate 3 — the ramp

Ingest in widening scopes. Each step must produce a **clean manifest** (no
`status: "failed"` lines for the scope just run) before the next step.

```bash
OUT=data/lake

# 3a. One file, then prove resume and idempotency.
cargo run --release -- ingest data/raw/Year1/beer/beer_drug_1114_1165 --output-root $OUT
cargo run --release -- ingest data/raw/Year1/beer/beer_drug_1114_1165 --output-root $OUT   # expect SKIP
ls $OUT/bronze/iri_sales/year=1/category=beer/channel=drug/

# 3b. One category, whole year.
cargo run --release -- ingest-all --input data/raw --output-root $OUT --category beer

# 3c. One year.
cargo run --release -- ingest-all --input data/raw --output-root $OUT --year 1

# 3d. Dry run over the whole corpus — confirms counts, writes nothing.
cargo run --release -- ingest-all --input data/raw --output-root $OUT --dry-run
```

### Result (2026-10-03, into `data/lake`)

| step | scope | files | result |
|---|---|---:|---|
| 3a | one file, twice | 1 | ingested 660 096 rows; second run **SKIP**; 1 Parquet file, 0 `.tmp` |
| 3b | `--category beer` (all 12 years) | 24 | 23 completed, 1 skipped (3a's file), **0 failed**, 10.8 s |
| 3c | `--year 1` | 62 | 60 completed, 2 skipped (3a's), **0 failed**, 16.1 s |
| 3d | whole corpus, `--dry-run` | 744 | 744 selected (140.85 GiB, ~2.689 B rows); manifest and Parquet counts unchanged — 84 records, 365 files |

Every step produced a clean manifest: **84 records, all `success`, zero
`failed`, zero `in_progress`, zero stray `.tmp`**.

Skipping was exercised rather than assumed — 3a's file was skipped
twice, once per later scope — and the dry run confirmed it writes
nothing by leaving both the manifest (84 records) and the file count
(365) untouched.

An early sanity pass over what the ramp wrote, before committing to the
full run:

```
year=1 coverage      31 categories, 2 channels, 193,396,668 rows, weeks 1114-1165
beer, all years      12 years, 132,919,108 rows, $6,900,619,153.41
sentinels            zero_iri_key 0, negative_dollars 0, feature_code>4 0
manifest vs disk     317,129,053 = 317,129,053, rejected_rows 0
```

The manifest/disk agreement is the check that matters here: 317 129 053
rows claimed, 317 129 053 rows on disk, across two overlapping scopes
ingested by three separate invocations.

**Do not wipe `data/lake` before Gate 4.** The ramp's 84 files are
legitimate output recorded in the manifest, and Gate 4's
`ingest-all` will skip them and ingest the remaining 660. That is the
resume path working, not wasted work.

Note that 3b and 3c pass **no `--resume`**, and none is needed: skip-if-present
is the resolved default when neither `--resume` nor `--overwrite` is given.
Pass `--overwrite` only when you intend to discard and rewrite completed
output — it now genuinely re-ingests rather than silently skipping. Passing
both is a hard error.

### Interruption and restart

If a run is interrupted, in order of preference:

1. Re-run the identical command. Completed files skip, incomplete files
   restart.
2. Nothing else is required. `ingest` and `ingest-all` sweep stale
   `*.tmp` leftovers under the output root before they write anything,
   so the debris from a killed run is reaped automatically. Only
   leftovers older than `--tmp-max-age-hours` (default 24 h) are
   removed, which protects a `.tmp` that a *concurrent* run is still
   writing.

To sweep by hand, without starting a run:

```bash
make clean-tmp                                   # same policy, via find
cargo run --release -- ingest-all --input data/raw \
  --output-root data/lake --dry-run              # does NOT clean; dry-run writes nothing
```

```bash
# What the automatic sweep does, for inspection:
find $OUT -name '*.tmp' -print -delete            # unconditional: only when
                                                  # no other run shares $OUT
```

## 5. Gate 4 — the full run, and Gate 5 — reconciliation

The full run:

```bash
RUST_LOG=iri_lake=info cargo run --release -- \
  ingest-all --input data/raw --output-root data/lake 2>&1 | tee /tmp/full-run.log
```

Measured on the staged corpus (defaults, i.e. `--workers 8
--order smallest-first`):

```
ingest-all: 744 file(s) selected (140.85 GiB raw, ~2.689B rows) of 744 discovered
ingest-all done: completed=744 skipped=0 failed=0 workers=8 wall=406.12s
  throughput: raw=355.1 MiB/s rows=6.65 M/s out=9.40 GiB bytes=151236520079
              rows=2700651386 rejected_rows=1 slowest_file=25.5s
  note: 1 row(s) rejected as incomplete records (trailing bytes of a truncated source)
```

**~220 s on a cool box; quote the control with it.** Two forced
`--overwrite` runs back to back measured **221.10 s** and **220.29 s**
(744 files each, 0 skipped, 0 failed), with the `shasum` control steady
at 1 443–1 475 MiB/s across both. That 0.4 % agreement is the point:
the widely-quoted ±50 % band from the earlier 271 s / 406 s pair was a
hot machine, not a property of the pipeline — that pair is retained in
`parallelism.md` §6 as the hot-machine bracket. Against the ~17 min
single-threaded baseline the run is **~4.6x**. 744 files, **0
failures**, `rejected_rows = 1`, no stray `.tmp` files, and
**140.85 GiB raw in / 9.40 GiB Parquet out = 15.0x** (of which ~3/4 is
Parquet encoding rather than zstd).

`rows=2700651386` is the new reconciliation target: the inventory's
2 689 259 921 plus the soup file's 11 391 465 complete records. Note
that `inventory` counts an unaligned file as **0 rows**
(`expected_rows(..).unwrap_or(0)`), which is why the discovery total
has to be corrected by hand before it can serve as an expectation.

Re-running the identical command (the interrupted-run path) took
**110 s**, skipping all 744: SHA-256 is computed before the skip
decision, so a resume re-hashes the whole corpus. Budget for that when
a run is interrupted.

Reconciliation afterwards. These are the queries that decide whether the
lake is trustworthy, independent of anything the CLI reported. They live
in [`sql/gate5_reconciliation.sql`](../sql/gate5_reconciliation.sql)
and are run by:

```bash
make gate5                       # against data/lake
make gate5 LAKE=/tmp/lake-full2  # against any other output root
```

Every check returns a boolean column named `gate_5x_ok`, and
`scripts/run_sql.py` exits non-zero if any of them is false, so this is
a gate rather than a table to read and judge by eye.

| check | asserts |
|---|---|
| **5a** | one manifest record per source, all `success`, no `in_progress`, and `sum(written_rows)` equals the Parquet row count exactly |
| **5b** | the run lands on **2 700 651 386 rows across 744 successful records** with `rejected_rows = 1` — the inventory's 2 689 259 921 plus the soup file's 11 391 465 complete records |
| **5c** | 12 years, each with 31 categories and 2 channels on the Parquet side, and 62 sources each on the manifest side |
| **5d** | `iri_key = 0`, `dollars_cents < 0` and `feature_code > 4` are all zero |
| **5e** | every year's observed min/max `week` matches `fixed_width::week_to_year`'s table — the G4 backstop, since `week_range_strict` is off |

Three traps in writing these, all hit while writing them:

- **`parquet_metadata()` is per (row group, column).** `sum(row_group_num_rows)`
  over it returns rows x 11 — I got 29 707 165 246 against 2 700 651 386
  actual rows, exactly 11x. Use `count(*)` over `read_parquet` for row
  counts (0.4 s on 2.7 B rows) and `count(DISTINCT file_name)` over
  `parquet_metadata` for file counts.
- **A source is not a Parquet file.** One 1.4 GB source becomes many
  Parquet files, one per 1 M-row batch (232 for Year 1), so "62 files
  per year" is a statement about the *manifest*, not about
  `read_parquet`. 5c checks both sides against the right source.
- **DuckDB's Python package ships no CLI.** `python -m duckdb` fails.
  That is why Gate 5 runs through `scripts/run_sql.py` rather than the
  `duckdb` shell the older drafts of this document assumed.

DuckDB is a `uv` project (`pyproject.toml` + `uv.lock`); nothing in the
Rust build depends on it.

A finished run is one where: no manifest line has `status: "failed"` for a
file that has not since succeeded (failed lines *are* now written — see
decision 5 in §7), 5a matches 5b to within the documented file-count
tolerance, the manifest's `sum(rejected_rows)` is 1 (the one truncated
record), 5d is all zeros, and 5e's per-year week ranges match
`fixed_width::week_to_year` (1114–1165 → year 1 … 1687–1739 → year 12).

**Caveat on 5a vs 5b:** the Parquet `count(*)` can only ever equal the
manifest's `sum(written_rows)` — the same run writes both. The
*independent* check is against `inventory`, which is why 5b needs the
correction above.

## 6. Definition of done

- [x] G1 closed and end-to-end throughput measured on the real corpus
      (**~220 s**, two forced runs 0.4 % apart with a flat machine
      control; 744/744 files, 0 failures — `docs/parallelism.md`)
- [x] Corpus staged on NVMe, path recorded (140.85 GiB, resolves)
- [x] Gate 1 inventory reviewed — 744 files, 2 689 259 921 rows, all 12 years
- [x] Gate 2 validation sweep — **run 2026-10-03: 744 pass, 0 fail,
      385 s** (`make gate2`). The one imperfect file,
      `Year12/soup/soup_groc_1687_1739`, passes with a warning because
      it is missing only a line terminator, not any field (decision 6 in
      §7); its re-pull from the raw archive is follow-up item 7
- [x] Gate 3 ramp — **run 2026-10-03**: 3a/3b/3c/3d all clean, 84
      records all `success`, 0 failed, 0 `.tmp`; manifest rows equal
      disk rows (317,129,053). The ramp's output is intentionally left
      in place for Gate 4 to resume from.
- [x] Gate 4 full run complete — **two forced runs 2026-10-03**, both
      `--overwrite` with 0 skips so all 744 files were re-parsed:
      `completed=744 skipped=0 failed=0 workers=8` at **wall=221.10 s**
      and **wall=220.29 s**, with the `shasum` machine-state control flat
      at 1 443–1 475 MiB/s throughout. Both landed
      `rows=2700651386 rejected_rows=1 out=9.40 GiB`, and both left the
      lake at 9.4 GiB / 3148 Parquet files / one output set per source /
      0 `.tmp`. Space: **140.85 GiB raw in, 9.40 GiB Parquet out, 15.0x**
      (3.74 B/row against a fixed 56 B/row), of which ~3/4 of the ratio
      is Parquet encoding rather than zstd — see `parallelism.md` §6.
      Gate 5 re-run against the post-overwrite lake: all nine `*_ok`
      true, exit 0.
- [x] Gate 4 as a *resume*: **run 2026-10-03** against the production
      `data/lake`, resuming from Gate 3's 84 files as intended:
      `completed=660 skipped=84 failed=0 workers=8 wall=198.93s`,
      `rows=2383522333 rejected_rows=1 slowest_file=12.5s`. That run's
      2 383 522 333 rows plus the ramp's 317 129 053 is the
      2 700 651 386 Gate 5b asserts, so the resume path is measured on
      the real corpus and not just in tests. Note its 198.93 s is *not*
      comparable to the ~220 s above: it parsed 660 files and skipped 84.
- [x] `--overwrite` found to leak, fixed, and verified at corpus scale —
      see decision 7 in §7
- [x] Gate 5 queries 5a–5e run against the production `data/lake` —
      **all nine `*_ok` columns true**, `run_sql.py` exit 0:
      5a 744 records / 744 success / 0 failed / 0 in_progress,
      `manifest_rows = disk_rows = 2700651386`, `row_gap = 0`;
      5b `rejected_rows = 1`; 5c 12 years × 31 categories × 2 channels,
      62 sources and 62 records per year, 3148 Parquet files;
      5d `zero_iri_key`/`negative_dollars`/`unknown_feature` all 0;
      5e every year's observed week range matches
      `week_to_year` (1114–1165 → year 1 … 1687–1739 → year 12).
      The independent cross-check 5a cannot make: `inventory` reports
      2 689 259 921 expected rows, and +11 391 465 for the soup file's
      complete records is exactly 2 700 651 386.
- [x] README compression list checked against the enabled Parquet codecs.
      The list was already correct — all 8 canonical names are accepted by
      `parquet_output::compression_from_str`, and the lake's 15 433 row
      groups are uniformly `ZSTD`, matching the documented `zstd` default.
      The real defect ran the other way: `--compression`'s own help text
      and `Config::compression`'s doc comment both omitted `lz4_raw`.
      Fixed in `src/cli.rs` and `src/config.rs`; README additionally
      documents the `none` synonym and that `zstd` means level 1.
- [x] G5 automatic `*.tmp` cleanup (`src/cleanup.rs`, wired into ingest paths)
- [x] `make fixtures` removed — it referenced a test that never existed,
      and nothing read its output (G8)
- [x] Failure policy decided and implemented: misaligned source = ingest
      the aligned prefix with `rejected_rows`; genuine failure = a
      `status: "failed"` manifest line (decisions 4 and 5 in §7)

## 7. Decisions and open questions

### Decided

1. **G3, unexpected `F` tokens — keep `Fail` as-is.** No change. Failing
   loudly is the right default for a bronze layer, and because the failure
   is recorded in the manifest the run stays resumable. Revisit only if the
   real corpus turns out to contain such tokens, in which case
   `WarnAndTreatAsUnknown` (already defined in `config.rs`, just not
   reachable from the CLI) is the intended escape hatch.
2. **G4, week-range strictness — defer.** Assume the filename week ranges
   are correct. The enforcement mechanism exists (`week_range_strict`) but
   is neither on nor CLI-reachable; leaving it off keeps the parser free of
   a per-row check it was built to avoid. Add a catch later, once testing
   gives evidence about whether the assumption actually holds.
3. **G1, parallelism shape — one process, bounded rayon pool.** Measured
   against the multi-process alternative on identical work, the two tie at
   8 units (20.3 s vs 19.8 s on the Year-1 scope) and threads win on
   every operational property: one manifest, one command, one thing to
   resume, no shard bookkeeping. `--shard IDX/N` is kept for
   multi-machine runs, where a byte-balanced shard is the right unit of
   work; it is not the single-machine default. The work list is dealt
   **smallest-file-first**, which beat the largest-first ordering this
   plan originally specified by 20%: rayon's work-stealing deque hands
   work out from the back, so largest-first leaves the 1.4 GB file to be
   picked up last, when one worker is left to take it. Full measurements,
   including the multi-process shape that tied, are in
   [`docs/parallelism.md`](parallelism.md).
4. **A truncated trailing record is a rejection, not a failure.** A
   source that is not record-aligned is ingested up to its last aligned
   boundary; the trailing partial record is counted in `rejected_rows`,
   warned about, and the file is recorded as a `success`. Decided
   against the corpus's one damaged file
   (`Year12/soup/soup_groc_1687_1739`: 11 391 465 complete records then
   one truncated) — refusing it would leave 11.39 M good rows out of the
   lake to protect one bad record. The full run therefore lands **744/744
   files, 0 failures**. A file too short to hold a header is still an
   error; that is a different failure with no records in it at all.
5. **Failures are recorded in the manifest, not only in the log.** A
   source that genuinely cannot be ingested now gets a
   `status: "failed"` record carrying its error message, so
   `manifest.jsonl` is a complete account of what the lake contains *and*
   what it is missing. The first full run recorded its one failure in
   stdout and nowhere else — and stdout is exactly what a crashed or
   re-run job loses. A failed record never matches a skip, so the source
   is still retried next run.
6. **A terminator-only trailing defect is a warning; a truncated record
   is a loss.** The corpus's one imperfect file ends one byte into its
   final CRLF with all eleven fields intact. What matters for the parser
   and for every downstream query is whether the *data* of the rows
   exists — and a row's fields all live in offsets 0..54, so a missing
   terminator costs nothing anybody can read. That is now a warning, not
   a failure, and both the validator and the ingest path say so
   explicitly:

   | | validator | ingest |
   |---|---|---|
   | final record missing only its CRLF | passes, with a note; all 11 391 465 complete records still validated | info log; writes all 11 391 465; `rejected_rows = 1` |
   | final record missing field bytes | **fails**, `trailing` reports how many of 54 content bytes are present | warn log; `rejected_rows = 1` and flagged as a data loss in the summary |

   `fixed_width::classify_trailing` is the single place that decides
   which case applies, so the validator and the ingest path cannot
   disagree. Gate 2 consequently passes 744/744 on the staged corpus.

   The one row whose terminator is short is still not written to the
   lake, because the file is not a whole number of records and writing
   it would make `written_rows` exceed the size-derived row count that
   the manifest and Gate 5 reconcile against. Nothing is lost in
   practice — that row's data is complete and re-pulling the file
   recovers it — but if it is ever written, `expected_rows` and
   `written_rows` stop being interchangeable, so the choice is
   deliberate rather than incidental.
7. **`--overwrite` replaces a source's output set; it does not merely
   write beside it.** Found on 2026-10-03 while trying to force a full
   re-run, and it had been wrong since the flag was wired up in G2.
   `plan_output_paths` embeds the current run's short id in every
   output filename, so a rewrite can never collide with the prior run's
   files: the code comment claiming the writer "renames over the
   existing file" was simply false. One source ingested twice under
   `--overwrite` left **2 files and 5.0 MB where there should be 1 file
   and 2.5 MB**. Across the corpus that orphans all 3 148 existing
   files, doubles the lake, and doubles `count(*)` over the Parquet,
   while the manifest — append-only — reaches 1 488 records against
   744 sources. That fails Gate 5 on both 5a ("one record per source,
   no duplicates") and 5b, and it would have silently broken item 1
   below, whose remedy is "re-ingest that one source with
   `--overwrite`".

   Fixed by stashing the prior record's `output_paths` in the Overwrite
   arm and deleting them **after** the replacement is written *and*
   appended to the manifest, so an interruption leaves the old output
   rather than neither. Removal is best-effort like the `.tmp` sweep: a
   failed unlink logs the orphaned path and does not fail a successful
   ingest, because failing there would throw away a good row count over
   an inert file.

   Two things are worth recording about how it survived. The existing
   test asserted only that the second run *completed with 64 rows*,
   which is true of a run that leaked too; it now also asserts the lake
   holds exactly one Parquet file, and reverting the fix fails it with
   both paths named. And Gates 2–4 had never exercised the path at
   corpus scale at all, because the overlapping scopes in Gate 3
   *skipped* rather than overwrote — so "Gate 3 passed clean, so
   overwrite works" would have been the wrong inference, and the 199 s
   resume run never touched this code.

   Verified after the fix at corpus scale: two full `--overwrite` runs
   each left the lake at exactly 3 148 files with 744 distinct run
   shorts — one output set per source, no orphans.

### Still open

8. **Re-pull `Year12/soup/soup_groc_1687_1739` from the raw archive.**
   Low priority now: the defect is a line terminator, it passes the
   gate, and the lake is not missing any field data (decision 6). If
   the fresh copy is byte-complete, re-ingest that one source with
   `--overwrite` — which now reaps the superseded output rather than
   leaking it (decision 7) — and re-run Gate 5; the expected total then
   becomes 2 700 651 387 with `rejected_rows = 0`. Until then one row is
   not written, in one partition, and that fact is recorded in
   `manifest.jsonl`.

9. **Should a resume keep re-hashing the whole corpus?** SHA-256 is
   computed *before* the skip decision, because the hash is what makes
   the decision trustworthy, so restarting an interrupted run costs
   ~110 s of a ~220 s run to re-hash 141 GiB and discard it. Trusting
   size+mtime behind a flag would make resume ~10x cheaper and would
   weaken the guarantee from "these bytes" to "this inode". This is a
   change to the idempotence guard, so it wants its own gate. See
   [`docs/parallelism.md`](parallelism.md) §8.