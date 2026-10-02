# Corpus readiness — running the full IRI corpus

Status: **plan only.** No code changes land from this document. It exists so
that the first all-corpus ingest is a *gated* operation rather than an
exploratory one.

The design point is the full corpus: **≈143 GB raw / 2.7 B rows / 744 sales
files**, largest file ≈1.3 GB / 220 M rows. `ARCHITECTURE.md` describes the
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
| G1 | `ingest-all` processes files **sequentially** in a `for` loop (`src/main.rs:172`); `rayon` is a declared dependency but unused. The `--workers` flag is destructured as `workers: _` and discarded. | The full corpus runs at single-file speed. Wall time = sum of per-file times, so no overlap of I/O with compression. | benchmarking / follow-up |
| G2 | **Fixed on `planning`.** `--resume`/`--overwrite` were parsed but never reached the ingest path: `Cli::overwrite_mode` was never called and `config.overwrite` was never read, so `OverwriteMode` was dead code and `skip_decision` alone drove the skip. `--overwrite` silently skipped; `--resume --overwrite` resolved silently. Now wired through and covered by tests. | — | closed |
| G3 | `UnknownFeaturePolicy::Fail` is hard-coded (`Cli::unknown_feature_policy`), and the config field is not overridable from the CLI. | Any unexpected `F` token fails its whole file. On 744 files this is the single most likely cause of a partial run. | needs decision |
| G4 | `week_range_strict` defaults to `false` and is not settable from the CLI. | A file whose rows disagree with its filename week range will ingest silently as wrong data. The reconciliation queries in §5 are the only backstop. | needs decision |
| G5 | `ManifestStatus::InProgress` exists but is never written. | No crash-safe "this file was being written" record. An interrupted run leaves a `.tmp` sibling and no manifest line, so the file is simply re-ingested next pass. Correct, but it means **`*.tmp` files under `data/lake` are the marker of an interrupted run**, and nothing cleans them automatically. | operator step (§4) |
| G6 | `manifest.jsonl` is append-only and `last_for_path` re-reads the whole file per lookup. | O(records²) per run. At 744 records × a few passes this is seconds, not a problem. Not worth changing before the corpus run; would matter at ~10⁵ sources. | none |
| G7 | `data/raw` is a symlink to `~/.julia/dev/IRIData/data/IRI/Raw`, which is **dangling on this machine** (`du` reports 0 B). | Nothing runs until the real corpus is staged. See §1. | operator |
| G8 | `make fixtures` runs `cargo test --test fixtures_emit`, and there is no `tests/fixtures_emit.rs`. | Target fails. Harmless to a corpus run, but it is a broken target in the documented workflow. | trivial fix |

### The one that actually matters

G1 is the difference between a one-night run and a one-week run. Everything
else in this document can proceed in parallel with it, but the schedule for
the full ingest cannot be committed until G1 is closed and the resulting
throughput number is measured on real files. **Do not start the full run
until the `benchmarking` worktree has reported end-to-end file throughput.**

## 1. Stage the corpus to local NVMe

The corpus must be on local NVMe, not on network or spinning storage. The
runbook assumes a symlink so the repo layout is unchanged.

```bash
# Confirm the staged root is real and local before anything else.
readlink data/raw                       # where it currently points
df -h "$(readlink data/raw)"            # must be local NVMe, not network
ls data/raw/Year1                       # must exist
du -sh data/raw                         # expect ~143 G
```

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

Check, before going further:

- file count ≈ 744
- no `skipped[]` entry is a file we expected to ingest (PANEL, stubs,
  `Delivery_Stores`, `DEMOS.CSV` **should** appear here — that is the
  filter working)
- the `year=` spread covers 1..=12 with no year gaps
- category names look like real IRI categories, including the known
  `paptowl` / `paptowls` spelling split

If discovery is wrong, stop. Everything downstream inherits the mistake.

## 3. Gate 2 — full validation sweep

Validate **every** file before ingesting any of them. `validate --full`
checks header bytes and every record's alignment; it does not write output,
so it is safe to run against the whole corpus.

```bash
# Per file:
cargo run --release -- validate <path> --full

# Sweep, recording every failure:
find data/raw -type f -perm -u+r \
  | grep -Ev '\.(xls|xlsx|csv|doc|docx|pdf|zip)$' \
  | grep -v '~\$' \
  | while read -r f; do
      cargo run --release -- validate "$f" --full >/dev/null 2>&1 \
        || echo "FAIL $f"
    done > /tmp/validate-failures.txt
```

Expected outcome: **zero failures.** A non-empty failure list is the real
output of this gate — stop and triage before Gate 3. Because
`week_range_strict` is off (G4), this sweep does *not* catch filename/row
week disagreements; those are caught in Gate 5.

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

Note that 3b and 3c pass **no `--resume`**, and none is needed: skip-if-present
is the resolved default when neither `--resume` nor `--overwrite` is given.
Pass `--overwrite` only when you intend to discard and rewrite completed
output — it now genuinely re-ingests rather than silently skipping. Passing
both is a hard error.

### Interruption and restart

If a run is interrupted, in order of preference:

1. Re-run the identical command. Completed files skip, incomplete files
   restart.
2. Delete stray `*.tmp` siblings under `$OUT` before re-running. They are
   inert leftovers (G5) but they accumulate and they confuse anyone
   globbing `data/lake`.

```bash
find $OUT -name '*.tmp' -print -delete
```

## 5. Gate 4 — the full run, and Gate 5 — reconciliation

The full run:

```bash
RUST_LOG=iri_lake=info cargo run --release -- \
  ingest-all --input data/raw --output-root data/lake 2>&1 | tee /tmp/full-run.log
```

Reconciliation afterwards. These are the queries that decide whether the
lake is trustworthy, independent of anything the CLI reported.

```sql
-- 5a. Row-count reconciliation, manifest vs disk.
-- Every successful manifest record's written_rows must equal the Parquet
-- file's actual row count.
SELECT count(*) AS files, sum(written_rows) AS manifest_rows
FROM read_parquet('data/lake/bronze/iri_sales/**/*.parquet', hive_partitioning = true);

-- 5b. Expected total. This is the number the run must land on.
SELECT (143 * 1024*1024*1024) / 56 AS approx_rows;  -- 2.73 B

-- 5c. Coverage: every year/category/channel partition present.
SELECT year, count(DISTINCT category) AS categories, count(*) AS files
FROM read_parquet('data/lake/bronze/iri_sales/**/*.parquet', hive_partitioning = true)
GROUP BY year ORDER BY year;

-- 5d. Null / sentinel sweep. dollars_cents must never be negative beyond
-- returns; iri_key must never be 0.
SELECT count(*) FILTER (WHERE iri_key = 0)        AS zero_iri_key,
       count(*) FILTER (WHERE dollars_cents < 0)  AS negative_dollars,
       count(*) FILTER (WHERE feature_code > 4)   AS unknown_feature
FROM read_parquet('data/lake/bronze/iri_sales/**/*.parquet', hive_partitioning = true);

-- 5e. Week-range cross-check (the G4 backstop): rows must fall inside the
-- partition's declared week window.
SELECT year, min(week) AS min_week, max(week) AS max_week, count(*) AS rows
FROM read_parquet('data/lake/bronze/iri_sales/**/*.parquet', hive_partitioning = true)
GROUP BY year ORDER BY year;
```

A finished run is one where: no manifest line has `status: "failed"` for a
file that has not since succeeded, 5a matches 5b to within the documented
file-count tolerance, 5d is all zeros, and 5e's per-year week ranges match
`fixed_width::week_to_year` (1114–1165 → year 1 … 1687–1739 → year 12).

## 6. Definition of done

- [ ] G1 closed and end-to-end throughput measured on a real 1.3 GB file
- [ ] Corpus staged on NVMe, path recorded
- [ ] Gate 1 inventory reviewed, file count ≈ 744
- [ ] Gate 2 validation sweep, zero failures
- [ ] Gate 3 ramp, each scope with a clean manifest
- [ ] Gate 4 full run complete, log archived
- [ ] Gate 5 queries 5a–5e run and pasted into the run log
- [ ] README compression list corrected against the enabled Parquet codecs
- [ ] `make fixtures` fixed or removed

## 7. Open questions

1. **G3, unexpected `F` tokens.** Fail the file (current), or accept with
   `WarnAndTreatAsUnknown` and quarantine the rows? This needs an answer
   *before* the full run, because it changes whether the run can complete
   unattended.
2. **G4, week-range strictness.** Turn `week_range_strict` on for the
   corpus? It would catch mis-named files that currently pass silently, at
   the cost of a per-row check the parser was built to avoid.
3. **G1, parallelism shape.** File-level rayon pool (as `ARCHITECTURE.md`
   sketches) versus one process per machine per shard of years. The
   `benchmarking` worktree's numbers should decide.
4. **Failure policy for the full run.** Currently one bad file logs a
   warning and the run continues. That is probably right, but it should be
   an explicit decision rather than an emergent property of the loop.