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
| G1 | `ingest-all` processes files **sequentially** in a `for` loop (`src/main.rs:187`); `rayon` is a declared dependency but unused. The `--workers` flag is destructured as `workers: _` and discarded. | The full corpus runs at single-file speed. Wall time = sum of per-file times, so no overlap of I/O with compression. | benchmarking / follow-up |
| G2 | **Fixed on `planning`.** `--resume`/`--overwrite` were parsed but never reached the ingest path: `Cli::overwrite_mode` was never called and `config.overwrite` was never read, so `OverwriteMode` was dead code and `skip_decision` alone drove the skip. `--overwrite` silently skipped; `--resume --overwrite` resolved silently. Now wired through and covered by tests. | — | closed |
| G3 | `UnknownFeaturePolicy::Fail` is hard-coded (`Cli::unknown_feature_policy`), and the config field is not overridable from the CLI. | Any unexpected `F` token fails its whole file. **Decision: keep as-is** — failing loudly on an unexpected `F` token is the right default for a bronze layer, and the manifest records the failure so the run stays resumable. Revisit only if the real corpus turns out to contain such tokens. | accepted |
| G4 | `week_range_strict` defaults to `false` and is not settable from the CLI. | **Decision: defer.** Assume filename week ranges are recorded correctly for now; add a catch later once testing gives evidence about whether that assumption holds. §5's per-year week-range query stays as a cheap corpus-wide backstop. | deferred |
| G5 | **Fixed on `planning`.** `ManifestStatus::InProgress` is still never written, but the consequence — unattended `*.tmp` leftovers under `data/lake` — is now handled automatically. `src/cleanup.rs` sweeps `*.tmp` files under the output root at the start of every `ingest` / `ingest-all` (before anything is written) and deletes those older than 24 h (`--tmp-max-age-hours`, `IRI_LAKE_TMP_MAX_AGE_HOURS`). The age gate means a concurrent run's in-flight `.tmp` is never a candidate; failures are logged, never fatal. `make clean-tmp` runs the same sweep by hand. | — | closed |
| G6 | `manifest.jsonl` is append-only and `last_for_path` re-reads the whole file per lookup. | O(records²) per run. At 744 records × a few passes this is seconds, not a problem. Not worth changing before the corpus run; would matter at ~10⁵ sources. | none |
| G7 | **False alarm — corrected.** `data/raw` is a symlink to `~/.julia/dev/IRIData/data/IRI/Raw` and resolves fine; the corpus is staged and measures 140.85 GiB / 744 files / 2.69 B rows. An earlier draft of this table claimed the symlink was "dangling" on the strength of `du -sh data/raw` reporting 0 B. That command reports the *symlink itself*; it does not follow the link. Use `du -shL` or a trailing slash. | none | closed |
| G8 | **Fixed on `planning`.** `make fixtures` ran `cargo test --test fixtures_emit`, and there is no `tests/fixtures_emit.rs`. There is also nothing to emit: `tests/common/mod.rs` builds every fixture into a fresh tempdir at test time and no binaries are committed. The target and its help line are removed; `make test` is the entry point. | — | closed |

### The one that actually matters

G1 is the difference between a one-night run and a one-week run. Everything
else in this document can proceed in parallel with it, but the schedule for
the full ingest cannot be committed until G1 is closed and the resulting
throughput number is measured on real files. **Do not start the full run
until the `benchmarking` worktree has reported end-to-end file throughput.**

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

Reconciliation afterwards. These are the queries that decide whether the
lake is trustworthy, independent of anything the CLI reported.

```sql
-- 5a. Row-count reconciliation, manifest vs disk.
-- Every successful manifest record's written_rows must equal the Parquet
-- file's actual row count.
SELECT count(*) AS files, sum(written_rows) AS manifest_rows
FROM read_parquet('data/lake/bronze/iri_sales/**/*.parquet', hive_partitioning = true);

-- 5b. Expected total. This is the number the run must land on: the
-- inventory's expected_rows for the whole corpus.
SELECT 2689259921 AS expected_rows;

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
- [x] Corpus staged on NVMe, path recorded (140.85 GiB, resolves)
- [x] Gate 1 inventory reviewed — 744 files, 2 689 259 921 rows, all 12 years
- [ ] Gate 2 validation sweep, zero failures
- [ ] Gate 3 ramp, each scope with a clean manifest
- [ ] Gate 4 full run complete, log archived
- [ ] Gate 5 queries 5a–5e run and pasted into the run log
- [ ] README compression list corrected against the enabled Parquet codecs
- [x] G5 automatic `*.tmp` cleanup (`src/cleanup.rs`, wired into ingest paths)
- [x] `make fixtures` removed — it referenced a test that never existed

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

### Still open

3. **G1, parallelism shape.** File-level rayon pool (as `ARCHITECTURE.md`
   sketches) versus one process per machine per shard of years. The
   `benchmarking` worktree's numbers should decide. This is the one gap
   that gates the run schedule.
4. **Failure policy for the full run.** Currently one bad file logs a
   warning and the run continues. That is probably right, and it composes
   well with the G3 decision above, but it should be an explicit decision
   rather than an emergent property of the loop.