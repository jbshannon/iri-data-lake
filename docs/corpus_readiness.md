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
| G1 | **Closed.** `ingest-all` ran files sequentially in a `for` loop; `rayon` was a declared dependency and `--workers` was destructured as `workers: _`. Now: a bounded rayon pool over the file list, one shared lock-serialised manifest, `--order` / `--shard` scheduling flags, and per-file output verified **byte-identical** to the sequential run. Full corpus measured at **~270 s** (271 s and 406 s across two runs; the spread is machine thermal state — see `parallelism.md` §6), was ~17 min sequential. Implementation, measurements and the rejected designs are in [`docs/parallelism.md`](parallelism.md). | — | closed |
| G2 | **Fixed on `planning`.** `--resume`/`--overwrite` were parsed but never reached the ingest path: `Cli::overwrite_mode` was never called and `config.overwrite` was never read, so `OverwriteMode` was dead code and `skip_decision` alone drove the skip. `--overwrite` silently skipped; `--resume --overwrite` resolved silently. Now wired through and covered by tests. | — | closed |
| G3 | `UnknownFeaturePolicy::Fail` is hard-coded (`Cli::unknown_feature_policy`), and the config field is not overridable from the CLI. | Any unexpected `F` token fails its whole file. **Decision: keep as-is** — failing loudly on an unexpected `F` token is the right default for a bronze layer, and the manifest records the failure so the run stays resumable. Revisit only if the real corpus turns out to contain such tokens. | accepted |
| G4 | `week_range_strict` defaults to `false` and is not settable from the CLI. | **Decision: defer.** Assume filename week ranges are recorded correctly for now; add a catch later once testing gives evidence about whether that assumption holds. §5's per-year week-range query stays as a cheap corpus-wide backstop. | deferred |
| G5 | **Closed.** Two independent consequences of `ManifestStatus::InProgress` never being written are now handled. **(a) Failures visible only in stdout:** `ingest_all` writes a `status: "failed"` record carrying the error message for any source it could not ingest, so `manifest.jsonl` records its own gaps rather than leaving them in the log — which is exactly what a crashed or re-run job loses. A failed record never matches the skip check, so the source is still retried next run. **(b) Unattended `*.tmp` leftovers:** an interrupted run leaves a `.tmp` sibling with no manifest line, so `src/cleanup.rs` sweeps `*.tmp` under the output root at the start of every `ingest` / `ingest-all`, before anything is written, deleting only those older than 24 h (`--tmp-max-age-hours`, `IRI_LAKE_TMP_MAX_AGE_HOURS`). The age gate means a concurrent run's in-flight `.tmp` is never a candidate, and failures are logged, never fatal. `make clean-tmp` runs the same sweep by hand. (b) landed on `main` first, as `c24e59f`. | — | closed |
| G6 | **Closed as a side effect of G1.** `manifest.jsonl` was append-only and `last_for_path` re-read the whole file per lookup. | O(records²) per run was never actually a problem at 744 records. `manifest::SharedManifest` now loads the file once into an index and serialises appends behind a mutex, so appends are atomic by construction rather than by `O_APPEND` luck. | closed |
| G7 | **False alarm — corrected.** `data/raw` is a symlink to `~/.julia/dev/IRIData/data/IRI/Raw` and resolves fine; the corpus is staged and measures 140.85 GiB / 744 files / 2.69 B rows. An earlier draft of this table claimed the symlink was "dangling" on the strength of `du -sh data/raw` reporting 0 B. That command reports the *symlink itself*; it does not follow the link. Use `du -shL` or a trailing slash. | none | closed |
| G8 | **Fixed on `planning`.** `make fixtures` ran `cargo test --test fixtures_emit`, and there is no `tests/fixtures_emit.rs`. There is also nothing to emit: `tests/common/mod.rs` builds every fixture into a fresh tempdir at test time and no binaries are committed. The target and its help line are removed; `make test` is the entry point. | — | closed |

### The one that actually matters

G1 is the difference between a one-night run and a one-week run.
**Closed and measured**: the full corpus ingests in ~270 s (two runs:
271 s and 406 s, the difference being machine thermal state, not code —
normalised, ~257 s) at `--workers 8`, landing **744/744 files, 0
failures, 1 rejected row, 2 700 651 386 rows**. The schedule for the
full ingest can now be committed, and it is comfortably inside Gate 4's
window. Two things moved the number and are now the default:
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

> **One file in the staged corpus fails this gate**, so this is not
> hypothetical: `Year12/soup/soup_groc_1687_1739` is 637 922 152
> bytes and `(size - 57) % 56 == 55` — its final record is one byte
> short, truncated inside the last field with no CRLF. 11 391 465
> complete records followed by a partial one. The header bytes are
> fine, so only the alignment check catches it. Gate 2 still flags it,
> and that is correct: the *gate* is where you decide whether to
> re-pull the file from source. The ingest path does not need to — it
> takes the complete records and records `rejected_rows = 1`
> (decision 4 in §7).

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

Expected outcome: **zero failures — currently known to be one**, the
truncated `Year12/soup/soup_groc_1687_1739` named above. A non-empty failure list is the real
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

Measured on the staged corpus (defaults, i.e. `--workers 8
--order smallest-first`):

```
ingest-all: 744 file(s) selected (140.85 GiB raw, ~2.689B rows) of 744 discovered
ingest-all done: completed=744 skipped=0 failed=0 workers=8 wall=406.12s
  throughput: raw=355.1 MiB/s rows=6.65 M/s out=9.40 GiB bytes=151236520079
              rows=2700651386 rejected_rows=1 slowest_file=25.5s
  note: 1 row(s) rejected as incomplete records (trailing bytes of a truncated source)
```

**~270 s — quote it with a wide band.** Two full runs hours apart measured
271 s and 406 s; the machine itself had slowed by 37 % between them
(control: eight concurrent `shasum` processes, 1 544 -> 977 MiB/s), and
the second figure normalises to ~257 s against that control. Against the
~17 min single-threaded baseline the run is **~3.7x**. 744 files, **0
failures**, `rejected_rows = 1`, no stray `.tmp` files.

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
lake is trustworthy, independent of anything the CLI reported.

```sql
-- 5a. Row-count reconciliation, manifest vs disk.
-- Every successful manifest record's written_rows must equal the Parquet
-- file's actual row count.
SELECT count(*) AS files, sum(written_rows) AS manifest_rows
FROM read_parquet('data/lake/bronze/iri_sales/**/*.parquet', hive_partitioning = true);

-- 5b. Expected total. The inventory figure plus the complete records
-- inside the one non-aligned source; its single truncated record is
-- rejected (counted in rejected_rows) rather than written.
SELECT 2689259921 + 11391465 AS expected_rows;   -- 2 700 651 386

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
      (~270 s, ±50 % by machine state, 744/744 files, 0 failures —
      `docs/parallelism.md`)
- [x] Corpus staged on NVMe, path recorded (140.85 GiB, resolves)
- [x] Gate 1 inventory reviewed — 744 files, 2 689 259 921 rows, all 12 years
- [ ] Gate 2 validation sweep — **one known failure**
      (`Year12/soup/soup_groc_1687_1739`, 1 truncated record out of
      11 391 466); decide whether to re-pull it or accept the loss
- [ ] Gate 3 ramp, each scope with a clean manifest
- [ ] Gate 4 full run complete, log archived (dry-measured end to end at
      ~270 s into a scratch root; the production `data/lake` run is still
      to be issued)
- [ ] Gate 5 queries 5a–5e run and pasted into the run log
- [ ] README compression list corrected against the enabled Parquet codecs
- [x] G5 automatic `*.tmp` cleanup (`src/cleanup.rs`, wired into ingest paths)
- [x] `make fixtures` removed — it referenced a test that never existed
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

### Still open

6. **Failure policy residual.** "Log, count and continue" is now
   explicit and enforced (decisions 4 and 5), with the corpus's one
   truncated file as the evidence. What remains is an operator step:
   Gate 2 still surfaces that file, and someone has to accept losing
   exactly **one row** from it, or go back to the source and re-pull
   `Year12/soup/soup_groc_1687_1739`. Also open: whether a resume should
   keep re-hashing the whole corpus to make its skip decision (110 s on
   the finished 744-file lake) or trust size+mtime behind a flag. See
   [`docs/parallelism.md`](parallelism.md) §8.