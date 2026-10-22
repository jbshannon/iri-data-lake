# Silver release operations

Contract: [silver_specification.md](silver_specification.md). The earlier
[silver_layer.md](silver_layer.md) remains a design draft. Rust ingestion and
Bronze formats are unchanged. The legacy `materialize-product-week` command is
not a managed-release publisher.

## Local workflow

Run from the repository root with `uv run` (DuckDB and NumPy are locked Python
dependencies). These examples use disposable fixtures, **not the connected shared
lake**. Never run publication against a live/shared root without separate approval.

```sh
# A selected local profile; omitted --dataset flags require ALL public source classes.
uv run python -m scripts.silver plan --lake /tmp/fixture-lake \
  --dataset sales --dataset product_stub --dataset week_dimension

# First stop Bronze writers. --quiescent is an explicit operator acknowledgement.
# Default --scope partial is recorded permanently; it is not a corpus-ready claim.
uv run python -m scripts.silver build --lake /tmp/fixture-lake \
  --silver-root /tmp/fixture-silver --quiescent \
  --dataset sales --dataset product_stub --dataset week_dimension

# The build returns a unique staging path; review it, then publish explicitly.
uv run python -m scripts.silver publish --silver-root /tmp/fixture-silver \
  --staging /tmp/fixture-silver/.staging/FINGERPRINT-UUID

# Alternatively add --publish to build to validate and immediately publish locally.
uv run python -m scripts.silver validate --silver-root /tmp/fixture-silver
uv run python -m scripts.silver bootstrap --silver-root /tmp/fixture-silver \
  --query 'SELECT count(*) FROM silver_product_week_v'
uv run python -m scripts.silver rollback --silver-root /tmp/fixture-silver \
  --release-id ID --reason 'reviewed recovery'
```

`plan` reads the manifest, selected Parquet schemas/counts and checksums, but writes
nothing. It does not read unrelated sensitive Parquet. Partial scope still requires
all *selected* sources to succeed; the latest failed source attempt is not hidden by
an older success. Empty successful sources are accounted for, and deduplicated aliases
retain their annual scope without multiplying repeated category copies.

Public datasets: `sales`, `product_stub`, `product_attr`, `delivery_stores`,
`fips_market`, `chain_xref`, `manual_store_entry`, `week_dimension`. Select only classes needed for a local
profile; a sales profile needs a valid product mapping and the captured 626-week
calendar. Omitted domains are not fabricated. Unsupported/malformed selected data
fails closed rather than being silently dropped.

Build options include `--threads N`, `--memory-limit 4GB`, `--temp-directory PATH`,
and `--reference-root PATH`. The supported reviewed reference is
`stub_categories.csv`, with columns `edition,source_workbook,category`. Conflicting
basename/full-path mappings fail; absent mappings remain unresolved. Reference bytes
are captured and checksummed. Unknown files, nested reference files and reference symlinks are rejected.

The channel dictionary retains observed raw spellings and source-system/year scope,
including explicit unmatched codes. Resolved stores expose `channel_mapping_status`;
a known raw outlet value is not necessarily a supported normalized channel.

## Publication and replay

A managed root contains `.silver.lock`, `runs.jsonl`, `.staging/`, `releases/`, and
`current.json`. Building never changes `current.json` unless explicitly publishing.
Publication validates declared artifacts, fsyncs files/directories, renames the unique
immutable directory, then atomically replaces the pointer. A failure before the pointer
switch preserves the previous current release; a failure after replacement exposes
only the complete new release. A failed build cleans its own staging tree. Ready,
unpublished staging directories remain for explicit review/publication. There is no
automatic retention deletion or garbage collection.

The view-first release keeps source Parquet by hard link when supported (copy fallback)
and materializes product-week and small supporting metadata. It does not rewrite the
sales fact. Original source identities/row locators and Hive/edition/year scope survive
retention. Conflicting declared/source/output year scopes fail; a validated canonical
roster/demo can still be deliberately reused by dedup aliases in another annual scope.
Originals must remain immutable: in-place changes to a hard-linked source
invalidate the release, which subsequent checksum verification rejects. Stop writers
through snapshot capture; copy-on-write/versioned Bronze replacement is preferable to
in-place edits. Checksums are streamed rather than loading a whole fact file into RAM.

Each release archives SQL view definitions, supporting temporary tables, selected
source/schema/alias metadata, reference bytes, the exact ID codec version, output schemas,
and executable SQL assertion recipes. Paths inside the bundle are rebased after staging
rename. Fresh bootstrap does **not** invoke mutable domain registration or run archived
Python. The installed v1 key codec is checked against frozen vectors. Bootstrap verifies
listed artifact hashes; `validate` also reconstructs the release and actually executes
its archived semantic assertions. Replay spill uses a private transient directory outside
the immutable release. This is local trusted-artifact replay, not a sandbox for hostile
SQL bundles or a cryptographically signed supply-chain format.

```python
import duckdb
from pathlib import Path
from scripts.silver.bootstrap import load

with duckdb.connect() as con:
    pinned = load(con, Path('/tmp/fixture-silver'), release_id='ID')
    rows = con.execute('SELECT * FROM silver_product_week_v LIMIT 10').fetchall()
```

A session pins one descriptor and its paths. A later pointer switch does not change its
views. Use a fresh connection when loading another release. Matching builds perform
checksum and archived assertion validation before reporting `no-op`; they skip rebuilding
the fact aggregate, **not** integrity/conservation scans. Corruption is an error, never
a cache miss. Definition/reference/input changes produce a new fingerprint. Unrelated
private manifest appends do not invalidate a public release.

## Gate 7 and tests

```sh
make silver-test
make gate7 SILVER_ROOT=/tmp/fixture-silver RELEASE_ID=ID
# Optional additional static public-sales-profile assertions after pinned bootstrap:
uv run python scripts/run_sql.py sql/gate7_silver.sql \
  --silver-root /tmp/fixture-silver --release-id ID \
  --expect global_conservation_ok --expect annual_conservation_ok
```

`make gate7` is `silver-validate`: it executes the **complete archived** expected check
set for the selected domains, not stored passing booleans or current validator Python.
Schema name/order/type, key nullability/uniqueness, source conservation, exact global
and per-year integer units/cents/record counts, calendar and mapping/lineage/resolution
invariants are checked. The static SQL file is an additional sales-profile check, not a
replacement for archived optional-domain checks. SQL assertion columns must actually be
Boolean true: false, NULL, zero rows, missing `--expect` columns, and scripts with no
assertions fail.

The unittest suite uses synthetic actual-format Parquet and manifests, including fresh
bootstrap after deleting originals, changed references/domain code, no-op validation,
corruption, failed gates, pointer failure boundaries, rollback, and public/restricted
separation. Component tests do not substitute for full-corpus evidence.

Make equivalents: `silver-plan`, `silver-build`, `silver-publish STAGING=...`,
`silver-validate`, `silver-rollback RELEASE_ID=... REASON=...`; pass `SILVER_ROOT`,
`LAKE`, `SILVER_DATASETS`, and explicit `SILVER_FLAGS=--quiescent` as needed. Make
never supplies publication or quiescence acknowledgement implicitly.

## Restricted household data

Household processing is disabled by default. `panel`, `panel_trips`, `panelist_demos`,
`panel_static`, and `ads_demos` cannot enter a public release. An approved build requires
`--household --restricted-root PATH --approval POLICY_REFERENCE` and quiescent inputs.
The private root must be disjoint from both the lake and public Silver root (neither an
ancestor nor a descendant). It is mode 0700; released files are mode 0600, retained inputs
are copied rather than hard-linked, and spill stays private. Build returns no household
rows in logs. Do not put sensitive content in the approval reference.

Restricted bootstrap additionally requires `--restricted`, or Python
`load(..., allow_restricted=True)`. Its `.private-spill` directory is private; manage its
lifecycle according to the approved policy. Ads demographics remain a separate population.
Source observations/multiplicity and exact digits/scale pairs are retained. Unknown
COLUPC mappings, ambiguous purchase-trip links, outlet `KK`, undocumented demographic
codes and unproven cross-year/store links remain unresolved; no guessed normalization.

A policy-reference string records an operator's attestation, **not** independent policy
approval or OS access-control certification. Production household access, retention,
exports, audit and deletion policy must be separately approved before using real data.

## Corpus readiness and remaining operational limits

`--scope corpus --readiness FILE` requires a JSON evidence object containing:

- `manifest_sha256`: the exact captured manifest hash;
- `inventory`: independently captured expected records with `dataset`, `source_path`,
  and `source_sha256` matching the selected public source inventory;
- `gate5_ok: true` and `gate6_ok: true`: operator-supplied evidence from successful
  reconciliation against that snapshot.

All public source classes and 744 distinct sales year/category/channel sources across
IRI years 1–12 and both `drug`/`groc` must be present. Evidence is captured in the release;
it is not generated by self-consistency checks. The implementation does not independently
sign/attest these input assertions. A local fixture or missing evidence cannot become
corpus-ready merely by passing Silver gates.

No full licensed-corpus run, shared-lake publication, production performance benchmark,
access-control audit, or production household-policy approval is implied by this code or
its synthetic tests. Builds use conservative full invalidation of changed definitions;
there is no incremental aggregate patching. Optional governed semantics remain explicit
statuses, not blockers to truthful partial/local releases.
