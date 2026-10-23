# Silver staged dataset report

## Summary

The managed Silver build completed in explicit skip-validation mode. It materialized
its selected relations, but did not run semantic validation or fresh replay validation.
This report summarizes the staged descriptor and output metadata; it is **not** a
validation or release approval.

| Field | Value |
|---|---|
| State | `unvalidated` |
| Validation status | `skipped` |
| Scope | `partial` |
| Restricted | No |
| Fingerprint | `3ce4a4eef6ead5d8d9fa5430a21e287a56940825d56f00247e999fcaad85206b` |
| Input manifest SHA-256 | `f8e1f5dba26b5e4bd5a55058effba4b834209312ebd76a29817de081de79e432` |
| Staging directory | `/tmp/iri-silver/.staging/unvalidated/3ce4a4eef6ead5d8d9fa5430a21e287a56940825d56f00247e999fcaad85206b-343c88ce714d4f92879a39929126a6e9` |
| Publication | Not published; `/tmp/iri-silver/current.json` was absent at report time |

The run used Bronze at `data/lake`, output root `/tmp/iri-silver`, and selected
`sales`, `product_stub`, and `week_dimension`. Its captured selected input scope is
complete for those three selected datasets, but the build scope remains `partial` and
does not claim that omitted public datasets are present.

## Captured inputs

The descriptor records 869 selected manifest records in total:

| Dataset | Manifest records | Recorded written rows | Captured Parquet files | Captured file size |
|---|---:|---:|---:|---:|
| `sales` | 744 | 2,700,651,386 | 3,148 | 10,096,531,553 bytes |
| `product_stub` | 124 | 634,824 | 124 | 24,364,448 bytes |
| `week_dimension` | 1 | 626 | 1 | 7,960 bytes |

The sales manifest records cover source years 1–12. “Recorded written rows” comes
from the captured manifest, not from a validation scan performed for this report.

## Materialized relations

Two Parquet relations were materialized:

| Relation | Rows observed in staged Parquet | File size | SHA-256 in descriptor |
|---|---:|---:|---|
| `silver_product_key_xref_v` | 324,745 | 3,221,244 bytes | `beff6cac4fea032b8c2d5df7d2fb6c996bb7b3419b8b454bd8e2a1fda4e0ca89` |
| `silver_product_week_v` | 28,314,411 | 350,454,944 bytes | `69d4a89c6471e4204b65b97ffaec5b89b862e06a30972c2e42eeb2c72ae170a3` |

The row counts above are descriptive counts read from the staged Parquet files; they
are not semantic gate results. The descriptor lists 3,277 checksum-bearing artifacts,
including retained inputs, materialized outputs, and metadata. Combined listed artifact
size is 10,486,923,557 bytes. Per-file paths, schemas, and SHA-256 values are recorded
in the staging `release.json` descriptor.

## Validation and release status

The descriptor explicitly has `state: "unvalidated"` and
`validation_status: "skipped"`. It has no gate results and no replay bundle. The staged
`validation.json` records only that validation was skipped. Consequently:

- No claim is made here that aggregation semantics, conservation, key integrity, or
  other Silver gates passed.
- The output is not a managed or publishable release. Publication, bootstrap, release
  validation, and rollback must not treat it as one.
- Do not edit the descriptor to promote this artifact. If a publishable release is
  needed, run a normal managed build with validation enabled against suitable inputs.
- The earlier interrupted staging directory is separate and was not used or resumed by
  this completed run. Remove abandoned staging trees only after confirming their
  processes have stopped; do not delete the current pointer or another run's staging.

## Reproduction command

The completed stage-only invocation was:

```sh
uv run python -m scripts.silver build \
  --lake data/lake \
  --silver-root /tmp/iri-silver \
  --dataset sales --dataset product_stub --dataset week_dimension \
  --scope partial --quiescent --skip-validation
```
