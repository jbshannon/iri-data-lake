"""Finite input planning, validated staging, and explicit Silver publication."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import uuid
from pathlib import Path

import duckdb

from . import bundle, codes, validation
from .bootstrap import register
from .inputs import _PUBLIC_DATASETS, _RESTRICTED_DATASETS, retain_inputs, select_inputs
from .releases import build_fingerprint, exclusive_lock, load_release, publish_release, stage_release


def _file_hash(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def _write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str) + "\n")


def _module_hashes():
    return {path.name: _file_hash(path) for path in sorted(Path(__file__).parent.glob("*.py"))}


def _references(root):
    if root is None:
        return {}
    root = Path(root)
    if not root.is_dir() or root.is_symlink():
        raise ValueError("reference root must be an existing real directory")
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError("reference symlinks are not supported")
        if path.is_file():
            if path.relative_to(root) != Path("stub_categories.csv"):
                raise ValueError(f"unsupported reference file: {path.name}")
            result[str(path.relative_to(root))] = _file_hash(path)
    return result


def _selection(datasets, household):
    selected = set(datasets if datasets is not None else (_RESTRICTED_DATASETS if household else _PUBLIC_DATASETS))
    if not selected or selected - (_PUBLIC_DATASETS | _RESTRICTED_DATASETS):
        raise ValueError("unknown or empty dataset selection")
    if not household and selected & _RESTRICTED_DATASETS:
        raise ValueError("restricted datasets cannot be included in a public release")
    return sorted(selected)


def _readiness(snapshot, scope, readiness):
    if scope not in {"partial", "corpus"}:
        raise ValueError("scope must be partial or corpus")
    if scope == "partial":
        return None
    if readiness is None:
        raise ValueError("corpus scope requires snapshot-matched inventory and Gate 5/6 evidence")
    evidence = json.loads(Path(readiness).read_text())
    inventory = evidence.get("inventory", [])
    expected = {(r["dataset"], r["source_path"], r.get("source_sha256")) for r in snapshot["records"]}
    actual = {(r["dataset"], r["source_path"], r.get("source_sha256")) for r in inventory if r.get("dataset") in _PUBLIC_DATASETS}
    sales = [r for r in snapshot["records"] if r["dataset"] == "sales"]
    coverage = {(r.get("source_year"), r.get("category"), r.get("channel")) for r in sales}
    if (evidence.get("manifest_sha256") != snapshot["manifest_sha256"] or
            evidence.get("gate5_ok") is not True or evidence.get("gate6_ok") is not True or
            expected != actual or set(snapshot["scope"]["datasets"]) != _PUBLIC_DATASETS or
            len(sales) != 744 or len(coverage) != 744 or
            {r[0] for r in coverage} != set(range(1, 13)) or
            {r[2] for r in coverage} != {"drug", "groc"}):
        raise ValueError("corpus readiness evidence or required corpus coverage is incomplete")
    return evidence


def _fingerprint(snapshot, references, *, scope, household, readiness):
    # Global manifest hash is audit evidence, not a dependency on unrelated private appends.
    records = [{k: r.get(k) for k in ("dataset", "source_path", "source_sha256", "parser_version", "output_schema_version", "source_schema_fingerprint", "source_year", "category", "channel", "deduplicated_from", "written_rows")} for r in snapshot["records"]]
    records.sort(key=lambda row: (row["dataset"], row["source_path"]))
    payload = {"records": records, "files": snapshot["file_metadata"],
               "aliases": [{k: a.get(k) for k in ("dataset", "source_path", "canonical_path", "source_year", "category", "channel", "edition")} for a in snapshot["aliases"]],
               "scope": scope, "household": household, "references": references,
               "definitions": _module_hashes(), "duckdb": duckdb.__version__, "schema_version": 1,
               "readiness": readiness}
    return build_fingerprint(payload), payload


def plan(lake: Path, *, datasets=None, reference_root=None, scope="partial", readiness=None):
    selected = _selection(datasets, False)
    snapshot = select_inputs(lake, required_datasets=selected)
    evidence = _readiness(snapshot, scope, readiness)
    fingerprint, _ = _fingerprint(snapshot, _references(reference_root), scope=scope, household=False, readiness=evidence)
    return {"scope": scope, "input_scope": snapshot["scope"], "manifest_sha256": snapshot["manifest_sha256"],
            "files": {key: len(paths) for key, paths in snapshot["files"].items()}, "fingerprint": fingerprint}


def _event(root, run_id, state, **fields):
    path = root / "runs.jsonl"
    fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
    with os.fdopen(fd, "a") as stream:
        stream.write(json.dumps({"run_id": run_id, "state": state, **fields}, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def _portable(value, stage):
    if isinstance(value, str):
        return "${RELEASE_ROOT}" + value[len(str(stage)):] if value.startswith(str(stage) + "/") else value
    if isinstance(value, list):
        return [_portable(item, stage) for item in value]
    if isinstance(value, dict):
        return {key: _portable(item, stage) for key, item in value.items()}
    return value


def _registry(con, registry):
    for name, key in (("silver_source_v", ["_source_id"]), ("silver_source_alias_v", ["dataset", "source_path", "canonical_path"])):
        registry[name] = {"schema_version": 1, "primary_key": key, "materialize": False, "restricted": False}
    nullable = {"silver_source_alias_v": ["canonical_path"], "silver_coverage_v": ["category"],
                "silver_market_region_v": ["region_name"]}
    # Scope relations may represent an unknown label with NULL; identity keys may not.
    for name, fields in nullable.items():
        if name in registry:
            registry[name]["nullable_key_fields"] = fields
    return registry


def _replay(root, release_dir, descriptor):
    # Spill can contain source values: keep it private and outside immutable artifacts.
    with tempfile.TemporaryDirectory(prefix=".replay-", dir=root) as temporary:
        with duckdb.connect() as con:
            con.execute("SET temp_directory=?", [temporary])
            bundle.restore_bundle(con, release_dir, descriptor)
            return bundle.validate_bundle(con, release_dir, descriptor)


def validate_release(root: Path, release_id=None):
    root = Path(root).resolve()
    if not root.is_dir():
        raise FileNotFoundError(root)
    descriptor = load_release(root, release_id)
    release_dir = root / "releases" / descriptor["release_id"]
    checks = _replay(root, release_dir, descriptor)
    return {"release_id": descriptor["release_id"], "state": "validated", "scope": descriptor["scope"], "gate_results": checks}


def publish_staged(root: Path, staging: Path):
    root = Path(root).resolve()
    staging = Path(staging).resolve()
    if not staging.is_relative_to(root / ".staging") or staging == root / ".staging":
        raise ValueError("publish requires an existing managed staging release")
    with exclusive_lock(root):
        descriptor = json.loads((staging / "release.json").read_text())
        _replay(root, staging, descriptor)
        # Verify all artifact checksums as well as the bundle; no pointer moves on failure.
        return publish_release(root, staging, descriptor)


def rollback(root: Path, release_id, *, reason):
    from .releases import _write_json_atomic
    root = Path(root).resolve()
    with exclusive_lock(root):
        validate_release(root, release_id)
        if not reason or not reason.strip():
            raise ValueError("rollback reason is required")
        _write_json_atomic(root / "current.json", {"release_id": release_id, "descriptor": f"releases/{release_id}/release.json", "rollback_reason": reason})
    return {"release_id": release_id, "state": "rolled-back"}


def build(lake: Path, silver_root: Path, *, datasets=None, publish=False, household=False,
          restricted_root: Path | None = None, approval: str | None = None,
          reference_root=None, scope="partial", readiness=None, quiescent=False,
          threads=None, memory_limit=None, temp_directory=None):
    if not quiescent:
        raise ValueError("stop Bronze writers and explicitly acknowledge quiescent inputs")
    selected = _selection(datasets, household)
    root = Path(silver_root).resolve()
    lake = Path(lake).resolve()
    if household:
        if not restricted_root or not approval or not approval.strip():
            raise ValueError("household build requires separate restricted root and policy approval")
        private = Path(restricted_root)
        resolved = private.resolve()
        if private.is_symlink() or resolved.is_relative_to(root) or root.is_relative_to(resolved) or resolved.is_relative_to(lake) or lake.is_relative_to(resolved):
            raise ValueError("restricted root must be separate from the lake and public Silver root")
        root = resolved
    if root == lake or root.is_relative_to(lake / "bronze"):
        raise ValueError("Silver output must not replace Bronze or the lake root")
    snapshot = select_inputs(lake, required_datasets=selected)
    evidence = _readiness(snapshot, scope, readiness)
    references = _references(reference_root)
    fingerprint, payload = _fingerprint(snapshot, references, scope=scope, household=household, readiness=evidence)
    run_id = uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if household:
        root.chmod(0o700)
    with exclusive_lock(root):
        if (root / "current.json").exists():
            current = load_release(root)  # Corruption is not swallowed or treated as a cache miss.
            if current["fingerprint"] == fingerprint:
                validate_release(root, current["release_id"])
                return {"state": "no-op", "release_id": current["release_id"], "fingerprint": fingerprint}
        _event(root, run_id, "planned", fingerprint=fingerprint, scope=scope)
        stage = stage_release(root, fingerprint)
        try:
            _event(root, run_id, "building")
            retained = retain_inputs(snapshot, stage / "inputs", copy_only=household)
            ref_dir = stage / "references"
            for relative, digest in references.items():
                target = ref_dir / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(Path(reference_root) / relative, target)
                if _file_hash(target) != digest:
                    raise ValueError("reference changed during snapshot capture")
            spill = stage / ".spill" if household or temp_directory is None else Path(temp_directory).resolve()
            spill.mkdir(parents=True, exist_ok=True, mode=0o700)
            with duckdb.connect() as con:
                if threads is not None:
                    if threads < 1:
                        raise ValueError("threads must be positive")
                    con.execute(f"SET threads={int(threads)}")
                if memory_limit is not None:
                    con.execute("SET memory_limit=?", [memory_limit])
                con.execute("SET temp_directory=?", [str(spill)])
                registry = _registry(con, register(con, retained, reference_root=ref_dir if references else None, household=household))
                registry.update(codes.register(con))
                outputs = {}
                for name, meta in registry.items():
                    if meta.get("restricted") and not household:
                        raise ValueError("restricted relation in public registry")
                    if meta.get("materialize"):
                        relative = f"tables/{name.removeprefix('silver_').removesuffix('_v')}.parquet"
                        target = stage / relative
                        target.parent.mkdir(parents=True, exist_ok=True)
                        con.execute(f'COPY (SELECT * FROM "{name}") TO ? (FORMAT PARQUET, COMPRESSION ZSTD)', [str(target)])
                        con.execute(f'CREATE OR REPLACE TEMP VIEW "{name}" AS SELECT * FROM read_parquet({bundle._quote(str(target))})')
                        outputs[name] = {"path": relative, "sha256": _file_hash(target), "schema": bundle._schema(con, name)}
                _event(root, run_id, "validating")
                checks = validation.validate(con, registry, household=household)
                captured = bundle.capture_bundle(con, stage, registry, checks)
                outputs.update(captured["outputs"])
            if spill.is_relative_to(stage):
                shutil.rmtree(spill)
            _write_json(stage / "inputs.json", _portable(retained, stage))
            _write_json(stage / "validation.json", checks)
            if evidence is not None:
                _write_json(stage / "readiness.json", evidence)
            for path in sorted(stage.rglob("*")):
                if path.is_file():
                    relative = str(path.relative_to(stage))
                    if not any(entry["path"] == relative for entry in outputs.values()):
                        outputs[f"dependency:{relative}"] = {"path": relative, "sha256": _file_hash(path), "schema": {"kind": "dependency"}}
            descriptor = {"fingerprint": fingerprint, "schema_version": 1, "sources": payload,
                          "manifest_sha256": snapshot["manifest_sha256"], "scope": scope,
                          "input_scope": snapshot["scope"], "restricted": household,
                          "approval_reference": approval if household else None,
                          "registry": captured["registry"], "outputs": outputs, "gate_results": checks,
                          "bundle_path": captured["bundle_path"], "bundle_sha256": captured["bundle_sha256"],
                          "resources": {"threads": threads, "memory_limit": memory_limit},
                          "states": ["planned", "building", "validating", "ready"]}
            _write_json(stage / "release.json", descriptor)
            # Independent fresh session validates the exact staged artifact before any commit.
            _replay(root, stage, descriptor)
            if household:
                for path in stage.rglob("*"):
                    path.chmod(0o700 if path.is_dir() else 0o600)
            _event(root, run_id, "ready", staging=stage.name)
            if publish:
                result = publish_release(root, stage, descriptor)
                _event(root, run_id, "published", release_id=result["release_id"])
                return result
            return {"state": "ready", "staging": str(stage), "fingerprint": fingerprint, "scope": scope}
        except Exception as error:
            # Keep the old pointer; an isolated failed staging tree is safe to remove.
            _event(root, run_id, "failed", error_type=type(error).__name__)
            if stage.exists():
                shutil.rmtree(stage)
            raise
