"""Pinned, declarative DuckDB dependency bundles for immutable releases."""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from .keys import register_functions

KEY_CODEC_VERSION = "silver-id-v1"
BUNDLE_VERSION = 1


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _schema(con, relation: str) -> list[dict]:
    return [{"name": row[0], "type": str(row[1]), "nullable": str(row[2]).upper() != "NO"}
            for row in con.execute(f'DESCRIBE "{relation.replace(chr(34), chr(34)*2)}"').fetchall()]


def capture_bundle(con, stage: Path, registry: dict, checks: list[dict]) -> dict:
    """Persist user temp tables and SQL view recipes; never persist Python code."""
    stage = Path(stage).resolve()
    root_string = str(stage).replace("'", "''")
    from .validation import require_passing
    require_passing(checks)
    registry = {name: {**metadata, "schema": _schema(con, name)} for name, metadata in registry.items()}
    tables = []
    for row in con.execute("SELECT schema_name, table_name FROM duckdb_tables() WHERE internal=false ORDER BY schema_name, table_name").fetchall():
        schema_name, name = row
        if schema_name not in ("main", "temp", "" ):
            continue
        rel = f"bundle/tables/{len(tables):04d}.parquet"
        target = stage / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        quoted = '"' + name.replace('"', '""') + '"'
        con.execute(f"COPY (SELECT * FROM {quoted}) TO {_quote(str(target))} (FORMAT PARQUET)")
        tables.append({"name": name, "path": rel, "sha256": _digest(target), "schema": _schema(con, name)})
    views = []
    for schema_name, name, sql in con.execute("SELECT schema_name, view_name, sql FROM duckdb_views() WHERE internal=false ORDER BY schema_name, view_name").fetchall():
        if schema_name not in ("main", "temp", ""):
            continue
        # DuckDB's emitted statement is retained as SQL, but stage-root references are portable.
        sql = sql.replace(root_string, "${RELEASE_ROOT}")
        if not re.match(r'^CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?VIEW\s+', sql, re.I):
            raise ValueError(f"unsupported archived view definition: {name}")
        views.append({"name": name, "sql": sql})
    recipes, covered = [], set()
    for check in checks:
        for index, recipe in enumerate(check.get("recipes", [])):
            covers = recipe.get("covers", [check["check_id"]])
            covered.update(covers)
            recipes.append({**recipe, "check_id": f"replay:{check['check_id']}:{index}",
                            "sql": recipe["sql"].replace(root_string, "${RELEASE_ROOT}"), "covers": covers})
        if isinstance(check.get("sql"), str):
            recipes.append({"check_id": f"replay:{check['check_id']}", "sql": check["sql"],
                            "params": [], "expected_rows": check["expected"], "covers": [check["check_id"]]})
            covered.add(check["check_id"])
    if covered != {check["check_id"] for check in checks}:
        raise ValueError("every expected assertion must have a replayable SQL recipe")
    artifacts = {item["path"]: {"path": item["path"], "sha256": item["sha256"], "schema": item["schema"]} for item in tables}
    bundle = {"version": BUNDLE_VERSION, "key_codec": KEY_CODEC_VERSION, "tables": tables, "views": views,
              "registry": registry, "recipes": recipes, "expected_check_ids": sorted(c["check_id"] for c in checks)}
    path = stage / "views" / "bundle.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(bundle, sort_keys=True, separators=(",", ":")), encoding="utf-8")
    bundle_sha = _digest(path)
    artifacts["views/bundle.json"] = {"path": "views/bundle.json", "sha256": bundle_sha, "schema": {"kind": "sql_bundle"}}
    return {"bundle_path": "views/bundle.json", "bundle_sha256": bundle_sha, "outputs": artifacts, "registry": registry}


def _verify_release_file(release_dir: Path, relative: str, digest: str):
    path = Path(relative)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError("unsafe bundle artifact path")
    target = release_dir / path
    if target.is_symlink() or not target.is_file() or not target.resolve().is_relative_to(release_dir.resolve()):
        raise ValueError(f"missing or unsafe bundle artifact: {relative}")
    if _digest(target) != digest:
        raise ValueError(f"bundle artifact checksum mismatch: {relative}")
    return target


def _assert_key_codec():
    from .keys import stable_id
    vectors = {
        "null": "74234e98afe7498fb5daf1f36ac2d78acc339464f950703b8c019892f982b90b",
        '{"x":1}': "5041bf1f713df204784353e82f6a4a535931cb64f1f4b4a5aeaffcb720918b22",
        '{"x":0.5}': "a06e6072617c185b307ea48c0d0858ae9f0e3b49e70cf95d7faa031f964fde6f",
    }
    if any(stable_id(value) != expected for value, expected in vectors.items()):
        raise ValueError("installed Silver key codec does not match frozen v1 vectors")


def _read_bundle(release_dir: Path, descriptor: dict):
    relative = descriptor.get("bundle_path")
    if not isinstance(relative, str):
        raise ValueError("release has no bundle_path")
    target = _verify_release_file(Path(release_dir), relative, descriptor.get("bundle_sha256", ""))
    bundle = json.loads(target.read_text(encoding="utf-8"))
    if bundle.get("version") != BUNDLE_VERSION or bundle.get("key_codec") != KEY_CODEC_VERSION:
        raise ValueError("unsupported bundle or key codec version")
    return bundle


def restore_bundle(con, release_dir: Path, descriptor: dict) -> dict:
    """Restore a release without invoking live domain registration or Python from disk."""
    release_dir = Path(release_dir).resolve()
    bundle = _read_bundle(release_dir, descriptor)
    _assert_key_codec()
    register_functions(con)
    for item in bundle["tables"]:
        target = _verify_release_file(release_dir, item["path"], item["sha256"])
        name = item["name"]
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError("unsafe relation name in bundle")
        con.execute(f'CREATE TEMP TABLE "{name}" AS SELECT * FROM read_parquet({_quote(str(target))})')
    pending = list(bundle["views"])
    while pending:
        rest = []
        progressed = False
        for view in pending:
            sql = view.get("sql", "")
            name = view.get("name", "")
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
                raise ValueError("unsafe view name in bundle")
            sql = sql.strip()
            if sql.endswith(";"):
                sql = sql[:-1].rstrip()
            if not re.match(r'^CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?VIEW\s+', sql, re.I) or len(con.extract_statements(sql.replace("${RELEASE_ROOT}", "/release"))) != 1:
                raise ValueError("unsafe archived view SQL")
            sql = sql.replace("${RELEASE_ROOT}", str(release_dir).replace("'", "''"))
            try:
                con.execute(sql)
                progressed = True
            except Exception as exc:
                rest.append((view, exc))
        if rest and not progressed:
            raise ValueError(f"cannot resolve archived view dependencies: {[v.get('name') for v, _ in rest]}") from rest[0][1]
        pending = [v for v, _ in rest]
    return bundle["registry"]


def validate_bundle(con, release_dir: Path, descriptor: dict) -> list[dict]:
    """Replay saved SQL recipes and enforce basic integrity of restored registered relations."""
    bundle = _read_bundle(Path(release_dir).resolve(), descriptor)
    checks = []
    for item in bundle["tables"]:
        _verify_release_file(Path(release_dir), item["path"], item["sha256"])
    for name, meta in bundle["registry"].items():
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError("unsafe registry relation")
        cols = {r[0]: r for r in con.execute(f'DESCRIBE "{name}"').fetchall()}
        expected_schema = meta.get("schema") if isinstance(meta, dict) else None
        if expected_schema and isinstance(expected_schema, list):
            actual_schema = [{"name": row[0], "type": str(row[1])} for row in cols.values()]
            wanted_schema = [{"name": row["name"], "type": row["type"]} for row in expected_schema]
            checks.append(_result(f"registry-schema:{name}", actual_schema == wanted_schema, actual_schema, wanted_schema))
        checks.append(_result(f"registry-nonempty:{name}", bool(cols), len(cols), ">0 columns"))
        primary_key = meta.get("primary_key", []) if isinstance(meta, dict) else []
        if primary_key:
            if any(key not in cols for key in primary_key):
                checks.append(_result(f"registry-key-columns:{name}", False, primary_key, sorted(cols)))
            else:
                key_sql = ", ".join('"' + key.replace('"', '""') + '"' for key in primary_key)
                required_keys = [key for key in primary_key if key not in meta.get("nullable_key_fields", [])]
                null_sql = " OR ".join(f'"{key.replace(chr(34), chr(34)*2)}" IS NULL' for key in required_keys) or "FALSE"
                bad = con.execute(f'SELECT count(*) FROM "{name}" WHERE ' + null_sql).fetchone()[0]
                duplicates = con.execute(f'SELECT count(*) FROM (SELECT {key_sql}, count(*) n FROM "{name}" GROUP BY {key_sql} HAVING n > 1)').fetchone()[0]
                checks.append(_result(f"registry-key:{name}", bad == 0 and duplicates == 0, {"nulls": bad, "duplicate_keys": duplicates}, {"nulls": 0, "duplicate_keys": 0}))
    from .validation import json_rows, require_passing
    covered = set()
    for recipe in bundle["recipes"]:
        sql = recipe["sql"].replace("${RELEASE_ROOT}", str(Path(release_dir).resolve()).replace("'", "''"))
        if not sql.lstrip().upper().startswith(("SELECT", "WITH")) or len(con.extract_statements(sql)) != 1:
            raise ValueError("unsafe archived assertion SQL")
        actual = json_rows(con.execute(sql, recipe.get("params", [])).fetchall())
        expected = recipe.get("expected_rows")
        checks.append(_result(recipe["check_id"], actual == expected, actual, expected))
        covered.update(recipe.get("covers", []))
    if not bundle.get("expected_check_ids") or covered != set(bundle["expected_check_ids"]):
        raise ValueError("bundle is missing expected validation recipes")
    require_passing(checks)
    return checks


def _result(check_id, ok, actual, expected, severity="error"):
    return {"check_id": check_id, "ok": bool(ok), "severity": severity, "actual": actual, "expected": expected}
