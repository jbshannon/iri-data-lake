"""Strict, replayable validation of live or release-pinned Silver relations."""
from __future__ import annotations

import datetime
import decimal
import json

from . import calendar, geography, products, product_attributes, stores


def json_rows(rows):
    """A stable multiset encoding for scalar validation results, not fact exports."""
    def convert(value):
        if isinstance(value, (datetime.date, datetime.datetime, decimal.Decimal)):
            return str(value)
        if isinstance(value, bytes):
            return value.hex()
        raise TypeError(f"unsupported validation scalar: {type(value).__name__}")
    encoded = json.loads(json.dumps(rows, default=convert, allow_nan=False))
    return sorted(encoded, key=lambda row: json.dumps(row, sort_keys=True))


class _Result:
    def __init__(self, owner, sql, params):
        self.owner, self.sql, self.params = owner, sql, params

    def _record(self, rows):
        text = self.sql.lstrip().upper()
        discovery = any(token in text for token in ("INFORMATION_SCHEMA", "DUCKDB_VIEWS()", "DUCKDB_TABLES()", "DUCKDB_FUNCTIONS()"))
        if text.startswith(("SELECT", "WITH")) and not discovery:
            self.owner.recipes.append({"sql": self.sql, "params": self.params,
                                       "expected_rows": json_rows(rows)})

    def fetchone(self):
        row = self.owner.connection.fetchone()
        self._record([] if row is None else [row])
        return row

    def fetchall(self):
        rows = self.owner.connection.fetchall()
        self._record(rows)
        return rows


class _RecordingConnection:
    def __init__(self, connection):
        self.connection, self.recipes = connection, []

    def execute(self, sql, parameters=None):
        self.connection.execute(sql, parameters or [])
        return _Result(self, sql, parameters or [])


def _exists(con, name):
    return con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name=?", [name]).fetchone()[0] > 0


def _assert(con, check_id, sql):
    rows = con.execute(sql).fetchall()
    ok = len(rows) == 1 and len(rows[0]) == 1 and rows[0][0] is True
    return {"check_id": check_id, "ok": ok, "severity": "error", "actual": json_rows(rows),
            "expected": [[True]], "recipes": [{"sql": sql, "params": [], "expected_rows": [[True]], "covers": [check_id]}]}


def validate(con, registry=None, *, household=False):
    checks = []
    modules = [calendar, products, product_attributes, stores, geography]
    if household:
        from . import household as household_module
        modules.append(household_module)
    for module in modules:
        # Optional domains do not invent an assertion over nonexistent source data.
        if module is product_attributes and not _exists(con, "silver_product_attribute_observation_v"):
            continue
        recording = _RecordingConnection(con)
        results = module.validate(recording)
        if results:
            if not recording.recipes:
                raise ValueError(f"validator has no replayable queries: {module.__name__}")
            ids = [result["check_id"] for result in results]
            results[0]["recipes"] = [{**recipe, "covers": ids} for recipe in recording.recipes]
            checks.extend(results)
    registry = registry or {}
    for name, metadata in registry.items():
        if not name.replace("_", "").isalnum():
            raise ValueError("unsafe registry identifier")
        key = metadata.get("primary_key", [])
        nullable = set(metadata.get("nullable_key_fields", []))
        if key:
            fields = ",".join('"' + field.replace('"', '""') + '"' for field in key)
            nonnull = [field for field in key if field not in nullable]
            nulls = " OR ".join('"' + field.replace('"', '""') + '" IS NULL' for field in nonnull) or "FALSE"
            checks.append(_assert(con, f"key:{name}", f'SELECT NOT EXISTS(SELECT 1 FROM "{name}" WHERE {nulls}) AND NOT EXISTS(SELECT 1 FROM "{name}" GROUP BY {fields} HAVING count(*)>1)'))
        if name == "silver_product_week_v":
            schema = dict((row[0], row[1]) for row in con.execute(f'DESCRIBE "{name}"').fetchall())
            expected = {"upc": "VARCHAR", "week": "USMALLINT", "units": "BIGINT", "revenue_cents": "BIGINT", "sales_rows": "BIGINT"}
            if schema != expected:
                raise ValueError(f"product-week schema mismatch: {schema}")
    if _exists(con, "silver_sales_v"):
        checks.append(_assert(con, "sales_bronze_row_conservation", "SELECT (SELECT count(*) FROM silver_sales_v)=(SELECT count(*) FROM bronze_sales)"))
        checks.append(_assert(con, "sales_measures_not_null", "SELECT count(*)=0 FROM silver_sales_v WHERE units IS NULL OR dollars_cents IS NULL OR source_year IS NULL OR week IS NULL"))
        if _exists(con, "silver_week_dimension_v"):
            checks.append(_assert(con, "sales_calendar_coverage", "SELECT count(*)=0 FROM silver_sales_v s ANTI JOIN silver_week_dimension_v c USING(week)"))
        if _exists(con, "silver_store_v"):
            checks.append(_assert(con, "sales_annual_store_coverage", "SELECT count(*)=0 FROM silver_sales_v f ANTI JOIN silver_store_v s ON f.source_year=s.source_year AND f.iri_key=s.iri_key"))
    require_passing(checks)
    return checks


def require_passing(checks):
    if not isinstance(checks, list) or not checks:
        raise ValueError("Silver validation contains no expected assertions")
    ids = []
    for check in checks:
        if not isinstance(check, dict) or not check.get("check_id") or check.get("ok") is not True:
            raise ValueError(f"Silver validation failed: {check.get('check_id') if isinstance(check, dict) else 'malformed check'}")
        ids.append(check["check_id"])
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate Silver assertion identifiers")
