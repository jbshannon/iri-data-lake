#!/usr/bin/env python3
"""Materialize the Silver product-key crosswalk and UPC-week aggregate.

The SQL definitions and validation queries are shared with
sql/silver_product_week.sql. Run after Bronze Gates 5 and 6:

    uv run python scripts/materialize_product_week.py --lake data/lake

Existing outputs are left untouched unless --overwrite is given. Files are
written to temporary siblings, checked for full sales-row coverage, then renamed
into place.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

try:
    import duckdb
except ImportError:  # pragma: no cover
    sys.exit("duckdb is not installed; run with `uv run python scripts/materialize_product_week.py`.")

from run_sql import split_statements

ROOT = Path(__file__).resolve().parents[1]
SQL_FILE = ROOT / "sql" / "silver_product_week.sql"


def sql_string(value: str) -> str:
    """Quote a string literal for DuckDB SQL."""
    return "'" + value.replace("'", "''") + "'"


def show_result(con: duckdb.DuckDBPyConnection, statement: str, index: int) -> None:
    started = time.perf_counter()
    result = con.execute(statement)
    rows = result.fetchall()
    columns = [description[0] for description in (result.description or [])]
    elapsed = time.perf_counter() - started
    print(f"setup [{index}] ({elapsed:.2f}s)")
    for row in rows:
        if columns:
            print("  " + ", ".join(f"{name}={value}" for name, value in zip(columns, row)))
    for column_index, name in enumerate(columns):
        if name.endswith("_ok") and any(row[column_index] is False for row in rows):
            raise RuntimeError(f"validation failed: {name}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake", default=os.environ.get("LAKE", "data/lake"))
    parser.add_argument("--overwrite", action="store_true", help="replace existing Silver outputs")
    parser.add_argument("--threads", type=int, default=0, help="DuckDB worker threads (0 = default)")
    args = parser.parse_args()

    lake = Path(args.lake).expanduser().resolve()
    sql_text = SQL_FILE.read_text()
    sql_text = sql_text.replace("${LAKE}", str(lake))
    sql_text = sql_text.replace("${BRONZE}", str(lake / "bronze" / "iri_sales"))
    if "${" in sql_text:
        raise RuntimeError("unsubstituted variable remains in product-week SQL")
    statements = split_statements(sql_text)
    if len(statements) < 5 or "silver_product_week_v" not in statements[-1]:
        raise RuntimeError("unexpected SQL layout; expected view setup followed by benchmark query")

    destinations = {
        "product_key_xref": lake / "silver" / "product_key_xref" / "part-00000.parquet",
        "product_week": lake / "silver" / "product_week" / "part-00000.parquet",
    }
    existing = [path for path in destinations.values() if path.exists()]
    if existing and not args.overwrite:
        raise FileExistsError(
            "Silver output already exists; pass --overwrite to replace: "
            + ", ".join(str(path) for path in existing)
        )

    temp_paths = {
        name: path.with_name(f"{path.stem}.{os.getpid()}.tmp.parquet")
        for name, path in destinations.items()
    }
    for path in temp_paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.unlink(missing_ok=True)

    con = duckdb.connect()
    if args.threads:
        con.execute(f"SET threads = {int(args.threads)}")

    try:
        # The final SQL statement is the benchmark SELECT; this materializer
        # executes the shared preflight, crosswalk, and view definitions, then
        # measures the actual COPY operations instead.
        for index, statement in enumerate(statements[:-1], start=1):
            show_result(con, statement, index)

        for name, view_name in (
            ("product_key_xref", "silver_product_key_xref_v"),
            ("product_week", "silver_product_week_v"),
        ):
            temp_path = temp_paths[name]
            copy_sql = (
                f"COPY (SELECT * FROM {view_name}) TO {sql_string(str(temp_path))} "
                "(FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            started = time.perf_counter()
            con.execute(copy_sql)
            elapsed = time.perf_counter() - started
            print(f"materialized {name}: {elapsed:.2f}s, {temp_path.stat().st_size:,} bytes")

        expected = con.execute(
            f"""
            WITH latest_sales AS (
                SELECT written_rows, status,
                       row_number() OVER (
                           PARTITION BY source_path ORDER BY started_at DESC, run_id DESC
                       ) AS rn
                FROM read_json({sql_string(str(lake / 'metadata' / 'manifest.jsonl'))})
                WHERE coalesce(dataset, 'sales') = 'sales'
            )
            SELECT sum(written_rows)
            FROM latest_sales
            WHERE rn = 1 AND status = 'success'
            """
        ).fetchone()[0]
        xref_path = sql_string(str(temp_paths["product_key_xref"]))
        xref_rows, xref_upcs, xref_keys = con.execute(
            f"""
            SELECT count(*), count(DISTINCT upc),
                   count(DISTINCT (sy, ge, vend, item))
            FROM read_parquet({xref_path})
            """
        ).fetchone()
        week_path = sql_string(str(temp_paths["product_week"]))
        measure_types = con.execute(
            f"SELECT typeof(units), typeof(revenue_cents) FROM read_parquet({week_path}) LIMIT 1"
        ).fetchone()
        if measure_types != ("BIGINT", "BIGINT"):
            raise RuntimeError(f"sales measures must remain exact BIGINT cents/counts, got {measure_types}")
        output_rows, matched_rows, units, revenue_cents = con.execute(
            f"""
            SELECT count(*), sum(sales_rows), sum(units), sum(revenue_cents)
            FROM read_parquet({week_path})
            """
        ).fetchone()

        if xref_rows != xref_upcs or xref_rows != xref_keys:
            raise RuntimeError(
                f"materialized key crosswalk is not bijective: rows={xref_rows}, "
                f"upcs={xref_upcs}, iri_keys={xref_keys}"
            )
        if expected is None or matched_rows != expected:
            raise RuntimeError(
                f"product-week coverage mismatch: matched={matched_rows}, expected={expected}"
            )

        print(
            "validated product_week: "
            f"rows={output_rows:,}, sales_rows={matched_rows:,}, units={units:,}, "
            f"revenue_cents={revenue_cents:,}"
        )
        for name, destination in destinations.items():
            os.replace(temp_paths[name], destination)
            print(f"published {name}: {destination} ({destination.stat().st_size:,} bytes)")
    except Exception:
        for path in temp_paths.values():
            path.unlink(missing_ok=True)
        raise
    finally:
        con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
