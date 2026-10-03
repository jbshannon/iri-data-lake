#!/usr/bin/env python3
"""Run a .sql file through DuckDB and turn it into a pass/fail gate.

Why a Python runner rather than the `duckdb` CLI: the DuckDB PyPI
package ships no CLI (`python -m duckdb` fails), and a gate that has to
be *read and judged* is not a gate. This runner executes each statement,
prints the result tables, and exits non-zero if any returned column whose
name ends in `_ok` is false. So `sql/gate5_reconciliation.sql` is the
check; this is the assertion.

Usage:
    uv run python scripts/run_sql.py sql/gate5_reconciliation.sql
    uv run python scripts/run_sql.py sql/foo.sql --lake /tmp/some-lake
    uv run python scripts/run_sql.py sql/foo.sql --lake data/lake --var CAT=beer

Substitutions inside the .sql text:
    ${LAKE}     --lake value                (default: data/lake)
    ${BRONZE}   ${LAKE}/bronze/iri_sales
    ${VAR}      any --var NAME=value
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
    sys.exit("duckdb is not installed in this environment.\n"
             "  uv run python scripts/run_sql.py ...   # from the project root\n"
             "  (or: uv add duckdb)")


def strip_comments(line: str) -> str:
    """Remove a `--` comment from a line, respecting single-quoted strings."""
    out, in_str = [], False
    i = 0
    while i < len(line):
        ch = line[i]
        if ch == "'":
            in_str = not in_str
        elif ch == "-" and not in_str and line[i : i + 2] == "--":
            break
        out.append(ch)
        i += 1
    return "".join(out)


def split_statements(sql: str) -> list[str]:
    """Split a SQL script into statements on top-level semicolons.

    Deliberately small: it handles `--` comments and single-quoted
    strings, which is what the hand-written gate files in sql/ use. It
    does not understand dollar-quoted bodies or nested statement syntax,
    so it is not a general SQL lexer — if a file starts using those, put
    one statement per file.
    """
    text = "\n".join(strip_comments(l) for l in sql.splitlines())

    statements, buf, in_str = [], [], False
    for ch in text:
        if ch == "'":
            in_str = not in_str
        if ch == ";" and not in_str:
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            continue
        buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("sql_file", type=Path)
    ap.add_argument("--lake", default=os.environ.get("LAKE", "data/lake"),
                    help="lake output root (default: $LAKE or data/lake)")
    ap.add_argument("--var", action="append", default=[],
                    help="extra substitution NAME=value (repeatable)")
    ap.add_argument("--threads", type=int, default=0,
                    help="DuckDB worker threads (0 = leave default)")
    args = ap.parse_args()

    text = args.sql_file.read_text()
    subs = {
        "LAKE": str(args.lake).rstrip("/"),
        "BRONZE": f"{str(args.lake).rstrip('/')}/bronze/iri_sales",
    }
    for kv in args.var:
        name, _, value = kv.partition("=")
        subs[name] = value
    for name, value in subs.items():
        text = text.replace("${" + name + "}", value)

    if "${" in text:
        leftover = text[text.index("${"):].split("}")[0] + "}"
        sys.exit(f"unsubstituted variable in SQL: {leftover}")

    con = duckdb.connect()
    if args.threads:
        con.execute(f"SET threads = {int(args.threads)}")

    failures: list[str] = []
    for i, stmt in enumerate(split_statements(text), start=1):
        label = " ".join(stmt.split())[:70]
        try:
            started = time.time()
            result = con.execute(stmt)
            rows = result.fetchall()
            cols = [d[0] for d in (result.description or [])]
        except Exception as exc:  # noqa: BLE001 - report, don't mask
            print(f"\n=== [{i}] {label}\nERROR: {exc}", file=sys.stderr)
            failures.append(f"[{i}] {label}: {exc}")
            continue

        elapsed = time.time() - started
        print(f"\n=== [{i}] {label}   ({elapsed:.2f}s)")
        if not cols:
            continue
        widths = [max(len(c), *(len(str(r[j])) for r in rows)) if rows else len(c)
                  for j, c in enumerate(cols)]
        print("  " + "  ".join(c.ljust(widths[j]) for j, c in enumerate(cols)))
        print("  " + "  ".join("-" * w for w in widths))
        for r in rows:
            print("  " + "  ".join(str(v).ljust(widths[j]) for j, v in enumerate(r)))

        for j, col in enumerate(cols):
            if col.endswith("_ok"):
                for r in rows:
                    if r[j] is False:
                        failures.append(f"[{i}] {col} is false")

    print()
    if failures:
        print(f"GATE FAILED ({len(failures)}):")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("GATE PASSED: every `*_ok` column was true.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
