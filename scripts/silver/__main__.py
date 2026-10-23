"""Explicit local Silver planning, staging, publication, and pinned query commands."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

from . import bootstrap, build


def main(argv=None):
    parser = argparse.ArgumentParser(prog="python -m scripts.silver")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("plan", "build"):
        command = commands.add_parser(name)
        command.add_argument("--lake", type=Path, required=True)
        command.add_argument("--dataset", action="append", dest="datasets")
        command.add_argument("--reference-root", type=Path)
        command.add_argument("--scope", choices=("partial", "corpus"), default="partial")
        command.add_argument("--readiness", type=Path)
        if name == "build":
            command.add_argument("--silver-root", type=Path, required=True)
            command.add_argument("--publish", action="store_true")
            command.add_argument("--skip-validation", action="store_true",
                                 help="skip semantic and replay gates; output is unvalidated and never publishable")
            command.add_argument("--quiescent", action="store_true", help="acknowledge Bronze writers are stopped during capture")
            command.add_argument("--household", action="store_true")
            command.add_argument("--restricted-root", type=Path)
            command.add_argument("--approval")
            command.add_argument("--threads", type=int)
            command.add_argument("--memory-limit")
            command.add_argument("--temp-directory", type=Path)
    for name in ("publish", "validate", "bootstrap", "rollback"):
        command = commands.add_parser(name)
        command.add_argument("--silver-root", type=Path, required=True)
        if name == "publish":
            command.add_argument("--staging", type=Path, required=True)
        else:
            command.add_argument("--release-id", required=name == "rollback")
        if name == "rollback":
            command.add_argument("--reason", required=True)
        if name == "bootstrap":
            command.add_argument("--query", default="SELECT table_name FROM information_schema.views WHERE table_name LIKE 'silver_%' ORDER BY table_name")
            command.add_argument("--restricted", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            result = build.plan(args.lake, datasets=args.datasets, reference_root=args.reference_root,
                                scope=args.scope, readiness=args.readiness)
        elif args.command == "build":
            result = build.build(args.lake, args.silver_root, datasets=args.datasets, publish=args.publish,
                                 household=args.household, restricted_root=args.restricted_root, approval=args.approval,
                                 reference_root=args.reference_root, scope=args.scope, readiness=args.readiness,
                                 quiescent=args.quiescent, threads=args.threads, memory_limit=args.memory_limit,
                                 temp_directory=args.temp_directory, skip_validation=args.skip_validation)
        elif args.command == "publish":
            result = build.publish_staged(args.silver_root, args.staging)
        elif args.command == "validate":
            result = build.validate_release(args.silver_root, args.release_id)
        elif args.command == "rollback":
            result = build.rollback(args.silver_root, args.release_id, reason=args.reason)
        else:
            with duckdb.connect() as con:
                pinned = bootstrap.load(con, args.silver_root, args.release_id, allow_restricted=args.restricted)
                statements = con.extract_statements(args.query)
                if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
                    raise ValueError("bootstrap --query must contain exactly one SELECT")
                rows = con.execute(args.query).fetchall()
                result = {"release_id": pinned["descriptor"]["release_id"], "rows": rows}
        print(json.dumps(result, sort_keys=True, default=str))
        return 0
    except (ValueError, OSError, duckdb.Error) as error:
        # No source rows are printed; restricted exception messages may contain paths.
        print(json.dumps({"state": "failed", "error_type": type(error).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
