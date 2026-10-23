"""Synthetic actual-format release tests; never touch the connected lake."""
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from scripts.silver import bootstrap, build, bundle, inputs, products


DATASETS = ["sales", "product_stub", "week_dimension"]


def fixture(root):
    lake = root / "lake"
    records = []
    def source(dataset, relative, query, year=None, category=None, channel=None):
        target = lake / "bronze" / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        with duckdb.connect() as con:
            con.execute(f"COPY ({query}) TO {bundle._quote(str(target))} (FORMAT PARQUET)")
            count = con.execute("SELECT count(*) FROM read_parquet(?)", [str(target)]).fetchone()[0]
        records.append({"dataset": dataset, "source_path": str(root / "raw" / f"{dataset}.fixture"),
                        "source_sha256": hashlib.sha256(dataset.encode()).hexdigest(), "parser_version": "fixture:v1",
                        "output_schema_version": 1, "written_rows": count, "source_year": year,
                        "category": category, "channel": channel, "status": "success", "output_paths": [str(target)]})
    source("sales", "iri_sales/year=1/category=beer/channel=drug/part.parquet", """
        SELECT 42::UINTEGER iri_key,1114::USMALLINT AS week,1::UTINYINT sy,0::UTINYINT ge,
          123::UINTEGER vend,45::UINTEGER item,2::INTEGER units,150::BIGINT dollars_cents,
          0::UTINYINT feature_code,0::UTINYINT display,false price_reduction
        UNION ALL SELECT 42,1114,1,0,123,45,3,250,0,0,false
    """, 1, "beer", "drug")
    source("product_stub", "iri_product_stub/edition=2001-2006/part.parquet", """
        SELECT '01-00-00123-00045'::VARCHAR UPC,1::UTINYINT SY,0::UTINYINT GE,
          123::UINTEGER VEND,45::UINTEGER ITEM,2::USMALLINT AS Level,'fixture product'::VARCHAR L9,
          'MISSING'::VARCHAR FLAVOR_SCENT
    """)
    source("week_dimension", "iri_week_dimension/part.parquet", f"""
        SELECT (1114+i)::USMALLINT iri_week,
          (DATE '2001-01-01' + (i*7)::INTEGER)::DATE week_start,
          (DATE '2001-01-07' + (i*7)::INTEGER)::DATE week_end,
          ({products._year_sql('1114+i')})::UTINYINT source_year FROM range(626) t(i)
    """)
    # Sensitive marker must never appear in public snapshot metadata or dependencies.
    source("panel", "iri_panel/year=1/part.parquet", """SELECT 918273::UINTEGER panid,1114::USMALLINT AS week,1234::USMALLINT AS minute,
        'DR'::VARCHAR outlet,42::UINTEGER iri_key,'123'::VARCHAR colupc,
        1.5::DOUBLE units,15::BIGINT units_digits,1::UTINYINT units_scale,
        199::BIGINT dollars_cents,199::BIGINT dollars_digits,2::UTINYINT dollars_scale,
        'PRIVATE_MARKER'::VARCHAR AS private_marker""", 1)
    (lake / "metadata").mkdir(parents=True)
    (lake / "metadata" / "manifest.jsonl").write_text("".join(json.dumps(row) + "\n" for row in records))
    return lake


class ReleaseIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.lake = fixture(self.root)
        self.silver = self.root / "silver"

    def tearDown(self):
        self.temp.cleanup()

    def run_build(self, **kwargs):
        return build.build(self.lake, self.silver, datasets=DATASETS, quiescent=True, **kwargs)

    def test_plan_is_nonmutating_and_scope_is_explicit(self):
        before = {str(p): p.stat().st_mtime_ns for p in self.root.rglob("*")}
        result = build.plan(self.lake, datasets=DATASETS)
        after = {str(p): p.stat().st_mtime_ns for p in self.root.rglob("*")}
        self.assertEqual(before, after)
        self.assertEqual(result["scope"], "partial")
        self.assertFalse(self.silver.exists())
        with self.assertRaisesRegex(ValueError, "corpus"):
            build.plan(self.lake, datasets=DATASETS, scope="corpus")

    def test_stage_publish_pinned_bootstrap_and_revalidate_after_original_deletion(self):
        staged = self.run_build()
        self.assertEqual(staged["state"], "ready")
        self.assertFalse((self.silver / "current.json").exists())
        published = build.publish_staged(self.silver, Path(staged["staging"]))
        shutil.rmtree(self.lake)
        with patch.object(bootstrap, "register", side_effect=AssertionError("mutable registration")), patch.object(products, "validate", side_effect=AssertionError("mutable validation")):
            with duckdb.connect() as con:
                loaded = bootstrap.load(con, self.silver)
                self.assertEqual(loaded["descriptor"]["release_id"], published["release_id"])
                self.assertEqual(con.execute("SELECT upc,units,revenue_cents,sales_rows FROM silver_product_week_v").fetchall(), [("01-00-00123-00045", 5, 400, 2)])
                self.assertEqual(con.execute("SELECT count(*) FROM silver_week_dimension_v").fetchone()[0], 626)
            self.assertEqual(build.validate_release(self.silver)["state"], "validated")
        release = self.silver / "releases" / published["release_id"]
        metadata = (release / "inputs.json").read_text()
        self.assertNotIn("PRIVATE_MARKER", metadata)
        self.assertNotIn('"dataset":"panel"', metadata)
        self.assertFalse(any("iri_panel" in str(p) for p in release.rglob("*")))

    def test_skip_validation_is_distinct_unpublishable_and_integrity_checked(self):
        with (patch.object(build.validation, "validate", side_effect=AssertionError("semantic validation ran")),
              patch.object(build, "_replay", side_effect=AssertionError("replay validation ran"))):
            staged = self.run_build(skip_validation=True)
        self.assertEqual(staged["state"], "unvalidated")
        self.assertEqual(staged["validation_status"], "skipped")
        stage = Path(staged["staging"])
        self.assertIn("unvalidated", stage.parts)
        self.assertEqual(stage.parent.name, "unvalidated")
        descriptor = json.loads((stage / "release.json").read_text())
        self.assertEqual(descriptor["state"], "unvalidated")
        self.assertEqual(descriptor["validation_status"], "skipped")
        self.assertNotIn("gate_results", descriptor)
        self.assertNotIn("bundle_path", descriptor)
        self.assertEqual(json.loads((stage / "validation.json").read_text()), {"status": "skipped"})
        for info in descriptor["outputs"].values():
            self.assertEqual(hashlib.sha256((stage / info["path"]).read_bytes()).hexdigest(), info["sha256"])
        with self.assertRaisesRegex(ValueError, "unvalidated"):
            build.publish_staged(self.silver, stage)
        with self.assertRaisesRegex(ValueError, "unvalidated"):
            from scripts.silver.releases import publish_release
            publish_release(self.silver, stage, descriptor)
        self.assertFalse((self.silver / "current.json").exists())

    def test_skip_validation_publication_is_rejected_and_cli_option_is_exposed(self):
        with self.assertRaisesRegex(ValueError, "cannot be combined"):
            self.run_build(skip_validation=True, publish=True)
        from scripts.silver.__main__ import main
        with patch.object(build, "build", return_value={"state": "unvalidated"}) as mocked:
            self.assertEqual(main(["build", "--lake", str(self.lake), "--silver-root", str(self.silver),
                                   "--quiescent", "--skip-validation"]), 0)
        self.assertTrue(mocked.call_args.kwargs["skip_validation"])
        self.assertEqual(main(["build", "--lake", str(self.lake), "--silver-root", str(self.silver),
                               "--quiescent", "--skip-validation", "--publish"]), 1)

    def test_noop_skips_registration_and_ignores_unrelated_sensitive_append(self):
        published = self.run_build(publish=True)
        manifest = self.lake / "metadata" / "manifest.jsonl"
        rows = [json.loads(row) for row in manifest.read_text().splitlines()]
        with manifest.open("a") as stream:
            stream.write(json.dumps({**rows[-1], "status": "failed", "run_id": "unrelated-private-attempt"}) + "\n")
        with patch.object(build, "register", side_effect=AssertionError("aggregate rebuilt")):
            result = self.run_build(publish=True)
        self.assertEqual(result["state"], "no-op")
        self.assertEqual(result["release_id"], published["release_id"])

    def test_corruption_fails_noop_and_validation_without_switching_pointer(self):
        published = self.run_build(publish=True)
        pointer = (self.silver / "current.json").read_bytes()
        output = self.silver / "releases" / published["release_id"] / published["outputs"]["silver_product_week_v"]["path"]
        output.write_bytes(b"not parquet")
        with self.assertRaises(ValueError):
            build.validate_release(self.silver)
        with self.assertRaises(ValueError):
            self.run_build(publish=True)
        self.assertEqual(pointer, (self.silver / "current.json").read_bytes())

    def test_definition_change_creates_new_release_old_session_remains_pinned(self):
        first = self.run_build(publish=True)
        with duckdb.connect() as con:
            bootstrap.load(con, self.silver)
            definitions = build._module_hashes()
            with patch.object(build, "_module_hashes", return_value={**definitions, "fixture-change": "v2"}):
                second = self.run_build(publish=True)
            self.assertNotEqual(first["release_id"], second["release_id"])
            self.assertEqual(con.execute("SELECT sum(revenue_cents) FROM silver_product_week_v").fetchone()[0], 400)
        build.rollback(self.silver, first["release_id"], reason="fixture rollback")
        self.assertEqual(json.loads((self.silver / "current.json").read_text())["release_id"], first["release_id"])

    def test_references_are_pinned_and_fingerprinted(self):
        refs = self.root / "refs"
        refs.mkdir()
        path = refs / "stub_categories.csv"
        path.write_text("edition,source_workbook,category\n2001-2006,part.parquet,beer\n")
        first = self.run_build(publish=True, reference_root=refs)
        path.write_text("edition,source_workbook,category\n2001-2006,part.parquet,new-category\n")
        second = self.run_build(publish=True, reference_root=refs)
        self.assertNotEqual(first["fingerprint"], second["fingerprint"])
        shutil.rmtree(refs)
        with duckdb.connect() as con:
            bootstrap.load(con, self.silver, first["release_id"])
            self.assertEqual(con.execute("SELECT DISTINCT category FROM silver_stub_category_xref_v").fetchall(), [("beer",)])

    def test_unsupported_nested_reference_is_rejected_not_silently_ignored(self):
        refs = self.root / "refs"
        nested = refs / "nested"
        nested.mkdir(parents=True)
        (nested / "stub_categories.csv").write_text("edition,source_workbook,category\n")
        with self.assertRaisesRegex(ValueError, "unsupported reference"):
            self.run_build(reference_root=refs)
        self.assertFalse(self.silver.exists())

    def test_failed_gate_preserves_pointer_and_cleans_staging(self):
        self.run_build(publish=True)
        pointer = (self.silver / "current.json").read_bytes()
        with patch.object(build, "_module_hashes", return_value={"changed": "fixture"}), patch.object(build.validation, "validate", side_effect=ValueError("fixture failure")):
            with self.assertRaises(ValueError):
                self.run_build(publish=True)
        self.assertEqual(pointer, (self.silver / "current.json").read_bytes())
        self.assertFalse(list((self.silver / ".staging").iterdir()))

    def test_input_change_before_retention_is_rejected(self):
        snapshot = inputs.select_inputs(self.lake, required_datasets=DATASETS)
        Path(snapshot["files"]["sales"][0]).write_bytes(b"modified after selection")
        with self.assertRaisesRegex(ValueError, "changed after selection"):
            inputs.retain_inputs(snapshot, self.root / "retained")

    def test_restricted_selection_and_unapproved_household_fail_before_writes(self):
        with self.assertRaisesRegex(ValueError, "restricted"):
            build.build(self.lake, self.silver, datasets=["panel"], quiescent=True)
        with self.assertRaisesRegex(ValueError, "approval"):
            build.build(self.lake, self.silver, household=True, datasets=["panel"], quiescent=True)
        self.assertFalse(self.silver.exists())

    def test_restricted_build_uses_private_root_copies_and_explicit_bootstrap(self):
        private = self.root / "restricted"
        published = build.build(self.lake, self.silver, datasets=["panel"], quiescent=True,
                                household=True, restricted_root=private, approval="synthetic-policy", publish=True)
        self.assertFalse(self.silver.exists())
        self.assertTrue(published["restricted"])
        for path in [private, *private.rglob("*")]:
            self.assertEqual(path.stat().st_mode & 0o077, 0, str(path))
        original = next((self.lake / "bronze" / "iri_panel").rglob("*.parquet"))
        retained = next((private / "releases").rglob("inputs/panel/*.parquet"))
        self.assertNotEqual(original.stat().st_ino, retained.stat().st_ino)
        with duckdb.connect() as con:
            with self.assertRaisesRegex(ValueError, "restricted"):
                bootstrap.load(con, private)
        with duckdb.connect() as con:
            bootstrap.load(con, private, allow_restricted=True)
            self.assertEqual(con.execute("SELECT count(*) FROM silver_household_v").fetchone()[0], 1)
            self.assertEqual(con.execute("SELECT units_digits,units_scale FROM silver_panel_purchase_v").fetchall(), [(15, 1)])
        self.assertEqual(build.validate_release(private)["state"], "validated")

    def test_restricted_bootstrap_rejects_spill_symlink_without_chmod_external_target(self):
        private = self.root / "restricted"
        build.build(self.lake, self.silver, datasets=["panel"], quiescent=True,
                    household=True, restricted_root=private, approval="synthetic-policy", publish=True)
        outside = self.root / "outside"
        outside.mkdir(mode=0o755)
        mode = outside.stat().st_mode
        (private / ".private-spill").symlink_to(outside, target_is_directory=True)
        with duckdb.connect() as con:
            with self.assertRaisesRegex(ValueError, "symlink"):
                bootstrap.load(con, private, allow_restricted=True)
        self.assertEqual(outside.stat().st_mode, mode)
        self.assertEqual(list(outside.iterdir()), [])

    def test_restricted_root_cannot_be_ancestor_of_public_or_lake(self):
        with self.assertRaisesRegex(ValueError, "separate"):
            build.build(self.lake, self.silver, datasets=["panel"], quiescent=True,
                        household=True, restricted_root=self.root, approval="synthetic-policy")
        self.assertFalse(self.silver.exists())

    def test_cli_build_bootstrap_validate_and_static_gate7(self):
        command = [sys.executable, "-m", "scripts.silver", "build", "--lake", str(self.lake),
                   "--silver-root", str(self.silver), "--quiescent", "--publish"]
        for dataset in DATASETS:
            command += ["--dataset", dataset]
        result = subprocess.run(command, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        published = json.loads(result.stdout)
        self.assertEqual(published["state"], "published")
        for operation in ("bootstrap", "validate"):
            command = [sys.executable, "-m", "scripts.silver", operation, "--silver-root", str(self.silver)]
            if operation == "bootstrap":
                command += ["--query", "SELECT count(*) FROM silver_product_week_v"]
            result = subprocess.run(command, text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        result = subprocess.run([sys.executable, "scripts/run_sql.py", "sql/gate7_silver.sql",
                                 "--silver-root", str(self.silver), "--expect", "annual_conservation_ok",
                                 "--expect", "global_conservation_ok"], text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        result = subprocess.run(["make", "-n", "silver-plan", "silver-build", "silver-publish", "gate7",
                                 "silver-rollback", "silver-test", "STAGING=fixture", "RELEASE_ID=fixture", "REASON=fixture"],
                                text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_cli_plan_and_missing_root_validation(self):
        command = [sys.executable, "-m", "scripts.silver", "plan", "--lake", str(self.lake)]
        for dataset in DATASETS:
            command += ["--dataset", dataset]
        result = subprocess.run(command, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(json.loads(result.stdout)["scope"], "partial")
        result = subprocess.run([sys.executable, "-m", "scripts.silver", "validate", "--silver-root", str(self.silver)], text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.silver.exists())
