import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from scripts.silver import bootstrap, bundle
from scripts.silver.releases import build_fingerprint, publish_release, stage_release


class BootstrapTests(unittest.TestCase):
    def published(self, temporary):
        root = Path(temporary) / "silver"
        fingerprint = build_fingerprint({"fixture": 1})
        stage = stage_release(root, fingerprint)
        with duckdb.connect() as con:
            con.execute("CREATE TEMP TABLE source_rows(id INTEGER, value VARCHAR)")
            con.execute("INSERT INTO source_rows VALUES (1, 'kept')")
            con.execute("CREATE TEMP VIEW silver_example_v AS SELECT * FROM source_rows")
            registry = {"silver_example_v": {"primary_key": ["id"], "schema_version": 1}}
            captured = bundle.capture_bundle(con, stage, registry, [{"check_id": "rows", "ok": True, "sql": "SELECT count(*) FROM silver_example_v", "expected": [[1]], "severity": "error"}])
        descriptor = {"fingerprint": fingerprint, "schema_version": 1, "sources": {"fixture": True},
                      "gate_results": [{"check_id": "initial", "ok": True}], **captured}
        return root, publish_release(root, stage, descriptor)

    def test_capture_publish_fresh_restore_and_replay(self):
        with tempfile.TemporaryDirectory() as temp:
            root, descriptor = self.published(temp)
            with patch.object(bootstrap, "register", side_effect=AssertionError("live registration called")):
                with duckdb.connect() as con:
                    loaded = bootstrap.load(con, root, descriptor["release_id"])
                    self.assertIn("silver_example_v", loaded["registry"])
                    self.assertEqual(con.execute("SELECT * FROM silver_example_v").fetchall(), [(1, "kept")])
                    checks = bundle.validate_bundle(con, root / "releases" / descriptor["release_id"], descriptor)
                    self.assertTrue(all(check["ok"] for check in checks))
                    con.execute("INSERT INTO source_rows VALUES (2,'wrong')")
                    with self.assertRaisesRegex(ValueError, "validation failed"):
                        bundle.validate_bundle(con, root / "releases" / descriptor["release_id"], descriptor)

    def test_restore_rejects_corrupt_artifact(self):
        with tempfile.TemporaryDirectory() as temp:
            root, descriptor = self.published(temp)
            release = root / "releases" / descriptor["release_id"]
            path = release / descriptor["bundle_path"]
            saved = json.loads(path.read_text())
            saved["views"][0]["sql"] = "DROP TABLE source_rows"
            path.write_text(json.dumps(saved))
            with duckdb.connect() as con:
                with self.assertRaisesRegex(ValueError, "checksum"):
                    bootstrap.load(con, root, descriptor["release_id"])

    def test_load_does_not_create_missing_root(self):
        with tempfile.TemporaryDirectory() as temp:
            missing = Path(temp) / "missing"
            with duckdb.connect() as con:
                with self.assertRaises(FileNotFoundError):
                    bootstrap.load(con, missing)
            self.assertFalse(missing.exists())
