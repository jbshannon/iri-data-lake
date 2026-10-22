import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.silver.releases import (
    build_fingerprint,
    exclusive_lock,
    load_release,
    publish_release,
    rollback_release,
    stage_release,
)


class ReleaseTests(unittest.TestCase):
    def _candidate(self, root, marker):
        fingerprint = build_fingerprint({"build": marker})
        stage = stage_release(root, fingerprint)
        output = stage / "tables" / "facts.parquet"
        output.parent.mkdir()
        output.write_bytes(marker.encode())
        descriptor = {
            "fingerprint": fingerprint,
            "schema_version": 1,
            "sources": {"snapshot": marker},
            "gate_results": {"complete": {"ok": True}},
            "outputs": [{
                "path": "tables/facts.parquet",
                "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
                "schema": {"columns": ["value"]},
            }],
        }
        return stage, descriptor

    def test_fingerprint_is_order_independent(self):
        self.assertEqual(build_fingerprint({"a": 1, "b": 2}), build_fingerprint({"b": 2, "a": 1}))

    def test_publishes_complete_release_and_rolls_back(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "silver"
            stage1, desc1 = self._candidate(root, "old")
            first = publish_release(root, stage1, desc1)
            stage2, desc2 = self._candidate(root, "new")
            second = publish_release(root, stage2, desc2)
            self.assertEqual(load_release(root)["release_id"], second["release_id"])
            rolled = rollback_release(root, first["release_id"], reason="fixture recovery")
            self.assertEqual(rolled["release_id"], first["release_id"])
            self.assertEqual(load_release(root)["release_id"], first["release_id"])
            self.assertEqual((root / "releases" / second["release_id"] / "tables/facts.parquet").read_bytes(), b"new")

    def test_invalid_gates_and_checksum_never_switch_pointer(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "silver"
            stage, descriptor = self._candidate(root, "stable")
            old = publish_release(root, stage, descriptor)
            stage, descriptor = self._candidate(root, "bad")
            descriptor["gate_results"]["complete"]["ok"] = None
            with self.assertRaises(ValueError):
                publish_release(root, stage, descriptor)
            descriptor["gate_results"]["complete"]["ok"] = True
            (stage / "tables/facts.parquet").write_bytes(b"tampered")
            with self.assertRaises(ValueError):
                publish_release(root, stage, descriptor)
            self.assertEqual(load_release(root)["release_id"], old["release_id"])

    def test_traversal_and_corrupt_persisted_file_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "silver"
            stage, descriptor = self._candidate(root, "safe")
            descriptor["outputs"][0]["path"] = "../outside"
            with self.assertRaises(ValueError):
                publish_release(root, stage, descriptor)
            stage, descriptor = self._candidate(root, "integrity")
            release = publish_release(root, stage, descriptor)
            (root / "releases" / release["release_id"] / "tables/facts.parquet").write_bytes(b"altered")
            with self.assertRaises(ValueError):
                load_release(root)

    def test_pointer_write_failure_leaves_old_release_current(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "silver"
            stage, descriptor = self._candidate(root, "old")
            old = publish_release(root, stage, descriptor)
            stage, descriptor = self._candidate(root, "new")
            import scripts.silver.releases as releases
            original = releases._write_json_atomic

            def fail_pointer(path, value):
                if path.name == "current.json":
                    raise OSError("injected pointer failure")
                original(path, value)

            with mock.patch.object(releases, "_write_json_atomic", side_effect=fail_pointer):
                with self.assertRaises(OSError):
                    publish_release(root, stage, descriptor)
            self.assertEqual(load_release(root)["release_id"], old["release_id"])

    def test_failure_after_pointer_replace_exposes_only_complete_new_release(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "silver"
            stage, descriptor = self._candidate(root, "old")
            publish_release(root, stage, descriptor)
            stage, descriptor = self._candidate(root, "new")
            import scripts.silver.releases as releases
            original = releases._write_json_atomic

            def replace_then_fail(path, value):
                original(path, value)
                if path.name == "current.json":
                    raise OSError("injected post-commit failure")

            with mock.patch.object(releases, "_write_json_atomic", side_effect=replace_then_fail):
                with self.assertRaises(OSError):
                    publish_release(root, stage, descriptor)
            current = load_release(root)
            self.assertEqual(current["sources"]["snapshot"], "new")
            self.assertEqual((root / "releases" / current["release_id"] / "tables/facts.parquet").read_bytes(), b"new")

    def test_lock_creates_managed_lock_file(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "silver"
            with exclusive_lock(root):
                self.assertTrue((root / ".silver.lock").is_file())


if __name__ == "__main__":
    unittest.main()
