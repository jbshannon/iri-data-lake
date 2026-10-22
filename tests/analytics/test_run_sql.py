import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

class RunSqlAssertionsTest(unittest.TestCase):
    def run_sql(self, sql, *arguments):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'gate.sql'
            path.write_text(sql)
            return subprocess.run([sys.executable, str(ROOT/'scripts/run_sql.py'), str(path), *arguments],
                                  cwd=ROOT, capture_output=True, text=True)

    def test_false_and_null_fail(self):
        for value in ('FALSE', 'NULL'):
            with self.subTest(value=value):
                result = self.run_sql(f'SELECT {value} AS gate_ok;')
                self.assertNotEqual(result.returncode, 0)

    def test_empty_assertion_result_fails(self):
        result = self.run_sql('SELECT TRUE AS gate_ok WHERE FALSE;')
        self.assertNotEqual(result.returncode, 0)

    def test_missing_assertion_fails(self):
        result = self.run_sql('SELECT 1 AS ordinary_value;')
        self.assertNotEqual(result.returncode, 0)

    def test_missing_named_assertion_fails_even_if_another_assertion_passes(self):
        result = self.run_sql('SELECT TRUE AS present_ok;', '--expect', 'missing_ok')
        self.assertNotEqual(result.returncode, 0)

    def test_integer_one_is_not_boolean_true(self):
        result = self.run_sql('SELECT 1 AS gate_ok;')
        self.assertNotEqual(result.returncode, 0)

    def test_true_passes(self):
        result = self.run_sql('SELECT TRUE AS gate_ok;')
        self.assertEqual(result.returncode, 0, result.stderr)

if __name__ == '__main__':
    unittest.main()
