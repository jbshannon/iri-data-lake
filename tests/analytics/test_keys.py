import json
import math
import unittest
from scripts.silver.keys import stable_id, register_functions


class StableIdTests(unittest.TestCase):
    def test_stable_compact_json_and_types(self):
        self.assertEqual(stable_id('["store:v1",1,2]'), stable_id('["store:v1",1,2]'))
        self.assertNotEqual(stable_id('["x",null]'), stable_id('["x",""]'))
        self.assertNotEqual(stable_id('["x",1]'), stable_id('["x",1.0]'))

    def test_float_bits(self):
        self.assertNotEqual(stable_id('["x",0.0]'), stable_id('["x",-0.0]'))

    def test_nonfinite_rejected(self):
        with self.assertRaises(ValueError):
            stable_id('["x",NaN]')

    def test_existing_sql_hash_macro_is_not_accepted_as_exact_codec(self):
        import duckdb
        with duckdb.connect() as con:
            con.execute("CREATE TEMP MACRO silver_id(payload) AS sha256(payload)")
            with self.assertRaisesRegex(ValueError, "incompatible"):
                register_functions(con)

    def test_duckdb_udf_uses_exact_codec_and_is_idempotent(self):
        import duckdb
        con=duckdb.connect()
        try:
            register_functions(con)
            register_functions(con)
            expected=stable_id('["store:v1",1,null]')
            self.assertEqual(con.execute("select silver_id(json_array('store:v1', 1, NULL))").fetchone()[0],expected)
        finally:
            con.close()


if __name__ == "__main__":
    unittest.main()
