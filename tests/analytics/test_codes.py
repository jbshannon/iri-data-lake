import unittest

import duckdb

from scripts.silver import codes


class ChannelCodeTests(unittest.TestCase):
    def test_observed_codes_respect_system_year_and_keep_raw_spelling(self):
        with duckdb.connect() as con:
            con.execute("CREATE TEMP TABLE bronze_sales(source_year UTINYINT,channel VARCHAR)")
            con.execute("INSERT INTO bronze_sales VALUES (1,'drug'),(1,'UNREVIEWED')")
            con.execute("CREATE TEMP TABLE bronze_delivery_stores(source_year UTINYINT,ou VARCHAR)")
            con.execute("INSERT INTO bronze_delivery_stores VALUES (1,' gr '),(1,'XX'),(1,NULL)")
            con.execute("CREATE TEMP TABLE bronze_panel(source_year UTINYINT,outlet VARCHAR)")
            con.execute("INSERT INTO bronze_panel VALUES (1,'DR'),(8,'DR'),(8,'DK'),(12,'KK')")
            registry = codes.register(con)
            self.assertTrue(registry['silver_channel_code_v']['restricted'])
            for system, raw, year, channel, status in [
                ('sales', 'drug', 1, 'drug', 'known'),
                ('sales', 'UNREVIEWED', 1, None, 'unmatched'),
                ('roster', ' gr ', 1, 'groc', 'known'),
                ('roster', 'XX', 1, None, 'unmatched'),
                ('panel', 'DR', 1, 'drug', 'known'),
                ('panel', 'DR', 8, None, 'unmatched'),
                ('panel', 'DK', 8, 'drug', 'known'),
                ('panel', 'KK', 12, None, 'unmatched'),
            ]:
                self.assertEqual(con.execute("SELECT channel_code,mapping_status FROM silver_channel_code_v WHERE code_system=? AND raw_code=? AND source_year=?",
                                             [system, raw, year]).fetchone(), (channel, status))
            self.assertEqual(con.execute("SELECT count(*) FROM (SELECT code_system,raw_code,source_year FROM silver_channel_code_v GROUP BY ALL HAVING count(*)>1)").fetchone()[0], 0)

    def test_reference_only_registration_is_nonsensitive(self):
        with duckdb.connect() as con:
            registry = codes.register(con)
            self.assertFalse(registry['silver_channel_code_v']['restricted'])
            self.assertEqual(con.execute("SELECT channel_code,mapping_status FROM silver_channel_code_v WHERE code_system='panel' AND raw_code='KK' AND source_year=8").fetchone(), (None, 'unmatched'))
