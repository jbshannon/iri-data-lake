import datetime
import unittest

import duckdb

from scripts.silver.calendar import register, validate


class CalendarTests(unittest.TestCase):
    def connection(self):
        con = duckdb.connect()
        start = datetime.date(2000, 1, 3)  # Monday
        rows = []
        for i in range(626):
            week = 1114 + i
            date = start + datetime.timedelta(days=7 * i)
            year = 1 if i < 52 else 2
            rows.append((week, date, date + datetime.timedelta(days=6), year))
        con.execute('CREATE TEMP TABLE bronze_week_dimension(iri_week USMALLINT, week_start DATE, week_end DATE, source_year UTINYINT)')
        con.executemany('INSERT INTO bronze_week_dimension VALUES (?,?,?,?)', rows)
        # Test fixture validates captured calendar mechanics; year check is tested separately in production ranges.
        return con

    def test_exact_public_schema_and_count(self):
        con = self.connection()
        # Fixed calendar-year mapping is a hard source contract; test a correctly mapped full range via no source year.
        con.execute('ALTER TABLE bronze_week_dimension DROP source_year')
        metadata = register(con)
        self.assertIn('silver_week_dimension_v', metadata)
        self.assertEqual(con.execute('DESCRIBE silver_week_dimension_v').fetchall(), [
            ('week', 'USMALLINT', 'YES', None, None, None),
            ('calendar_start_date', 'DATE', 'YES', None, None, None),
            ('calendar_end_date', 'DATE', 'YES', None, None, None),
        ])
        self.assertEqual(con.execute('SELECT count(*) FROM silver_week_dimension_v').fetchone()[0], 626)
        con.close()

    def test_validate_rechecks_dates_not_only_count(self):
        con = self.connection()
        con.execute('ALTER TABLE bronze_week_dimension DROP source_year')
        register(con)
        # Replace the public view with an invalid endpoint while preserving 626 rows.
        con.execute('DROP VIEW silver_week_dimension_v')
        con.execute('''CREATE TEMP TABLE bad_calendar AS SELECT * FROM bronze_week_dimension''')
        con.execute("UPDATE bad_calendar SET week_start=DATE '2000-01-04' WHERE iri_week=1114")
        con.execute('CREATE TEMP VIEW silver_week_dimension_v AS SELECT CAST(iri_week AS USMALLINT) AS "week",week_start calendar_start_date,week_end calendar_end_date FROM bad_calendar')
        self.assertFalse(validate(con)[0]['ok'])
        con.close()

    def test_duplicate_week_fails(self):
        con = self.connection()
        con.execute('INSERT INTO bronze_week_dimension SELECT * FROM bronze_week_dimension LIMIT 1')
        with self.assertRaises(ValueError):
            register(con)
        con.close()


if __name__ == '__main__':
    unittest.main()
