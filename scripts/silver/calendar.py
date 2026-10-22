"""Captured IRI calendar relations."""
from pathlib import Path


def _has(con, name: str) -> bool:
    return bool(con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name=?", [name]).fetchone()[0])


def _calendar_checks(con) -> dict:
    return con.execute("""
      WITH c AS (SELECT * FROM silver_week_dimension_v),
      w AS (SELECT *,lag(week) OVER(ORDER BY week) pw,
                       lag(calendar_start_date) OVER(ORDER BY week) ps FROM c)
      SELECT count(*) n, count(DISTINCT week) unique_weeks,min(week) min_week,max(week) max_week,
        count(*) FILTER(WHERE week IS NULL OR calendar_start_date IS NULL OR calendar_end_date IS NULL) null_count,
        count(*) FILTER(WHERE calendar_start_date IS NOT NULL AND calendar_end_date IS NOT NULL
          AND (date_diff('day',calendar_start_date,calendar_end_date)<>6 OR dayofweek(calendar_start_date)<>1 OR dayofweek(calendar_end_date)<>0)) bad_endpoints,
        count(*) FILTER(WHERE pw IS NOT NULL AND (week<>pw+1 OR date_diff('day',ps,calendar_start_date)<>7)) bad_steps
      FROM w
    """).fetchone()


def register(con, *, reference_root: Path | None = None) -> dict[str, dict]:
    del reference_root
    if not _has(con,'bronze_week_dimension'):
        return {}
    cols={r[0].lower():r[0] for r in con.execute('DESCRIBE bronze_week_dimension').fetchall()}
    if not all(x in cols for x in ('iri_week','week_start','week_end')):
        raise ValueError('bronze_week_dimension missing required calendar fields')
    con.execute(f'''CREATE OR REPLACE TEMP VIEW silver_week_dimension_v AS
      SELECT CAST("{cols['iri_week']}" AS USMALLINT) AS "week",
             CAST("{cols['week_start']}" AS DATE) calendar_start_date,
             CAST("{cols['week_end']}" AS DATE) calendar_end_date FROM bronze_week_dimension''')
    failed=[x for x in validate(con) if not x['ok']]
    if failed:
        raise ValueError(f'calendar integrity check failed: {failed}')
    if 'source_year' in cols:
        bad=con.execute(f'''SELECT count(*) FROM bronze_week_dimension
          WHERE "{cols['source_year']}" IS NOT NULL AND "{cols['source_year']}" <> CASE
            WHEN "{cols['iri_week']}" BETWEEN 1114 AND 1165 THEN 1 WHEN "{cols['iri_week']}" BETWEEN 1166 AND 1217 THEN 2
            WHEN "{cols['iri_week']}" BETWEEN 1218 AND 1269 THEN 3 WHEN "{cols['iri_week']}" BETWEEN 1270 AND 1321 THEN 4
            WHEN "{cols['iri_week']}" BETWEEN 1322 AND 1373 THEN 5 WHEN "{cols['iri_week']}" BETWEEN 1374 AND 1426 THEN 6
            WHEN "{cols['iri_week']}" BETWEEN 1427 AND 1478 THEN 7 WHEN "{cols['iri_week']}" BETWEEN 1479 AND 1530 THEN 8
            WHEN "{cols['iri_week']}" BETWEEN 1531 AND 1582 THEN 9 WHEN "{cols['iri_week']}" BETWEEN 1583 AND 1634 THEN 10
            WHEN "{cols['iri_week']}" BETWEEN 1635 AND 1686 THEN 11 WHEN "{cols['iri_week']}" BETWEEN 1687 AND 1739 THEN 12 END''').fetchone()[0]
        if bad: raise ValueError('calendar captured source_year disagrees with week-year ranges')
    return {'silver_week_dimension_v':{'schema_version':1,'primary_key':['week'],'materialize':False,'restricted':False}}


def validate(con) -> list[dict]:
    if not _has(con,'silver_week_dimension_v'):
        return []
    n,unique,lo,hi,null_count,endpoints,steps=_calendar_checks(con)
    valid=(n==626 and unique==626 and lo==1114 and hi==1739 and null_count==0 and endpoints==0 and steps==0)
    return [{'check_id':'calendar_full_coverage_and_dates','ok':valid,'severity':'error',
             'actual':{'rows':n,'unique_weeks':unique,'min_week':lo,'max_week':hi,'nulls':null_count,'bad_endpoints':endpoints,'bad_steps':steps},
             'expected':{'rows':626,'unique_weeks':626,'range':[1114,1739],'nulls':0,'bad_endpoints':0,'bad_steps':0}}]
