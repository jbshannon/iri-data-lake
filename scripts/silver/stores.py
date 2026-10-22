"""Annual store identity and conservative roster consensus views."""
from pathlib import Path


def register(con, *, reference_root: Path | None = None) -> dict[str, dict]:
    """Register store relations over normalized ``bronze_delivery_stores``."""
    del reference_root
    if not _view_exists(con, "bronze_delivery_stores"):
        return {}
    con.execute("""
        CREATE OR REPLACE TEMP VIEW _silver_store_roster AS
        SELECT CAST(source_year AS UTINYINT) source_year,
               CAST(iri_key AS UINTEGER) iri_key,
               CAST(ou AS VARCHAR) ou, CAST(est_acv AS DOUBLE) est_acv,
               CAST(market_name AS VARCHAR) market_name,
               CAST(open_week AS USMALLINT) open_week,
               CAST(closed_week AS USMALLINT) closed_week,
               CAST(masked_name AS VARCHAR) masked_name,
               _source_file, _source_row
        FROM bronze_delivery_stores
        WHERE source_year IS NOT NULL AND iri_key IS NOT NULL
    """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW _silver_store_payload AS
        SELECT source_year, iri_key, ou, est_acv, market_name, open_week,
               closed_week, masked_name, count(*)::BIGINT source_record_count
        FROM _silver_store_roster
        GROUP BY source_year, iri_key, ou, est_acv, market_name,
                 open_week, closed_week, masked_name
    """)
    # The ID function is provided by scripts.silver.keys in an integrated build.
    con.execute("""
        CREATE OR REPLACE TEMP VIEW silver_store_observation_v AS
        SELECT silver_id(json_array('store-observation:v1', source_year, iri_key,
                    ou, est_acv, market_name, open_week, closed_week, masked_name))
                    store_observation_id,
               silver_id(json_array('store:v1', source_year, iri_key)) store_id,
               source_year, iri_key, ou, est_acv, market_name, open_week,
               closed_week, masked_name, source_record_count
        FROM _silver_store_payload
    """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW silver_store_observation_lineage_v AS
        SELECT o.store_observation_id, r._source_file, r._source_row
        FROM _silver_store_roster r
        JOIN silver_store_observation_v o
          ON r.source_year=o.source_year AND r.iri_key=o.iri_key
         AND r.ou IS NOT DISTINCT FROM o.ou AND r.est_acv IS NOT DISTINCT FROM o.est_acv
         AND r.market_name IS NOT DISTINCT FROM o.market_name
         AND r.open_week IS NOT DISTINCT FROM o.open_week
         AND r.closed_week IS NOT DISTINCT FROM o.closed_week
         AND r.masked_name IS NOT DISTINCT FROM o.masked_name
    """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW _silver_store_long AS
        SELECT store_id, source_year, iri_key, 'ou' attribute_name,
               CAST(ou AS VARCHAR) observed_value FROM silver_store_observation_v
        UNION ALL SELECT store_id, source_year, iri_key, 'est_acv', CAST(est_acv AS VARCHAR)
          FROM silver_store_observation_v
        UNION ALL SELECT store_id, source_year, iri_key, 'market_name', market_name
          FROM silver_store_observation_v
        UNION ALL SELECT store_id, source_year, iri_key, 'open_week', CAST(open_week AS VARCHAR)
          FROM silver_store_observation_v
        UNION ALL SELECT store_id, source_year, iri_key, 'closed_week', CAST(closed_week AS VARCHAR)
          FROM silver_store_observation_v
        UNION ALL SELECT store_id, source_year, iri_key, 'masked_name', masked_name
          FROM silver_store_observation_v
    """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW silver_store_attribute_status_v AS
        WITH a AS (
          SELECT store_id, attribute_name, count(DISTINCT observed_value) FILTER (WHERE observed_value IS NOT NULL)::BIGINT distinct_known_values,
                 count(*)::BIGINT observation_count,
                 count(*) FILTER (WHERE observed_value IS NULL)::BIGINT missing_count
          FROM _silver_store_long GROUP BY store_id, attribute_name
        )
        SELECT store_id, attribute_name,
          CASE WHEN distinct_known_values > 1 THEN 'conflict'
               WHEN distinct_known_values = 0 THEN 'unknown'
               WHEN missing_count > 0 THEN 'partial' ELSE 'known' END status,
          distinct_known_values, observation_count
        FROM a
    """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW _silver_store_consensus AS
        SELECT store_id, source_year, iri_key,
          count(*)::BIGINT observation_count,
          sum(source_record_count)::BIGINT source_record_count,
          CASE WHEN count(DISTINCT ou) FILTER (WHERE ou IS NOT NULL)=1
                    AND count(*) FILTER (WHERE ou IS NULL)=0
                    THEN CASE upper(trim(first(ou))) WHEN 'DR' THEN 'drug' WHEN 'GR' THEN 'groc' ELSE NULL END END channel_code,
          CASE WHEN count(DISTINCT ou) FILTER (WHERE ou IS NOT NULL)>1 THEN 'conflict'
               WHEN count(DISTINCT ou) FILTER (WHERE ou IS NOT NULL)=0 THEN 'unknown'
               WHEN count(*) FILTER (WHERE ou IS NULL)>0 THEN 'partial'
               WHEN upper(trim(min(ou))) IN ('DR','GR') THEN 'known' ELSE 'unmatched' END channel_mapping_status,
          CASE WHEN count(DISTINCT est_acv) FILTER (WHERE est_acv IS NOT NULL)=1 AND count(*) FILTER (WHERE est_acv IS NULL)=0 THEN first(est_acv) END est_acv,
          CASE WHEN count(DISTINCT open_week) FILTER (WHERE open_week IS NOT NULL)=1 AND count(*) FILTER (WHERE open_week IS NULL)=0 THEN first(open_week) END open_week,
          CASE WHEN count(DISTINCT closed_week) FILTER (WHERE closed_week IS NOT NULL)=1 AND count(*) FILTER (WHERE closed_week IS NULL)=0 THEN first(closed_week) END closed_week
        FROM silver_store_observation_v GROUP BY store_id, source_year, iri_key
    """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW silver_store_v AS
        SELECT c.store_id, c.source_year, c.iri_key, c.observation_count, c.source_record_count,
               c.channel_code, c.channel_mapping_status, c.est_acv, c.open_week, c.closed_week,
               CASE WHEN bool_or(s.status='conflict') THEN 'conflict'
                    WHEN bool_or(c.channel_mapping_status='unmatched') OR bool_or(s.status='partial') OR
                         (bool_or(s.status='known') AND bool_or(s.status='unknown')) THEN 'partial'
                    WHEN bool_or(s.status='known') THEN 'known' ELSE 'unknown' END resolution_status
        FROM _silver_store_consensus c JOIN silver_store_attribute_status_v s USING(store_id)
        GROUP BY ALL
    """)
    return {
        "silver_store_v": _meta(["store_id"]),
        "silver_store_observation_v": _meta(["store_observation_id"]),
        "silver_store_attribute_status_v": _meta(["store_id", "attribute_name"]),
        "silver_store_observation_lineage_v": _meta(["store_observation_id", "_source_file", "_source_row"]),
    }


def validate(con) -> list[dict]:
    """Return structural accounting checks for store identity and evidence."""
    if not _view_exists(con, "silver_store_v"):
        return []
    values = con.execute("""
      SELECT (SELECT count(*) FROM silver_store_v) =
             (SELECT count(DISTINCT (source_year, iri_key)) FROM _silver_store_roster),
             (SELECT coalesce(sum(source_record_count),0) FROM silver_store_observation_v) =
             (SELECT count(*) FROM _silver_store_roster),
             (SELECT count(*) FROM silver_store_v) =
             (SELECT count(*) FROM (SELECT source_year, iri_key FROM silver_store_observation_v GROUP BY 1,2)),
             (SELECT count(*) FROM (SELECT store_id FROM silver_store_v GROUP BY store_id)) =
             (SELECT count(*) FROM silver_store_v),
             NOT EXISTS(SELECT 1 FROM silver_store_v
               WHERE channel_mapping_status IS NULL
                  OR channel_mapping_status NOT IN ('known','unmatched','unknown','partial','conflict')
                  OR (channel_code IS NOT NULL) <> (channel_mapping_status='known'))
    """).fetchone()
    return [
      _check('store_annual_identity_unique', values[0], 'one per annual key', 'unique annual identities'),
      _check('store_observation_conservation', values[1], 'all roster records represented', 'record count conserved'),
      _check('store_identity_cardinality', values[2], 'one assignment per annual store', 'store cardinality'),
      _check('store_id_no_collision', values[3], 'one annual key per store id', 'store id collision check'),
      _check('store_channel_mapping_status', values[4], 'explicit supported/unresolved mapping status', 'channel mapping consistency'),
    ]


def _view_exists(con, name: str) -> bool:
    return con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name=?", [name]).fetchone()[0] > 0


def _meta(key: list[str]) -> dict:
    return {"schema_version": 1, "primary_key": key, "materialize": False, "restricted": False}


def _check(check_id: str, ok: bool, expected: str, actual: str) -> dict:
    return {"check_id": check_id, "ok": bool(ok), "severity": "error", "actual": actual, "expected": expected}
