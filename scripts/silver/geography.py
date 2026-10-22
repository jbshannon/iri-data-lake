"""Chain crosswalk and conservative market/geography relations."""
from pathlib import Path


def register(con, *, reference_root: Path | None = None) -> dict[str, dict]:
    """Register chain, county, market and annual assignment views."""
    del reference_root
    from . import stores

    if not _exists(con, "silver_store_v"):
        stores.register(con)
    registered: dict[str, dict] = {}
    if _exists(con, "bronze_chain_xref"):
        columns = {r[0].lower() for r in con.execute("DESCRIBE bronze_chain_xref").fetchall()}
        years = [y for y in range(1, 13) if f"year{y}" in columns]
        if years:
            selects = " UNION ALL ".join(
                f"SELECT CAST(chain AS UINTEGER) chain_id, CAST({y} AS UTINYINT) source_year, "
                f"CAST(year{y} AS USMALLINT) masked_chain_number, _source_file, _source_row "
                f"FROM bronze_chain_xref WHERE chain IS NOT NULL AND year{y} IS NOT NULL"
                for y in years
            )
            con.execute("CREATE OR REPLACE TEMP VIEW _silver_chain_cells AS " + selects)
            con.execute("""
              CREATE OR REPLACE TEMP VIEW silver_chain_year_xref_lineage_v AS
              SELECT DISTINCT source_year, masked_chain_number, chain_id, _source_file, _source_row
              FROM _silver_chain_cells
            """)
            con.execute("""
              CREATE OR REPLACE TEMP VIEW silver_chain_year_xref_v AS
              SELECT source_year, masked_chain_number,
                     CASE WHEN count(DISTINCT chain_id)=1 THEN min(chain_id) END chain_id,
                     CASE WHEN count(DISTINCT chain_id)>1 THEN 'ambiguous' ELSE 'known' END mapping_status,
                     count(DISTINCT chain_id)::BIGINT candidate_count
              FROM _silver_chain_cells GROUP BY source_year, masked_chain_number
            """)
            con.execute("""
              CREATE OR REPLACE TEMP VIEW silver_chain_v AS
              SELECT chain_id, min(source_year)::UTINYINT first_source_year,
                     max(source_year)::UTINYINT last_source_year,
                     count(DISTINCT source_year)::BIGINT source_year_count
              FROM _silver_chain_cells GROUP BY chain_id
            """)
            _add(registered, "silver_chain_v", ["chain_id"])
            _add(registered, "silver_chain_year_xref_v", ["source_year", "masked_chain_number"])
            _add(registered, "silver_chain_year_xref_lineage_v", ["source_year", "masked_chain_number", "chain_id", "_source_file", "_source_row"])
            if _exists(con, "silver_store_v"):
                con.execute("""
                  CREATE OR REPLACE TEMP VIEW silver_store_chain_v AS
                  WITH labels AS (
                    SELECT store_id, source_year,
                      CASE WHEN (SELECT status FROM silver_store_attribute_status_v s
                                 WHERE s.store_id=o.store_id AND s.attribute_name='masked_name')='known'
                           THEN masked_name END masked_name,
                      (SELECT status FROM silver_store_attribute_status_v s
                       WHERE s.store_id=o.store_id AND s.attribute_name='masked_name') label_status
                    FROM silver_store_observation_v o
                    QUALIFY row_number() OVER(PARTITION BY store_id ORDER BY store_observation_id)=1
                  ), parsed AS (
                    SELECT *, CASE WHEN regexp_full_match(trim(masked_name), '(?i)Chain[0-9]+')
                                   THEN try_cast(regexp_extract(trim(masked_name), '([0-9]+)$', 1) AS USMALLINT) END mask
                    FROM labels
                  ), targets AS (
                    SELECT p.store_id, p.source_year, p.label_status, p.mask,
                           coalesce(max(x.candidate_count), 0) candidate_count,
                           max(x.chain_id) chain_id,
                           max(x.mapping_status) xref_status
                    FROM parsed p LEFT JOIN silver_chain_year_xref_v x
                      ON x.source_year=p.source_year AND x.masked_chain_number=p.mask
                    GROUP BY p.store_id, p.source_year, p.label_status, p.mask
                  )
                  SELECT store_id, source_year,
                    CASE WHEN label_status='conflict' THEN NULL
                         WHEN label_status IS DISTINCT FROM 'known' OR mask IS NULL THEN NULL
                         WHEN candidate_count=1 THEN chain_id ELSE NULL END chain_id,
                    CASE WHEN label_status='conflict' THEN 'conflict'
                         WHEN label_status IS DISTINCT FROM 'known' OR mask IS NULL THEN 'unmatched'
                         WHEN candidate_count=0 THEN 'unmatched'
                         WHEN candidate_count>1 OR xref_status='ambiguous' THEN 'ambiguous' ELSE 'known' END mapping_status,
                    CAST(NULL AS VARCHAR) rule_id
                  FROM targets
                """)
                _add(registered, "silver_store_chain_v", ["store_id"])
    if _exists(con, "bronze_manual_store_entry"):
        # Parser output is retained as evidence only: its year columns and business-field
        # alignment have not been verified, so it cannot drive store assignments.
        con.execute("CREATE OR REPLACE TEMP VIEW silver_manual_store_evidence_v AS SELECT * FROM bronze_manual_store_entry")
        _add(registered, "silver_manual_store_evidence_v", ["_source_file", "_source_row"])
    if _exists(con, "bronze_fips_market"):
        con.execute("""
          CREATE OR REPLACE TEMP VIEW silver_fips_market_v AS
          SELECT CAST(fips_key AS UINTEGER) fips_key,
                 lpad(CAST(fips_key AS VARCHAR), 5, '0') fips_code,
                 name, county, state, CAST(iri_market_number AS USMALLINT) iri_market_number,
                 market_name, market_name2, region_name
          FROM bronze_fips_market
        """)
        con.execute("""
          CREATE OR REPLACE TEMP VIEW silver_market_region_v AS
          SELECT iri_market_number, region_name, count(DISTINCT fips_key)::BIGINT county_count
          FROM silver_fips_market_v
          WHERE iri_market_number IS NOT NULL AND region_name IS NOT NULL
          GROUP BY iri_market_number, region_name
        """)
        con.execute("""
          CREATE OR REPLACE TEMP VIEW _silver_market_names AS
          SELECT iri_market_number, market_name AS name FROM silver_fips_market_v WHERE iri_market_number IS NOT NULL AND market_name IS NOT NULL
          UNION ALL SELECT iri_market_number, market_name2 FROM silver_fips_market_v WHERE iri_market_number IS NOT NULL AND market_name2 IS NOT NULL
        """)
        con.execute("""
          CREATE OR REPLACE TEMP VIEW silver_market_v AS
          WITH n AS (SELECT iri_market_number, count(DISTINCT name)::BIGINT n,
                            count(DISTINCT upper(trim(name)))::BIGINT nn,
                            count(DISTINCT name)::BIGINT raw_name_count,
                            min(name) market_label FROM _silver_market_names GROUP BY iri_market_number),
               c AS (SELECT iri_market_number, count(DISTINCT fips_key)::BIGINT county_count
                     FROM silver_fips_market_v WHERE iri_market_number IS NOT NULL GROUP BY iri_market_number)
          SELECT c.iri_market_number,
                 CASE WHEN coalesce(n.raw_name_count,0)=1 THEN n.market_label END market_name,
                 CASE WHEN coalesce(n.nn,0)=0 THEN 'unknown'
                      WHEN n.raw_name_count=1 THEN 'known' ELSE 'conflict' END name_status,
                 c.county_count
          FROM c LEFT JOIN n USING(iri_market_number)
        """)
        # Include observed zero even if a future source omits counties only if it exists in FIPS;
        # no synthetic market codes are introduced.
        _add(registered, "silver_fips_market_v", ["fips_key"])
        _add(registered, "silver_market_v", ["iri_market_number"])
        _add(registered, "silver_market_region_v", ["iri_market_number", "region_name"])
    if _exists(con, "silver_store_v") and _exists(con, "silver_fips_market_v"):
        con.execute("""
          CREATE OR REPLACE TEMP VIEW silver_market_name_xref_evidence_v AS
          SELECT DISTINCT src.source_system, src.source_label, src.normalized_label,
                 src.source_year, f.iri_market_number
          FROM (
            SELECT DISTINCT 'delivery_stores' source_system, market_name source_label,
                   upper(trim(market_name)) normalized_label, source_year
            FROM silver_store_observation_v WHERE market_name IS NOT NULL
          ) src
          JOIN (SELECT iri_market_number, upper(trim(market_name)) n FROM silver_fips_market_v WHERE market_name IS NOT NULL
                UNION SELECT iri_market_number, upper(trim(market_name2)) FROM silver_fips_market_v WHERE market_name2 IS NOT NULL) f
            ON src.normalized_label=f.n
        """)
        con.execute("""
          CREATE OR REPLACE TEMP VIEW silver_market_name_xref_v AS
          WITH src AS (
            SELECT DISTINCT 'delivery_stores' source_system, market_name source_label,
                   upper(trim(market_name)) normalized_label, source_year
            FROM silver_store_observation_v WHERE market_name IS NOT NULL
          ), cand AS (
            SELECT source_system, source_label, normalized_label, source_year,
                   count(DISTINCT iri_market_number)::BIGINT candidate_count,
                   min(iri_market_number) iri_market_number
            FROM silver_market_name_xref_evidence_v
            GROUP BY source_system, source_label, normalized_label, source_year
          )
          SELECT s.source_system, s.source_label, s.normalized_label, s.source_year,
                 CASE WHEN c.candidate_count=1 THEN c.iri_market_number END iri_market_number,
                 CASE WHEN c.candidate_count IS NULL THEN 'unmatched'
                      WHEN c.candidate_count>1 THEN 'ambiguous' ELSE 'known' END mapping_status,
                 CAST(NULL AS VARCHAR) rule_id
          FROM src s LEFT JOIN cand c USING(source_system, source_label, normalized_label, source_year)
        """)
        con.execute("""
          CREATE OR REPLACE TEMP VIEW silver_store_market_v AS
          WITH labels AS (
            SELECT s.store_id, s.source_year,
              CASE WHEN a.status='known' THEN (SELECT min(market_name) FROM silver_store_observation_v o WHERE o.store_id=s.store_id) END source_label,
              a.status label_status
            FROM silver_store_v s JOIN silver_store_attribute_status_v a USING(store_id)
            WHERE a.attribute_name='market_name'
          )
          SELECT l.store_id, l.source_year,
            CASE WHEN l.label_status='known' AND max(x.mapping_status)='known' THEN max(x.iri_market_number) END iri_market_number,
            CASE WHEN l.label_status='conflict' THEN 'conflict'
                 WHEN l.label_status IS DISTINCT FROM 'known' THEN 'unmatched'
                 WHEN max(x.mapping_status) IS NULL THEN 'unmatched'
                 ELSE max(x.mapping_status) END mapping_status,
            CAST(NULL AS VARCHAR) rule_id
          FROM labels l LEFT JOIN silver_market_name_xref_v x
            ON x.source_year=l.source_year AND x.source_label=l.source_label
          GROUP BY l.store_id, l.source_year, l.label_status
        """)
        _add(registered, "silver_market_name_xref_v", ["source_system", "source_label", "source_year"])
        _add(registered, "silver_market_name_xref_evidence_v", ["source_system", "source_label", "source_year", "iri_market_number"])
        _add(registered, "silver_store_market_v", ["store_id"])
    if _exists(con, "silver_store_v") and not _exists(con, "silver_store_chain_v"):
        con.execute("""
          CREATE OR REPLACE TEMP VIEW silver_store_chain_v AS
          SELECT store_id, source_year, CAST(NULL AS UINTEGER) chain_id,
                 'unmatched' mapping_status, CAST(NULL AS VARCHAR) rule_id
          FROM silver_store_v
        """)
        _add(registered, "silver_store_chain_v", ["store_id"])
    if _exists(con, "silver_store_v") and not _exists(con, "silver_store_market_v"):
        con.execute("""
          CREATE OR REPLACE TEMP VIEW silver_store_market_v AS
          SELECT store_id, source_year, CAST(NULL AS USMALLINT) iri_market_number,
                 'unmatched' mapping_status, CAST(NULL AS VARCHAR) rule_id
          FROM silver_store_v
        """)
        _add(registered, "silver_store_market_v", ["store_id"])
    return registered


def validate(con) -> list[dict]:
    """Check unique keys and annual assignment conservation."""
    checks = []
    for view, key in (("silver_fips_market_v", "fips_key"), ("silver_market_v", "iri_market_number"), ("silver_chain_year_xref_v", "source_year, masked_chain_number")):
        if _exists(con, view):
            n, d = con.execute(f"SELECT count(*), count(DISTINCT ({key})) FROM {view}").fetchone()
            checks.append(_check(view + "_key_unique", n == d, n, d))
    for view, target in (("silver_store_chain_v", "chain_id"), ("silver_store_market_v", "iri_market_number")):
        if _exists(con, view) and _exists(con, "silver_store_v"):
            n, expected = con.execute(f"SELECT count(*), (SELECT count(*) FROM silver_store_v) FROM {view}").fetchone()
            checks.append(_check(view + "_one_per_store", n == expected, n, expected))
            invalid = con.execute(f"SELECT count(*) FROM {view} WHERE (mapping_status='known' AND {target} IS NULL) OR (mapping_status<>'known' AND {target} IS NOT NULL)").fetchone()[0]
            checks.append(_check(view + "_status_matches_value", invalid == 0, invalid, 0))
    return checks


def _exists(con, name: str) -> bool:
    return con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name=?", [name]).fetchone()[0] > 0


def _add(registry: dict, view: str, key: list[str]) -> None:
    registry[view] = {"schema_version": 1, "primary_key": key, "materialize": False, "restricted": False}


def _check(check_id: str, ok: bool, actual, expected) -> dict:
    return {"check_id": check_id, "ok": bool(ok), "severity": "error", "actual": str(actual), "expected": str(expected)}
