"""Opt-in, restricted Silver relations for household-panel source records.

Call :func:`register` only from an explicitly approved restricted build. This
module has no import-time registration or source discovery behavior.
"""
from __future__ import annotations

from pathlib import Path


def _views(con) -> set[str]:
    return {r[0] for r in con.execute("SELECT table_name FROM information_schema.views").fetchall()}


def _columns(con, view: str) -> list[str]:
    return [r[0] for r in con.execute(f'DESCRIBE "{view}"').fetchall()]


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _expr(columns: list[str], name: str, fallback: str = "NULL") -> str:
    return _q(name) if name in columns else fallback


def _validate_source_columns(con, view: str, required: set[str]) -> list[str]:
    columns = _columns(con, view)
    missing = required - set(columns)
    if missing:
        raise ValueError(f"{view} is missing required columns: {', '.join(sorted(missing))}")
    return columns


def _panelist_column(cols: list[str]) -> str | None:
    """Return the source panelist identifier, including the physical quoted header."""
    return next((c for c in cols if c.lower() in {"panid", "panelist id"}), None)


def _household_views(con, sources: list[tuple[str, str]]) -> None:
    """Build identities/years from all available household source evidence."""
    pieces = []
    for view, present in sources:
        if not present:
            continue
        cols = _columns(con, view)
        panelist = _panelist_column(cols)
        year_expr = '"source_year"' if "source_year" in cols else (_q("Year") if "Year" in cols else None)
        if panelist is None or year_expr is None:
            continue
        pieces.append(f"SELECT {_q(panelist)} AS panid, TRY_CAST({year_expr} AS UTINYINT) AS source_year, '{view}' AS evidence_source FROM {view} WHERE {_q(panelist)} IS NOT NULL AND TRY_CAST({year_expr} AS UTINYINT) BETWEEN 1 AND 12")
    union = " UNION ALL ".join(pieces) if pieces else "SELECT CAST(NULL AS UINTEGER) panid, CAST(NULL AS UTINYINT) source_year, CAST(NULL AS VARCHAR) evidence_source WHERE FALSE"
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW silver_household_v AS
        SELECT silver_id(json_array('household:v1', panid)::VARCHAR) household_id,
               panid, min(source_year)::UTINYINT first_observed_year,
               max(source_year)::UTINYINT last_observed_year,
               count(DISTINCT source_year)::BIGINT observed_year_count
        FROM ({union}) household_evidence GROUP BY panid
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW silver_household_year_v AS
        SELECT silver_id(json_array('household-year:v1', source_year, panid)::VARCHAR) household_year_id,
               panid, source_year::UTINYINT source_year,
               bool_or(evidence_source='bronze_panel') has_purchase,
               bool_or(evidence_source='bronze_panel_trips') has_trip,
               bool_or(evidence_source='bronze_panelist_demos') has_demo,
               bool_or(evidence_source='bronze_panel_static') has_static
        FROM ({union}) household_evidence GROUP BY panid, source_year
    """)


def _demographic_relations(con, source: str, target: str, ad_population: bool = False) -> None:
    cols = _validate_source_columns(con, source, {"_source_file", "_source_row"})
    year_expr = '"source_year"' if "source_year" in cols else (_q("Year") if "Year" in cols else None)
    if year_expr is None:
        raise ValueError(f"{source} is missing source_year/Year")
    panelist = _panelist_column(cols)
    if panelist is None:
        raise ValueError(f"{source} is missing Panelist ID/panid")
    # The advertising population is separate, but its source identifier is still
    # retained as an ads-namespace identifier rather than discarded.
    excluded = {panelist, "panid", "source_year", "Year", "_source_file", "_source_row", "_source_id"}
    excluded = {name.lower() for name in excluded}
    attributes = [c for c in cols if c.lower() not in excluded]
    # Keep schema identity in the payload; differently named source codes are not decoded
    # or silently conflated. JSON values preserve each original column independently.
    payload_fields = [f"{_q(c)} := {_q(c)}" for c in attributes]
    payload = "to_json(struct_pack(" + ", ".join(payload_fields) + "))" if payload_fields else "'{}'::JSON"
    pk = _q(panelist)
    entity = "ads-household:v1" if ad_population else "household:v1"
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW {target} AS
        WITH raw AS (
            SELECT {pk} panid, TRY_CAST({year_expr} AS UTINYINT) source_year, {payload}::JSON source_payload,
                   _source_file source_file, _source_row source_row
            FROM {source}
        ), distinct_payload AS (
            SELECT panid, source_year, source_payload, count(*)::BIGINT source_record_count
            FROM raw GROUP BY panid, source_year, source_payload
        )
        SELECT silver_id(json_array('{target}:v1', source_year, panid, source_payload)::VARCHAR) observation_id,
               panid, source_year, source_payload, source_record_count
        FROM distinct_payload
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW {target}_lineage_v AS
        SELECT silver_id(json_array('{target}:v1', TRY_CAST({year_expr} AS UTINYINT), {pk}, {payload})::VARCHAR) observation_id,
               {pk} panid, TRY_CAST({year_expr} AS UTINYINT) source_year, _source_file source_file, _source_row source_row
        FROM {source}
    """)
    if ad_population:
        return
    # Attribute-level consensus is useful without interpreting source code meanings.
    if attributes:
        vals = ", ".join(f"('{c.replace(chr(39), chr(39) * 2)}', CAST({_q(c)} AS VARCHAR))" for c in attributes)
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW silver_household_attribute_v AS
            WITH evidence AS (
                SELECT {_q(panelist)} AS panid, TRY_CAST({year_expr} AS UTINYINT) AS source_year, attr.attribute_name, attr.raw_value
                FROM {source}, LATERAL (VALUES {vals}) attr(attribute_name, raw_value)
                WHERE {_q(panelist)} IS NOT NULL AND TRY_CAST({year_expr} AS UTINYINT) BETWEEN 1 AND 12
            ), grouped AS (
                SELECT panid, source_year, attribute_name,
                       count(DISTINCT raw_value) FILTER (WHERE raw_value IS NOT NULL) known_count,
                       count(*) FILTER (WHERE raw_value IS NOT NULL) present_count,
                       count(*) observation_count,
                       min(raw_value) FILTER (WHERE raw_value IS NOT NULL) sole_value
                FROM evidence GROUP BY panid, source_year, attribute_name
            )
            SELECT silver_id(json_array('household-attribute:v1', panid, source_year, attribute_name)::VARCHAR) household_attribute_id,
                   silver_id(json_array('household-year:v1', source_year, panid)::VARCHAR) household_year_id,
                   panid, source_year, attribute_name,
                   CASE WHEN known_count > 1 THEN NULL
                        WHEN known_count = 1 AND present_count = observation_count THEN sole_value
                        ELSE NULL END AS value,
                   CASE WHEN known_count > 1 THEN 'conflict'
                        WHEN known_count = 1 AND present_count < observation_count THEN 'partial'
                        WHEN known_count = 1 THEN 'known' ELSE 'unknown' END AS status,
                   observation_count::BIGINT AS evidence_count
            FROM grouped
        """)


def register(con, *, reference_root: Path | None = None) -> dict[str, dict]:
    """Register restricted household views for available ``bronze_*`` inputs.

    This function must only be called by an approved restricted build. Unsupported
    semantic links (COLUPC-to-UPC and purchase-to-trip) deliberately stay unresolved.
    """
    available = _views(con)
    mappings: dict[str, dict] = {}
    relation_sources = [
        ("bronze_panel", "silver_panel_purchase_v"),
        ("bronze_panel_trips", "silver_panel_trip_record_v"),
        ("bronze_panelist_demos", "silver_household_demographic_observation_v"),
        ("bronze_panel_static", "silver_panelist_static_v"),
        ("bronze_ads_demos", "silver_ads_demographic_observation_v"),
    ]
    for src, _ in relation_sources:
        if src in available:
            cols = _validate_source_columns(con, src, {"_source_file", "_source_row"})
            if "source_year" not in cols and "Year" not in cols:
                raise ValueError(f"{src} is missing source_year/Year")

    if "bronze_panel" in available:
        cols = _validate_source_columns(con, "bronze_panel", {"_source_file", "_source_row", "source_year", "panid", "week", "minute", "outlet", "iri_key", "colupc", "units", "units_digits", "units_scale", "dollars_cents", "dollars_digits", "dollars_scale"})
        panid = _panelist_column(cols)
        locator = "_source_id" if "_source_id" in cols else "_source_file"
        roster_match = "EXISTS (SELECT 1 FROM bronze_delivery_stores r WHERE r.source_year = _household_purchase_source.source_year AND r.iri_key = _household_purchase_source.iri_key)" if "bronze_delivery_stores" in available else "FALSE"
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW _household_purchase_source AS
            SELECT silver_id(json_array('panel_purchase:v1', {_q(panid)}, source_year, week, minute, outlet, {_expr(cols, 'category')}, iri_key, colupc, units, units_digits, units_scale, dollars_cents, dollars_digits, dollars_scale, {locator}, _source_row)::VARCHAR) purchase_record_id,
                   {_q(panid)} AS panid, source_year, week, minute, outlet, {_expr(cols, 'category')} AS category, iri_key, colupc, units, units_digits,
                   units_scale, dollars_cents, dollars_digits, dollars_scale,
                   _source_file source_file, _source_row source_row FROM bronze_panel
        """)
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW silver_panel_purchase_v AS
            SELECT purchase_record_id,
                   CASE WHEN panid IS NOT NULL THEN silver_id(json_array('household:v1', panid)::VARCHAR) END household_id,
                   CASE WHEN panid IS NOT NULL AND source_year BETWEEN 1 AND 12 THEN silver_id(json_array('household-year:v1', source_year, panid)::VARCHAR) END household_year_id,
                   panid, source_year::UTINYINT AS source_year, week::USMALLINT AS week, minute AS minute_raw,
                   category, outlet outlet_raw, NULL::VARCHAR channel_code,
                   CASE WHEN outlet IS NULL THEN 'unknown' ELSE 'unresolved' END channel_mapping_status,
                   iri_key, CASE WHEN iri_key IS NOT NULL AND {roster_match} THEN silver_id(json_array('store:v1', source_year, iri_key)::VARCHAR) END store_id,
                   CASE WHEN iri_key IS NULL THEN 'missing_key' WHEN {roster_match} THEN 'matched' ELSE 'unmatched' END store_mapping_status,
                   colupc colupc_raw, NULL::VARCHAR upc,
                   CASE WHEN colupc IS NULL THEN 'missing_key' ELSE 'unresolved' END product_mapping_status,
                   units, units_digits, units_scale, dollars_cents dollars_cents_rounded,
                   dollars_digits, dollars_scale, source_file, source_row,
                   CASE WHEN panid IS NULL THEN 'missing_household_key' ELSE 'retained' END record_status
            FROM _household_purchase_source
        """)
        mappings["silver_panel_purchase_v"] = {"schema_version": 1, "primary_key": ["purchase_record_id"], "materialize": False, "restricted": True}
        # One unresolved row for each purchase; never fabricate a trip association.
        con.execute("CREATE OR REPLACE TEMP VIEW silver_purchase_trip_link_v AS SELECT purchase_record_id, NULL::VARCHAR trip_record_id, 'unresolved'::VARCHAR link_status, NULL::BIGINT candidate_count, NULL::VARCHAR rule_id FROM silver_panel_purchase_v")
        mappings["silver_purchase_trip_link_v"] = {"schema_version": 1, "primary_key": ["purchase_record_id"], "materialize": False, "restricted": True}

    if "bronze_panel_trips" in available:
        src = "bronze_panel_trips"
        _validate_source_columns(con, src, {"_source_file", "_source_row", "source_year"})
        trip_locator = "_source_id" if "_source_id" in _columns(con, src) else "_source_file"
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW silver_panel_trip_record_v AS
            SELECT silver_id(json_array('panel_trip:v1', panid, source_year, week, iri_key, iri_key2, minute, cents998_digits, cents998_scale, cents999_digits, cents999_scale, kryscents_digits, kryscents_scale, {trip_locator}, _source_row)::VARCHAR) trip_record_id,
                   panid, CASE WHEN panid IS NOT NULL THEN silver_id(json_array('household:v1', panid)::VARCHAR) END household_id,
                   CASE WHEN panid IS NOT NULL AND source_year BETWEEN 1 AND 12 THEN silver_id(json_array('household-year:v1', source_year, panid)::VARCHAR) END household_year_id,
                   source_year, week, iri_key, iri_key2,
                   minute minute_raw, cents998_digits, cents998_scale, cents999_digits, cents999_scale,
                   kryscents_digits, kryscents_scale, _source_file source_file, _source_row source_row,
                   CASE WHEN panid IS NULL THEN 'missing_household_key' ELSE 'retained' END record_status
            FROM bronze_panel_trips
        """)
        mappings["silver_panel_trip_record_v"] = {"schema_version": 1, "primary_key": ["trip_record_id"], "materialize": False, "restricted": True}

    for src, target, ads in [("bronze_panelist_demos", "silver_household_demographic_observation_v", False), ("bronze_ads_demos", "silver_ads_demographic_observation_v", True)]:
        if src in available:
            _demographic_relations(con, src, target, ads)
            mappings[target] = {"schema_version": 1, "primary_key": ["observation_id"], "materialize": False, "restricted": True}

    if "bronze_panel_static" in available:
        c = _validate_source_columns(con, "bronze_panel_static", {"_source_file", "_source_row", "source_year", "panid"})
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW silver_panelist_static_v AS
            SELECT silver_id(json_array('panel-static:v1', source_year, panid)::VARCHAR) panelist_static_id,
                   panid, source_year::UTINYINT source_year,
                   {_expr(c, 'trip_count')} trip_count, {_expr(c, 'make_static')} make_static,
                   _source_file source_file, _source_row source_row
            FROM bronze_panel_static
        """)
        mappings["silver_panelist_static_v"] = {"schema_version": 1, "primary_key": ["panid", "source_year"], "materialize": False, "restricted": True}

    household_sources = [("bronze_panel", "bronze_panel" in available), ("bronze_panel_trips", "bronze_panel_trips" in available), ("bronze_panelist_demos", "bronze_panelist_demos" in available), ("bronze_panel_static", "bronze_panel_static" in available)]
    if any(present for _, present in household_sources):
        _household_views(con, household_sources)
        mappings["silver_household_v"] = {"schema_version": 1, "primary_key": ["household_id"], "materialize": False, "restricted": True}
        mappings["silver_household_year_v"] = {"schema_version": 1, "primary_key": ["household_year_id"], "materialize": False, "restricted": True}
    if "silver_household_attribute_v" in _views(con):
        mappings["silver_household_attribute_v"] = {"schema_version": 1, "primary_key": ["panid", "source_year", "attribute_name"], "materialize": False, "restricted": True}
    return mappings


def validate(con) -> list[dict]:
    """Return non-sensitive conservation and identity checks for registered views."""
    views = _views(con)
    checks = []
    pairs = [("bronze_panel", "silver_panel_purchase_v", "purchase_record_id"), ("bronze_panel_trips", "silver_panel_trip_record_v", "trip_record_id")]
    for source, output, key in pairs:
        if source in views and output in views:
            source_count = con.execute(f"SELECT count(*) FROM {source}").fetchone()[0]
            output_count = con.execute(f"SELECT count(*) FROM {output}").fetchone()[0]
            duplicate_ids = con.execute(f"SELECT count(*) FROM (SELECT {key} FROM {output} GROUP BY {key} HAVING count(*) > 1)").fetchone()[0]
            checks.extend([
                {"check_id": f"{output}_row_conservation", "ok": source_count == output_count, "severity": "error", "actual": output_count, "expected": source_count},
                {"check_id": f"{output}_id_unique", "ok": duplicate_ids == 0, "severity": "error", "actual": duplicate_ids, "expected": 0},
            ])
    if "silver_purchase_trip_link_v" in views and "silver_panel_purchase_v" in views:
        a = con.execute("SELECT count(*) FROM silver_panel_purchase_v").fetchone()[0]
        b = con.execute("SELECT count(*) FROM silver_purchase_trip_link_v").fetchone()[0]
        checks.append({"check_id": "purchase_trip_link_conservation", "ok": a == b, "severity": "error", "actual": b, "expected": a})
    if "silver_household_year_v" in views:
        for output in ("silver_panel_purchase_v", "silver_panel_trip_record_v"):
            if output in views:
                bad = con.execute(f"""SELECT count(*) FROM {output} f
                    LEFT JOIN silver_household_year_v h USING (household_year_id)
                    WHERE f.household_year_id IS NOT NULL AND h.household_year_id IS NULL""").fetchone()[0]
                checks.append({"check_id": f"{output}_household_year_reference", "ok": bad == 0, "severity": "error", "actual": bad, "expected": 0})
        bad_identity = con.execute("""SELECT count(*) FROM silver_household_year_v
            WHERE household_year_id <> silver_id(json_array('household-year:v1', source_year, panid)::VARCHAR)
               OR source_year IS NULL OR panid IS NULL""").fetchone()[0]
        checks.append({"check_id": "household_year_identity_encoding", "ok": bad_identity == 0, "severity": "error", "actual": bad_identity, "expected": 0})
    for observation, source in (("silver_household_demographic_observation_v", "bronze_panelist_demos"), ("silver_ads_demographic_observation_v", "bronze_ads_demos")):
        if observation in views and source in views:
            distinct_count, source_count = con.execute(f"SELECT coalesce(sum(source_record_count),0), (SELECT count(*) FROM {source}) FROM {observation}").fetchone()
            checks.append({"check_id": f"{observation}_observation_accounting", "ok": distinct_count == source_count, "severity": "error", "actual": distinct_count, "expected": source_count})
    return checks
