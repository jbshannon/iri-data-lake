"""Versioned code-system mappings, including explicit observed unmatched codes."""


def register(con):
    con.execute("CREATE TEMP TABLE _silver_channel_rules(code_system VARCHAR,raw_code VARCHAR,source_year UTINYINT,channel_code VARCHAR,mapping_status VARCHAR,rule_id VARCHAR)")
    rows = []
    for year in range(1, 13):
        for system, pairs in (("sales", [("drug", "drug"), ("groc", "groc")]),
                              ("roster", [("DR", "drug"), ("GR", "groc")]),
                              ("panel", [("DR", "drug"), ("GR", "groc"), ("MA", "mass")] if year <= 7 else [("DK", "drug"), ("GK", "groc"), ("MK", "mass")])):
            rows.extend((system, raw, year, value, "known", "iri-channels:v1") for raw, value in pairs)
    rows.append(("panel", "KK", 8, None, "unmatched", "iri-channels:v1"))
    con.executemany("INSERT INTO _silver_channel_rules VALUES (?,?,?,?,?,?)", rows)
    available = {row[0] for row in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
    pieces = []
    restricted = False
    for name, system, field in (("bronze_sales", "sales", "channel"),
                                ("bronze_delivery_stores", "roster", "ou"),
                                ("bronze_panel", "panel", "outlet"),
                                ("bronze_panel_trips", "panel", "outlet")):
        if name not in available:
            continue
        columns = {row[0].lower(): row[0] for row in con.execute(f'DESCRIBE "{name}"').fetchall()}
        if field not in columns or "source_year" not in columns:
            continue
        quoted = '"' + columns[field].replace('"', '""') + '"'
        pieces.append(f"SELECT '{system}'::VARCHAR code_system,CAST({quoted} AS VARCHAR) raw_code,source_year::UTINYINT source_year FROM {name} WHERE {quoted} IS NOT NULL AND source_year BETWEEN 1 AND 12")
        restricted = restricted or system == "panel"
    observed = " UNION ".join(pieces) if pieces else "SELECT NULL::VARCHAR code_system,NULL::VARCHAR raw_code,NULL::UTINYINT source_year WHERE FALSE"
    con.execute(f"CREATE TEMP VIEW _silver_observed_channels_v AS SELECT DISTINCT * FROM ({observed})")
    con.execute("""CREATE TEMP VIEW silver_channel_code_v AS
      SELECT o.code_system,o.raw_code,o.source_year,r.channel_code,
        coalesce(r.mapping_status,'unmatched') mapping_status,'iri-channels:v1'::VARCHAR rule_id
      FROM _silver_observed_channels_v o LEFT JOIN _silver_channel_rules r
        ON o.code_system=r.code_system AND o.source_year=r.source_year
        AND CASE WHEN o.code_system='sales' THEN lower(trim(o.raw_code)) ELSE upper(trim(o.raw_code)) END=r.raw_code
      UNION ALL
      SELECT r.* FROM _silver_channel_rules r WHERE NOT EXISTS (
        SELECT 1 FROM _silver_observed_channels_v o
        WHERE o.code_system=r.code_system AND o.source_year=r.source_year AND o.raw_code=r.raw_code)
    """)
    return {"silver_channel_code_v": {"schema_version": 1, "primary_key": ["code_system", "raw_code", "source_year"], "materialize": False, "restricted": restricted}}
