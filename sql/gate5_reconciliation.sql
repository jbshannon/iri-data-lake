-- Gate 5 — reconcile the lake against the manifest and the inventory.
--
-- Run with:  make gate5        (or, against another root:
--           LAKE=/tmp/somewhere make gate5)
--
-- Every check returns a boolean column whose name ends in `_ok`.
-- `scripts/run_sql.py` exits non-zero if any of them is false, so this
-- file doubles as the gate: it is not something to read and judge by
-- eye.
--
-- Substitutions: ${LAKE} (output root), ${BRONZE} (its bronze dir).

-- ---------------------------------------------------------------------------
-- 5a. Row-count reconciliation, manifest vs disk.
--
-- The manifest is the authority on what we *meant* to write; the
-- Parquet files are the authority on what we *did*. They are written by
-- the same run, so agreement is a check on the writer and the rename
-- path, not on the source.
--
-- Note the earlier form of this query in docs/corpus_readiness.md read
-- `sum(written_rows)` off read_parquet(), which has no such column, and
-- a tempting rewrite via parquet_metadata() is worse:
-- parquet_metadata() emits one row per (row group, COLUMN), so
-- sum(row_group_num_rows) over it returns rows x 11. count(*) over
-- read_parquet() is the correct call and takes ~0.4 s on 2.7 B rows.
-- ---------------------------------------------------------------------------
WITH m AS (
    SELECT source_path, source_year, status, written_rows, rejected_rows
    FROM read_json_auto('${LAKE}/metadata/manifest.jsonl')
),
disk AS (
    SELECT count(*) AS disk_rows
    FROM read_parquet('${BRONZE}/**/*.parquet', hive_partitioning = true)
)
SELECT
    count(*)                                        AS manifest_records,
    count(*) FILTER (WHERE status = 'success')       AS success_records,
    count(*) FILTER (WHERE status = 'failed')        AS failed_records,
    count(*) FILTER (WHERE status = 'in_progress')   AS in_progress_records,
    count(DISTINCT source_path)                      AS distinct_sources,
    sum(written_rows)                                AS manifest_rows,
    (SELECT disk_rows FROM disk)                     AS disk_rows,
    sum(written_rows) - (SELECT disk_rows FROM disk) AS row_gap,
    sum(rejected_rows)                               AS rejected_rows,
    -- "done" is: one record per source, all successful, no duplicates,
    -- and the rows on disk are exactly the rows we claimed to write.
    (SELECT count(*) FROM m)
        = count(DISTINCT source_path)
        AND count(*) FILTER (WHERE status = 'success') = count(*)
        AND count(*) FILTER (WHERE status = 'in_progress') = 0
        AND sum(written_rows) = (SELECT disk_rows FROM disk)  AS gate_5a_ok
FROM m;

-- ---------------------------------------------------------------------------
-- 5b. Expected total.
--
-- inventory reports 2 689 259 921 expected rows, but it scores an
-- unaligned file as zero rows (`expected_rows(..).unwrap_or(0)`), and
-- the corpus contains one: Year12/soup/soup_groc_1687_1739 is one byte
-- short on its final record. That file contributes 11 391 465 complete
-- records, so the run's real expectation is the sum below, and the one
-- truncated record is accounted for as rejected_rows rather than
-- silently absorbed.
-- ---------------------------------------------------------------------------
WITH m AS (
    SELECT sum(written_rows) AS manifest_rows,
           sum(rejected_rows) AS rejected_rows,
           count(*) FILTER (WHERE status = 'success') AS success_records
    FROM read_json_auto('${LAKE}/metadata/manifest.jsonl')
)
SELECT
    manifest_rows,
    rejected_rows,
    2689259921 + 11391465                         AS expected_rows,  -- 2 700 651 386
    744                                           AS expected_files,
    success_records,
    manifest_rows = 2700651386
        AND rejected_rows = 1
        AND success_records = 744                  AS gate_5b_ok
FROM m;

-- ---------------------------------------------------------------------------
-- 5c. Coverage: every year/category/channel present.
--
-- Two different units get confused here, so they are checked against two
-- different sources. A *source* file becomes many *Parquet* files
-- (one per 1 M-row batch — 232 of them for Year 1 alone), so
-- count(*) over read_parquet is not a source count. Source coverage
-- comes from the manifest; the Parquet side is checked for the
-- category/channel fan-out it should carry.
--
-- The corpus is strikingly regular — 31 sources per year per channel,
-- 12 years x 2 channels x 31 = 744 — so any year that does not report
-- 31/2/62 is a discovery or write gap, not a data quirk.
-- ---------------------------------------------------------------------------
SELECT
    year,
    count(DISTINCT category)                        AS categories,
    count(DISTINCT channel)                         AS channels,
    count(*)                                        AS rows,
    count(DISTINCT category) = 31 AND count(DISTINCT channel) = 2 AS ok
FROM read_parquet('${BRONZE}/**/*.parquet', hive_partitioning = true)
GROUP BY year
ORDER BY year;

-- The Parquet *file* count, which `count(*)` cannot give you (it counts
-- rows). parquet_metadata() is one row per (row group, column), so use
-- count(DISTINCT file_name) — and never sum(row_group_num_rows) over it
-- directly, which would multiply by the column count.
SELECT count(*) AS parquet_files FROM (
    SELECT DISTINCT file_name
    FROM parquet_metadata('${BRONZE}/**/*.parquet')
);

-- Source coverage, straight from the manifest.
SELECT
    source_year,
    count(DISTINCT source_path) AS sources,
    count(*)                    AS records,
    count(DISTINCT source_path) = 62 AND count(*) = 62 AS ok
FROM read_json_auto('${LAKE}/metadata/manifest.jsonl')
GROUP BY source_year
ORDER BY source_year;

-- One boolean over the whole of 5c: 12 years, each fully covered on
-- both the source side and the Parquet side.
SELECT
    (SELECT count(*) FROM (
        SELECT year FROM read_parquet('${BRONZE}/**/*.parquet', hive_partitioning = true)
        GROUP BY year
        HAVING count(DISTINCT category) = 31 AND count(DISTINCT channel) = 2
    )) = 12
    AND
    (SELECT count(*) FROM (
        SELECT source_year FROM read_json_auto('${LAKE}/metadata/manifest.jsonl')
        GROUP BY source_year
        HAVING count(DISTINCT source_path) = 62 AND count(*) = 62
    )) = 12                                   AS gate_5c_ok;

-- ---------------------------------------------------------------------------
-- 5d. Null / sentinel sweep. dollars_cents must never be negative beyond
-- returns; iri_key must never be 0; feature_code must be <= 4.
-- ---------------------------------------------------------------------------
SELECT
    count(*) FILTER (WHERE iri_key = 0)                      AS zero_iri_key,
    count(*) FILTER (WHERE dollars_cents < 0)                AS negative_dollars,
    count(*) FILTER (WHERE feature_code > 4)                 AS unknown_feature,
    count(*) FILTER (WHERE iri_key = 0 OR dollars_cents < 0
                        OR feature_code > 4) = 0             AS gate_5d_ok
FROM read_parquet('${BRONZE}/**/*.parquet', hive_partitioning = true);

-- ---------------------------------------------------------------------------
-- 5e. Week-range cross-check — the G4 backstop.
--
-- `week_range_strict` is off and the filename's week range is assumed
-- correct (decision 2 in docs/corpus_readiness.md §7). This is the
-- corpus-wide check that the assumption holds: the observed min/max
-- week per partition must match fixed_width::week_to_year()'s table,
-- which is hard-coded in src/fixed_width.rs and quoted here verbatim.
--
-- A mismatch does not mean a bad row — it means a file landed in the
-- wrong partition directory.
-- ---------------------------------------------------------------------------
WITH expected(year, min_week, max_week) AS (
    VALUES (1, 1114, 1165), (2, 1166, 1217), (3, 1218, 1269),
           (4, 1270, 1321), (5, 1322, 1373), (6, 1374, 1426),
           (7, 1427, 1478), (8, 1479, 1530), (9, 1531, 1582),
           (10, 1583, 1634), (11, 1635, 1686), (12, 1687, 1739)
),
observed AS (
    SELECT year, min(week) AS min_week, max(week) AS max_week, count(*) AS rows
    FROM read_parquet('${BRONZE}/**/*.parquet', hive_partitioning = true)
    GROUP BY year
)
SELECT
    o.year,
    o.min_week                                       AS observed_min,
    e.min_week                                       AS expected_min,
    o.max_week                                       AS observed_max,
    e.max_week                                       AS expected_max,
    o.rows,
    (o.min_week = e.min_week AND o.max_week = e.max_week) AS ok
FROM observed o
JOIN expected e USING (year)
ORDER BY o.year;

SELECT count(*) = 0 AS gate_5e_ok
FROM (
    SELECT o.year
    FROM (
        SELECT year, min(week) AS min_week, max(week) AS max_week
        FROM read_parquet('${BRONZE}/**/*.parquet', hive_partitioning = true)
        GROUP BY year
    ) o
    JOIN (
        VALUES (1, 1114, 1165), (2, 1166, 1217), (3, 1218, 1269),
               (4, 1270, 1321), (5, 1322, 1373), (6, 1374, 1426),
               (7, 1427, 1478), (8, 1479, 1530), (9, 1531, 1582),
               (10, 1583, 1634), (11, 1635, 1686), (12, 1687, 1739)
    ) e(year, min_week, max_week) USING (year)
    WHERE o.min_week <> e.min_week OR o.max_week <> e.max_week
);
