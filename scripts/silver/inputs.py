"""Manifest-pinned Bronze input selection and registration."""
from __future__ import annotations
import hashlib
import json
import os
import re
import shutil
from pathlib import Path

from .keys import register_functions

_PUBLIC_DATASETS = {"sales", "delivery_stores", "product_stub", "product_attr", "week_dimension", "fips_market", "chain_xref", "manual_store_entry"}
_RESTRICTED_DATASETS = {"panel", "panel_trips", "panel_static", "panelist_demos", "ads_demos"}


def _norm(value):
    return str(value or "sales").strip().lower().replace("-", "_")


def _digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _resolve(raw, lake):
    p = Path(raw)
    if p.is_absolute():
        return p.resolve()
    # Rust manifests conventionally record source-relative paths in the lake; repo-relative
    # legacy paths are accepted only when their lake-relative candidate does not exist.
    candidate = (lake / p).resolve()
    if candidate.exists() or not p.parts or p.parts[0] not in {"data", "src", "tests", "reference"}:
        return candidate
    return (Path(__file__).resolve().parents[2] / p).resolve()


def _paths(record, lake):
    return [_resolve(raw, lake) for raw in record.get("output_paths") or []]


def _year_scope(rec, path=None):
    # Read every declaration independently; never let a preferred source hide conflict.
    values = []
    if rec.get("source_year") is not None:
        values.append(str(rec["source_year"]))
    for candidate in {str(rec.get("source_path", "")), str(path or "")}:
        for part in Path(candidate).parts:
            match = re.fullmatch(r"Year(\d+)", part, re.IGNORECASE)
            if match:
                values.append(match.group(1))
            elif part.lower().startswith("year="):
                values.append(part.split("=", 1)[1])
    if any(not re.fullmatch(r"\d+", value) or not 1 <= int(value) <= 12 for value in values):
        raise ValueError("invalid source year declaration or path scope")
    years = {int(value) for value in values}
    if len(years) > 1:
        raise ValueError("source year disagrees between manifest and path scopes")
    return next(iter(years), None)


def _edition_scope(rec, path):
    if rec.get("edition") is not None:
        return str(rec["edition"])
    for part in reversed(Path(str(path)).parts):
        match = re.fullmatch(r"(?:edition=|edition[_-]?)(\d{4}(?:-\d{4})?)", part, re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def _partition_scope(path):
    scope = {}
    for part in Path(str(path)).parts:
        if "=" in part:
            key, value = part.split("=", 1)
            if key.lower() in {"year", "category", "channel", "edition"}:
                scope[key.lower()] = value
    return scope


def _metadata_count(path):
    import duckdb
    con = duckdb.connect()
    try:
        return int(con.execute("SELECT num_rows FROM parquet_file_metadata(?)", [str(path)]).fetchone()[0])
    finally:
        con.close()


def select_inputs(lake: Path, *, required_datasets: list[str] | None = None) -> dict:
    lake = Path(lake).resolve()
    manifest = lake / "metadata" / "manifest.jsonl"
    blob = manifest.read_bytes()
    if blob and not blob.endswith(b"\n"):
        raise ValueError("manifest has an incomplete trailing record")
    records = []
    for ordinal, line in enumerate(blob.splitlines()):
        try:
            item = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"malformed manifest record {ordinal}") from exc
        item["append_ordinal"] = ordinal
        item["dataset"] = _norm(item.get("dataset"))
        records.append(item)
    latest = {}
    for rec in records:
        latest[(rec["dataset"], str(rec.get("source_path", "")))] = rec
    selected_all = list(latest.values())
    included = {_norm(d) for d in required_datasets} if required_datasets is not None else _PUBLIC_DATASETS
    selected = [r for r in selected_all if r["dataset"] in included]
    for rec in selected:
        if rec.get("status") != "success":
            raise ValueError(f"latest attempt failed for included source: {rec.get('source_path')}")
    present = {r["dataset"] for r in selected}
    absent = included - present
    if absent:
        raise ValueError(f"missing required datasets: {sorted(absent)}")
    # Lookup identity and target records within dataset; same source path in two classes is unrelated.
    by_source = {(r["dataset"], str(r.get("source_path", ""))): r for r in selected_all}
    canonical = {}
    aliases = []
    for rec in selected:
        key = (rec["dataset"], str(rec.get("source_path", "")))
        target = str(rec.get("deduplicated_from") or rec.get("source_path", ""))
        visited = set()
        while True:
            target_key = (rec["dataset"], target)
            if target_key in visited:
                raise ValueError(f"cycle in deduplication pointers for {rec['dataset']}")
            visited.add(target_key)
            target_rec = by_source.get(target_key)
            if not target_rec:
                raise ValueError(f"missing deduplication target: {target}")
            if target_rec.get("status") != "success":
                raise ValueError(f"deduplication target is not successful: {target}")
            if target_rec.get("source_sha256") != rec.get("source_sha256"):
                raise ValueError("deduplication pointer hash mismatch")
            next_target = target_rec.get("deduplicated_from")
            if not next_target:
                break
            target = str(next_target)
        out_paths = _paths(target_rec, lake)
        if not out_paths:
            if int(target_rec.get("written_rows", -1)) != 0:
                raise ValueError(f"selected source has no output: {target}")
            if rec.get("source_path") == target_rec.get("source_path"):
                aliases.append({"dataset":rec["dataset"], "source_path":rec.get("source_path"), "canonical_path":None,
                                "source_year":_year_scope(rec,rec.get("source_path")), "category":rec.get("category"),
                                "channel":rec.get("channel"), "edition":rec.get("edition"),
                                "deduplicated_from":rec.get("deduplicated_from"), "source_sha256":rec.get("source_sha256"),
                                "append_ordinal":rec["append_ordinal"]})
            continue
        for path in out_paths:
            if not path.is_relative_to(lake):
                raise ValueError("selected output escapes the lake root")
            if not path.is_file():
                raise FileNotFoundError(path)
            actual_rows = _metadata_count(path)
            expected = int(target_rec.get("written_rows", actual_rows))
            # A source may have multiple partition outputs; compare aggregate below.
            canonical.setdefault((rec["dataset"], str(path)), {"dataset":rec["dataset"], "path":str(path), "record":target_rec})
            path_scope=_partition_scope(path)
            canonical_year = _year_scope(target_rec, path)
            alias_year = _year_scope(rec)
            # A deduplicated roster/demo may intentionally reuse bytes in another year.
            # Validate the canonical output against its own record, not the alias year.
            resolved_year = alias_year if alias_year is not None else canonical_year
            aliases.append({"dataset":rec["dataset"], "source_path":rec.get("source_path"), "canonical_path":str(path),
                            "source_year":resolved_year,
                            "category":rec.get("category") or path_scope.get("category"), "channel":rec.get("channel") or path_scope.get("channel"),
                            "edition":rec.get("edition") or path_scope.get("edition") or _edition_scope(rec,path),
                            "deduplicated_from":rec.get("deduplicated_from"), "source_sha256":rec.get("source_sha256"), "append_ordinal":rec["append_ordinal"]})
        if len(out_paths) == 1 and "written_rows" in target_rec and _metadata_count(out_paths[0]) != int(target_rec["written_rows"]):
            raise ValueError(f"manifest written_rows mismatch for {target}")
        if len(out_paths) > 1 and "written_rows" in target_rec and sum(_metadata_count(p) for p in out_paths) != int(target_rec["written_rows"]):
            raise ValueError(f"manifest written_rows mismatch for {target}")
    files = {}
    for entry in canonical.values():
        files.setdefault(entry["dataset"], []).append(entry["path"])
    for ds in files:
        files[ds] = sorted(set(files[ds]))
    # Physical file schema catalog preserves absence across union-by-name.
    schemas = []
    file_metadata = []
    for (ds, path), entry in sorted(canonical.items()):
        import duckdb
        con = duckdb.connect()
        try:
            desc = con.execute("DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)", [path]).fetchall()
        finally:
            con.close()
        alias = next((a for a in aliases if a["dataset"] == ds and a["canonical_path"] == path and a["source_path"] == entry["record"].get("source_path")), None)
        sid = _source_id(ds, entry["record"], path)
        for ordinal, row in enumerate(desc):
            schemas.append({"_source_id":sid, "dataset":ds, "column_name":row[0], "data_type":row[1], "column_ordinal":ordinal})
        schema_hash=hashlib.sha256(json.dumps([(r[0],r[1]) for r in desc],separators=(",",":" )).encode()).hexdigest()
        file_metadata.append({"dataset":ds,"path":path,"_source_id":sid,"rows":_metadata_count(Path(path)),"sha256":_digest(Path(path)),"schema_sha256":schema_hash,"source_path":entry["record"].get("source_path"),"output_schema_version":entry["record"].get("output_schema_version")})
    record_datasets={r["dataset"] for r in selected}
    empty_datasets=sorted(record_datasets-set(files))
    return {"manifest_sha256":hashlib.sha256(blob).hexdigest(), "lake":str(lake), "records":selected, "aliases":aliases,
            "files":files, "schemas":schemas, "file_metadata":file_metadata,
            "scope":{"datasets":sorted(record_datasets), "included_datasets":sorted(included),
                     "empty_datasets":empty_datasets, "complete":not bool(included-record_datasets)}}


def _source_id(dataset, record, original_path):
    # Include the canonical physical output identity: one manifest source can emit
    # multiple independently schematized files. original_path survives retention.
    identity = ["silver-source:v1", _norm(dataset), str(record.get("source_path", "")),
                str(record.get("source_sha256", "")), str(original_path)]
    return hashlib.sha256(json.dumps(identity, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def retain_inputs(snapshot: dict, destination: Path, *, copy_only=False) -> dict:
    dest = Path(destination).resolve()
    dest.mkdir(parents=True, exist_ok=True)
    retained = {k:(list(v) if isinstance(v,list) else dict(v) if isinstance(v,dict) else v) for k,v in snapshot.items()}
    retained_files, integrity, mapping = {}, [], {}
    for dataset, paths in snapshot["files"].items():
        result=[]
        for index, original in enumerate(paths):
            src=Path(original); before=_digest(src)
            expected = next((row["sha256"] for row in snapshot.get("file_metadata", []) if row["path"] == original), before)
            if before != expected:
                raise ValueError(f"input changed after selection: {src}")
            target=dest/dataset/f"{index:05d}-{src.name}"; target.parent.mkdir(parents=True,exist_ok=True)
            if target.exists():
                if _digest(target)!=before: raise ValueError(f"retained destination differs: {target}")
            else:
                if copy_only:
                    shutil.copy2(src, target)
                else:
                    try: os.link(src,target)
                    except OSError: shutil.copy2(src,target)
            after=_digest(src); retained_hash=_digest(target)
            if before!=after or before!=retained_hash:
                target.unlink(missing_ok=True); raise RuntimeError(f"input changed during retention: {src}")
            result.append(str(target)); mapping[str(src)]=str(target)
            integrity.append({"dataset":dataset,"original_path":str(src),"retained_path":str(target),"sha256":before})
        retained_files[dataset]=result
    retained["files"]=retained_files
    retained["integrity"]=integrity
    retained["original_files"]=dict(snapshot["files"])
    retained["retained_root"]=str(dest)
    # Remap aliases while retaining original identity and scopes.
    retained["aliases"]=[{**a,"original_canonical_path":a["canonical_path"],"canonical_path":mapping.get(a["canonical_path"],a["canonical_path"])} for a in snapshot.get("aliases",[])]
    return retained


def register_bronze(con, snapshot: dict) -> None:
    register_functions(con)
    import duckdb
    expected={m["path"]:m for m in snapshot.get("file_metadata",[])}
    retained={i["retained_path"]:i for i in snapshot.get("integrity",[])}
    for paths in snapshot.get("files",{}).values():
        for path in paths:
            expected_hash=(retained.get(path) or expected.get(path) or {}).get("sha256")
            if expected_hash and _digest(Path(path))!=expected_hash:
                raise ValueError(f"selected input changed after snapshot: {path}")
    # Build source records per selected physical output; source ids are derived from original manifest identity.
    file_meta={}
    for ds, paths in snapshot.get("files",{}).items():
        for p in paths:
            original=next((x["original_path"] for x in snapshot.get("integrity",[]) if x["retained_path"]==p),p)
            source_metadata = next((item for item in snapshot.get("file_metadata", []) if item["path"] == original and item["dataset"] == ds), None)
            candidates=[r for r in snapshot.get("records",[]) if r.get("status")=="success" and r["dataset"]==ds and ((source_metadata and r.get("source_path") == source_metadata.get("source_path")) or _resolve_output_match(r, original))]
            rec=next((r for r in candidates if not r.get("deduplicated_from")), candidates[0] if candidates else None)
            if rec is None: raise ValueError(f"no source metadata for selected file: {original}")
            file_meta[p]=(ds,rec,_source_id(ds,rec,original),original)
    for ds, paths in snapshot.get("files",{}).items():
        safe=_norm(ds)
        if not paths: continue
        quoted=", ".join("'"+str(p).replace("'","''")+"'" for p in paths)
        relation=f"read_parquet([{quoted}], union_by_name=true, hive_partitioning=true, filename=true, file_row_number=true)"
        columns={r[0].lower():r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()}
        if "filename" in columns or "file_row_number" in columns: pass
        year_col=columns.get("source_year") or columns.get("year")
        # exact annual scope by Year<N> ancestors and declared manifest metadata
        scope_cases=[]
        for p in paths:
            _,rec,_,original=file_meta[p]
            part_scope=_partition_scope(original)
            year=_year_scope(rec, original)
            if year is not None: scope_cases.append((p,year))
        source_year_expr=(f"try_cast({year_col} AS UTINYINT)" if year_col else "NULL::UTINYINT")
        case="CASE filename " + " ".join(f"WHEN '{p.replace(chr(39), chr(39)*2)}' THEN {y}" for p,y in scope_cases) + " ELSE NULL END"
        # Keep physical year when present, but ensure it agrees with declared scope.
        rawyear=f"try_cast({year_col} AS UTINYINT)" if year_col else "NULL::UTINYINT"
        declared=case if scope_cases else "NULL::UTINYINT"
        if year_col:
            con.execute(f"CREATE TEMP VIEW _silver_year_check AS SELECT count(*) n FROM {relation} WHERE {year_col} IS NOT NULL AND (try_cast({year_col} AS UTINYINT) IS NULL OR try_cast({year_col} AS UTINYINT) NOT BETWEEN 1 AND 12)")
            invalid=con.execute("SELECT n FROM _silver_year_check").fetchone()[0]
            con.execute("DROP VIEW _silver_year_check")
            if invalid: raise ValueError(f"invalid source year value for {ds}")
            if scope_cases:
                con.execute(f"CREATE TEMP VIEW _silver_year_check AS SELECT count(*) n FROM {relation} WHERE {declared} IS NOT NULL AND {rawyear} IS NOT NULL AND {declared}!={rawyear}")
                if con.execute("SELECT n FROM _silver_year_check").fetchone()[0]: raise ValueError(f"source year disagrees with manifest/path for {ds}")
                con.execute("DROP VIEW _silver_year_check")
        year_expr=f"coalesce({rawyear}, {declared})" if year_col else declared
        id_case="CASE filename " + " ".join(f"WHEN '{p.replace(chr(39), chr(39)*2)}' THEN '{file_meta[p][2]}'" for p in paths) + " END"
        original_case="CASE filename " + " ".join(f"WHEN '{p.replace(chr(39), chr(39)*2)}' THEN '{file_meta[p][3].replace(chr(39), chr(39)*2)}'" for p in paths) + " END"
        # panelist demos copied under categories: each physical record gets each unique scope alias, once.
        extra=[]
        if ds in {"delivery_stores","panelist_demos"}:
            scopes=[]
            for a in snapshot.get("aliases",[]):
                if a.get("dataset")==ds and a.get("source_year") is not None:
                    p=a.get("canonical_path")
                    if p not in paths: p=a.get("original_canonical_path")
                    if p in paths:
                        item=(p,int(a["source_year"]))
                        if item not in scopes: scopes.append(item)
            if scopes:
                values=", ".join(f"('{p.replace(chr(39),chr(39)*2)}',{y})" for p,y in scopes)
                extra=[f"CROSS JOIN (SELECT DISTINCT * FROM (VALUES {values}) v(_scope_file,_scope_year)) sc WHERE filename=sc._scope_file"]
                year_expr="sc._scope_year::UTINYINT"
        projections=["* EXCLUDE (filename, file_row_number)",f"{year_expr}::UTINYINT AS source_year",f"{id_case}::VARCHAR AS _source_id",f"{original_case}::VARCHAR AS _source_file","file_row_number::UBIGINT AS _source_row"]
        for scope_name in ("category", "channel"):
            if scope_name not in columns:
                scope_cases=[]
                for p in paths:
                    original=file_meta[p][3]
                    value=_partition_scope(original).get(scope_name) or file_meta[p][1].get(scope_name)
                    if value is not None:
                        scope_cases.append(f"WHEN '{p.replace(chr(39),chr(39)*2)}' THEN '{str(value).replace(chr(39),chr(39)*2)}'")
                if scope_cases:
                    projections.append(("CASE filename "+" ".join(scope_cases)+" ELSE NULL END")+f"::VARCHAR AS {scope_name}")
        if ds=="product_stub" and "edition" not in columns:
            edition_cases=[]
            for p in paths:
                edition=_edition_scope(file_meta[p][1],file_meta[p][3])
                if edition is not None: edition_cases.append(f"WHEN '{p.replace(chr(39),chr(39)*2)}' THEN '{edition.replace(chr(39),chr(39)*2)}'")
            projections.append(("CASE filename "+" ".join(edition_cases)+" ELSE NULL END")+"::VARCHAR AS edition")
        query=f"SELECT {', '.join(projections)} FROM {relation} " + (extra[0] if extra else "")
        # Replace existing year field before standardized alias to avoid duplicate column name.
        if year_col and year_col.lower()=="source_year":
            projections[0]="* EXCLUDE (filename, file_row_number, source_year)"
            query=f"SELECT {', '.join(projections)} FROM {relation} " + (extra[0] if extra else "")
        if "panelist id" in columns and "panid" not in columns:
            query=f"SELECT *, \"Panelist ID\"::UINTEGER AS panid FROM ({query}) q"
        con.execute(f'CREATE OR REPLACE VIEW "bronze_{safe}" AS {query}')
    _register_metadata_views(con,snapshot,file_meta)


def _resolve_output_match(record, original):
    wanted=Path(original).resolve()
    return any(Path(p).resolve()==wanted for p in (record.get("output_paths") or []))


def _register_metadata_views(con,snapshot,file_meta):
    def table(name, schema, rows):
        cols=", ".join(f'"{n}" {t}' for n,t in schema)
        con.execute(f'CREATE TEMP TABLE IF NOT EXISTS "{name}" ({cols})')
        con.execute(f'DELETE FROM "{name}"')
        if rows:
            marks=", ".join("?" for _ in schema)
            con.executemany(f'INSERT INTO "{name}" VALUES ({marks})',rows)
        view=name.replace("_data", "_v")
        present=con.execute("SELECT count(*) FROM duckdb_views() WHERE view_name=?",[view]).fetchone()[0]
        if not present:
            con.execute(f'CREATE TEMP VIEW "{view}" AS SELECT * FROM "{name}"')
    rows=[]
    for ds,rec,sid,original in file_meta.values():
        rows.append((sid,ds,str(rec.get("source_path","")),str(rec.get("source_sha256","")),str(rec.get("parser_version","")),rec.get("output_schema_version"),rec.get("written_rows"),rec.get("append_ordinal")))
    table("silver_source_data",[("_source_id","VARCHAR"),("dataset","VARCHAR"),("source_path","VARCHAR"),("source_sha256","VARCHAR"),("parser_version","VARCHAR"),("output_schema_version","INTEGER"),("written_rows","BIGINT"),("append_ordinal","BIGINT")],rows)
    alias_schema=[("dataset","VARCHAR"),("source_path","VARCHAR"),("canonical_path","VARCHAR"),("source_year","UTINYINT"),("category","VARCHAR"),("channel","VARCHAR"),("edition","VARCHAR"),("deduplicated_from","VARCHAR"),("source_sha256","VARCHAR"),("append_ordinal","BIGINT")]
    alias_rows=[tuple(a.get(k) for k,_ in alias_schema) for a in snapshot.get("aliases",[])
                if a.get("dataset") in {r.get("dataset") for r in snapshot.get("records",[])}]
    table("silver_source_alias_data",alias_schema,alias_rows)
    schema_rows=[]
    by_path={str(p):(sid,ds) for p,(ds,rec,sid,orig) in file_meta.items()}
    supplied=snapshot.get("schemas") or []
    for row in supplied:
        sid=row.get("_source_id"); schema_rows.append((sid,row.get("column_name"),row.get("data_type"),int(row.get("column_ordinal",0))))
    if not schema_rows:
        for p,(sid,ds) in by_path.items():
            desc=con.execute(f"DESCRIBE SELECT * FROM read_parquet('{p.replace(chr(39),chr(39)*2)}')").fetchall()
            schema_rows.extend((sid,r[0],r[1],i) for i,r in enumerate(desc))
    table("silver_input_column_data",[("_source_id","VARCHAR"),("column_name","VARCHAR"),("data_type","VARCHAR"),("column_ordinal","INTEGER")],schema_rows)
