"""Versioned deterministic keys used by Silver relations."""
import hashlib
import json
import math
import struct


def _typed(value):
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite floats are not valid key values")
        return {"$f64": struct.pack(">d", value).hex()}
    if isinstance(value, list):
        return [_typed(v) for v in value]
    if isinstance(value, dict):
        return {k: _typed(v) for k, v in value.items()}
    return value


def stable_id(payload_json: str) -> str:
    """Hash compact UTF-8 JSON; floating values use their exact IEEE-754 bits."""
    value = json.loads(payload_json)
    encoded = json.dumps(_typed(value), ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def register_functions(con) -> None:
    """Register the exact Python stable_id codec as an idempotent SQL UDF."""
    try:
        import duckdb
        exists=con.execute("SELECT count(*) FROM duckdb_functions() WHERE function_name='silver_id'").fetchone()[0]
        if not exists:
            con.create_function("silver_id", stable_id, [duckdb.sqltypes.VARCHAR], duckdb.sqltypes.VARCHAR)
        vector = '["codec:v1",1,null,0.5,-0.0]'
        if con.execute("SELECT silver_id(?)", [vector]).fetchone()[0] != stable_id(vector):
            raise ValueError("existing silver_id function is incompatible with the frozen codec")
    except ImportError as exc:
        raise RuntimeError("DuckDB and NumPy are required to register silver_id") from exc
