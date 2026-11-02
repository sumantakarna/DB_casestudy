#!/usr/bin/env python3
"""Load a generated batch (JSONL / CSV / XML / Parquet) into DB_TRADE_DB.RAW.TRADE_EVENTS.

Same orchestration for every file format:
  1. verify every file against the manifest (bytes, SHA-256, record count, UTF-8)
  2. PUT files to the internal stage
  3. COPY INTO RAW.TRADE_EVENTS with a format-specific column mapping
  4. only if loaded rows match the manifest, CALL OPS.REGISTER_MANIFEST (the commit signal)

``--print-sql <format>`` prints the COPY statement so the exact same mapping can be
reused in a Snowpipe definition for an external stage.
"""
import argparse
import csv
import hashlib
import json
import xml.etree.ElementTree as ET
from pathlib import Path

DB = "DB_TRADE_DB"
TARGET = f"{DB}.RAW.TRADE_EVENTS"
INT_STAGE = f"@{DB}.RAW.TRADE_INT_STAGE"
FILE_FORMATS = {"jsonl": f"{DB}.RAW.FF_JSON", "json": f"{DB}.RAW.FF_JSON",
                "csv": f"{DB}.RAW.FF_CSV", "xml": f"{DB}.RAW.FF_XML",
                "parquet": f"{DB}.RAW.FF_PARQUET"}
# Business fields in the order they appear in generated CSV files.
FIELDS = ["event_id", "source_system", "source_type", "batch_id", "trade_id", "version",
          "event_type", "trade_date", "maturity_date", "counterparty", "instrument",
          "currency", "notional", "price", "side", "source_sequence"]
NUMERIC = {"version", "source_sequence"}
HASH_FIELDS = ["trade_id", "version", "trade_date", "maturity_date", "counterparty",
               "instrument", "currency", "notional", "price", "side"]
COLUMNS = [f.upper() for f in FIELDS] + ["PAYLOAD", "SOURCE_FILE", "FILE_ROW_NUMBER",
                                         "RECORD_HASH", "SCHEMA_VERSION"]
TEXT_FORMATS = {"jsonl", "json", "csv", "xml"}


class IncompleteBatch(Exception):
    """Raised when files do not match the manifest; nothing is loaded or registered."""


def _field_expr(fmt, field):
    idx = FIELDS.index(field) + 1
    if fmt in ("jsonl", "json", "parquet"):
        raw = f"$1:{field}::STRING"
    elif fmt == "csv":
        raw = f"NULLIF(${idx},'')"
    elif fmt == "xml":
        raw = f"NULLIF(XMLGET($1,'{field}'):\"$\"::STRING,'')"
    else:
        raise ValueError(f"unsupported format: {fmt}")
    return f"TRY_TO_NUMBER({raw})" if field in NUMERIC else raw


def select_list(fmt):
    """One SQL expression per entry in COLUMNS, in the same order."""
    exprs = [_field_expr(fmt, f) for f in FIELDS]
    if fmt == "csv":
        pairs = ",".join(f"'{f}',${i + 1}" for i, f in enumerate(FIELDS))
        payload = f"OBJECT_CONSTRUCT_KEEP_NULL({pairs})"
    else:
        payload = "$1"
    hash_in = ",".join(_field_expr(fmt, f) for f in HASH_FIELDS)
    exprs += [payload, "METADATA$FILENAME", "METADATA$FILE_ROW_NUMBER",
              f"SHA2(CONCAT_WS('|',{hash_in}))", "'v1'"]
    assert len(exprs) == len(COLUMNS)
    return exprs


def build_copy_sql(fmt, from_ref, files=None, on_error="ABORT_STATEMENT"):
    cols = ",".join(COLUMNS)
    sel = ",\n       ".join(select_list(fmt))
    files_clause = ""
    if files:
        files_clause = "\nFILES=(" + ",".join("'" + f.replace("'", "''") + "'" for f in files) + ")"
    return (f"COPY INTO {TARGET} ({cols})\n"
            f"FROM (SELECT {sel}\n       FROM {from_ref}){files_clause}\n"
            f"FILE_FORMAT=(FORMAT_NAME={FILE_FORMATS[fmt]})\nON_ERROR='{on_error}'")


def count_records(path, fmt):
    path = Path(path)
    if fmt in ("jsonl", "json"):
        with path.open(encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())
    if fmt == "csv":
        with path.open(newline="", encoding="utf-8") as f:
            return max(sum(1 for _ in csv.reader(f)) - 1, 0)
    if fmt == "xml":
        return sum(1 for _, el in ET.iterparse(path, events=("end",)) if el.tag == "Trade")
    return None  # parquet: row count is verified by the COPY result instead


def verify_file(directory, meta):
    """Return a list of problems for one manifest file entry (empty list = OK)."""
    path = Path(directory) / meta["file_name"]
    if not path.is_file():
        return [f"{meta['file_name']}: missing"]
    problems, data = [], path.read_bytes()
    if "bytes" in meta and len(data) != meta["bytes"]:
        problems.append(f"{path.name}: size {len(data)} != manifest {meta['bytes']} (truncated or partial?)")
    if meta.get("sha256") and hashlib.sha256(data).hexdigest() != meta["sha256"]:
        problems.append(f"{path.name}: SHA-256 mismatch")
    fmt = meta["format"]
    if fmt in TEXT_FORMATS:
        try:
            data.decode("utf-8")
        except UnicodeDecodeError as exc:
            problems.append(f"{path.name}: not valid UTF-8 ({exc.reason} at byte {exc.start})")
            return problems
        try:
            n = count_records(path, fmt)
        except (ET.ParseError, ValueError) as exc:
            problems.append(f"{path.name}: unreadable {fmt} ({exc})")
            return problems
        if n is not None and n != meta["expected_records"]:
            problems.append(f"{path.name}: {n} records != manifest {meta['expected_records']}")
    return problems


def verify_batch(directory, manifest):
    problems = []
    if not (Path(directory) / manifest.get("completion_marker", "_SUCCESS")).is_file():
        problems.append("completion marker missing: producer has not finished the batch")
    if manifest["expected_files"] != len(manifest["files"]):
        problems.append("manifest expected_files does not match its file list")
    if manifest["expected_records"] != sum(f["expected_records"] for f in manifest["files"]):
        problems.append("manifest expected_records does not match the sum of its files")
    for meta in manifest["files"]:
        problems += verify_file(directory, meta)
    return problems


def load_batch(directory, manifest_path, conn=None):
    directory = Path(directory)
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    problems = verify_batch(directory, manifest)
    if problems:
        raise IncompleteBatch("; ".join(problems))
    own_conn = conn is None
    if own_conn:
        from scripts.snowflake_conn import connect
        conn = connect()
    batch = manifest["batch_id"]
    prefix = f"{INT_STAGE}/{batch}/"
    try:
        cur = conn.cursor()
        for meta in manifest["files"]:
            path = (directory / meta["file_name"]).resolve().as_posix()
            cur.execute(f"PUT 'file://{path}' {prefix} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
        for meta in manifest["files"]:
            cur.execute(build_copy_sql(meta["format"], prefix.rstrip('/'), files=[meta["file_name"]]))
            # COPY result columns: file, status, rows_parsed, rows_loaded, ...
            loaded = sum(int(r[3]) for r in cur.fetchall() if str(r[1]).upper() == "LOADED")
            if loaded != meta["expected_records"]:
                raise IncompleteBatch(f"{meta['file_name']}: loaded {loaded} != expected {meta['expected_records']}; manifest NOT registered")
            print(f"loaded {loaded:>8} rows from {meta['file_name']}")
        cur.execute("CALL DB_TRADE_DB.OPS.REGISTER_MANIFEST(PARSE_JSON(%s))", (json.dumps(manifest),))
        print(cur.fetchone()[0])
    finally:
        if own_conn:
            conn.close()
    return manifest


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--dir", default="sample_data", help="folder with data files and manifest")
    p.add_argument("--batch-id", help="batch id (manifest file is <batch-id lower>_manifest.json)")
    p.add_argument("--manifest", help="explicit manifest path")
    p.add_argument("--dry-run", action="store_true", help="verify files only, do not touch Snowflake")
    p.add_argument("--print-sql", choices=sorted(FILE_FORMATS), help="print the COPY statement and exit")
    p.add_argument("--from-ref", default=f"{INT_STAGE}/<path>", help="stage reference used with --print-sql")
    a = p.parse_args(argv)
    if a.print_sql:
        print(build_copy_sql(a.print_sql, a.from_ref) + ";")
        return 0
    manifest_path = a.manifest or (Path(a.dir) / f"{a.batch_id.lower()}_manifest.json" if a.batch_id else None)
    if not manifest_path:
        p.error("provide --batch-id or --manifest")
    if a.dry_run:
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
        problems = verify_batch(a.dir, manifest)
        print("OK: batch complete and consistent" if not problems else "PROBLEMS:\n- " + "\n- ".join(problems))
        return 1 if problems else 0
    load_batch(a.dir, manifest_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
