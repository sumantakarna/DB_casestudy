import json
from datetime import date

from scripts.generate_trades import build_batch
from scripts.load_to_snowflake import (COLUMNS, FILE_FORMATS, build_copy_sql, select_list,
                                       verify_batch)

TODAY = date(2026, 11, 2)


def test_select_list_matches_columns_for_every_format():
    for fmt in FILE_FORMATS:
        assert len(select_list(fmt)) == len(COLUMNS), fmt


def test_copy_sql_per_format():
    assert "XMLGET($1,'trade_id')" in build_copy_sql("xml", "@s")
    assert "$5" in build_copy_sql("csv", "@s")          # trade_id is the 5th CSV column
    assert "$1:trade_id::STRING" in build_copy_sql("jsonl", "@s")
    sql = build_copy_sql("jsonl", "@s/b", files=["a.jsonl"])
    assert "FILES=('a.jsonl')" in sql and "METADATA$FILENAME" in sql and "ABORT_STATEMENT" in sql


def test_clean_batch_verifies(tmp_path):
    m = build_batch(tmp_path, 40, "all", "L-1", today=TODAY)
    assert verify_batch(tmp_path, m) == []


def test_incomplete_or_corrupt_batch_is_detected(tmp_path):
    m = build_batch(tmp_path, 40, "all", "L-2", today=TODAY)
    csv_path = tmp_path / "l-2_trades.csv"
    csv_path.write_bytes(csv_path.read_bytes()[:200])            # truncated upload
    assert any("size" in p or "SHA-256" in p for p in verify_batch(tmp_path, m))
    (tmp_path / "l-2_trades.jsonl").unlink()                       # missing file
    assert any("missing" in p for p in verify_batch(tmp_path, m))
    (tmp_path / "_SUCCESS").unlink()                               # producer never finished
    assert any("completion marker" in p for p in verify_batch(tmp_path, m))


def test_invalid_utf8_is_detected(tmp_path):
    m = build_batch(tmp_path, 10, "jsonl", "L-3", today=TODAY)
    p = tmp_path / "l-3_trades.jsonl"
    data = p.read_bytes()
    p.write_bytes(data[:-5] + b"\xff\xfe" + data[-3:])
    meta = json.loads(json.dumps(m))
    for f in meta["files"]:
        f.pop("sha256"); f.pop("bytes")                            # isolate the UTF-8 check
    assert any("UTF-8" in x for x in verify_batch(tmp_path, meta))
