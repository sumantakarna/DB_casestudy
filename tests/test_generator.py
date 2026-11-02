import json
from datetime import date

from scripts.generate_trades import FIELDS, build_batch, control_totals, make_rows, write_file

TODAY = date(2026, 11, 2)


def test_rows_shape_and_determinism():
    a = make_rows(10, "B-1", today=TODAY)
    b = make_rows(10, "B-1", today=TODAY)
    assert a == b                                   # same input -> same event ids (idempotent replays)
    assert set(a[0]) == set(FIELDS)
    assert len({r["event_id"] for r in a}) == 10
    assert all(r["maturity_date"] > TODAY.isoformat() for r in a)  # clean data has no past maturities


def test_all_formats_split_without_duplicates(tmp_path):
    m = build_batch(tmp_path, 30, "all", "B-2", today=TODAY)
    assert m["expected_files"] == 3 and m["expected_records"] == 30
    assert sum(f["expected_records"] for f in m["files"]) == 30
    assert (tmp_path / "_SUCCESS").read_text() == "B-2"
    saved = json.loads((tmp_path / "b-2_manifest.json").read_text())
    assert saved["control_totals"] == m["control_totals"]


def test_tiny_batch_has_no_empty_files(tmp_path):
    m = build_batch(tmp_path, 2, "all", "B-3", today=TODAY)
    assert all(f["expected_records"] > 0 for f in m["files"])
    assert m["expected_files"] == len(m["files"])


def test_control_totals_are_per_currency():
    rows = [{"currency": "usd", "notional": 1.10}, {"currency": "USD", "notional": 2.20}, {"currency": "EUR", "notional": 5}]
    assert control_totals(rows) == {"EUR": "5.00", "USD": "3.30"}


def test_error_injection_plants_each_defect():
    rows = make_rows(100, "B-4", today=TODAY, inject_errors=True)
    assert any(r["side"] == "HOLD" for r in rows)
    assert any(r["notional"] < 0 for r in rows)
    assert any(r["maturity_date"] < TODAY.isoformat() for r in rows)
    assert any("\ufffd" in r["counterparty"] for r in rows)
    assert any("\t" in r["counterparty"] for r in rows)
    assert any(r["trade_id"] == "" for r in rows)
    assert any(r["version"] == 0 for r in rows)
    ids = [r["event_id"] for r in rows]
    assert len(ids) != len(set(ids))                # exact duplicate delivery present


def test_write_file_metadata(tmp_path):
    meta = write_file(tmp_path / "t.jsonl", "jsonl", make_rows(3, "B-5", today=TODAY))
    assert meta["expected_records"] == 3 and len(meta["sha256"]) == 64
    assert len((tmp_path / "t.jsonl").read_text().splitlines()) == 3
