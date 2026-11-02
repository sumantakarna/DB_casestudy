#!/usr/bin/env python3
"""Generate deterministic sample trade files plus a completion manifest.

With ``--format all`` the rows of a batch are *split* across JSONL, CSV and XML
files (each record appears exactly once), so one batch exercises several source
types.  ``--inject-errors`` plants known bad records so every data-quality rule
has something to catch.
"""
import argparse
import csv
import hashlib
import json
import random
import uuid
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

FIELDS = ["event_id", "source_system", "source_type", "batch_id", "trade_id", "version",
          "event_type", "trade_date", "maturity_date", "counterparty", "instrument",
          "currency", "notional", "price", "side", "source_sequence"]
FORMATS = ["jsonl", "csv", "xml"]
SOURCE_SYSTEM = "SIMULATED_TRADE_FEED"
_NAMESPACE = uuid.UUID("6f1c2a52-8d4e-4c55-9f0e-2d6f5f1a7b11")

# Row index (mod 50) -> planted defect, used by --inject-errors.
INJECTED = {
    7: "DOMAIN_VALUES",      # side = HOLD
    13: "NUMERIC_PARSE",     # negative notional
    21: "MATURITY_PAST",     # maturity yesterday
    29: "CONTROL_CHARS",     # U+FFFD replacement character in counterparty
    31: "CONTROL_CHARS",     # tab inside counterparty
    37: "REQUIRED_FIELDS",   # empty trade_id
    43: "DUPLICATE",         # exact copy of the previous row
    47: "VERSION_INVALID",   # version 0
}


def make_rows(n, batch_id, seed=7, today=None, inject_errors=False):
    rng = random.Random(seed)
    today = today or date.today()
    rows = []
    for i in range(n):
        row = {
            "event_id": str(uuid.uuid5(_NAMESPACE, f"{batch_id}:{seed}:{i}")),
            "source_system": SOURCE_SYSTEM,
            "source_type": "FILE",
            "batch_id": batch_id,
            "trade_id": f"TRD-{batch_id.upper()}-{i // 2:07d}",
            "version": (i % 2) + 1,
            "event_type": "UPSERT",
            "trade_date": today.isoformat(),
            "maturity_date": (today + timedelta(days=rng.randint(1, 1500))).isoformat(),
            "counterparty": f"CP-{i % 250:04d}",
            "instrument": rng.choice(["IRS", "FX_FORWARD", "BOND", "OPTION"]),
            "currency": rng.choice(["USD", "EUR", "GBP", "JPY"]),
            "notional": round(rng.uniform(10000, 50000000), 2),
            "price": round(rng.uniform(0.1, 150), 6),
            "side": rng.choice(["BUY", "SELL"]),
            "source_sequence": i + 1,
        }
        defect = INJECTED.get(i % 50) if inject_errors else None
        if defect == "DOMAIN_VALUES":
            row["side"] = "HOLD"
        elif defect == "NUMERIC_PARSE":
            row["notional"] = -1.0
        elif defect == "MATURITY_PAST":
            row["maturity_date"] = (today - timedelta(days=1)).isoformat()
        elif defect == "CONTROL_CHARS":
            row["counterparty"] = ("Bank\ufffd" if i % 50 == 29 else "Bank\tLtd")
        elif defect == "REQUIRED_FIELDS":
            row["trade_id"] = ""
        elif defect == "VERSION_INVALID":
            row["version"] = 0
        elif defect == "DUPLICATE" and rows:
            row = dict(rows[-1])
        rows.append(row)
    return rows


def control_totals(rows):
    """Per-currency notional totals (never mix currencies)."""
    totals = {}
    for r in rows:
        ccy = str(r["currency"]).upper()
        totals[ccy] = totals.get(ccy, Decimal("0")) + Decimal(str(r["notional"]))
    return {k: str(v.quantize(Decimal("0.01"))) for k, v in sorted(totals.items())}


def write_file(path, fmt, rows):
    path = Path(path)
    if fmt == "jsonl":
        with path.open("w", encoding="utf-8", newline="\n") as f:
            for r in rows:
                f.write(json.dumps(r, separators=(",", ":"), ensure_ascii=False) + "\n")
    elif fmt == "csv":
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            w.writeheader()
            w.writerows(rows)
    elif fmt == "xml":
        root = ET.Element("Trades")
        for r in rows:
            t = ET.SubElement(root, "Trade")
            for k in FIELDS:
                v = r.get(k)
                ET.SubElement(t, k).text = "" if v is None else str(v)
        ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)
    else:
        raise ValueError(f"unsupported format: {fmt}")
    data = path.read_bytes()
    return {"file_name": path.name, "format": fmt, "bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(), "expected_records": len(rows)}


def split_rows(rows, parts):
    n = len(rows)
    return [rows[i * n // parts:(i + 1) * n // parts] for i in range(parts)]


def build_batch(out, rows_n, fmt, batch_id, seed=7, inject_errors=False, today=None):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    rows = make_rows(rows_n, batch_id, seed=seed, today=today, inject_errors=inject_errors)
    formats = FORMATS if fmt == "all" else [fmt]
    files = []
    for f, chunk in zip(formats, split_rows(rows, len(formats))):
        if chunk:  # an empty file would never show up as a loaded file
            files.append(write_file(out / f"{batch_id.lower()}_trades.{f}", f, chunk))
    manifest = {
        "batch_id": batch_id,
        "source_system": SOURCE_SYSTEM,
        "created_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "expected_files": len(files),
        "expected_records": sum(x["expected_records"] for x in files),
        "control_totals": control_totals(rows),
        "files": files,
        "completion_marker": "_SUCCESS",
    }
    (out / f"{batch_id.lower()}_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    (out / "_SUCCESS").write_text(batch_id, encoding="utf-8")  # written last = commit signal
    return manifest


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--out", default="sample_data")
    p.add_argument("--rows", type=int, default=1000)
    p.add_argument("--format", choices=FORMATS + ["all"], default="all")
    p.add_argument("--batch-id", default="DEMO-001")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--inject-errors", action="store_true", help="plant known bad records")
    a = p.parse_args(argv)
    m = build_batch(a.out, a.rows, a.format, a.batch_id, a.seed, a.inject_errors)
    print(f"Generated {m['expected_files']} file(s), {m['expected_records']} records in {a.out}")


if __name__ == "__main__":
    main()
