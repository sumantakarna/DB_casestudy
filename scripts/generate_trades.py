"""Generate JSONL trade events, including deterministic rule-test examples."""
import argparse
import hashlib
import json
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


def make_event(trade_id, version, *, maturity_date=None, quantity=None, price=None,
               event_timestamp=None, source_system="SIMULATOR", batch_id="demo-batch"):
    event_timestamp = event_timestamp or datetime.now(timezone.utc).replace(microsecond=0)
    payload = {
        "trade_id": trade_id, "version": version,
        "instrument": "BOND" if int(hashlib.sha256(trade_id.encode()).hexdigest(), 16) % 2 else "EQUITY",
        "trade_side": "BUY", "quantity": quantity if quantity is not None else round(random.uniform(1, 1000), 4),
        "price": price if price is not None else round(random.uniform(10, 250), 4),
        "currency": "USD", "trade_date": date.today().isoformat(),
        "maturity_date": (maturity_date or (date.today() + timedelta(days=365))).isoformat(),
        "counterparty": f"CP-{int(hashlib.sha256(trade_id.encode()).hexdigest(), 16) % 100:03d}",
        "event_timestamp": event_timestamp.isoformat().replace("+00:00", "Z"),
        "source_system": source_system, "batch_id": batch_id,
    }
    stable = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    payload["event_id"] = hashlib.sha256(stable.encode()).hexdigest()
    return payload


def generate_events(count, batch_id, seed=42):
    random.seed(seed)
    events = [make_event(f"TRD-{i+1:07d}", 1, batch_id=batch_id) for i in range(count)]
    if count >= 4:
        events += [
            make_event("TRD-DEMO-001", 2, batch_id=batch_id),
            make_event("TRD-DEMO-001", 1, batch_id=batch_id),
            make_event("TRD-DEMO-002", 1, maturity_date=date.today()-timedelta(days=1), batch_id=batch_id),
            make_event("TRD-DEMO-003", 1, quantity=-10, batch_id=batch_id),
        ]
    return events


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="data/trades.jsonl")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be >= 1")
    batch_id = args.batch_id or f"batch-{uuid.uuid4().hex[:12]}"
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    events = generate_events(args.count, batch_id, args.seed)
    with output.open("w", encoding="utf-8") as stream:
        for event in events:
            stream.write(json.dumps(event, separators=(",", ":")) + "\n")
    print(f"Wrote {len(events)} events to {output}; batch_id={batch_id}")


if __name__ == "__main__":
    main()
