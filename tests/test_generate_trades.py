from datetime import date, datetime, timedelta, timezone
from scripts.generate_trades import generate_events, make_event


def test_event_id_stable_for_same_payload():
    kwargs = dict(maturity_date=date.today()+timedelta(days=30),
                  event_timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
                  batch_id="B")
    assert make_event("T-1", 1, **kwargs)["event_id"] == make_event("T-1", 1, **kwargs)["event_id"]


def test_generator_has_rule_cases():
    events = generate_events(5, "BATCH", seed=10)
    assert any(e["trade_id"] == "TRD-DEMO-001" and e["version"] == 2 for e in events)
    assert any(e["maturity_date"] < date.today().isoformat() for e in events)
    assert any(e["quantity"] <= 0 for e in events)
