import json
from pathlib import Path
from scripts.generate_trades import make_rows, write_file

def test_generator_has_expected_shape(tmp_path):
    rows=make_rows(5,'B-1')
    assert len(rows)==5
    assert {'event_id','trade_id','version','maturity_date','notional'} <= set(rows[0])

def test_jsonl_writer_and_manifest_metadata(tmp_path):
    rows=make_rows(3,'B-2')
    p=tmp_path/'trades.jsonl'
    meta=write_file(p,'jsonl',rows)
    assert meta['expected_records']==3
    assert len(meta['sha256'])==64
    assert len(p.read_text().splitlines())==3
