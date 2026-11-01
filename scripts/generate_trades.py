#!/usr/bin/env python3
"""Generate deterministic sample trade files and a completion manifest."""
import argparse, csv, hashlib, json, random, uuid
from datetime import date, timedelta
from pathlib import Path
import xml.etree.ElementTree as ET

FIELDS = ["event_id","source_system","source_type","batch_id","trade_id","version","event_type","trade_date","maturity_date","counterparty","instrument","currency","notional","price","side","source_sequence"]

def make_rows(n, batch_id, seed=7):
    random.seed(seed); rows=[]
    for i in range(n):
        maturity = date.today() + timedelta(days=random.randint(-10, 1500))
        rows.append({"event_id":str(uuid.uuid4()),"source_system":"SIMULATED_TRADE_FEED","source_type":"FILE","batch_id":batch_id,"trade_id":f"TRD-{i//2:09d}","version":(i%2)+1,"event_type":"UPSERT","trade_date":date.today().isoformat(),"maturity_date":maturity.isoformat(),"counterparty":f"CP-{i%250:04d}","instrument":random.choice(["IRS","FX_FORWARD","BOND","OPTION"]),"currency":random.choice(["USD","EUR","GBP","JPY"]),"notional":round(random.uniform(10000,50000000),2),"price":round(random.uniform(0.1,150),6),"side":random.choice(["BUY","SELL"]),"source_sequence":i+1})
    return rows

def write_file(path, fmt, rows):
    if fmt == "jsonl":
        with path.open("w",encoding="utf-8") as f:
            for r in rows: f.write(json.dumps(r,separators=(",",":"))+"\n")
    elif fmt == "csv":
        with path.open("w",newline="",encoding="utf-8") as f:
            w=csv.DictWriter(f,fieldnames=FIELDS); w.writeheader(); w.writerows(rows)
    elif fmt == "xml":
        root=ET.Element("Trades")
        for r in rows:
            t=ET.SubElement(root,"Trade")
            for k,v in r.items(): ET.SubElement(t,k).text=str(v)
        ET.ElementTree(root).write(path,encoding="utf-8",xml_declaration=True)
    else: raise ValueError(fmt)
    b=path.read_bytes(); return {"file_name":path.name,"format":fmt,"bytes":len(b),"sha256":hashlib.sha256(b).hexdigest(),"expected_records":len(rows)}

def main():
    p=argparse.ArgumentParser(); p.add_argument("--out",default="sample_data"); p.add_argument("--rows",type=int,default=1000); p.add_argument("--format",choices=["jsonl","csv","xml","all"],default="all"); p.add_argument("--batch-id",default="DEMO-001"); a=p.parse_args()
    out=Path(a.out); out.mkdir(parents=True,exist_ok=True); rows=make_rows(a.rows,a.batch_id); formats=["jsonl","csv","xml"] if a.format=="all" else [a.format]; files=[]
    for fmt in formats:
        path=out/f"{a.batch_id.lower()}_trades.{fmt}"; files.append(write_file(path,fmt,rows))
    manifest={"batch_id":a.batch_id,"source_system":"SIMULATED_TRADE_FEED","created_utc":date.today().isoformat(),"expected_files":len(files),"expected_records":sum(x["expected_records"] for x in files),"files":files,"completion_marker":"_SUCCESS"}
    (out/f"{a.batch_id.lower()}_manifest.json").write_text(json.dumps(manifest,indent=2),encoding="utf-8")
    (out/"_SUCCESS").write_text(a.batch_id,encoding="utf-8")
    print(f"Generated {len(files)} files, {a.rows} records per file in {out}")
if __name__=="__main__": main()
