import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.snowflake_conn import connect  # noqa: E402

st.set_page_config(page_title="Trade Pipeline Monitor", layout="wide")
st.title("Trade Processing Monitor")
st.caption("Current trade status, rejection audit and batch reconciliation (DB_TRADE_DB).")


@st.cache_resource
def connection():
    import os
    return connect(role=os.getenv("SNOWFLAKE_BI_ROLE", "TRADE_BI_ROLE"),
                   warehouse=os.getenv("SNOWFLAKE_BI_WAREHOUSE", "TRADE_BI_WH"), schema="CURATED")


def query(sql):
    with connection().cursor() as cur:
        cur.execute(sql)
        return pd.DataFrame(cur.fetchall(), columns=[c[0] for c in cur.description])


try:
    status = query("SELECT DERIVED_LIFECYCLE_STATUS AS TRADE_STATUS, COUNT(*) AS TRADE_COUNT "
                   "FROM DB_TRADE_DB.CURATED.V_CURRENT_TRADES GROUP BY 1")
    rejected = query("SELECT REJECT_REASON, COUNT(*) AS REJECTED_COUNT FROM DB_TRADE_DB.AUDIT.REJECTED_TRADES GROUP BY 1")
    left, right = st.columns(2)
    left.subheader("Current trade status")
    left.dataframe(status)
    if not status.empty:
        left.bar_chart(status.set_index("TRADE_STATUS"))
    right.subheader("Rejection reasons")
    right.dataframe(rejected)
    if not rejected.empty:
        right.bar_chart(rejected.set_index("REJECT_REASON"))
    st.subheader("Batch reconciliation")
    st.dataframe(query("SELECT BATCH_ID, RECON_STATUS, EXPECTED_FILES, ARRIVED_FILES, EXPECTED_RECORDS, INGESTED_RECORDS, "
                       "ACCEPTED_RECORDS, REJECTED_RECORDS, CHECKED_AT FROM DB_TRADE_DB.OPS.RECON_RESULTS "
                       "ORDER BY CHECKED_AT DESC LIMIT 50"))
    st.subheader("Latest rejected events")
    st.dataframe(query("SELECT EVENT_ID, TRADE_ID, VERSION, REJECT_REASON, BATCH_ID, REJECTED_AT "
                       "FROM DB_TRADE_DB.AUDIT.REJECTED_TRADES ORDER BY REJECTED_AT DESC LIMIT 100"))
except Exception as exc:  # noqa: BLE001
    st.error(f"Could not query Snowflake: {exc}. Check connection variables, object names and grants.")
