import os
import pandas as pd
import streamlit as st
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
st.set_page_config(page_title="Trade Pipeline Monitor", layout="wide")
st.title("Trade Processing Monitor")
st.caption("Current trade status and rejection audit.")


@st.cache_resource
def connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"], role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "TRADE_PIPELINE_WH"),
        database="TRADE_DB", schema="CURATED")


def query(sql):
    with connection().cursor() as cur:
        cur.execute(sql)
        return pd.DataFrame(cur.fetchall(), columns=[c[0] for c in cur.description])


try:
    status = query("SELECT TRADE_STATUS, COUNT(*) AS TRADE_COUNT FROM TRADE_DB.CURATED.CURRENT_TRADES GROUP BY 1")
    rejected = query("SELECT REJECTION_REASON, COUNT(*) AS REJECTED_COUNT FROM TRADE_DB.AUDIT.REJECTED_TRADES GROUP BY 1")
    left, right = st.columns(2)
    left.subheader("Current trade status")
    left.dataframe(status, use_container_width=True)
    if not status.empty:
        left.bar_chart(status.set_index("TRADE_STATUS"))
    right.subheader("Rejection reasons")
    right.dataframe(rejected, use_container_width=True)
    if not rejected.empty:
        right.bar_chart(rejected.set_index("REJECTION_REASON"))
    st.subheader("Latest rejected events")
    st.dataframe(query("""SELECT EVENT_ID, TRADE_ID, VERSION, REJECTION_REASON, BATCH_ID, REJECTED_AT
                          FROM TRADE_DB.AUDIT.REJECTED_TRADES ORDER BY REJECTED_AT DESC LIMIT 100"""),
                 use_container_width=True)
except Exception:
    st.error("Could not query Snowflake. Verify connection variables, object names, and grants.")
    st.exception(Exception("Dashboard query failed; see the app logs for details."))
