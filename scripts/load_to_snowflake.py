"""Upload JSONL to Snowflake internal stage and bulk load via COPY INTO."""
import argparse
import os
from pathlib import Path
import snowflake.connector
from dotenv import load_dotenv


def connect():
    load_dotenv()
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError("Missing environment variables: " + ", ".join(missing))
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"], role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "TRADE_PIPELINE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "TRADE_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"), autocommit=False,
    )


def load_file(path):
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(path)
    stage = os.getenv("SNOWFLAKE_STAGE", "TRADE_DB.RAW.TRADE_STAGE")
    target = os.getenv("SNOWFLAKE_RAW_TABLE", "TRADE_DB.RAW.TRADE_EVENTS")
    prefix = f"uploads/{path.stem}"
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"PUT file://{path.resolve().as_posix()} @{stage}/{prefix} AUTO_COMPRESS=TRUE OVERWRITE=FALSE")
            sql = f"""
            COPY INTO {target} (
              EVENT_ID, TRADE_ID, VERSION, INSTRUMENT, TRADE_SIDE, QUANTITY, PRICE,
              CURRENCY, TRADE_DATE, MATURITY_DATE, COUNTERPARTY, EVENT_TIMESTAMP,
              SOURCE_SYSTEM, BATCH_ID, RAW_PAYLOAD
            )
            FROM (
              SELECT $1:event_id::STRING, $1:trade_id::STRING,
                TRY_TO_NUMBER($1:version::STRING), $1:instrument::STRING,
                UPPER($1:trade_side::STRING), TRY_TO_DECIMAL($1:quantity::STRING,38,8),
                TRY_TO_DECIMAL($1:price::STRING,38,8), UPPER($1:currency::STRING),
                TRY_TO_DATE($1:trade_date::STRING), TRY_TO_DATE($1:maturity_date::STRING),
                $1:counterparty::STRING,
                TRY_TO_TIMESTAMP_NTZ(REPLACE($1:event_timestamp::STRING,'Z','')),
                $1:source_system::STRING, $1:batch_id::STRING, $1
              FROM @{stage}/{prefix}
            )
            FILE_FORMAT=(FORMAT_NAME=TRADE_DB.RAW.TRADE_JSON_FORMAT)
            ON_ERROR='CONTINUE'
            """
            cur.execute(sql)
            for row in cur.fetchall():
                print(row)
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    load_file(parser.parse_args().file)
