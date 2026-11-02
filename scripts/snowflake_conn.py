"""Single place that builds a Snowflake connection (key-pair auth preferred)."""
import os


def connect(**overrides):
    import snowflake.connector  # imported lazily so unit tests need no Snowflake packages
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:  # python-dotenv is optional
        pass
    missing = [k for k in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER") if not os.getenv(k)]
    if missing:
        raise RuntimeError("Missing environment variables: " + ", ".join(missing))
    params = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.getenv("SNOWFLAKE_ROLE", "TRADE_INGEST_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "TRADE_LOAD_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "DB_TRADE_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    )
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    if key_path:
        params["private_key_file"] = key_path
        if os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"):
            params["private_key_file_pwd"] = os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"]
    elif os.getenv("SNOWFLAKE_PASSWORD"):  # local demos only
        params["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    else:
        raise RuntimeError("Set SNOWFLAKE_PRIVATE_KEY_PATH (preferred) or SNOWFLAKE_PASSWORD")
    params.update(overrides)
    return snowflake.connector.connect(**params)
