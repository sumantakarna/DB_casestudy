.PHONY: setup generate verify load test dashboard dbt-parse
BATCH ?= DEMO-001

setup:
	pip install -r requirements.txt -r requirements-dev.txt

generate:
	python scripts/generate_trades.py --out sample_data --rows 1000 --format all --batch-id $(BATCH) --inject-errors

verify:
	python scripts/load_to_snowflake.py --dir sample_data --batch-id $(BATCH) --dry-run

load:
	python scripts/load_to_snowflake.py --dir sample_data --batch-id $(BATCH)

test:
	python -m pytest -q

dbt-parse:
	cd dbt_project && dbt parse --profiles-dir ci

dashboard:
	streamlit run streamlit_app/app.py
