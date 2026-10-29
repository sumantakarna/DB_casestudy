.PHONY: setup generate load test dashboard
setup:
	pip install -r requirements.txt
generate:
	python scripts/generate_trades.py --output data/trades.jsonl --count 100 --batch-id demo-batch
load:
	python scripts/load_to_snowflake.py --file data/trades.jsonl
test:
	pytest -q
dashboard:
	streamlit run streamlit_app/app.py
