.PHONY: init gen gen-dummy schema load mart kpi advanced-kpi api

init:
	python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

gen:
	python etl/generate_sample_logs.py

gen-dummy:
	python etl/generate_dummy_data.py --seed 20260317 --output-dir data/raw

schema:
	psql -h localhost -p 5432 -U postgres -d analytics -f sql/03_dw_schema.sql

load:
	python etl/load_to_postgres.py

mart:
	python etl/build_mart.py

kpi:
	psql -h localhost -p 5432 -U postgres -d analytics -f sql/02_kpi_queries.sql

advanced-kpi:
	psql -h localhost -p 5432 -U postgres -d analytics -f sql/04_advanced_analytics.sql

api:
	uvicorn backend.main:app --reload --port 8000
