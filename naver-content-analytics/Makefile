.PHONY: init gen schema load mart kpi api

init:
	python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

gen:
	python etl/generate_sample_logs.py

schema:
	psql -h localhost -p 5432 -U postgres -d analytics -f sql/01_schema.sql

load:
	python etl/load_to_postgres.py

mart:
	python etl/build_mart.py

kpi:
	psql -h localhost -p 5432 -U postgres -d analytics -f sql/02_kpi_queries.sql

api:
	uvicorn backend.main:app --reload --port 8000
