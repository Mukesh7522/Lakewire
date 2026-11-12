.PHONY: install lint test fixtures dbt-ci up down logs health
install:
	pip install -r requirements-dev.txt
lint:
	ruff check .
test:
	pytest -q
fixtures:
	python scripts/make_fixtures.py
dbt-ci:
	cd dbt && DBT_FIXTURES_DIR=../tests/fixtures DUCKDB_PATH=/tmp/ci.duckdb dbt build --target ci --profiles-dir .
up:
	docker compose up -d --build
down:
	docker compose down
logs:
	docker compose logs -f --tail=100
health:
	python3 scripts/healthcheck.py
