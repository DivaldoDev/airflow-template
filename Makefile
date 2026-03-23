.PHONY: up down logs restart init fix-perms

# Ensure .env exists
fix-perms:
	@if [ ! -f .env ]; then cp .env.example .env; fi

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f airflow

restart: down up

init: fix-perms
	docker compose up -d postgres-airflow
	docker compose run --rm airflow-init
