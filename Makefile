.PHONY: up down logs restart build test rebuild

build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f

test:
	docker-compose exec testrunner uv run pytest

restart: down up

rebuild: down build up
