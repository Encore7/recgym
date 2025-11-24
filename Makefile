COMPOSE_FILE=docker-compose.yml

.PHONY: up down ps logs restart build rebuild

up:
	@docker compose -f $(COMPOSE_FILE) up -d

down:
	@docker compose -f $(COMPOSE_FILE) down -v

ps:
	@docker compose -f $(COMPOSE_FILE) ps

logs:
	@docker compose -f $(COMPOSE_FILE) logs -f

restart:
	@$(MAKE) down && $(MAKE) up

build:
	@docker compose -f $(COMPOSE_FILE) build

rebuild:
	@docker compose -f $(COMPOSE_FILE) build --no-cache

feast-apply:
	docker compose run --rm feast feast apply --repo /app/feast

feast-materialize:
	docker compose run --rm feast feast materialize-incremental $(shell date -Iseconds)

feast-materialize-backfill:
	docker compose run --rm feast feast materialize 2020-01-01T00:00:00Z $(shell date -Iseconds)

