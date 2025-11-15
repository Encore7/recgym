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
