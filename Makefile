COMPOSE_FILE=infra/docker-compose.yml

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
