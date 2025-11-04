up:
\tdocker compose -f infra/docker-compose.yml up -d

down:
\tdocker compose -f infra/docker-compose.yml down -v

ps:
\tdocker compose -f infra/docker-compose.yml ps

logs:
\tdocker compose -f infra/docker-compose.yml logs -f

restart:
\tmake down && make up
