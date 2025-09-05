DOCKER_COMPOSE = docker compose -f docker/docker-compose.yml

build: ##Build environment
	$(DOCKER_COMPOSE) build --no-cache

start: ##Start containers in detached mode
	$(DOCKER_COMPOSE) up -d

stop: ##Stop containers
	$(DOCKER_COMPOSE) stop

restart: ##Restart running containers
	make stop && make start

logs: ##Display container logs
	$(DOCKER_COMPOSE) logs -f

tests: ##Run unit & integration tests
	go test -tags=unit,integration ./...

swagger: ##Generate swagger documentation
	@swag fmt
	@swag init
.PHONY: swagger