DOCKER_COMPOSE = docker compose -f docker/docker-compose.yml

help: ##Show this help.
	@echo
	@echo "Choose a command to run in "$(PROJECT_NAME)":"
	@echo
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

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

bash: ##Connect to container bash
	$(DOCKER) exec -it db-integration-tests /bin/bash