ifndef COMPOSE
COMPOSE := $(shell if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then echo "docker compose"; fi)
ifeq ($(strip $(COMPOSE)),)
COMPOSE := $(shell if command -v docker-compose >/dev/null 2>&1; then echo "docker-compose"; fi)
endif
ifeq ($(strip $(COMPOSE)),)
$(error "docker compose or docker-compose not found. Set COMPOSE=/path/to/binary to override")
endif
endif
DATASET_NAME ?= customers
DATASET_PLATFORM ?= postgres
TIMEOUT ?= 600

.PHONY: build up ingest trigger-ui trigger-api wait-status verify-idempotent e2e down diag

build:
	$(COMPOSE) build datahub-actions

up:
	$(COMPOSE) up -d
	$(COMPOSE) ps mysql
	@CONTAINER=$$($(COMPOSE) ps -q mysql); \
	if [ -n "$$CONTAINER" ]; then \
			docker inspect --format '{{.State.Health.Status}}' $$CONTAINER | grep -q healthy || { \
					$(COMPOSE) logs --no-color mysql datahub-custom-action-mysql-setup || true; \
					exit 1; \
			}; \
	else \
			echo "Failed to resolve mysql container"; \
			$(COMPOSE) logs --no-color mysql datahub-custom-action-mysql-setup || true; \
			exit 1; \
	fi
	$(COMPOSE) ps zookeeper
	@CONTAINER=$$($(COMPOSE) ps -q zookeeper); \
	if [ -n "$$CONTAINER" ]; then \
	docker inspect --format '{{.State.Health.Status}}' $$CONTAINER | grep -q healthy || { \
		$(COMPOSE) logs --no-color zookeeper broker || true; \
		exit 1; \
	}; \
	else \
	echo "Failed to resolve zookeeper container"; \
	$(COMPOSE) logs --no-color zookeeper broker || true; \
	exit 1; \
	fi
	$(COMPOSE) wait datahub-gms datahub-actions postgres
	./scripts/seed_pg.sh

ingest:
	$(COMPOSE) exec -T datahub-actions datahub ingest -c /app/ingestion/postgres.yml

trigger-ui:
	@URN=$$(python3 scripts/find_dataset_urn.py $(DATASET_NAME) $(DATASET_PLATFORM) | head -n 1 | cut -f1) && \
		echo "Triggering via tag for $$URN" && \
		./scripts/add_tag.sh $$URN

trigger-api:
	@URN=$$(python3 scripts/find_dataset_urn.py $(DATASET_NAME) $(DATASET_PLATFORM) | head -n 1 | cut -f1) && \
		echo "Triggering via API for $$URN" && \
		curl -s -X POST -H 'Content-Type: application/json' -d "{\"dataset\": \"$$URN\"}" http://localhost:8081/trigger | tee /tmp/tokenize-api.json

wait-status:
	@URN=$$(python3 scripts/find_dataset_urn.py $(DATASET_NAME) $(DATASET_PLATFORM) | head -n 1 | cut -f1) && \
		./scripts/poll_status.sh $$URN $(TIMEOUT)

verify-idempotent:
	@URN=$$(python3 scripts/find_dataset_urn.py $(DATASET_NAME) $(DATASET_PLATFORM) | head -n 1 | cut -f1) && \
	RESPONSE=$$(curl -s -X POST -H 'Content-Type: application/json' -d "{\"dataset\": \"$$URN\"}" http://localhost:8081/trigger) && \
	echo "Idempotency response: $$RESPONSE" && \
	python3 - "$$RESPONSE" <<'PY'
	import json, sys
	response = json.loads(sys.argv[1])
	rows = response.get("rows_updated")
	if rows not in (0, "0"):
	    raise SystemExit(f"Expected 0 rows updated, got {rows}")
	PY

e2e: build up ingest trigger-ui wait-status verify-idempotent

down:
	$(COMPOSE) down -v

diag:
	$(COMPOSE) ps
	$(COMPOSE) logs --tail=200 zookeeper broker
