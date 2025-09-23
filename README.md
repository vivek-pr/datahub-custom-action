# DataHub Tokenization Action POC

This repository packages a minimal DataHub deployment (via `docker-compose`) together with a custom action service that demonstrates end-to-end PII tokenization. The action is triggered when the `tokenize/run` tag is applied to a dataset or individual fields and performs the following:

* discovers PII columns via tags or name heuristics
* tokenizes values in the source systems (Postgres in this POC, Databricks optional)
* records run status, documentation and editable properties back in DataHub
* flips dataset tags between `tokenize/run`, `tokenize/done` and `tokenize/status:*`

Everything is runnable from a clean checkout using the provided Makefile targets and shell helpers.

## Prerequisites

* Docker Engine and Docker Compose Plugin (or `docker-compose` v1)
* Python 3.8+ available on the host for helper scripts
* `curl` for API examples

If you plan to ingest from Databricks, set the Databricks connection variables in `.env` before running the optional ingestion recipe.

## Quickstart

```bash
# Copy defaults and update credentials if needed
cp .env.example .env

# Build the custom action image and start the stack
make build
make up

# Ingest the sample Postgres dataset (automatically tagged with tokenize/run)
make ingest

# End-to-end demo: tag trigger -> wait for success -> verify idempotency
make e2e

# Tear everything down, removing volumes
make down
```

The `make e2e` target chains `build → up → ingest → trigger-ui → wait-status → verify-idempotent`. Use the individual targets if you prefer to run steps manually.

## Repository Layout

```
.
├─ docker-compose.yml            # Minimal DataHub + dependencies + Postgres + action
├─ .env.example                  # Default environment variable values
├─ Makefile                      # Convenience targets for the entire flow
├─ docker/
│  └─ action.Dockerfile          # Builds the FastAPI + consumer service used as datahub-actions
├─ action/                       # Custom action implementation
│  ├─ app.py                     # FastAPI app exposing /healthz and /trigger
│  ├─ mcl_consumer.py            # Kafka MetadataChangeLog consumer (tag triggers)
│  ├─ run_manager.py             # Run orchestration, status updates and tag flips
│  ├─ datahub_client.py          # GraphQL + REST helpers for DataHub
│  ├─ pii_detector.py            # PII detection logic
│  ├─ token_logic.py             # Deterministic tok_<base64>_poc implementation
│  ├─ db_pg.py                   # Postgres tokenization (transactional)
│  ├─ db_dbx.py                  # Optional Databricks support (skips if unconfigured)
│  ├─ types.py                   # Shared dataclasses
│  └─ requirements.txt           # Python dependencies bundled into the action image
├─ ingestion/
│  ├─ postgres.yml               # Postgres ingestion recipe (adds tokenize/run tag)
│  └─ databricks.yml             # Template for optional Databricks ingestion
└─ scripts/
   ├─ seed_pg.sh                 # Seeds the Postgres customers table with sample data
   ├─ add_tag.sh                 # Applies tokenize/run to a dataset via the action container
   ├─ poll_status.sh             # Polls dataset status until SUCCESS/FAILED
   ├─ find_dataset_urn.py        # Helper to resolve dataset URNs via GraphQL
   └─ e2e.sh                     # Orchestrates trigger → wait → API trigger demo
```

## Triggering Tokenization

### From the UI

1. Open the DataHub UI (`http://localhost:9002` by default).
2. Navigate to the dataset you want to tokenize (e.g. **customers** under the Postgres platform).
3. Click **Add Tag** and select `tokenize/run` on the dataset or specific fields (e-mail, phone).
4. The action service consumes the MetadataChangeLog event and starts a tokenization run. Progress is reflected on the dataset page under **Documentation**, **Custom Properties**, and **Tags**.

### From the API

Send a POST request to the FastAPI endpoint running inside the action container:

```bash
curl -X POST http://localhost:8081/trigger \
  -H 'Content-Type: application/json' \
  -d '{
        "dataset": "urn:li:dataset:(urn:li:dataPlatform:postgres,tokenize.public.customers,PROD)",
        "columns": ["email", "phone"]
      }'
```

The response contains run metadata (run id, row counts, status). If `columns` is omitted the action falls back to tag detection heuristics.

## Inspecting Results in DataHub

After a successful run:

* Dataset **Documentation** shows a Markdown summary including timestamps, columns, row counts, and run id.
* **Editable Properties** includes a JSON blob under `last_tokenization_run` with the full payload.
* Tags are rotated so the dataset holds `tokenize/done` and `tokenize/status:SUCCESS`. On failure the dataset retains `tokenize/run` alongside `tokenize/status:FAILED`.

The Postgres `customers` table is updated in place using the deterministic `tok_<base64>_poc` format. Re-triggering the same dataset (via UI or API) results in `rows_updated=0`, proving idempotency.

## Optional Databricks Path

Populate the following environment variables in `.env` to enable Databricks runs:

* `DBX_JDBC_URL`, `DBX_TOKEN`, `DBX_HTTP_PATH`, `DBX_CATALOG`

Update `ingestion/databricks.yml` with the correct host/catalog and run:

```bash
make ingest DBX_ENABLED=true   # or invoke docker compose exec ... manually
```

When Databricks credentials are missing, the action logs a skip message and returns a `TokenizationResult` with `rows_updated=0` and a `details` note.

## Troubleshooting

* **Zookeeper stays unhealthy** – set `DOCKER_PLATFORM=linux/amd64` and retry `make up` if you're on an architecture unsupported by the bundled images. You can confirm the health check manually:

  ```bash
  docker exec -it <zookeeper> sh -lc 'printf ruok | nc -w 2 localhost 2181'
  # expect: imok
  ```
* **Services keep restarting** – check `docker compose logs datahub-gms` and confirm Schema Registry, Kafka, and MySQL are healthy (`docker compose ps`).
* **find_dataset_urn.py returns nothing** – ensure the dataset has been ingested (`make ingest`) and that `DATAHUB_GMS` / `DATAHUB_TOKEN` are set in your environment when running the script.
* **poll_status.sh times out** – inspect the action logs (`docker compose logs datahub-actions`) for errors. The run summary stored on the dataset will include any exception message.
* **psql command fails during seeding** – confirm the Postgres container is healthy (`docker compose ps postgres`). You can rerun `./scripts/seed_pg.sh` at any time.
* **Databricks tokenization skipped** – verify the optional environment variables are set; otherwise the connector intentionally returns with `details="Databricks connection not configured"`.

## Cleanup

Run `make down` to stop containers and remove volumes. This also resets Postgres and DataHub state, making the POC repeatable from scratch.
