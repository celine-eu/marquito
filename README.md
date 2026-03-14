# marquito

Python/FastAPI port of the [Marquez](https://marquezproject.ai) data lineage API, compatible with the Marquez web frontend and OpenLineage clients.

This project is intended as a minimal alternative for the original Marquez to store lineage over HTTP and browse in the native Marquez UI.

## Quick start

### Basic usage

```bash
# 1. Start Postgres
docker compose up -d

# Server listening on :5000/:5001
```

### Development setup

```bash
# 1. Start Postgres
docker compose up db -d

# 2. Install deps
uv sync

# 3. Run migrations
uv run alembic upgrade head

# 4. Start the API
task run
# or: uv run uvicorn marquito.main:create_app --factory --host 0.0.0.0 --port 5000 --reload
```

## Docker

```bash
docker compose up --build
```

## API endpoints

### Admin
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/ping` | Liveness probe |
| GET | `/api/v1/healthcheck` | Health + DB check |

### Namespaces
| Method | Path | Description |
|--------|------|-------------|
| PUT | `/api/v1/namespaces/{namespace}` | Create or update namespace |
| GET | `/api/v1/namespaces/{namespace}` | Get namespace |
| GET | `/api/v1/namespaces` | List namespaces |
| DELETE | `/api/v1/namespaces/{namespace}` | Soft-delete namespace |

### Sources
| Method | Path | Description |
|--------|------|-------------|
| PUT | `/api/v1/sources/{source}` | Create or update source |
| GET | `/api/v1/sources/{source}` | Get source |
| GET | `/api/v1/sources` | List sources |

### Datasets
| Method | Path | Description |
|--------|------|-------------|
| PUT | `/api/v1/namespaces/{ns}/datasets/{ds}` | Create or update dataset |
| GET | `/api/v1/namespaces/{ns}/datasets/{ds}` | Get dataset |
| GET | `/api/v1/namespaces/{ns}/datasets` | List datasets |
| DELETE | `/api/v1/namespaces/{ns}/datasets/{ds}` | Soft-delete dataset |
| GET | `/api/v1/namespaces/{ns}/datasets/{ds}/versions` | List dataset versions |

### Jobs
| Method | Path | Description |
|--------|------|-------------|
| PUT | `/api/v1/namespaces/{ns}/jobs/{job}` | Create or update job |
| GET | `/api/v1/namespaces/{ns}/jobs/{job}` | Get job |
| GET | `/api/v1/namespaces/{ns}/jobs` | List jobs in namespace |
| DELETE | `/api/v1/namespaces/{ns}/jobs/{job}` | Soft-delete job |
| GET | `/api/v1/jobs` | List all jobs (global, supports `?lastRunStates=`) |

### Runs
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/namespaces/{ns}/jobs/{job}/runs` | Create run |
| GET | `/api/v1/namespaces/{ns}/jobs/{job}/runs` | List runs for job |
| GET | `/api/v1/runs/{id}` | Get run by UUID |
| POST | `/api/v1/runs/{id}/start` | Mark run as RUNNING |
| POST | `/api/v1/runs/{id}/complete` | Mark run as COMPLETED |
| POST | `/api/v1/runs/{id}/fail` | Mark run as FAILED |
| POST | `/api/v1/runs/{id}/abort` | Mark run as ABORTED |

### OpenLineage & Lineage
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/lineage` | Ingest OpenLineage event |
| GET | `/api/v1/lineage?nodeId=job:ns:name` | Get lineage graph for a node |
| GET | `/api/v1/events/lineage` | List raw lineage events |

### Search
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/search?q=...` | Search datasets and jobs by name |

Supports `filter=DATASET\|JOB`, `sort=name\|UPDATE_AT`, `limit`, `namespace`.

### Tags & Stats
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/tags` | List all tags |
| GET | `/api/v1/stats/lineage-events` | Lineage event metrics |
| GET | `/api/v1/stats/jobs` | Job metrics |
| GET | `/api/v1/stats/datasets` | Dataset metrics |

### GraphQL
| Method | Path | Description |
|--------|------|-------------|
| GET/POST | `/graphql` | GraphQL endpoint (GraphiQL UI at `/graphql`) |

## Environment variables

All variables are prefixed with `MARQUITO_`:

| Variable | Default | Description |
|----------|---------|-------------|
| `MARQUITO_DB_HOST` | `localhost` | Postgres host |
| `MARQUITO_DB_PORT` | `5432` | Postgres port |
| `MARQUITO_DB_NAME` | `marquito` | Database name |
| `MARQUITO_DB_USER` | `marquito` | Database user |
| `MARQUITO_DB_PASSWORD` | `marquito` | Database password |

## Tasks

```bash
task run           # Start dev server (hot reload)
task db:migrate    # Apply pending migrations
task db:revision   # Generate a new migration (-- -m "message")
```

## Tests

```bash
uv run pytest tests/ -v
```

Tests use SQLite in-memory by default. For full PostgreSQL testing:

```bash
TEST_DATABASE_URL=postgresql+asyncpg://user:pass@localhost/testdb uv run pytest tests/ -v
```

## Architecture

```
src/marquito/
├── main.py                    # FastAPI app factory, router registration
├── core/config.py             # Pydantic settings (env vars)
├── db/session.py              # Async SQLAlchemy engine + session
├── models/orm.py              # SQLAlchemy ORM models
├── schemas/
│   ├── api.py                 # Request/response schemas (camelCase output)
│   ├── dataset_versions.py    # Dataset version schema
│   ├── tags.py                # Tag schema
│   └── stats.py               # Stats schema
├── services/
│   ├── lineage.py             # Core CRUD, lineage graph, OL ingestion
│   ├── facets.py              # Facet read/write helpers
│   └── stats.py               # Metrics queries
├── graphql/schema.py          # Strawberry GraphQL schema
└── api/v1/endpoints/
    ├── admin.py               # /ping, /healthcheck
    ├── namespaces.py          # Namespaces, Sources
    ├── datasets.py            # Datasets, Jobs, Runs (namespace-scoped)
    ├── jobs.py                # Global jobs list
    ├── openlineage.py         # POST /lineage ingest, GET /lineage graph
    ├── events.py              # GET /events/lineage
    ├── search.py              # GET /search
    ├── tags.py                # GET /tags
    └── stats.py               # GET /stats/*
```

## Migrations

```
alembic/versions/
├── 0001_initial.py            # Core schema (namespaces, datasets, jobs, runs, lineage_events)
├── 0002_add_tags_table.py     # tags table
└── 0003_add_facets_columns.py # facets JSONB on datasets + jobs
```
