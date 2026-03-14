# marquito-py — Python/FastAPI rewrite of the Marquez metadata service

## Quick start

```bash
# 1. Start Postgres
docker compose up db -d

# 2. Install deps
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Run migrations
alembic upgrade head

# 4. Start the API
uvicorn app.main:app --reload --port 5000
```

## Or with Docker

```bash
docker compose up --build
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/ping` | Liveness probe |
| GET | `/api/v1/healthcheck` | Health + DB check |
| PUT | `/api/v1/namespaces/{namespace}` | Create/update namespace |
| GET | `/api/v1/namespaces` | List namespaces |
| PUT | `/api/v1/sources/{source}` | Create/update source |
| GET | `/api/v1/sources` | List sources |
| PUT | `/api/v1/namespaces/{ns}/datasets/{ds}` | Create/update dataset |
| GET | `/api/v1/namespaces/{ns}/datasets` | List datasets |
| PUT | `/api/v1/namespaces/{ns}/jobs/{job}` | Create/update job |
| GET | `/api/v1/namespaces/{ns}/jobs` | List jobs |
| POST | `/api/v1/namespaces/{ns}/jobs/{job}/runs` | Create run |
| POST | `/api/v1/runs/{id}/start` | Mark run started |
| POST | `/api/v1/runs/{id}/complete` | Mark run complete |
| POST | `/api/v1/runs/{id}/fail` | Mark run failed |
| POST | `/api/v1/lineage` | Ingest OpenLineage event |
| GET | `/api/v1/lineage?nodeId=ns:name&nodeType=JOB` | Get lineage graph |
| GET/POST | `/graphql` | GraphQL (+ GraphiQL UI) |

## Environment variables

All variables are prefixed with `MARQUITO_`:

| Variable | Default |
|----------|---------|
| `MARQUITO_DB_HOST` | `localhost` |
| `MARQUITO_DB_PORT` | `5432` |
| `MARQUITO_DB_NAME` | `marquito` |
| `MARQUITO_DB_USER` | `marquito` |
| `MARQUITO_DB_PASSWORD` | `marquito` |

## Running tests

```bash
pip install aiosqlite pytest-asyncio
pytest tests/ -v
```

## Architecture

```
app/
├── main.py              # FastAPI app factory
├── core/config.py       # Pydantic settings
├── db/session.py        # Async SQLAlchemy engine + session
├── models/orm.py        # SQLAlchemy ORM models
├── schemas/api.py       # Pydantic request/response schemas
├── services/lineage.py  # Business logic (CRUD + lineage graph)
├── graphql/schema.py    # Strawberry GraphQL schema
└── api/v1/endpoints/
    ├── admin.py         # /ping, /healthcheck
    ├── namespaces.py    # Namespaces + Sources
    ├── datasets.py      # Datasets, Jobs, Runs
    └── openlineage.py   # POST /lineage ingest, GET /lineage graph
```
