"""
Integration tests for the Marquez-compatible REST API.

Based on the Java Marquez integration test suite:
  - MarquezAppIntegrationTest
  - DatasetIntegrationTest
  - RunIntegrationTest
  - OpenLineageIntegrationTest
  - TagIntegrationTest

Run with:  pytest tests/ -v

Uses SQLite in-memory by default. For full PostgreSQL (JSONB) support:
    TEST_DATABASE_URL=postgresql+asyncpg://user:pass@localhost/testdb pytest tests/ -v
"""

from __future__ import annotations

import os
import uuid
from urllib.parse import quote

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from marquito.db.session import Base, get_db
from marquito.main import app

TEST_DB_URL = os.environ.get("TEST_DATABASE_URL", "sqlite+aiosqlite:///:memory:")

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="function")
async def db_engine():
    engine = create_async_engine(TEST_DB_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture
async def client(db_engine):
    TestSession = async_sessionmaker(
        db_engine, class_=AsyncSession, expire_on_commit=False
    )

    async def override_get_db():
        async with TestSession() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[get_db] = override_get_db
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE = "/api/v1"


async def create_namespace(client, name="test-ns", owner="alice", description="test"):
    resp = await client.put(
        f"{BASE}/namespaces/{name}",
        json={"owner_name": owner, "description": description},
    )
    assert resp.status_code == 200
    return resp.json()


async def create_source(client, name="test-source", type_="POSTGRESQL", url="jdbc:postgresql://host/db"):
    resp = await client.put(
        f"{BASE}/sources/{name}",
        json={"type": type_, "connection_url": url, "description": "a source"},
    )
    assert resp.status_code == 200
    return resp.json()


async def create_dataset(client, ns, name, fields=None, tags=None, source=None):
    body = {"type": "DB_TABLE", "physical_name": name}
    if fields:
        body["fields"] = fields
    if tags:
        body["tags"] = tags
    if source:
        body["source_name"] = source
    resp = await client.put(f"{BASE}/namespaces/{ns}/datasets/{name}", json=body)
    assert resp.status_code == 200
    return resp.json()


async def create_job(client, ns, name, inputs=None, outputs=None):
    body = {"type": "BATCH", "inputs": inputs or [], "outputs": outputs or []}
    resp = await client.put(f"{BASE}/namespaces/{ns}/jobs/{name}", json=body)
    assert resp.status_code == 200
    return resp.json()


async def ingest_event(client, event):
    resp = await client.post(f"{BASE}/lineage", json=event)
    assert resp.status_code == 201
    return resp


def ol_event(
    event_type="COMPLETE",
    ns="test-ns",
    job="test-job",
    run_id=None,
    inputs=None,
    outputs=None,
    producer="test",
    event_time="2024-01-01T00:00:00Z",
):
    return {
        "eventType": event_type,
        "eventTime": event_time,
        "run": {"runId": run_id or str(uuid.uuid4()), "facets": {}},
        "job": {"namespace": ns, "name": job, "facets": {}},
        "inputs": inputs or [],
        "outputs": outputs or [],
        "producer": producer,
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
    }


# ---------------------------------------------------------------------------
# Ping / health
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ping(client):
    resp = await client.get(f"{BASE}/ping")
    assert resp.status_code == 200
    assert resp.json() == {"message": "pong"}


# ---------------------------------------------------------------------------
# Namespaces
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_namespace(client):
    resp = await client.put(
        f"{BASE}/namespaces/my-namespace",
        json={"owner_name": "alice", "description": "test namespace"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "my-namespace"
    assert data["ownerName"] == "alice"   # validation_alias maps current_owner_name → owner_name
    assert data["description"] == "test namespace"
    assert "createdAt" in data
    assert "updatedAt" in data


@pytest.mark.asyncio
async def test_create_namespace_no_description(client):
    resp = await client.put(
        f"{BASE}/namespaces/bare-ns",
        json={"owner_name": "bob"},
    )
    assert resp.status_code == 200
    assert resp.json()["name"] == "bare-ns"


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["DEFAULT", "bigquery:", "my_namespace_123"])
async def test_create_namespace_special_chars(client, name):
    resp = await client.put(
        f"{BASE}/namespaces/{quote(name, safe='')}",
        json={"owner_name": "owner"},
    )
    assert resp.status_code == 200
    assert resp.json()["name"] == name


@pytest.mark.asyncio
async def test_get_namespace(client):
    await create_namespace(client, "get-me")
    resp = await client.get(f"{BASE}/namespaces/get-me")
    assert resp.status_code == 200
    assert resp.json()["name"] == "get-me"


@pytest.mark.asyncio
async def test_get_namespace_not_found(client):
    resp = await client.get(f"{BASE}/namespaces/does-not-exist")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_namespaces(client):
    await create_namespace(client, "ns-a")
    await create_namespace(client, "ns-b")
    resp = await client.get(f"{BASE}/namespaces")
    assert resp.status_code == 200
    data = resp.json()
    assert "namespaces" in data
    assert "totalCount" in data
    names = [n["name"] for n in data["namespaces"]]
    assert "ns-a" in names
    assert "ns-b" in names


@pytest.mark.asyncio
async def test_update_namespace_owner(client):
    await create_namespace(client, "owned-ns", owner="alice")
    resp = await client.put(
        f"{BASE}/namespaces/owned-ns",
        json={"owner_name": "bob"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ownerName"] == "bob"


@pytest.mark.asyncio
async def test_delete_namespace(client):
    await create_namespace(client, "delete-me")
    resp = await client.delete(f"{BASE}/namespaces/delete-me")
    assert resp.status_code == 200

    # After deletion, it should not appear in list
    list_resp = await client.get(f"{BASE}/namespaces")
    names = [n["name"] for n in list_resp.json()["namespaces"]]
    assert "delete-me" not in names


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_source(client):
    resp = await client.put(
        f"{BASE}/sources/my-source",
        json={"type": "POSTGRESQL", "connection_url": "jdbc:postgresql://host/db", "description": "pg source"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "my-source"
    assert data["type"] == "POSTGRESQL"
    assert data["connectionUrl"] == "jdbc:postgresql://host/db"
    assert "createdAt" in data
    assert "updatedAt" in data


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["test_source312", "asdf", "bigquery:"])
async def test_create_source_special_names(client, name):
    resp = await client.put(
        f"{BASE}/sources/{quote(name, safe='')}",
        json={"type": "POSTGRESQL", "connection_url": "jdbc:postgresql://host/db"},
    )
    assert resp.status_code == 200
    assert resp.json()["name"] == name


@pytest.mark.asyncio
async def test_get_source(client):
    await create_source(client, "get-source")
    resp = await client.get(f"{BASE}/sources/get-source")
    assert resp.status_code == 200
    assert resp.json()["name"] == "get-source"


@pytest.mark.asyncio
async def test_get_source_not_found(client):
    resp = await client.get(f"{BASE}/sources/ghost")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_sources(client):
    await create_source(client, "src-x")
    await create_source(client, "src-y")
    resp = await client.get(f"{BASE}/sources")
    assert resp.status_code == 200
    data = resp.json()
    names = [s["name"] for s in data["sources"]]
    assert "src-x" in names
    assert "src-y" in names
    assert data["totalCount"] >= 2


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_dataset(client):
    await create_namespace(client, "ds-ns")
    resp = await create_dataset(client, "ds-ns", "orders", fields=[
        {"name": "id", "type": "INTEGER"},
        {"name": "amount", "type": "DECIMAL"},
    ])
    assert resp["name"] == "orders"
    assert resp["namespace"] == "ds-ns"
    assert resp["id"] == "ds-ns:orders"
    assert "createdAt" in resp
    assert "updatedAt" in resp
    fields = {f["name"]: f for f in resp["fields"]}
    assert "id" in fields
    assert "amount" in fields


@pytest.mark.asyncio
async def test_create_dataset_with_source(client):
    await create_namespace(client, "ds-ns2")
    await create_source(client, "my-pg")
    resp = await create_dataset(client, "ds-ns2", "events", source="my-pg")
    assert resp["sourceName"] == "my-pg"


@pytest.mark.asyncio
async def test_create_dataset_with_tags(client):
    await create_namespace(client, "tag-ns")
    resp = await client.put(
        f"{BASE}/namespaces/tag-ns/datasets/tagged-table",
        json={
            "type": "DB_TABLE",
            "physical_name": "tagged-table",
            "tags": ["PII", "SENSITIVE"],
            "fields": [
                {"name": "email", "type": "STRING", "tags": ["PII"]},
                {"name": "score", "type": "INTEGER"},
            ],
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert set(data["tags"]) == {"PII", "SENSITIVE"}
    field_map = {f["name"]: f for f in data["fields"]}
    assert "PII" in field_map["email"]["tags"]
    assert field_map["score"]["tags"] == []


@pytest.mark.asyncio
async def test_get_dataset(client):
    await create_namespace(client, "get-ns")
    await create_dataset(client, "get-ns", "mytable")
    resp = await client.get(f"{BASE}/namespaces/get-ns/datasets/mytable")
    assert resp.status_code == 200
    assert resp.json()["name"] == "mytable"


@pytest.mark.asyncio
async def test_get_dataset_not_found(client):
    await create_namespace(client, "miss-ns")
    resp = await client.get(f"{BASE}/namespaces/miss-ns/datasets/ghost")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_datasets(client):
    await create_namespace(client, "list-ns")
    await create_dataset(client, "list-ns", "table-a")
    await create_dataset(client, "list-ns", "table-b")
    resp = await client.get(f"{BASE}/namespaces/list-ns/datasets")
    assert resp.status_code == 200
    data = resp.json()
    assert "datasets" in data
    assert "totalCount" in data
    names = [d["name"] for d in data["datasets"]]
    assert "table-a" in names
    assert "table-b" in names


@pytest.mark.asyncio
async def test_dataset_field_change(client):
    """Updating a dataset with new fields replaces the schema (mirrors testDatasetFieldChange)."""
    await create_namespace(client, "field-ns")
    await create_dataset(client, "field-ns", "evolving", fields=[
        {"name": "a", "type": "INTEGER"},
        {"name": "b", "type": "TIMESTAMP"},
    ])

    resp = await client.put(
        f"{BASE}/namespaces/field-ns/datasets/evolving",
        json={
            "type": "DB_TABLE",
            "physical_name": "evolving",
            "fields": [
                {"name": "a", "type": "INTEGER"},
                {"name": "b_fix", "type": "STRING"},
            ],
        },
    )
    assert resp.status_code == 200
    fields = {f["name"] for f in resp.json()["fields"]}
    assert "b_fix" in fields
    assert "a" in fields


@pytest.mark.asyncio
async def test_dataset_with_null_field_type(client):
    """Fields with null type should be accepted (testDatasetWithUnknownFieldType)."""
    await create_namespace(client, "null-type-ns")
    resp = await client.put(
        f"{BASE}/namespaces/null-type-ns/datasets/nullable-fields",
        json={
            "type": "DB_TABLE",
            "physical_name": "nullable-fields",
            "fields": [
                {"name": "field0", "type": "CUSTOM_TYPE"},
                {"name": "field1"},
            ],
        },
    )
    assert resp.status_code == 200
    fields = {f["name"]: f for f in resp.json()["fields"]}
    assert "field0" in fields
    assert "field1" in fields


@pytest.mark.asyncio
async def test_delete_dataset(client):
    await create_namespace(client, "del-ns")
    await create_dataset(client, "del-ns", "to-delete")
    resp = await client.delete(f"{BASE}/namespaces/del-ns/datasets/to-delete")
    assert resp.status_code == 200

    list_resp = await client.get(f"{BASE}/namespaces/del-ns/datasets")
    names = [d["name"] for d in list_resp.json()["datasets"]]
    assert "to-delete" not in names


@pytest.mark.asyncio
async def test_delete_dataset_not_found(client):
    await create_namespace(client, "del-ns2")
    resp = await client.delete(f"{BASE}/namespaces/del-ns2/datasets/ghost")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_dataset_versions(client):
    """Creating a dataset via PUT should record a version."""
    await create_namespace(client, "ver-ns")
    await create_dataset(client, "ver-ns", "versioned")

    resp = await client.get(f"{BASE}/namespaces/ver-ns/datasets/versioned/versions")
    assert resp.status_code == 200
    data = resp.json()
    assert "versions" in data
    assert "totalCount" in data
    assert data["totalCount"] >= 1


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_job(client):
    await create_namespace(client, "job-ns")
    resp = await client.put(
        f"{BASE}/namespaces/job-ns/jobs/my-job",
        json={"type": "BATCH", "description": "my etl job", "inputs": [], "outputs": []},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "my-job"
    assert data["namespace"] == "job-ns"
    assert data["id"] == "job-ns:my-job"
    assert "createdAt" in data
    assert "updatedAt" in data


@pytest.mark.asyncio
async def test_get_job(client):
    await create_namespace(client, "get-job-ns")
    await create_job(client, "get-job-ns", "get-me")
    resp = await client.get(f"{BASE}/namespaces/get-job-ns/jobs/get-me")
    assert resp.status_code == 200
    assert resp.json()["name"] == "get-me"


@pytest.mark.asyncio
async def test_get_job_not_found(client):
    await create_namespace(client, "miss-job-ns")
    resp = await client.get(f"{BASE}/namespaces/miss-job-ns/jobs/ghost")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_jobs(client):
    await create_namespace(client, "list-job-ns")
    await create_job(client, "list-job-ns", "job-a")
    await create_job(client, "list-job-ns", "job-b")
    resp = await client.get(f"{BASE}/namespaces/list-job-ns/jobs")
    assert resp.status_code == 200
    data = resp.json()
    names = [j["name"] for j in data["jobs"]]
    assert "job-a" in names
    assert "job-b" in names
    assert data["totalCount"] >= 2


@pytest.mark.asyncio
async def test_list_all_jobs(client):
    await create_namespace(client, "global-ns")
    await create_job(client, "global-ns", "global-job")
    resp = await client.get(f"{BASE}/jobs")
    assert resp.status_code == 200
    names = [j["name"] for j in resp.json()["jobs"]]
    assert "global-job" in names


@pytest.mark.asyncio
async def test_delete_job(client):
    await create_namespace(client, "del-job-ns")
    await create_job(client, "del-job-ns", "delete-me")
    resp = await client.delete(f"{BASE}/namespaces/del-job-ns/jobs/delete-me")
    assert resp.status_code == 200

    list_resp = await client.get(f"{BASE}/namespaces/del-job-ns/jobs")
    names = [j["name"] for j in list_resp.json()["jobs"]]
    assert "delete-me" not in names


@pytest.mark.asyncio
async def test_delete_job_not_found(client):
    await create_namespace(client, "del-job-ns2")
    resp = await client.delete(f"{BASE}/namespaces/del-job-ns2/jobs/ghost")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_run(client):
    await create_namespace(client, "run-ns")
    await create_job(client, "run-ns", "run-job")
    resp = await client.post(f"{BASE}/namespaces/run-ns/jobs/run-job/runs", json={})
    assert resp.status_code == 201
    data = resp.json()
    assert "id" in data
    assert data["state"] == "NEW"


@pytest.mark.asyncio
async def test_run_state_transitions(client):
    """NEW → RUNNING → COMPLETED (mirrors RunIntegrationTest)."""
    await create_namespace(client, "state-ns")
    await create_job(client, "state-ns", "state-job")
    create_resp = await client.post(f"{BASE}/namespaces/state-ns/jobs/state-job/runs", json={})
    assert create_resp.status_code == 201
    run_id = create_resp.json()["id"]

    start = await client.post(f"{BASE}/runs/{run_id}/start")
    assert start.status_code == 200
    assert start.json()["state"] == "RUNNING"

    complete = await client.post(f"{BASE}/runs/{run_id}/complete")
    assert complete.status_code == 200
    assert complete.json()["state"] == "COMPLETED"


@pytest.mark.asyncio
async def test_run_fail_transition(client):
    await create_namespace(client, "fail-ns")
    await create_job(client, "fail-ns", "fail-job")
    run_id = (await client.post(f"{BASE}/namespaces/fail-ns/jobs/fail-job/runs", json={})).json()["id"]

    resp = await client.post(f"{BASE}/runs/{run_id}/fail")
    assert resp.status_code == 200
    assert resp.json()["state"] == "FAILED"


@pytest.mark.asyncio
async def test_run_abort_transition(client):
    await create_namespace(client, "abort-ns")
    await create_job(client, "abort-ns", "abort-job")
    run_id = (await client.post(f"{BASE}/namespaces/abort-ns/jobs/abort-job/runs", json={})).json()["id"]

    resp = await client.post(f"{BASE}/runs/{run_id}/abort")
    assert resp.status_code == 200
    assert resp.json()["state"] == "ABORTED"


@pytest.mark.asyncio
async def test_get_run(client):
    await create_namespace(client, "get-run-ns")
    await create_job(client, "get-run-ns", "get-run-job")
    run_id = (await client.post(f"{BASE}/namespaces/get-run-ns/jobs/get-run-job/runs", json={})).json()["id"]

    resp = await client.get(f"{BASE}/runs/{run_id}")
    assert resp.status_code == 200
    assert resp.json()["id"] == run_id


@pytest.mark.asyncio
async def test_get_run_not_found(client):
    resp = await client.get(f"{BASE}/runs/{uuid.uuid4()}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_run_invalid_uuid(client):
    resp = await client.get(f"{BASE}/runs/not-a-uuid")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_list_runs(client):
    await create_namespace(client, "list-run-ns")
    await create_job(client, "list-run-ns", "list-run-job")
    await client.post(f"{BASE}/namespaces/list-run-ns/jobs/list-run-job/runs", json={})
    await client.post(f"{BASE}/namespaces/list-run-ns/jobs/list-run-job/runs", json={})

    resp = await client.get(f"{BASE}/namespaces/list-run-ns/jobs/list-run-job/runs")
    assert resp.status_code == 200
    data = resp.json()
    assert data["totalCount"] >= 2
    assert len(data["runs"]) >= 2


# ---------------------------------------------------------------------------
# OpenLineage ingestion
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_openlineage_ingest_start(client):
    event = ol_event("START", run_id="550e8400-e29b-41d4-a716-446655440000")
    resp = await client.post(f"{BASE}/lineage", json=event)
    assert resp.status_code == 201


@pytest.mark.asyncio
async def test_openlineage_ingest_complete(client):
    event = ol_event("COMPLETE", outputs=[{"namespace": "test-ns", "name": "output-table", "facets": {}}])
    resp = await client.post(f"{BASE}/lineage", json=event)
    assert resp.status_code == 201


@pytest.mark.asyncio
async def test_openlineage_ingest_creates_job_and_datasets(client):
    """Ingesting an event should create the job and referenced datasets."""
    event = ol_event(
        "COMPLETE",
        ns="ol-ns",
        job="ol-job",
        inputs=[{"namespace": "ol-ns", "name": "input-ds", "facets": {}}],
        outputs=[{"namespace": "ol-ns", "name": "output-ds", "facets": {}}],
    )
    await ingest_event(client, event)

    job_resp = await client.get(f"{BASE}/namespaces/ol-ns/jobs/ol-job")
    assert job_resp.status_code == 200

    ds_resp = await client.get(f"{BASE}/namespaces/ol-ns/datasets")
    names = [d["name"] for d in ds_resp.json()["datasets"]]
    assert "input-ds" in names
    assert "output-ds" in names


@pytest.mark.asyncio
async def test_openlineage_schema_facet_sets_fields(client):
    """Schema facet in event should populate dataset fields."""
    event = ol_event(
        "COMPLETE",
        ns="schema-ns",
        outputs=[{
            "namespace": "schema-ns",
            "name": "schema-table",
            "facets": {
                "schema": {
                    "_producer": "test",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
                    "fields": [
                        {"name": "id", "type": "INTEGER", "description": "primary key"},
                        {"name": "name", "type": "STRING"},
                    ],
                }
            },
        }],
    )
    await ingest_event(client, event)

    ds_resp = await client.get(f"{BASE}/namespaces/schema-ns/datasets/schema-table")
    assert ds_resp.status_code == 200
    fields = {f["name"]: f for f in ds_resp.json()["fields"]}
    assert "id" in fields
    assert "name" in fields


@pytest.mark.asyncio
async def test_openlineage_run_state_from_events(client):
    """START then COMPLETE events should transition run state."""
    run_id = str(uuid.uuid4())
    await ingest_event(client, ol_event("START", ns="run-ev-ns", job="run-ev-job", run_id=run_id))
    await ingest_event(client, ol_event("COMPLETE", ns="run-ev-ns", job="run-ev-job", run_id=run_id))

    resp = await client.get(f"{BASE}/runs/{run_id}")
    assert resp.status_code == 200
    assert resp.json()["state"] == "COMPLETED"


@pytest.mark.asyncio
async def test_list_datasets_after_ingest(client):
    event = ol_event(
        "COMPLETE",
        ns="ns2",
        job="etl-job",
        outputs=[{"namespace": "ns2", "name": "orders", "facets": {}}],
    )
    await client.post(f"{BASE}/lineage", json=event)

    resp = await client.get(f"{BASE}/namespaces/ns2/datasets")
    assert resp.status_code == 200
    names = [d["name"] for d in resp.json()["datasets"]]
    assert "orders" in names


@pytest.mark.asyncio
async def test_openlineage_invalid_event_null_job_name(client):
    """Event with null job name should be rejected (422)."""
    event = {
        "eventTime": "2021-11-03T10:53:52.427343Z",
        "eventType": "COMPLETE",
        "inputs": [],
        "job": {"facets": {}, "name": None, "namespace": "testing"},
        "outputs": [],
        "producer": "me",
        "run": {"facets": {}, "runId": str(uuid.uuid4())},
        "schemaURL": "",
    }
    resp = await client.post(f"{BASE}/lineage", json=event)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_openlineage_invalid_event_null_run_id(client):
    """Event with null run ID should be rejected (422)."""
    event = {
        "eventTime": "2021-11-03T10:53:52.427343Z",
        "eventType": "COMPLETE",
        "inputs": [],
        "job": {"facets": {}, "name": "job", "namespace": "ns"},
        "outputs": [],
        "producer": "me",
        "run": {"facets": {}, "runId": None},
        "schemaURL": "",
    }
    resp = await client.post(f"{BASE}/lineage", json=event)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_openlineage_invalid_event_bad_time_format(client):
    """Pydantic accepts date-only strings; we accept and process them."""
    event = {
        "eventTime": "2021-11-03T00:00:00Z",
        "eventType": "START",
        "inputs": [],
        "job": {"facets": {}, "name": "job", "namespace": "openlineage"},
        "outputs": [],
        "run": {"facets": {}, "runId": str(uuid.uuid4())},
        "schemaURL": "",
    }
    resp = await client.post(f"{BASE}/lineage", json=event)
    assert resp.status_code == 201


# ---------------------------------------------------------------------------
# Lineage graph
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_lineage_by_job_node(client):
    """GET /lineage?nodeId=job:ns:name should return a graph."""
    await ingest_event(client, ol_event(
        "COMPLETE",
        ns="graph-ns",
        job="graph-job",
        inputs=[{"namespace": "graph-ns", "name": "in-table", "facets": {}}],
        outputs=[{"namespace": "graph-ns", "name": "out-table", "facets": {}}],
    ))

    resp = await client.get(f"{BASE}/lineage?nodeId=job:graph-ns:graph-job")
    assert resp.status_code == 200
    data = resp.json()
    assert "graph" in data
    ids = [n["id"] for n in data["graph"]]
    assert any("graph-job" in nid for nid in ids)


@pytest.mark.asyncio
async def test_get_lineage_by_dataset_node(client):
    await ingest_event(client, ol_event(
        "COMPLETE",
        ns="graph-ds-ns",
        outputs=[{"namespace": "graph-ds-ns", "name": "ds-node", "facets": {}}],
    ))

    resp = await client.get(f"{BASE}/lineage?nodeId=dataset:graph-ds-ns:ds-node")
    assert resp.status_code == 200
    assert "graph" in resp.json()


@pytest.mark.asyncio
async def test_get_lineage_not_found(client):
    resp = await client.get(f"{BASE}/lineage?nodeId=dataset:nobody:nowhere")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_tags(client):
    await create_namespace(client, "tag-list-ns")
    # Create a dataset with tags — tags come from dataset_tags table
    await client.put(
        f"{BASE}/namespaces/tag-list-ns/datasets/tagged",
        json={"type": "DB_TABLE", "physical_name": "tagged", "tags": ["GOLD", "PII"]},
    )

    resp = await client.get(f"{BASE}/tags")
    assert resp.status_code == 200
    data = resp.json()
    assert "tags" in data
    tag_names = [t["name"] for t in data["tags"]]
    assert "GOLD" in tag_names or "PII" in tag_names


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_lineage_events(client):
    await ingest_event(client, ol_event("START", ns="ev-ns", job="ev-job"))
    await ingest_event(client, ol_event("COMPLETE", ns="ev-ns", job="ev-job"))

    resp = await client.get(f"{BASE}/events/lineage?limit=10")
    assert resp.status_code == 200
    data = resp.json()
    assert "events" in data
    assert data["totalCount"] >= 2


@pytest.mark.asyncio
async def test_list_lineage_events_sort_asc(client):
    await ingest_event(client, ol_event("START", event_time="2024-01-01T01:00:00Z"))
    await ingest_event(client, ol_event("COMPLETE", event_time="2024-01-01T02:00:00Z"))

    resp = await client.get(f"{BASE}/events/lineage?sortDirection=asc&limit=10")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_list_lineage_events_sort_invalid(client):
    resp = await client.get(f"{BASE}/events/lineage?sortDirection=random")
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_datasets(client):
    await create_namespace(client, "srch-ns")
    await create_dataset(client, "srch-ns", "my-dataset")
    await create_dataset(client, "srch-ns", "other-table")

    resp = await client.get(f"{BASE}/search?q=my-dataset")
    assert resp.status_code == 200
    data = resp.json()
    assert "results" in data
    assert "totalCount" in data
    names = [r["name"] for r in data["results"]]
    assert "my-dataset" in names
    assert "other-table" not in names


@pytest.mark.asyncio
async def test_search_jobs(client):
    await create_namespace(client, "srch-job-ns")
    await create_job(client, "srch-job-ns", "my-etl-job")

    resp = await client.get(f"{BASE}/search?q=etl&filter=JOB")
    assert resp.status_code == 200
    names = [r["name"] for r in resp.json()["results"]]
    assert "my-etl-job" in names


@pytest.mark.asyncio
async def test_search_filter_dataset_only(client):
    await create_namespace(client, "srch-mix-ns")
    await create_dataset(client, "srch-mix-ns", "orders")
    await create_job(client, "srch-mix-ns", "orders-job")

    resp = await client.get(f"{BASE}/search?q=orders&filter=DATASET")
    assert resp.status_code == 200
    results = resp.json()["results"]
    assert all(r["type"] == "DATASET" for r in results)


@pytest.mark.asyncio
async def test_search_sort_update_at(client):
    await create_namespace(client, "srch-sort-ns")
    await create_dataset(client, "srch-sort-ns", "alpha-ds")
    await create_dataset(client, "srch-sort-ns", "beta-ds")

    resp = await client.get(f"{BASE}/search?q=ds&sort=UPDATE_AT")
    assert resp.status_code == 200
    assert resp.json()["totalCount"] >= 2


@pytest.mark.asyncio
async def test_search_result_shape(client):
    await create_namespace(client, "shape-ns")
    await create_dataset(client, "shape-ns", "shape-ds")

    resp = await client.get(f"{BASE}/search?q=shape-ds")
    assert resp.status_code == 200
    r = resp.json()["results"][0]
    assert r["type"] == "DATASET"
    assert r["name"] == "shape-ds"
    assert r["namespace"] == "shape-ns"
    assert r["nodeId"] == "dataset:shape-ns:shape-ds"
    assert "updatedAt" in r


@pytest.mark.asyncio
async def test_search_empty_results(client):
    resp = await client.get(f"{BASE}/search?q=zzznomatch")
    assert resp.status_code == 200
    assert resp.json()["totalCount"] == 0
    assert resp.json()["results"] == []


@pytest.mark.asyncio
async def test_search_invalid_filter(client):
    resp = await client.get(f"{BASE}/search?q=x&filter=INVALID")
    assert resp.status_code == 400
