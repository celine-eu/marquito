"""
Pydantic v2 schemas.
These match the Marquez REST API contract (camelCase JSON) so existing clients keep working.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic.alias_generators import to_camel


# Shared model config for response models: camelCase output + ORM attribute access
_RESPONSE_CONFIG = ConfigDict(
    from_attributes=True,
    alias_generator=to_camel,
    populate_by_name=True,
)

# Shared model config for request (input) models: accept both camelCase and snake_case
_REQUEST_CONFIG = ConfigDict(
    alias_generator=to_camel,
    populate_by_name=True,
)


# ---------------------------------------------------------------------------
# Shared
# ---------------------------------------------------------------------------


class TagSchema(BaseModel):
    name: str


# ---------------------------------------------------------------------------
# Namespace
# ---------------------------------------------------------------------------


class NamespaceCreate(BaseModel):
    model_config = _REQUEST_CONFIG

    owner_name: str
    description: str | None = None


class NamespaceResponse(BaseModel):
    model_config = _RESPONSE_CONFIG

    name: str
    created_at: datetime
    updated_at: datetime
    owner_name: str | None = Field(None, validation_alias="current_owner_name")
    description: str | None = None


class NamespaceList(BaseModel):
    namespaces: list[NamespaceResponse]
    totalCount: int = 0


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------


class SourceCreate(BaseModel):
    model_config = _REQUEST_CONFIG

    type: str
    connection_url: str | None = None
    description: str | None = None


class SourceResponse(BaseModel):
    model_config = _RESPONSE_CONFIG

    type: str
    name: str
    created_at: datetime
    updated_at: datetime
    connection_url: str | None = None
    description: str | None = None


class SourceList(BaseModel):
    sources: list[SourceResponse]
    totalCount: int = 0


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------


class FieldSchema(BaseModel):
    model_config = _RESPONSE_CONFIG

    name: str
    type: str | None = None
    description: str | None = None
    tags: list[str] = []


class DatasetCreate(BaseModel):
    model_config = _REQUEST_CONFIG

    type: str = "DB_TABLE"
    physical_name: str | None = None
    source_name: str | None = None
    description: str | None = None
    schema_location: str | None = None
    fields: list[FieldSchema] = []
    tags: list[str] = []


class DatasetResponse(BaseModel):
    model_config = _RESPONSE_CONFIG

    id: str = ""
    type: str
    name: str
    physical_name: str | None = None
    created_at: datetime
    updated_at: datetime
    namespace: str = ""
    source_name: str | None = None
    fields: list[Any] = []
    tags: list[Any] = []
    last_modified_at: datetime | None = None
    description: str | None = None
    facets: dict[str, Any] = {}

    @field_validator("namespace", mode="before")
    @classmethod
    def _coerce_namespace(cls, v: object) -> str:
        return v.name if hasattr(v, "name") else (v or "")

    @field_validator("tags", mode="before")
    @classmethod
    def _coerce_tags(cls, v: object) -> list:
        if isinstance(v, list):
            return [t.name if hasattr(t, "name") else t for t in v]
        return v or []

    @field_validator("fields", mode="before")
    @classmethod
    def _coerce_fields(cls, v: object) -> list:
        # ORM DatasetField objects — return empty; _enrich_dataset will rebuild
        if isinstance(v, list) and v and hasattr(v[0], "dataset_uuid"):
            return []
        return v or []


class DatasetList(BaseModel):
    datasets: list[DatasetResponse]
    totalCount: int = 0


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------


class JobCreate(BaseModel):
    model_config = _REQUEST_CONFIG

    type: str = "BATCH"
    inputs: list[dict[str, str]] = []
    outputs: list[dict[str, str]] = []
    location: str | None = None
    description: str | None = None
    context: dict[str, Any] = {}


class JobResponse(BaseModel):
    model_config = _RESPONSE_CONFIG

    id: str = ""
    type: str
    name: str
    created_at: datetime
    updated_at: datetime
    namespace: str = ""
    inputs: list[dict] = []
    outputs: list[dict] = []
    tags: list[str] = []
    location: str | None = None
    description: str | None = None
    context: dict[str, Any] = {}
    facets: dict[str, Any] = {}
    latest_run: RunResponse | None = None
    latest_runs: list[RunResponse] = []

    @field_validator("namespace", mode="before")
    @classmethod
    def _coerce_namespace(cls, v: object) -> str:
        return v.name if hasattr(v, "name") else (v or "")


class JobList(BaseModel):
    jobs: list[JobResponse]
    totalCount: int = 0


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


class RunCreate(BaseModel):
    model_config = _REQUEST_CONFIG

    id: uuid.UUID | None = None
    nominal_start_time: datetime | None = None
    nominal_end_time: datetime | None = None
    args: dict[str, Any] = {}


class RunResponse(BaseModel):
    model_config = _RESPONSE_CONFIG

    id: uuid.UUID = Field(validation_alias="uuid")
    created_at: datetime
    updated_at: datetime
    nominal_start_time: datetime | None = None
    nominal_end_time: datetime | None = None
    state: str = Field("NEW", validation_alias="current_run_state")
    started_at: datetime | None = None
    ended_at: datetime | None = None
    duration_ms: int | None = None
    facets: dict[str, Any] = {}

    @classmethod
    def model_validate(cls, obj, **kw):  # type: ignore[override]
        instance = super().model_validate(obj, **kw)
        if instance.started_at and instance.ended_at:
            delta = instance.ended_at - instance.started_at
            instance.duration_ms = int(delta.total_seconds() * 1000)
        return instance


class RunList(BaseModel):
    runs: list[RunResponse]
    totalCount: int = 0


# ---------------------------------------------------------------------------
# OpenLineage ingest
# ---------------------------------------------------------------------------


class OpenLineageInputDataset(BaseModel):
    namespace: str
    name: str
    facets: dict[str, Any] = {}


class OpenLineageOutputDataset(BaseModel):
    namespace: str
    name: str
    facets: dict[str, Any] = {}


class OpenLineageJob(BaseModel):
    namespace: str
    name: str
    facets: dict[str, Any] = {}


class OpenLineageRun(BaseModel):
    runId: str
    facets: dict[str, Any] = {}


class OpenLineageEvent(BaseModel):
    eventType: str
    eventTime: datetime
    run: OpenLineageRun
    job: OpenLineageJob
    inputs: list[OpenLineageInputDataset] = []
    outputs: list[OpenLineageOutputDataset] = []
    producer: str = ""
    schemaURL: str = ""


# ---------------------------------------------------------------------------
# Lineage graph
# ---------------------------------------------------------------------------


class LineageEdge(BaseModel):
    origin: str
    destination: str


class LineageNode(BaseModel):
    id: str
    type: str          # "DATASET" | "JOB"
    data: dict[str, Any] = {}
    inEdges: list[LineageEdge] = []
    outEdges: list[LineageEdge] = []


class LineageGraph(BaseModel):
    graph: list[LineageNode] = []


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    status: str = "healthy"
    version: str
    db: str = "ok"
