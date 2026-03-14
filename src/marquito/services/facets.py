"""
Facet read/write helpers.

`dataset_facets` and `job_facets` are part of the original Marquez schema.
All functions degrade gracefully when those tables don't exist (fresh DB
running only against our own schema).
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# ---------------------------------------------------------------------------
# Read helpers
# ---------------------------------------------------------------------------

_DATASET_FACETS_SQL = text(
    """
    SELECT DISTINCT ON (name) name, facet
    FROM dataset_facets
    WHERE dataset_uuid = :uuid
    ORDER BY name, lineage_event_time DESC
    """
)

_JOB_FACETS_SQL = text(
    """
    SELECT DISTINCT ON (name) name, facet
    FROM job_facets
    WHERE job_uuid = :uuid
    ORDER BY name, lineage_event_time DESC
    """
)


async def get_dataset_facets(db: AsyncSession, dataset_uuid: UUID) -> dict[str, Any]:
    try:
        async with db.begin_nested():
            rows = (await db.execute(_DATASET_FACETS_SQL, {"uuid": str(dataset_uuid)})).all()
        return {row.name: row.facet for row in rows}
    except Exception:
        return {}


async def get_job_facets(db: AsyncSession, job_uuid: UUID) -> dict[str, Any]:
    try:
        async with db.begin_nested():
            rows = (await db.execute(_JOB_FACETS_SQL, {"uuid": str(job_uuid)})).all()
        return {row.name: row.facet for row in rows}
    except Exception:
        return {}


def fields_from_schema_facet(facets: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract field definitions from the OpenLineage schema facet."""
    schema = facets.get("schema") or {}
    if isinstance(schema, dict):
        return [
            {"name": f.get("name", ""), "type": f.get("type"), "description": f.get("description")}
            for f in (schema.get("fields") or [])
            if f.get("name")
        ]
    return []


# ---------------------------------------------------------------------------
# Write helpers (called from ingestion)
# ---------------------------------------------------------------------------

_INSERT_DATASET_FACET = text(
    """
    INSERT INTO dataset_facets
        (created_at, dataset_uuid, run_uuid,
         lineage_event_time, lineage_event_type, type, name, facet)
    VALUES
        (now(), :dataset_uuid::uuid, :run_uuid::uuid,
         :event_time, :event_type, 'dataset', :name, :facet::jsonb)
    """
)

_INSERT_JOB_FACET = text(
    """
    INSERT INTO job_facets
        (created_at, job_uuid, run_uuid,
         lineage_event_time, lineage_event_type, name, facet)
    VALUES
        (now(), :job_uuid::uuid, :run_uuid::uuid,
         :event_time, :event_type, :name, :facet::jsonb)
    """
)


async def write_dataset_facets(
    db: AsyncSession,
    dataset_uuid: UUID,
    run_uuid: UUID,
    event_time: datetime,
    event_type: str,
    facets: dict[str, Any],
) -> None:
    if not facets:
        return
    try:
        async with db.begin_nested():
            for name, value in facets.items():
                if name.startswith("_"):
                    continue
                await db.execute(
                    _INSERT_DATASET_FACET,
                    {
                        "dataset_uuid": str(dataset_uuid),
                        "run_uuid": str(run_uuid),
                        "event_time": event_time,
                        "event_type": event_type,
                        "name": name,
                        "facet": json.dumps(value),
                    },
                )
    except Exception:
        pass


async def write_job_facets(
    db: AsyncSession,
    job_uuid: UUID,
    run_uuid: UUID,
    event_time: datetime,
    event_type: str,
    facets: dict[str, Any],
) -> None:
    if not facets:
        return
    try:
        async with db.begin_nested():
            for name, value in facets.items():
                if name.startswith("_"):
                    continue
                await db.execute(
                    _INSERT_JOB_FACET,
                    {
                        "job_uuid": str(job_uuid),
                        "run_uuid": str(run_uuid),
                        "event_time": event_time,
                        "event_type": event_type,
                        "name": name,
                        "facet": json.dumps(value),
                    },
                )
    except Exception:
        pass
