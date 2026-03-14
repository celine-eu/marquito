from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from marquito.db.session import get_db
from marquito.models.orm import Dataset, Job, Namespace

router = APIRouter()


class SearchResult(BaseModel):
    type: str           # "DATASET" | "JOB"
    name: str
    updatedAt: datetime
    namespace: str
    nodeId: str         # "dataset:ns:name" or "job:ns:name"


class SearchResults(BaseModel):
    totalCount: int
    results: list[SearchResult]


@router.get("/search", response_model=SearchResults, tags=["Search"])
async def search(
    q: str = Query(..., min_length=1),
    filter: str | None = Query(None, description="DATASET or JOB"),
    sort: str = Query("name", description="name or UPDATE_AT"),
    limit: int = Query(10, ge=0, le=1000),
    namespace: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    if filter and filter.upper() not in ("DATASET", "JOB"):
        raise HTTPException(status_code=400, detail="filter must be DATASET or JOB")

    pattern = f"%{q}%"
    results: list[SearchResult] = []

    if filter is None or filter.upper() == "DATASET":
        stmt = (
            select(Dataset, Namespace.name.label("ns_name"))
            .join(Namespace, Dataset.namespace_uuid == Namespace.uuid)
            .where(Dataset.is_hidden.is_(False))
            .where(Dataset.name.ilike(pattern))
        )
        if namespace:
            stmt = stmt.where(Namespace.name == namespace)
        rows = await db.execute(stmt)
        for ds, ns_name in rows:
            results.append(SearchResult(
                type="DATASET",
                name=ds.name,
                updatedAt=ds.updated_at,
                namespace=ns_name,
                nodeId=f"dataset:{ns_name}:{ds.name}",
            ))

    if filter is None or filter.upper() == "JOB":
        stmt = (
            select(Job, Namespace.name.label("ns_name"))
            .join(Namespace, Job.namespace_uuid == Namespace.uuid)
            .where(Job.is_hidden.is_(False))
            .where(Job.name.ilike(pattern))
        )
        if namespace:
            stmt = stmt.where(Namespace.name == namespace)
        rows = await db.execute(stmt)
        for job, ns_name in rows:
            results.append(SearchResult(
                type="JOB",
                name=job.name,
                updatedAt=job.updated_at,
                namespace=ns_name,
                nodeId=f"job:{ns_name}:{job.name}",
            ))

    if sort.upper() == "UPDATE_AT":
        results.sort(key=lambda r: r.updatedAt, reverse=True)
    else:
        results.sort(key=lambda r: r.name.lower())

    total = len(results)
    if limit > 0:
        results = results[:limit]

    return SearchResults(totalCount=total, results=results)
