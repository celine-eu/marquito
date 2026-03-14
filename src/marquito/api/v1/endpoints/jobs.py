from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from marquito.db.session import get_db
from marquito.schemas.api import JobList, JobResponse, RunResponse
from marquito.services import facets as facet_svc
from marquito.services import lineage as svc

router = APIRouter()


async def _enrich_job(job, db) -> JobResponse:
    resp = JobResponse.model_validate(job)
    resp.namespace = job.namespace.name if job.namespace else ""
    resp.id = f"{resp.namespace}:{job.name}"
    resp.facets = job.facets or {}
    if job.runs:
        sorted_runs = sorted(job.runs, key=lambda r: r.created_at, reverse=True)
        resp.latest_run = RunResponse.model_validate(sorted_runs[0])
        resp.latest_runs = [RunResponse.model_validate(r) for r in sorted_runs[:10]]
    return resp


@router.get("/jobs", response_model=JobList, tags=["Jobs"])
async def list_all_jobs(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    last_run_states: list[str] | None = Query(None, alias="lastRunStates"),
    db: AsyncSession = Depends(get_db),
):
    items, total = (
        await svc.list_all_jobs(db, limit=limit, offset=offset, last_run_states=last_run_states),
        await svc.count_jobs(db),
    )
    enriched = [await _enrich_job(j, db) for j in items]
    return JobList(jobs=enriched, totalCount=total)
