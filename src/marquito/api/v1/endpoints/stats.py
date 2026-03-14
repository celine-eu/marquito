from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from marquito.db.session import get_db
from marquito.schemas.stats import IntervalMetric, LineageMetric
from marquito.services import stats as svc

router = APIRouter(prefix="/stats", tags=["Stats"])


@router.get("/lineage-events", response_model=list[LineageMetric])
async def lineage_event_stats(
    period: str = Query("DAY"),
    timezone: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    return await svc.get_lineage_event_stats(db, period.upper(), timezone)


@router.get("/jobs", response_model=list[IntervalMetric])
async def job_stats(
    period: str = Query("DAY"),
    timezone: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    return await svc.get_job_stats(db, period.upper(), timezone)


@router.get("/datasets", response_model=list[IntervalMetric])
async def dataset_stats(
    period: str = Query("DAY"),
    timezone: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    return await svc.get_dataset_stats(db, period.upper(), timezone)


@router.get("/sources", response_model=list[IntervalMetric])
async def source_stats(
    period: str = Query("DAY"),
    timezone: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    return await svc.get_source_stats(db, period.upper(), timezone)
