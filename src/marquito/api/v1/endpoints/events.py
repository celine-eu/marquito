from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from marquito.db.session import get_db
from marquito.models.orm import LineageEvent
from pydantic import BaseModel

router = APIRouter()

_DEFAULT_AFTER = datetime(1970, 1, 1, tzinfo=timezone.utc)
_DEFAULT_BEFORE = datetime(2030, 1, 1, tzinfo=timezone.utc)


class LineageEventsResponse(BaseModel):
    events: list[dict[str, Any]]
    totalCount: int


@router.get(
    "/events/lineage",
    response_model=LineageEventsResponse,
    tags=["Events"],
)
async def list_lineage_events(
    before: datetime = Query(default=_DEFAULT_BEFORE),
    after: datetime = Query(default=_DEFAULT_AFTER),
    sortDirection: str = Query(default="desc"),
    limit: int = Query(default=100, ge=0),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    if sortDirection.lower() not in ("asc", "desc"):
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="sortDirection must be 'asc' or 'desc'")
    order = (
        LineageEvent.event_time.desc()
        if sortDirection.lower() == "desc"
        else LineageEvent.event_time.asc()
    )

    base = (
        select(LineageEvent)
        .where(LineageEvent.event_time >= after)
        .where(LineageEvent.event_time < before)
    )

    rows = await db.execute(base.order_by(order).limit(limit).offset(offset))
    events = rows.scalars().all()

    count_result = await db.execute(
        select(func.count()).select_from(
            base.subquery()
        )
    )
    total = count_result.scalar_one()

    return LineageEventsResponse(
        events=[e.payload for e in events],
        totalCount=total,
    )
