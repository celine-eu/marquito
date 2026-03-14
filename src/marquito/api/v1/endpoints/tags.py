from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from marquito.db.session import get_db
from marquito.schemas.tags import TagList
from marquito.services import lineage as svc

router = APIRouter()


@router.get("/tags", response_model=TagList, tags=["Tags"])
async def list_tags(
    limit: int = Query(100, ge=0),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    names = await svc.list_tags(db, limit=limit, offset=offset)
    return TagList(tags=[{"name": n} for n in names])
