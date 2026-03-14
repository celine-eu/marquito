from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from marquito.db.session import get_db
from marquito.schemas.tags import TagList, TagResponse
from marquito.services import lineage as svc

router = APIRouter()


class TagCreate(BaseModel):
    description: str | None = None


@router.put("/tags/{name}", response_model=TagResponse, tags=["Tags"])
async def create_or_update_tag(
    name: str,
    body: TagCreate,
    db: AsyncSession = Depends(get_db),
):
    tag = await svc.upsert_tag(db, name, body.description or None)
    return TagResponse.model_validate(tag)


@router.get("/tags/{name}", response_model=TagResponse, tags=["Tags"])
async def get_tag(name: str, db: AsyncSession = Depends(get_db)):
    tag = await svc.get_tag(db, name)
    if tag is None:
        raise HTTPException(status_code=404, detail=f"Tag '{name}' not found")
    return TagResponse.model_validate(tag)


@router.get("/tags", response_model=TagList, tags=["Tags"])
async def list_tags(
    limit: int = Query(100, ge=0),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    items = await svc.list_tags(db, limit=limit, offset=offset)
    return TagList(tags=[TagResponse.from_any(item) for item in items])
