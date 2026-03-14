from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from marquito.db.session import get_db
from marquito.schemas.api import (
    NamespaceCreate,
    NamespaceList,
    NamespaceResponse,
    SourceCreate,
    SourceList,
    SourceResponse,
)
from marquito.services import lineage as svc

router = APIRouter()


# ---------------------------------------------------------------------------
# Namespaces
# ---------------------------------------------------------------------------


@router.put(
    "/namespaces/{namespace}", response_model=NamespaceResponse, tags=["Namespaces"]
)
async def create_or_update_namespace(
    namespace: str,
    body: NamespaceCreate,
    db: AsyncSession = Depends(get_db),
):
    return await svc.upsert_namespace(db, namespace, body)


@router.get(
    "/namespaces/{namespace}", response_model=NamespaceResponse, tags=["Namespaces"]
)
async def get_namespace(namespace: str, db: AsyncSession = Depends(get_db)):
    ns = await svc.get_namespace(db, namespace)
    if ns is None:
        raise HTTPException(
            status_code=404, detail=f"Namespace '{namespace}' not found"
        )
    return ns


@router.delete(
    "/namespaces/{namespace}", response_model=NamespaceResponse, tags=["Namespaces"]
)
async def delete_namespace(namespace: str, db: AsyncSession = Depends(get_db)):
    ns = await svc.soft_delete_namespace(db, namespace)
    if ns is None:
        raise HTTPException(status_code=404, detail=f"Namespace '{namespace}' not found")
    return ns


@router.get("/namespaces", response_model=NamespaceList, tags=["Namespaces"])
async def list_namespaces(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    items, total = await svc.list_namespaces(db, limit=limit, offset=offset), await svc.count_namespaces(db)
    return NamespaceList(namespaces=items, totalCount=total)


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------


@router.put("/sources/{source}", response_model=SourceResponse, tags=["Sources"])
async def create_or_update_source(
    source: str,
    body: SourceCreate,
    db: AsyncSession = Depends(get_db),
):
    return await svc.upsert_source(db, source, body)


@router.get("/sources/{source}", response_model=SourceResponse, tags=["Sources"])
async def get_source(source: str, db: AsyncSession = Depends(get_db)):
    src = await svc.get_source(db, source)
    if src is None:
        raise HTTPException(status_code=404, detail=f"Source '{source}' not found")
    return src


@router.get("/sources", response_model=SourceList, tags=["Sources"])
async def list_sources(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    items, total = await svc.list_sources(db, limit=limit, offset=offset), await svc.count_sources(db)
    return SourceList(sources=items, totalCount=total)
