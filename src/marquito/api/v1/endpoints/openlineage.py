from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from marquito.db.session import get_db
from marquito.schemas.api import LineageGraph, OpenLineageEvent
from marquito.services import lineage as svc

router = APIRouter()


@router.post(
    "/lineage",
    status_code=201,
    tags=["OpenLineage"],
    summary="Ingest an OpenLineage event",
)
async def post_lineage_event(
    event: OpenLineageEvent,
    db: AsyncSession = Depends(get_db),
):
    """
    Accepts any OpenLineage-compliant event (START, RUNNING, COMPLETE, FAIL, ABORT, OTHER).
    Upserts namespace, job, run, datasets, and lineage edges in one transaction.
    """
    await svc.ingest_openlineage_event(db, event)
    return {"message": "event accepted"}


_TYPE_PREFIXES = {"job": "JOB", "dataset": "DATASET"}


@router.get(
    "/lineage",
    response_model=LineageGraph,
    tags=["Lineage"],
    summary="Get the lineage graph for a node",
)
async def get_lineage(
    nodeId: str = Query(..., description="'type:namespace:name' or bare 'namespace:name'"),
    nodeType: str | None = Query(None, description="JOB or DATASET (inferred from nodeId prefix if omitted)"),
    depth: int = Query(2, ge=1, le=10),
    db: AsyncSession = Depends(get_db),
):
    # Support nodeId in the form "job:namespace:name" or "dataset:namespace:name"
    resolved_type = nodeType
    resolved_id = nodeId
    prefix, _, rest = nodeId.partition(":")
    if prefix.lower() in _TYPE_PREFIXES:
        resolved_type = resolved_type or _TYPE_PREFIXES[prefix.lower()]
        resolved_id = rest

    if resolved_type not in ("JOB", "DATASET"):
        raise HTTPException(status_code=400, detail="nodeType must be JOB or DATASET")

    graph = await svc.get_lineage_graph(db, resolved_id, resolved_type, depth=depth)
    nodes = graph.graph if hasattr(graph, "graph") else graph.get("graph", [])
    if not nodes:
        raise HTTPException(status_code=404, detail=f"Node '{nodeId}' not found")
    return graph
