from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from marquito.db.session import get_db
from marquito.schemas.api import (
    DatasetCreate,
    DatasetList,
    DatasetResponse,
    JobCreate,
    JobList,
    JobResponse,
    RunCreate,
    RunList,
    RunResponse,
)
from marquito.schemas.dataset_versions import DatasetVersionList, DatasetVersionResponse
from marquito.services import facets as facet_svc
from marquito.services import lineage as svc

router = APIRouter()


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------


async def _enrich_dataset(ds, namespace_name: str, db) -> DatasetResponse:
    """Attach computed fields that are not direct ORM columns."""
    resp = DatasetResponse.model_validate(ds)
    resp.namespace = namespace_name
    resp.id = f"{namespace_name}:{ds.name}"
    resp.source_name = ds.source.name if ds.source else None
    resp.facets = ds.facets or {}

    orm_fields = [
        {"name": f.name, "type": f.type, "description": f.description, "tags": [t.name for t in f.tags]}
        for f in ds.fields
    ]
    resp.fields = orm_fields or facet_svc.fields_from_schema_facet(resp.facets)
    resp.tags = [t.name for t in ds.tags]
    return resp


@router.put(
    "/namespaces/{namespace}/datasets/{dataset}",
    response_model=DatasetResponse,
    tags=["Datasets"],
)
async def create_or_update_dataset(
    namespace: str,
    dataset: str,
    body: DatasetCreate,
    db: AsyncSession = Depends(get_db),
):
    ds = await svc.upsert_dataset(db, namespace, dataset, body)
    return await _enrich_dataset(ds, namespace, db)


@router.get(
    "/namespaces/{namespace}/datasets/{dataset}",
    response_model=DatasetResponse,
    tags=["Datasets"],
)
async def get_dataset(namespace: str, dataset: str, db: AsyncSession = Depends(get_db)):
    ds = await svc.get_dataset(db, namespace, dataset)
    if ds is None:
        raise HTTPException(
            status_code=404, detail=f"Dataset '{namespace}/{dataset}' not found"
        )
    return await _enrich_dataset(ds, namespace, db)


@router.delete(
    "/namespaces/{namespace}/datasets/{dataset}",
    response_model=DatasetResponse,
    tags=["Datasets"],
)
async def delete_dataset(namespace: str, dataset: str, db: AsyncSession = Depends(get_db)):
    ds = await svc.soft_delete_dataset(db, namespace, dataset)
    if ds is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{namespace}/{dataset}' not found")
    return await _enrich_dataset(ds, namespace, db)


def _build_version_response(ds, v, namespace: str) -> DatasetVersionResponse:
    return DatasetVersionResponse(
        id=f"{namespace}:{ds.name}",
        type=ds.type,
        name=ds.name,
        physicalName=ds.physical_name,
        createdAt=v.created_at,
        version=v.version,
        namespace=namespace,
        description=ds.description,
        lifecycleState=v.lifecycle_state,
        facets=v.facets or {},
        createdByRun={"uuid": str(v.run_uuid)} if v.run_uuid else None,
    )


@router.get(
    "/namespaces/{namespace}/datasets/{dataset}/versions",
    response_model=DatasetVersionList,
    tags=["Datasets"],
)
async def list_dataset_versions(
    namespace: str,
    dataset: str,
    limit: int = Query(100, ge=0),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    ds = await svc.get_dataset(db, namespace, dataset)
    if ds is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{namespace}/{dataset}' not found")
    versions, total = (
        await svc.list_dataset_versions(db, namespace, dataset, limit=limit, offset=offset),
        await svc.count_dataset_versions(db, namespace, dataset),
    )
    items = [_build_version_response(ds, v, namespace) for v in versions]
    return DatasetVersionList(versions=items, totalCount=total)


@router.get(
    "/namespaces/{namespace}/datasets/{dataset}/versions/{version}",
    response_model=DatasetVersionResponse,
    tags=["Datasets"],
)
async def get_dataset_version(
    namespace: str,
    dataset: str,
    version: str,
    db: AsyncSession = Depends(get_db),
):
    import uuid as uuidlib
    try:
        vid = uuidlib.UUID(version)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid version UUID")
    ds = await svc.get_dataset(db, namespace, dataset)
    if ds is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{namespace}/{dataset}' not found")
    v = await svc.get_dataset_version(db, namespace, dataset, vid)
    if v is None:
        raise HTTPException(status_code=404, detail=f"Version '{version}' not found")
    return _build_version_response(ds, v, namespace)


@router.get(
    "/namespaces/{namespace}/datasets",
    response_model=DatasetList,
    tags=["Datasets"],
)
async def list_datasets(
    namespace: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    items, total = (
        await svc.list_datasets(db, namespace, limit=limit, offset=offset),
        await svc.count_datasets(db, namespace),
    )
    enriched = [await _enrich_dataset(d, namespace, db) for d in items]
    return DatasetList(datasets=enriched, totalCount=total)


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


async def _enrich_job(job, namespace_name: str, db) -> JobResponse:
    resp = JobResponse.model_validate(job)
    resp.namespace = namespace_name
    resp.id = f"{namespace_name}:{job.name}"
    resp.facets = job.facets or {}
    if job.runs:
        sorted_runs = sorted(job.runs, key=lambda r: r.created_at, reverse=True)
        resp.latest_run = RunResponse.model_validate(sorted_runs[0])
        resp.latest_runs = [RunResponse.model_validate(r) for r in sorted_runs[:10]]
    if job.versions:
        latest_jv = job.versions[-1]  # already ordered by created_at ASC
        resp.inputs = latest_jv.inputs or []
        resp.outputs = latest_jv.outputs or []
    return resp


@router.put(
    "/namespaces/{namespace}/jobs/{job}",
    response_model=JobResponse,
    tags=["Jobs"],
)
async def create_or_update_job(
    namespace: str,
    job: str,
    body: JobCreate,
    db: AsyncSession = Depends(get_db),
):
    j = await svc.upsert_job(db, namespace, job, body)
    return await _enrich_job(j, namespace, db)


@router.delete(
    "/namespaces/{namespace}/jobs/{job}",
    response_model=JobResponse,
    tags=["Jobs"],
)
async def delete_job(namespace: str, job: str, db: AsyncSession = Depends(get_db)):
    j = await svc.soft_delete_job(db, namespace, job)
    if j is None:
        raise HTTPException(status_code=404, detail=f"Job '{namespace}/{job}' not found")
    return await _enrich_job(j, namespace, db)


@router.get(
    "/namespaces/{namespace}/jobs/{job}",
    response_model=JobResponse,
    tags=["Jobs"],
)
async def get_job(namespace: str, job: str, db: AsyncSession = Depends(get_db)):
    j = await svc.get_job(db, namespace, job)
    if j is None:
        raise HTTPException(
            status_code=404, detail=f"Job '{namespace}/{job}' not found"
        )
    return await _enrich_job(j, namespace, db)


@router.get(
    "/namespaces/{namespace}/jobs",
    response_model=JobList,
    tags=["Jobs"],
)
async def list_jobs(
    namespace: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    items, total = (
        await svc.list_jobs(db, namespace, limit=limit, offset=offset),
        await svc.count_jobs(db, namespace),
    )
    enriched = [await _enrich_job(j, namespace, db) for j in items]
    return JobList(jobs=enriched, totalCount=total)


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------


def _enrich_run(run) -> RunResponse:
    resp = RunResponse.model_validate(run)
    resp.input_datasets = [
        {
            "namespace": m.dataset.namespace.name,
            "name": m.dataset.name,
            "datasetVersionUuid": str(m.dataset_version_uuid) if m.dataset_version_uuid else None,
        }
        for m in (run.input_datasets or [])
        if m.dataset and m.dataset.namespace
    ]
    resp.output_datasets = [
        {
            "namespace": m.dataset.namespace.name,
            "name": m.dataset.name,
            "datasetVersionUuid": str(m.dataset_version_uuid) if m.dataset_version_uuid else None,
        }
        for m in (run.output_datasets or [])
        if m.dataset and m.dataset.namespace
    ]
    return resp


@router.post(
    "/namespaces/{namespace}/jobs/{job}/runs",
    response_model=RunResponse,
    status_code=201,
    tags=["Runs"],
)
async def create_run(
    namespace: str,
    job: str,
    body: RunCreate,
    db: AsyncSession = Depends(get_db),
):
    run = await svc.create_run(db, namespace, job, body)
    return RunResponse.model_validate(run)


@router.get(
    "/namespaces/{namespace}/jobs/{job}/runs",
    response_model=RunList,
    tags=["Runs"],
)
async def list_runs(
    namespace: str,
    job: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    items, total = (
        await svc.list_runs(db, namespace, job, limit=limit, offset=offset),
        await svc.count_runs(db, namespace, job),
    )
    return RunList(runs=[RunResponse.model_validate(r) for r in items], totalCount=total)


@router.get("/runs/{run_id}", response_model=RunResponse, tags=["Runs"])
async def get_run(run_id: str, db: AsyncSession = Depends(get_db)):
    import uuid

    try:
        uid = uuid.UUID(run_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid run UUID")
    run = await svc.get_run(db, uid)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    return _enrich_run(run)


@router.post("/runs/{run_id}/start", response_model=RunResponse, tags=["Runs"])
async def mark_run_start(run_id: str, db: AsyncSession = Depends(get_db)):
    import uuid

    run = await svc.transition_run(db, uuid.UUID(run_id), "RUNNING")
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    return _enrich_run(run)


@router.post("/runs/{run_id}/complete", response_model=RunResponse, tags=["Runs"])
async def mark_run_complete(run_id: str, db: AsyncSession = Depends(get_db)):
    import uuid

    run = await svc.transition_run(db, uuid.UUID(run_id), "COMPLETED")
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    return _enrich_run(run)


@router.post("/runs/{run_id}/fail", response_model=RunResponse, tags=["Runs"])
async def mark_run_fail(run_id: str, db: AsyncSession = Depends(get_db)):
    import uuid

    run = await svc.transition_run(db, uuid.UUID(run_id), "FAILED")
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    return _enrich_run(run)


@router.post("/runs/{run_id}/abort", response_model=RunResponse, tags=["Runs"])
async def mark_run_abort(run_id: str, db: AsyncSession = Depends(get_db)):
    import uuid

    run = await svc.transition_run(db, uuid.UUID(run_id), "ABORTED")
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    return _enrich_run(run)
