"""
Service layer — thin wrappers around SQLAlchemy queries.
All business logic lives here; routers stay thin.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from marquito.models.orm import (
    Dataset,
    DatasetField,
    DatasetFieldTag,
    DatasetTag,
    Job,
    LineageEvent,
    Namespace,
    Run,
    RunDatasetInput,
    RunDatasetOutput,
    Source,
)
from marquito.schemas.api import (
    DatasetCreate,
    JobCreate,
    NamespaceCreate,
    OpenLineageEvent,
    RunCreate,
    SourceCreate,
)


# ---------------------------------------------------------------------------
# Namespace
# ---------------------------------------------------------------------------


async def get_namespace(db: AsyncSession, name: str) -> Namespace | None:
    result = await db.execute(select(Namespace).where(Namespace.name == name))
    return result.scalar_one_or_none()


async def list_namespaces(
    db: AsyncSession, limit: int = 100, offset: int = 0
) -> list[Namespace]:
    result = await db.execute(
        select(Namespace)
        .where(Namespace.is_hidden.is_(False))
        .order_by(Namespace.name)
        .limit(limit)
        .offset(offset)
    )
    return list(result.scalars().all())


async def count_namespaces(db: AsyncSession) -> int:
    result = await db.execute(
        select(func.count()).select_from(Namespace).where(Namespace.is_hidden.is_(False))
    )
    return result.scalar_one()


async def soft_delete_namespace(db: AsyncSession, name: str) -> Namespace | None:
    ns = await get_namespace(db, name)
    if ns is None:
        return None
    ns.is_hidden = True
    await db.flush()
    await db.refresh(ns)
    return ns


async def upsert_namespace(
    db: AsyncSession, name: str, body: NamespaceCreate
) -> Namespace:
    ns = await get_namespace(db, name)
    if ns is None:
        ns = Namespace(name=name)
        db.add(ns)
    ns.current_owner_name = body.owner_name
    ns.description = body.description
    await db.flush()
    await db.refresh(ns)
    return ns


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------


async def get_source(db: AsyncSession, name: str) -> Source | None:
    result = await db.execute(select(Source).where(Source.name == name))
    return result.scalar_one_or_none()


async def list_sources(
    db: AsyncSession, limit: int = 100, offset: int = 0
) -> list[Source]:
    result = await db.execute(
        select(Source).order_by(Source.name).limit(limit).offset(offset)
    )
    return list(result.scalars().all())


async def count_sources(db: AsyncSession) -> int:
    result = await db.execute(select(func.count()).select_from(Source))
    return result.scalar_one()


async def upsert_source(db: AsyncSession, name: str, body: SourceCreate) -> Source:
    source = await get_source(db, name)
    if source is None:
        source = Source(name=name)
        db.add(source)
    source.type = body.type
    source.connection_url = body.connection_url
    source.description = body.description
    await db.flush()
    await db.refresh(source)
    return source


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------


async def get_dataset(
    db: AsyncSession, namespace_name: str, dataset_name: str
) -> Dataset | None:
    ns = await get_namespace(db, namespace_name)
    if ns is None:
        return None
    result = await db.execute(
        select(Dataset)
        .options(
            selectinload(Dataset.fields).selectinload(DatasetField.tags),
            selectinload(Dataset.tags),
            selectinload(Dataset.source),
            selectinload(Dataset.namespace),
        )
        .where(Dataset.namespace_uuid == ns.uuid, Dataset.name == dataset_name)
    )
    return result.scalar_one_or_none()


async def list_datasets(
    db: AsyncSession, namespace_name: str, limit: int = 100, offset: int = 0
) -> list[Dataset]:
    ns = await get_namespace(db, namespace_name)
    if ns is None:
        return []
    result = await db.execute(
        select(Dataset)
        .options(
            selectinload(Dataset.fields).selectinload(DatasetField.tags),
            selectinload(Dataset.tags),
            selectinload(Dataset.source),
            selectinload(Dataset.namespace),
        )
        .where(Dataset.namespace_uuid == ns.uuid, Dataset.is_hidden.is_(False))
        .order_by(Dataset.name)
        .limit(limit)
        .offset(offset)
    )
    return list(result.scalars().all())


async def count_datasets(db: AsyncSession, namespace_name: str) -> int:
    ns = await get_namespace(db, namespace_name)
    if ns is None:
        return 0
    result = await db.execute(
        select(func.count()).select_from(Dataset)
        .where(Dataset.namespace_uuid == ns.uuid, Dataset.is_hidden.is_(False))
    )
    return result.scalar_one()


async def list_dataset_versions(
    db: AsyncSession, namespace_name: str, dataset_name: str, limit: int = 100, offset: int = 0
) -> list:
    from marquito.models.orm import DatasetVersion
    ds = await get_dataset(db, namespace_name, dataset_name)
    if ds is None:
        return []
    result = await db.execute(
        select(DatasetVersion)
        .where(DatasetVersion.dataset_uuid == ds.uuid)
        .order_by(DatasetVersion.created_at.desc())
        .limit(limit)
        .offset(offset)
    )
    return list(result.scalars().all())


async def count_dataset_versions(
    db: AsyncSession, namespace_name: str, dataset_name: str
) -> int:
    from marquito.models.orm import DatasetVersion
    ds = await get_dataset(db, namespace_name, dataset_name)
    if ds is None:
        return 0
    result = await db.execute(
        select(func.count()).select_from(DatasetVersion)
        .where(DatasetVersion.dataset_uuid == ds.uuid)
    )
    return result.scalar_one()


async def soft_delete_dataset(
    db: AsyncSession, namespace_name: str, dataset_name: str
) -> Dataset | None:
    ds = await get_dataset(db, namespace_name, dataset_name)
    if ds is None:
        return None
    ds.is_hidden = True
    await db.flush()
    await db.refresh(ds)
    return ds


async def upsert_dataset(
    db: AsyncSession, namespace_name: str, dataset_name: str, body: DatasetCreate
) -> Dataset:
    ns = await get_namespace(db, namespace_name)
    if ns is None:
        ns = Namespace(name=namespace_name, current_owner_name="anonymous")
        db.add(ns)
        await db.flush()

    source: Source | None = None
    if body.source_name:
        source = await get_source(db, body.source_name)

    result = await db.execute(
        select(Dataset).where(
            Dataset.namespace_uuid == ns.uuid, Dataset.name == dataset_name
        )
    )
    ds = result.scalar_one_or_none()
    if ds is None:
        ds = Dataset(namespace_uuid=ns.uuid, name=dataset_name)
        db.add(ds)

    ds.type = body.type
    ds.physical_name = body.physical_name
    ds.description = body.description
    if source:
        ds.source_uuid = source.uuid
    await db.flush()

    # Upsert fields
    for f in body.fields:
        field_result = await db.execute(
            select(DatasetField).where(
                DatasetField.dataset_uuid == ds.uuid, DatasetField.name == f.name
            )
        )
        field = field_result.scalar_one_or_none()
        if field is None:
            field = DatasetField(dataset_uuid=ds.uuid, name=f.name)
            db.add(field)
        field.type = f.type
        field.description = f.description
        await db.flush()

        # Upsert field tags
        for tag_name in f.tags:
            ft_result = await db.execute(
                select(DatasetFieldTag).where(
                    DatasetFieldTag.field_uuid == field.uuid, DatasetFieldTag.name == tag_name
                )
            )
            if ft_result.scalar_one_or_none() is None:
                db.add(DatasetFieldTag(field_uuid=field.uuid, name=tag_name))
        await db.flush()

    # Upsert dataset tags
    for tag_name in body.tags:
        tag_result = await db.execute(
            select(DatasetTag).where(
                DatasetTag.dataset_uuid == ds.uuid, DatasetTag.name == tag_name
            )
        )
        if tag_result.scalar_one_or_none() is None:
            db.add(DatasetTag(dataset_uuid=ds.uuid, name=tag_name))

    await db.flush()

    # Record a new dataset version for this upsert
    from marquito.models.orm import DatasetVersion
    db.add(DatasetVersion(
        dataset_uuid=ds.uuid,
        version=uuid.uuid4(),
        namespace_name=namespace_name,
        dataset_name=dataset_name,
    ))
    await db.flush()

    return await get_dataset(db, namespace_name, dataset_name)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------


async def get_job(db: AsyncSession, namespace_name: str, job_name: str) -> Job | None:
    ns = await get_namespace(db, namespace_name)
    if ns is None:
        return None
    result = await db.execute(
        select(Job)
        .options(selectinload(Job.namespace), selectinload(Job.runs))
        .where(Job.namespace_uuid == ns.uuid, Job.name == job_name)
    )
    return result.scalar_one_or_none()


async def list_jobs(
    db: AsyncSession, namespace_name: str, limit: int = 100, offset: int = 0
) -> list[Job]:
    ns = await get_namespace(db, namespace_name)
    if ns is None:
        return []
    result = await db.execute(
        select(Job)
        .options(selectinload(Job.namespace), selectinload(Job.runs))
        .where(Job.namespace_uuid == ns.uuid, Job.is_hidden.is_(False))
        .order_by(Job.name)
        .limit(limit)
        .offset(offset)
    )
    return list(result.scalars().all())


async def list_all_jobs(
    db: AsyncSession, limit: int = 100, offset: int = 0,
    last_run_states: list[str] | None = None
) -> list[Job]:
    from sqlalchemy.orm import contains_eager
    q = (
        select(Job)
        .options(selectinload(Job.namespace), selectinload(Job.runs))
        .where(Job.is_hidden.is_(False))
        .order_by(Job.updated_at.desc())
        .limit(limit)
        .offset(offset)
    )
    result = await db.execute(q)
    jobs = list(result.scalars().all())
    if last_run_states:
        states = [s.upper() for s in last_run_states]
        jobs = [
            j for j in jobs
            if j.runs and any(
                r.current_run_state in states
                for r in sorted(j.runs, key=lambda r: r.created_at, reverse=True)[:1]
            )
        ]
    return jobs


async def count_jobs(db: AsyncSession, namespace_name: str | None = None) -> int:
    q = select(func.count()).select_from(Job).where(Job.is_hidden.is_(False))
    if namespace_name is not None:
        ns = await get_namespace(db, namespace_name)
        if ns is None:
            return 0
        q = q.where(Job.namespace_uuid == ns.uuid)
    result = await db.execute(q)
    return result.scalar_one()


async def soft_delete_job(
    db: AsyncSession, namespace_name: str, job_name: str
) -> Job | None:
    job = await get_job(db, namespace_name, job_name)
    if job is None:
        return None
    job.is_hidden = True
    await db.flush()
    await db.refresh(job)
    return job


async def count_runs(db: AsyncSession, namespace_name: str, job_name: str) -> int:
    job = await get_job(db, namespace_name, job_name)
    if job is None:
        return 0
    result = await db.execute(
        select(func.count()).select_from(Run).where(Run.job_uuid == job.uuid)
    )
    return result.scalar_one()


async def upsert_job(
    db: AsyncSession, namespace_name: str, job_name: str, body: JobCreate
) -> Job:
    ns = await get_namespace(db, namespace_name)
    if ns is None:
        ns = Namespace(name=namespace_name, current_owner_name="anonymous")
        db.add(ns)
        await db.flush()

    result = await db.execute(
        select(Job).where(Job.namespace_uuid == ns.uuid, Job.name == job_name)
    )
    job = result.scalar_one_or_none()
    if job is None:
        job = Job(namespace_uuid=ns.uuid, name=job_name)
        db.add(job)

    job.type = body.type
    job.description = body.description
    await db.flush()
    return await get_job(db, namespace_name, job_name)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


async def get_run(db: AsyncSession, run_id: uuid.UUID) -> Run | None:
    result = await db.execute(
        select(Run)
        .options(
            selectinload(Run.input_datasets).selectinload(RunDatasetInput.dataset),
            selectinload(Run.output_datasets).selectinload(RunDatasetOutput.dataset),
        )
        .where(Run.uuid == run_id)
    )
    return result.scalar_one_or_none()


async def list_runs(
    db: AsyncSession,
    namespace_name: str,
    job_name: str,
    limit: int = 100,
    offset: int = 0,
) -> list[Run]:
    job = await get_job(db, namespace_name, job_name)
    if job is None:
        return []
    result = await db.execute(
        select(Run)
        .where(Run.job_uuid == job.uuid)
        .order_by(Run.created_at.desc())
        .limit(limit)
        .offset(offset)
    )
    return list(result.scalars().all())


async def create_run(
    db: AsyncSession, namespace_name: str, job_name: str, body: RunCreate
) -> Run:
    job = await get_job(db, namespace_name, job_name)
    if job is None:
        job = await upsert_job(db, namespace_name, job_name, JobCreate())

    run = Run(
        uuid=body.id or uuid.uuid4(),
        job_uuid=job.uuid,
        nominal_start_time=body.nominal_start_time,
        nominal_end_time=body.nominal_end_time,
        current_run_state="NEW",
        facets=body.args,
    )
    db.add(run)
    await db.flush()
    await db.refresh(run)
    return run


async def transition_run(
    db: AsyncSession, run_id: uuid.UUID, new_state: str
) -> Run | None:
    run = await get_run(db, run_id)
    if run is None:
        return None
    run.current_run_state = new_state
    now = datetime.now(timezone.utc)
    if new_state == "RUNNING":
        run.started_at = now
    elif new_state in ("COMPLETED", "FAILED", "ABORTED"):
        run.ended_at = now
    await db.flush()
    await db.refresh(run)
    return run


# ---------------------------------------------------------------------------
# OpenLineage ingest
# ---------------------------------------------------------------------------


async def ingest_openlineage_event(db: AsyncSession, event: OpenLineageEvent) -> None:
    """
    Store the raw event and upsert the derived entities (namespace, job, run,
    datasets, lineage edges) — mirrors what the Java backend does.
    """
    # 1. Persist raw event
    raw = LineageEvent(
        event_time=event.eventTime,
        event_type=event.eventType,
        run_uuid=uuid.UUID(event.run.runId),
        job_name=event.job.name,
        job_namespace=event.job.namespace,
        producer=event.producer,
        payload=event.model_dump(mode="json"),
    )
    db.add(raw)

    # 2. Upsert namespace + job
    ns = await get_namespace(db, event.job.namespace)
    if ns is None:
        ns = Namespace(name=event.job.namespace, current_owner_name="openlineage")
        db.add(ns)
        await db.flush()

    job_result = await db.execute(
        select(Job).where(Job.namespace_uuid == ns.uuid, Job.name == event.job.name)
    )
    job = job_result.scalar_one_or_none()
    if job is None:
        job = Job(namespace_uuid=ns.uuid, name=event.job.name)
        db.add(job)
        await db.flush()
    job_facets = {k: v for k, v in event.job.facets.items() if not k.startswith("_")}
    if job_facets:
        job.facets = {**(job.facets or {}), **job_facets}
        await db.flush()

    # 3. Upsert run
    run_uuid = uuid.UUID(event.run.runId)
    run_result = await db.execute(select(Run).where(Run.uuid == run_uuid))
    run = run_result.scalar_one_or_none()
    if run is None:
        run = Run(uuid=run_uuid, job_uuid=job.uuid, facets=event.run.facets)
        db.add(run)

    now = datetime.now(timezone.utc)
    event_type = event.eventType.upper()
    _STATE_MAP = {"START": "RUNNING", "COMPLETE": "COMPLETED", "FAIL": "FAILED", "ABORT": "ABORTED"}
    run.current_run_state = _STATE_MAP.get(event_type, event_type)
    if event_type == "START":
        run.started_at = event.eventTime
    elif event_type in ("COMPLETE", "FAIL", "ABORT"):
        run.ended_at = event.eventTime
    await db.flush()

    # 4. Upsert datasets + lineage edges
    async def _ensure_dataset(ns_name: str, ds_name: str, facets: dict) -> Dataset:
        ds_ns = await get_namespace(db, ns_name)
        if ds_ns is None:
            ds_ns = Namespace(name=ns_name, current_owner_name="openlineage")
            db.add(ds_ns)
            await db.flush()
        ds_result = await db.execute(
            select(Dataset).where(
                Dataset.namespace_uuid == ds_ns.uuid, Dataset.name == ds_name
            )
        )
        ds = ds_result.scalar_one_or_none()
        if ds is None:
            ds = Dataset(namespace_uuid=ds_ns.uuid, name=ds_name)
            db.add(ds)
            await db.flush()

        # Extract and upsert schema fields from the OpenLineage schema facet
        schema_fields = (facets.get("schema") or {}).get("fields") or []
        for f in schema_fields:
            field_name = f.get("name")
            if not field_name:
                continue
            field_result = await db.execute(
                select(DatasetField).where(
                    DatasetField.dataset_uuid == ds.uuid,
                    DatasetField.name == field_name,
                )
            )
            field = field_result.scalar_one_or_none()
            if field is None:
                field = DatasetField(dataset_uuid=ds.uuid, name=field_name)
                db.add(field)
            field.type = f.get("type") or field.type
            field.description = f.get("description") or field.description
        if schema_fields:
            await db.flush()

        # Merge incoming facets (skip internal _ keys) into the stored column
        clean_facets = {k: v for k, v in facets.items() if not k.startswith("_")}
        if clean_facets:
            ds.facets = {**(ds.facets or {}), **clean_facets}
            await db.flush()

        return ds

    for inp in event.inputs:
        ds = await _ensure_dataset(inp.namespace, inp.name, inp.facets)
        edge_result = await db.execute(
            select(RunDatasetInput).where(
                RunDatasetInput.run_uuid == run.uuid,
                RunDatasetInput.dataset_uuid == ds.uuid,
            )
        )
        if edge_result.scalar_one_or_none() is None:
            db.add(RunDatasetInput(run_uuid=run.uuid, dataset_uuid=ds.uuid))

    for out in event.outputs:
        ds = await _ensure_dataset(out.namespace, out.name, out.facets)
        edge_result = await db.execute(
            select(RunDatasetOutput).where(
                RunDatasetOutput.run_uuid == run.uuid,
                RunDatasetOutput.dataset_uuid == ds.uuid,
            )
        )
        if edge_result.scalar_one_or_none() is None:
            db.add(RunDatasetOutput(run_uuid=run.uuid, dataset_uuid=ds.uuid))

    await db.flush()


# ---------------------------------------------------------------------------
# Lineage graph
# ---------------------------------------------------------------------------


async def get_lineage_graph(
    db: AsyncSession,
    node_id: str,
    node_type: str,
    depth: int = 2,
) -> dict:
    """
    Walk run_input/output_mapping tables to build a lineage graph.
    Returns {graph: [{id, type, data, inEdges, outEdges}, ...]} matching
    the Marquez frontend contract.
    """
    visited_jobs: set[uuid.UUID] = set()
    visited_datasets: set[uuid.UUID] = set()
    # node_id_str → node dict (mutable so we can append edges)
    node_map: dict[str, dict] = {}
    edges: list[dict] = []

    def _get_or_create_node(node_id_str: str, node_type_str: str, data: dict) -> dict:
        if node_id_str not in node_map:
            node_map[node_id_str] = {
                "id": node_id_str,
                "type": node_type_str,
                "data": data,
                "inEdges": [],
                "outEdges": [],
            }
        return node_map[node_id_str]

    def _add_edge(origin: str, destination: str) -> None:
        edge = {"origin": origin, "destination": destination}
        edges.append(edge)
        if origin in node_map:
            node_map[origin]["outEdges"].append(edge)
        if destination in node_map:
            node_map[destination]["inEdges"].append(edge)

    async def _walk_job(job_uuid: uuid.UUID, remaining: int) -> None:
        if job_uuid in visited_jobs or remaining < 0:
            return
        visited_jobs.add(job_uuid)
        job_result = await db.execute(
            select(Job).options(selectinload(Job.namespace)).where(Job.uuid == job_uuid)
        )
        job = job_result.scalar_one_or_none()
        if job is None:
            return
        job_node_id = f"job:{job.namespace.name}:{job.name}"
        _get_or_create_node(job_node_id, "JOB", {"namespace": job.namespace.name, "name": job.name})

        runs_result = await db.execute(select(Run).where(Run.job_uuid == job_uuid))
        for run in runs_result.scalars().all():
            out_result = await db.execute(
                select(RunDatasetOutput)
                .options(selectinload(RunDatasetOutput.dataset).selectinload(Dataset.namespace))
                .where(RunDatasetOutput.run_uuid == run.uuid)
            )
            for mapping in out_result.scalars().all():
                ds = mapping.dataset
                if ds is None:
                    continue
                ds_node_id = f"dataset:{ds.namespace.name}:{ds.name}"
                _get_or_create_node(ds_node_id, "DATASET", await _dataset_data(ds))
                _add_edge(job_node_id, ds_node_id)
                await _walk_dataset(ds.uuid, remaining - 1)

            in_result = await db.execute(
                select(RunDatasetInput)
                .options(selectinload(RunDatasetInput.dataset).selectinload(Dataset.namespace))
                .where(RunDatasetInput.run_uuid == run.uuid)
            )
            for mapping in in_result.scalars().all():
                ds = mapping.dataset
                if ds is None:
                    continue
                ds_node_id = f"dataset:{ds.namespace.name}:{ds.name}"
                _get_or_create_node(ds_node_id, "DATASET", await _dataset_data(ds))
                _add_edge(ds_node_id, job_node_id)
                await _walk_dataset(ds.uuid, remaining - 1)

    async def _dataset_data(ds: Dataset) -> dict:
        fields_result = await db.execute(
            select(DatasetField).where(DatasetField.dataset_uuid == ds.uuid)
        )
        fields = [{"name": f.name, "type": f.type} for f in fields_result.scalars().all()]
        return {
            "namespace": ds.namespace.name,
            "name": ds.name,
            "fields": fields,
        }

    async def _walk_dataset(dataset_uuid: uuid.UUID, remaining: int) -> None:
        if dataset_uuid in visited_datasets or remaining < 0:
            return
        visited_datasets.add(dataset_uuid)
        ds_result = await db.execute(
            select(Dataset)
            .options(selectinload(Dataset.namespace))
            .where(Dataset.uuid == dataset_uuid)
        )
        ds = ds_result.scalar_one_or_none()
        if ds is None:
            return
        ds_node_id = f"dataset:{ds.namespace.name}:{ds.name}"
        _get_or_create_node(ds_node_id, "DATASET", await _dataset_data(ds))

        in_result = await db.execute(
            select(RunDatasetInput).where(RunDatasetInput.dataset_uuid == dataset_uuid)
        )
        for mapping in in_result.scalars().all():
            run_result = await db.execute(
                select(Run)
                .options(selectinload(Run.job).selectinload(Job.namespace))
                .where(Run.uuid == mapping.run_uuid)
            )
            run = run_result.scalar_one_or_none()
            if run and run.job and run.job.namespace:
                job_node_id = f"job:{run.job.namespace.name}:{run.job.name}"
                _get_or_create_node(job_node_id, "JOB", {"namespace": run.job.namespace.name, "name": run.job.name})
                _add_edge(ds_node_id, job_node_id)
                await _walk_job(run.job.uuid, remaining - 1)

    parts = node_id.split(":", 1)
    if len(parts) == 2:
        if node_type == "JOB":
            job = await get_job(db, parts[0], parts[1])
            if job:
                await _walk_job(job.uuid, depth)
        elif node_type == "DATASET":
            ds = await get_dataset(db, parts[0], parts[1])
            if ds:
                await _walk_dataset(ds.uuid, depth)

    return {"graph": list(node_map.values())}


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


async def list_tags(
    db: AsyncSession, limit: int = 100, offset: int = 0
) -> list[str]:
    from sqlalchemy import union, literal_column
    from marquito.models.orm import DatasetTag, DatasetFieldTag

    q = (
        union(
            select(DatasetTag.name),
            select(DatasetFieldTag.name),
        )
        .order_by(literal_column("name"))
        .limit(limit)
        .offset(offset)
    )
    result = await db.execute(q)
    return [row[0] for row in result.all()]
