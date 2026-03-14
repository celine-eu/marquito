"""
GraphQL schema via Strawberry.
Exposes the same data as the REST API but lets clients fetch exactly
what they need in a single round-trip.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

import strawberry
from strawberry.fastapi import GraphQLRouter
from strawberry.types import Info

from marquito.db.session import AsyncSessionLocal
from marquito.services import lineage as svc


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


@strawberry.type
class GqlNamespace:
    name: str
    owner_name: Optional[str]
    description: Optional[str]
    created_at: datetime
    updated_at: datetime


@strawberry.type
class GqlSource:
    name: str
    type: str
    connection_url: Optional[str]
    description: Optional[str]
    created_at: datetime
    updated_at: datetime


@strawberry.type
class GqlField:
    name: str
    type: Optional[str]
    description: Optional[str]
    tags: list[str]


@strawberry.type
class GqlDataset:
    name: str
    namespace: str
    type: str
    physical_name: Optional[str]
    description: Optional[str]
    created_at: datetime
    updated_at: datetime
    tags: list[str]
    fields: list[GqlField]


@strawberry.type
class GqlRun:
    id: str
    state: Optional[str]
    started_at: Optional[datetime]
    ended_at: Optional[datetime]
    nominal_start_time: Optional[datetime]
    nominal_end_time: Optional[datetime]
    created_at: datetime


@strawberry.type
class GqlJob:
    name: str
    namespace: str
    type: str
    description: Optional[str]
    created_at: datetime
    updated_at: datetime


# ---------------------------------------------------------------------------
# Resolvers
# ---------------------------------------------------------------------------


@strawberry.type
class Query:
    @strawberry.field
    async def namespace(self, name: str, info: Info) -> Optional[GqlNamespace]:
        async with AsyncSessionLocal() as db:
            ns = await svc.get_namespace(db, name)
            if ns is None:
                return None
            return GqlNamespace(
                name=ns.name,
                owner_name=ns.current_owner_name,
                description=ns.description,
                created_at=ns.created_at,
                updated_at=ns.updated_at,
            )

    @strawberry.field
    async def namespaces(
        self, limit: int = 100, offset: int = 0, info: Info = None
    ) -> list[GqlNamespace]:
        async with AsyncSessionLocal() as db:
            items = await svc.list_namespaces(db, limit=limit, offset=offset)
            return [
                GqlNamespace(
                    name=n.name,
                    owner_name=n.current_owner_name,
                    description=n.description,
                    created_at=n.created_at,
                    updated_at=n.updated_at,
                )
                for n in items
            ]

    @strawberry.field
    async def dataset(
        self, namespace: str, name: str, info: Info = None
    ) -> Optional[GqlDataset]:
        async with AsyncSessionLocal() as db:
            ds = await svc.get_dataset(db, namespace, name)
            if ds is None:
                return None
            return _map_dataset(ds, namespace)

    @strawberry.field
    async def datasets(
        self, namespace: str, limit: int = 100, offset: int = 0, info: Info = None
    ) -> list[GqlDataset]:
        async with AsyncSessionLocal() as db:
            items = await svc.list_datasets(db, namespace, limit=limit, offset=offset)
            return [_map_dataset(d, namespace) for d in items]

    @strawberry.field
    async def job(
        self, namespace: str, name: str, info: Info = None
    ) -> Optional[GqlJob]:
        async with AsyncSessionLocal() as db:
            job = await svc.get_job(db, namespace, name)
            if job is None:
                return None
            return _map_job(job, namespace)

    @strawberry.field
    async def jobs(
        self, namespace: str, limit: int = 100, offset: int = 0, info: Info = None
    ) -> list[GqlJob]:
        async with AsyncSessionLocal() as db:
            items = await svc.list_jobs(db, namespace, limit=limit, offset=offset)
            return [_map_job(j, namespace) for j in items]

    @strawberry.field
    async def run(self, id: str, info: Info = None) -> Optional[GqlRun]:
        async with AsyncSessionLocal() as db:
            run = await svc.get_run(db, uuid.UUID(id))
            if run is None:
                return None
            return _map_run(run)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _map_dataset(ds, namespace_name: str) -> GqlDataset:
    return GqlDataset(
        name=ds.name,
        namespace=namespace_name,
        type=ds.type,
        physical_name=ds.physical_name,
        description=ds.description,
        created_at=ds.created_at,
        updated_at=ds.updated_at,
        tags=[t.name for t in ds.tags],
        fields=[
            GqlField(
                name=f.name,
                type=f.type,
                description=f.description,
                tags=[t.name for t in f.tags],
            )
            for f in ds.fields
        ],
    )


def _map_job(job, namespace_name: str) -> GqlJob:
    return GqlJob(
        name=job.name,
        namespace=namespace_name,
        type=job.type,
        description=job.description,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


def _map_run(run) -> GqlRun:
    return GqlRun(
        id=str(run.uuid),
        state=run.current_run_state,
        started_at=run.started_at,
        ended_at=run.ended_at,
        nominal_start_time=run.nominal_start_time,
        nominal_end_time=run.nominal_end_time,
        created_at=run.created_at,
    )


# ---------------------------------------------------------------------------
# Router (mounted in main app)
# ---------------------------------------------------------------------------

schema = strawberry.Schema(query=Query)
graphql_router = GraphQLRouter(schema, graphql_ide="graphiql")
