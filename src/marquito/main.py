from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from marquito.api.v1.endpoints.admin import router as admin_router
from marquito.api.v1.endpoints.datasets import router as datasets_router
from marquito.api.v1.endpoints.events import router as events_router
from marquito.api.v1.endpoints.jobs import router as jobs_router
from marquito.api.v1.endpoints.namespaces import router as namespaces_router
from marquito.api.v1.endpoints.openlineage import router as lineage_router
from marquito.api.v1.endpoints.stats import router as stats_router
from marquito.api.v1.endpoints.search import router as search_router
from marquito.api.v1.endpoints.tags import router as tags_router
from marquito.core.config import settings
from marquito.graphql.schema import graphql_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: nothing needed if using Alembic for migrations
    yield
    # Shutdown: SQLAlchemy pools drain automatically


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.api_title,
        description=settings.api_description,
        version=settings.api_version,
        lifespan=lifespan,
        docs_url="/api/v1/docs",
        redoc_url="/api/v1/redoc",
        openapi_url="/api/v1/openapi.json",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # REST API  (v1 prefix)
    prefix = "/api/v1"
    app.include_router(admin_router, prefix=prefix)
    app.include_router(namespaces_router, prefix=prefix)
    app.include_router(datasets_router, prefix=prefix)
    app.include_router(jobs_router, prefix=prefix)
    app.include_router(tags_router, prefix=prefix)
    app.include_router(stats_router, prefix=prefix)
    app.include_router(events_router, prefix=prefix)
    app.include_router(lineage_router, prefix=prefix)
    app.include_router(search_router, prefix=prefix)

    # GraphQL  (/graphql)
    app.include_router(graphql_router, prefix="/graphql")

    return app


app = create_app()
