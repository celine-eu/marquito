import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel


class DatasetVersionResponse(BaseModel):
    id: str = ""              # "namespace:name"
    type: str = "DB_TABLE"
    name: str = ""
    physicalName: str | None = None
    createdAt: datetime
    version: uuid.UUID
    namespace: str = ""
    sourceName: str | None = None
    fields: list[dict[str, Any]] = []
    tags: list[str] = []
    lifecycleState: str | None = None
    description: str | None = None
    createdByRun: dict[str, Any] | None = None
    facets: dict[str, Any] = {}


class DatasetVersionList(BaseModel):
    versions: list[DatasetVersionResponse]
    totalCount: int = 0
