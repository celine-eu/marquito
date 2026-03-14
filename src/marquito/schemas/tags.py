from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator
from pydantic.alias_generators import to_camel


class TagResponse(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
        alias_generator=to_camel,
        populate_by_name=True,
    )

    name: str
    description: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_any(cls, obj: Any) -> "TagResponse":
        """Accept either a Tag ORM object or a bare name string."""
        if isinstance(obj, str):
            return cls(name=obj)
        return cls.model_validate(obj)


class TagList(BaseModel):
    tags: list[TagResponse]
