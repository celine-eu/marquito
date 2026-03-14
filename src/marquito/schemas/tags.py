from pydantic import BaseModel


class TagResponse(BaseModel):
    name: str


class TagList(BaseModel):
    tags: list[TagResponse]
