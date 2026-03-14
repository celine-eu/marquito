from datetime import datetime

from pydantic import BaseModel


class LineageMetric(BaseModel):
    start_interval: datetime
    end_interval: datetime
    fail: int
    start: int
    complete: int
    abort: int


class IntervalMetric(BaseModel):
    start_interval: datetime
    end_interval: datetime
    count: int
