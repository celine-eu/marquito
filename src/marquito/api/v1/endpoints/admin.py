from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from marquito.core.config import settings
from marquito.db.session import get_db
from marquito.schemas.api import HealthResponse

router = APIRouter()


@router.get("/healthcheck", response_model=HealthResponse, tags=["Admin"])
async def healthcheck(db: AsyncSession = Depends(get_db)):
    db_status = "ok"
    try:
        await db.execute(text("SELECT 1"))
    except Exception:
        db_status = "error"

    return HealthResponse(
        status="healthy" if db_status == "ok" else "degraded",
        version=settings.api_version,
        db=db_status,
    )


@router.get("/ping", tags=["Admin"])
async def ping():
    """Lightweight liveness probe — no DB call."""
    return {"message": "pong"}
