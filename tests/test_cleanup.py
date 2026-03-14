"""
Tests for the database cleanup service and CLI.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from marquito.db.session import Base
from marquito.models.orm import (
    Dataset,
    DatasetVersion,
    Job,
    JobVersion,
    LineageEvent,
    Namespace,
    Run,
    RunDatasetInput,
    RunDatasetOutput,
)
from marquito.services.cleanup import CleanupResult, count_stale, parse_retain, run_cleanup

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def db():
    engine = create_async_engine(TEST_DB_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with Session() as session:
        yield session
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ts(days_ago: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days_ago)


async def _seed(db: AsyncSession, *, event_age_days: float, run_age_days: float) -> dict:
    """
    Insert one namespace, job, dataset, lineage event, run (with mappings),
    and version rows.  All timestamps are set explicitly so tests are
    deterministic regardless of wall-clock.
    """
    ns = Namespace(name=f"ns-{uuid.uuid4().hex[:8]}", current_owner_name="test")
    db.add(ns)
    await db.flush()

    job = Job(namespace_uuid=ns.uuid, name=f"job-{uuid.uuid4().hex[:8]}")
    db.add(job)
    await db.flush()

    jv = JobVersion(
        job_uuid=job.uuid,
        version=uuid.uuid4(),
        inputs=[],
        outputs=[],
    )
    jv.created_at = _ts(run_age_days)
    db.add(jv)
    await db.flush()

    # Make this the current version
    job.current_version_uuid = jv.uuid
    await db.flush()

    ds = Dataset(namespace_uuid=ns.uuid, name=f"ds-{uuid.uuid4().hex[:8]}")
    db.add(ds)
    await db.flush()

    dv = DatasetVersion(
        dataset_uuid=ds.uuid,
        version=uuid.uuid4(),
        namespace_name=ns.name,
        dataset_name=ds.name,
        facets={},
    )
    dv.created_at = _ts(run_age_days)
    db.add(dv)
    await db.flush()

    # Make this the current version
    ds.current_version_uuid = dv.uuid
    await db.flush()

    event = LineageEvent(
        event_time=_ts(event_age_days),
        event_type="COMPLETE",
        run_uuid=uuid.uuid4(),
        job_name=job.name,
        job_namespace=ns.name,
        producer="test",
        payload={},
    )
    event.created_at = _ts(event_age_days)
    db.add(event)

    run = Run(uuid=uuid.uuid4(), job_uuid=job.uuid, current_run_state="COMPLETED")
    run.created_at = _ts(run_age_days)
    db.add(run)
    await db.flush()

    inp = RunDatasetInput(run_uuid=run.uuid, dataset_uuid=ds.uuid)
    out = RunDatasetOutput(run_uuid=run.uuid, dataset_uuid=ds.uuid)
    db.add(inp)
    db.add(out)
    await db.flush()

    return {
        "ns": ns, "job": job, "jv": jv,
        "ds": ds, "dv": dv,
        "event": event, "run": run,
    }


# ---------------------------------------------------------------------------
# parse_retain
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value,expected_days", [
    ("15d", 15),
    ("7d", 7),
    ("1d", 1),
])
def test_parse_retain_days(value, expected_days):
    assert parse_retain(value) == timedelta(days=expected_days)


@pytest.mark.parametrize("value,expected_seconds", [
    ("12h", 12 * 3600),
    ("30m", 30 * 60),
    ("90s", 90),
])
def test_parse_retain_sub_day(value, expected_seconds):
    assert parse_retain(value).total_seconds() == expected_seconds


@pytest.mark.parametrize("bad", ["15", "d", "15x", "", "1.5d", "-5d"])
def test_parse_retain_invalid(bad):
    with pytest.raises(ValueError, match="Invalid retain"):
        parse_retain(bad)


# ---------------------------------------------------------------------------
# count_stale (dry-run)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_count_stale_old_rows(db):
    """Rows created 30 days ago should all be counted when cutoff=7d."""
    await _seed(db, event_age_days=30, run_age_days=30)

    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    result = await count_stale(db, cutoff)

    assert result.lineage_events >= 1
    assert result.runs >= 1
    assert result.run_input_mappings >= 1
    assert result.run_output_mappings >= 1
    # dataset_version is current pointer — should be protected
    assert result.dataset_versions == 0
    assert result.job_versions == 0


@pytest.mark.asyncio
async def test_count_stale_recent_rows(db):
    """Rows created 1 day ago should not be counted with a 7-day cutoff."""
    await _seed(db, event_age_days=1, run_age_days=1)

    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    result = await count_stale(db, cutoff)

    assert result.lineage_events == 0
    assert result.runs == 0
    assert result.run_input_mappings == 0
    assert result.run_output_mappings == 0
    assert result.dataset_versions == 0
    assert result.job_versions == 0


@pytest.mark.asyncio
async def test_count_stale_protects_current_version(db):
    """
    dataset_versions / job_versions that are current pointers must
    never be counted for deletion, even if old.
    """
    seeded = await _seed(db, event_age_days=30, run_age_days=30)

    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    result = await count_stale(db, cutoff)

    assert result.dataset_versions == 0, "current dataset_version must be protected"
    assert result.job_versions == 0, "current job_version must be protected"


@pytest.mark.asyncio
async def test_count_stale_non_current_versions_counted(db):
    """Non-current old versions ARE counted for deletion."""
    ns = Namespace(name=f"ns-{uuid.uuid4().hex[:8]}", current_owner_name="test")
    db.add(ns)
    await db.flush()

    ds = Dataset(namespace_uuid=ns.uuid, name="ds-noncurrent")
    db.add(ds)
    await db.flush()

    # Old version — NOT set as current
    old_dv = DatasetVersion(
        dataset_uuid=ds.uuid,
        version=uuid.uuid4(),
        namespace_name=ns.name,
        dataset_name=ds.name,
        facets={},
    )
    old_dv.created_at = _ts(30)
    db.add(old_dv)
    await db.flush()

    # Newer version — set as current
    new_dv = DatasetVersion(
        dataset_uuid=ds.uuid,
        version=uuid.uuid4(),
        namespace_name=ns.name,
        dataset_name=ds.name,
        facets={},
    )
    db.add(new_dv)
    ds.current_version_uuid = new_dv.uuid
    await db.flush()

    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    result = await count_stale(db, cutoff)

    assert result.dataset_versions >= 1, "old non-current version should be counted"


# ---------------------------------------------------------------------------
# run_cleanup (actual deletion)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_cleanup_deletes_old_rows(db):
    seeded = await _seed(db, event_age_days=30, run_age_days=30)

    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    result = await run_cleanup(db, cutoff)

    assert result.lineage_events >= 1
    assert result.runs >= 1
    assert result.run_input_mappings >= 1
    assert result.run_output_mappings >= 1


@pytest.mark.asyncio
async def test_run_cleanup_spares_recent_rows(db):
    seeded = await _seed(db, event_age_days=1, run_age_days=1)

    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    result = await run_cleanup(db, cutoff)

    assert result.total == 0


@pytest.mark.asyncio
async def test_run_cleanup_preserves_current_versions(db):
    seeded = await _seed(db, event_age_days=30, run_age_days=30)

    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    result = await run_cleanup(db, cutoff)

    assert result.dataset_versions == 0, "current dataset_version pointer must survive"
    assert result.job_versions == 0, "current job_version pointer must survive"


@pytest.mark.asyncio
async def test_run_cleanup_deletes_non_current_versions(db):
    ns = Namespace(name=f"ns-{uuid.uuid4().hex[:8]}", current_owner_name="test")
    db.add(ns)
    await db.flush()

    ds = Dataset(namespace_uuid=ns.uuid, name="ds-prune")
    db.add(ds)
    await db.flush()

    old_dv = DatasetVersion(
        dataset_uuid=ds.uuid, version=uuid.uuid4(),
        namespace_name=ns.name, dataset_name=ds.name, facets={},
    )
    old_dv.created_at = _ts(30)
    db.add(old_dv)
    await db.flush()

    new_dv = DatasetVersion(
        dataset_uuid=ds.uuid, version=uuid.uuid4(),
        namespace_name=ns.name, dataset_name=ds.name, facets={},
    )
    db.add(new_dv)
    ds.current_version_uuid = new_dv.uuid
    await db.flush()

    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    result = await run_cleanup(db, cutoff)

    assert result.dataset_versions == 1


@pytest.mark.asyncio
async def test_run_cleanup_mixed_ages(db):
    """Old rows deleted, recent rows untouched in a single pass."""
    await _seed(db, event_age_days=30, run_age_days=30)
    await _seed(db, event_age_days=1, run_age_days=1)

    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    result = await run_cleanup(db, cutoff)

    # Exactly the old batch should be gone
    assert result.lineage_events == 1
    assert result.runs == 1
    assert result.run_input_mappings == 1
    assert result.run_output_mappings == 1


# ---------------------------------------------------------------------------
# CLI smoke-test (parse_retain edge cases via the service layer)
# ---------------------------------------------------------------------------


def test_cleanup_result_total():
    r = CleanupResult(
        cutoff=datetime.now(timezone.utc),
        lineage_events=10,
        runs=5,
        run_input_mappings=3,
        run_output_mappings=2,
        dataset_versions=1,
        job_versions=1,
    )
    assert r.total == 22
