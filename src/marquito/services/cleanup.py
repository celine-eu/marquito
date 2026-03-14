"""
Database history cleanup service.

Purges time-bounded history rows while preserving reference data
(namespaces, jobs, datasets, sources) and the currently-active version
pointers (Dataset.current_version_uuid, Job.current_version_uuid).

Tables pruned:
  lineage_events        — raw OL event log
  runs_input_mapping    — cascade before runs
  runs_output_mapping   — cascade before runs
  runs                  — run history
  dataset_versions      — version snapshots (skips current pointers)
  job_versions          — version snapshots (skips current pointers)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import delete, func, not_, select
from sqlalchemy.ext.asyncio import AsyncSession


# ---------------------------------------------------------------------------
# Duration parsing
# ---------------------------------------------------------------------------

_DURATION_RE = re.compile(r"^(\d+)(d|h|m|s)$")
_UNIT_SECONDS = {"d": 86400, "h": 3600, "m": 60, "s": 1}


def parse_retain(value: str) -> timedelta:
    """Parse a retention string like '15d', '12h', '30m' into a timedelta."""
    m = _DURATION_RE.match(value.strip())
    if not m:
        raise ValueError(
            f"Invalid retain value '{value}'. "
            "Use a number followed by d (days), h (hours), m (minutes), or s (seconds). "
            "Examples: 15d, 12h, 30m"
        )
    amount, unit = int(m.group(1)), m.group(2)
    return timedelta(seconds=amount * _UNIT_SECONDS[unit])


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class CleanupResult:
    cutoff: datetime
    lineage_events: int = 0
    runs: int = 0
    run_input_mappings: int = 0
    run_output_mappings: int = 0
    dataset_versions: int = 0
    job_versions: int = 0

    @property
    def total(self) -> int:
        return (
            self.lineage_events
            + self.runs
            + self.run_input_mappings
            + self.run_output_mappings
            + self.dataset_versions
            + self.job_versions
        )


# ---------------------------------------------------------------------------
# Dry-run counts
# ---------------------------------------------------------------------------


async def count_stale(db: AsyncSession, cutoff: datetime) -> CleanupResult:
    """Return how many rows would be deleted without touching the DB."""
    from marquito.models.orm import (
        Dataset,
        Job,
        JobVersion,
        LineageEvent,
        Run,
        RunDatasetInput,
        RunDatasetOutput,
    )
    from marquito.models.orm import DatasetVersion as DV

    result = CleanupResult(cutoff=cutoff)

    result.lineage_events = (
        await db.execute(
            select(func.count()).select_from(LineageEvent).where(LineageEvent.created_at < cutoff)
        )
    ).scalar_one()

    # Stale runs (used to cascade mappings count)
    stale_run_uuids_q = select(Run.uuid).where(Run.created_at < cutoff)

    result.run_input_mappings = (
        await db.execute(
            select(func.count())
            .select_from(RunDatasetInput)
            .where(RunDatasetInput.run_uuid.in_(stale_run_uuids_q))
        )
    ).scalar_one()

    result.run_output_mappings = (
        await db.execute(
            select(func.count())
            .select_from(RunDatasetOutput)
            .where(RunDatasetOutput.run_uuid.in_(stale_run_uuids_q))
        )
    ).scalar_one()

    result.runs = (
        await db.execute(
            select(func.count()).select_from(Run).where(Run.created_at < cutoff)
        )
    ).scalar_one()

    # Protect dataset_versions still referenced as current
    protected_dv = select(Dataset.current_version_uuid).where(
        Dataset.current_version_uuid.isnot(None)
    )
    result.dataset_versions = (
        await db.execute(
            select(func.count())
            .select_from(DV)
            .where(DV.created_at < cutoff, not_(DV.uuid.in_(protected_dv)))
        )
    ).scalar_one()

    # Protect job_versions still referenced as current
    protected_jv = select(Job.current_version_uuid).where(
        Job.current_version_uuid.isnot(None)
    )
    result.job_versions = (
        await db.execute(
            select(func.count())
            .select_from(JobVersion)
            .where(JobVersion.created_at < cutoff, not_(JobVersion.uuid.in_(protected_jv)))
        )
    ).scalar_one()

    return result


# ---------------------------------------------------------------------------
# Actual deletion
# ---------------------------------------------------------------------------


async def run_cleanup(db: AsyncSession, cutoff: datetime) -> CleanupResult:
    """Delete stale history rows and return counts of what was removed."""
    from marquito.models.orm import (
        Dataset,
        Job,
        JobVersion,
        LineageEvent,
        Run,
        RunDatasetInput,
        RunDatasetOutput,
    )
    from marquito.models.orm import DatasetVersion as DV

    result = CleanupResult(cutoff=cutoff)

    # 1. Lineage events
    r = await db.execute(
        delete(LineageEvent).where(LineageEvent.created_at < cutoff)
    )
    result.lineage_events = r.rowcount

    # 2. Run mappings (must precede run delete to avoid FK violations)
    stale_run_uuids_q = select(Run.uuid).where(Run.created_at < cutoff)

    r = await db.execute(
        delete(RunDatasetInput).where(RunDatasetInput.run_uuid.in_(stale_run_uuids_q))
    )
    result.run_input_mappings = r.rowcount

    r = await db.execute(
        delete(RunDatasetOutput).where(RunDatasetOutput.run_uuid.in_(stale_run_uuids_q))
    )
    result.run_output_mappings = r.rowcount

    # 3. Runs
    r = await db.execute(delete(Run).where(Run.created_at < cutoff))
    result.runs = r.rowcount

    # 4. Dataset versions (keep current pointers)
    protected_dv = select(Dataset.current_version_uuid).where(
        Dataset.current_version_uuid.isnot(None)
    )
    r = await db.execute(
        delete(DV).where(DV.created_at < cutoff, not_(DV.uuid.in_(protected_dv)))
    )
    result.dataset_versions = r.rowcount

    # 5. Job versions (keep current pointers)
    protected_jv = select(Job.current_version_uuid).where(
        Job.current_version_uuid.isnot(None)
    )
    r = await db.execute(
        delete(JobVersion).where(
            JobVersion.created_at < cutoff, not_(JobVersion.uuid.in_(protected_jv))
        )
    )
    result.job_versions = r.rowcount

    await db.flush()
    return result
