"""
Stats service — raw SQL against the Marquez PostgreSQL schema.
Queries run directly against base tables (no materialized views required).
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from marquito.schemas.stats import IntervalMetric, LineageMetric

# ---------------------------------------------------------------------------
# Lineage events
# ---------------------------------------------------------------------------

_LINEAGE_DAY_SQL = text(
    """
    WITH hour_series AS (
        SELECT generate_series(
            date_trunc('hour', now() - interval '23 hours'),
            date_trunc('hour', now()),
            '1 hour'
        ) AS start_interval
    )
    SELECT
        hs.start_interval,
        hs.start_interval + interval '1 hour' AS end_interval,
        COALESCE(SUM(CASE WHEN le.event_type = 'FAIL'     THEN 1 ELSE 0 END), 0) AS fail,
        COALESCE(SUM(CASE WHEN le.event_type = 'START'    THEN 1 ELSE 0 END), 0) AS start,
        COALESCE(SUM(CASE WHEN le.event_type = 'COMPLETE' THEN 1 ELSE 0 END), 0) AS complete,
        COALESCE(SUM(CASE WHEN le.event_type = 'ABORT'    THEN 1 ELSE 0 END), 0) AS abort
    FROM hour_series hs
    LEFT JOIN lineage_events le
           ON le.event_time >= hs.start_interval
          AND le.event_time <  hs.start_interval + interval '1 hour'
    GROUP BY hs.start_interval
    ORDER BY hs.start_interval
    """
)

_LINEAGE_WEEK_SQL = text(
    """
    WITH day_series AS (
        SELECT generate_series(
            date_trunc('day', now() AT TIME ZONE :tz) - interval '6 days',
            date_trunc('day', now() AT TIME ZONE :tz),
            '1 day'
        ) AS start_interval
    )
    SELECT
        ds.start_interval,
        ds.start_interval + interval '1 day' AS end_interval,
        COALESCE(SUM(CASE WHEN le.event_type = 'FAIL'     THEN 1 ELSE 0 END), 0) AS fail,
        COALESCE(SUM(CASE WHEN le.event_type = 'START'    THEN 1 ELSE 0 END), 0) AS start,
        COALESCE(SUM(CASE WHEN le.event_type = 'COMPLETE' THEN 1 ELSE 0 END), 0) AS complete,
        COALESCE(SUM(CASE WHEN le.event_type = 'ABORT'    THEN 1 ELSE 0 END), 0) AS abort
    FROM day_series ds
    LEFT JOIN lineage_events le
           ON (le.event_time AT TIME ZONE :tz) >= ds.start_interval
          AND (le.event_time AT TIME ZONE :tz) <  ds.start_interval + interval '1 day'
    GROUP BY ds.start_interval
    ORDER BY ds.start_interval
    """
)


async def get_lineage_event_stats(
    db: AsyncSession, period: str, timezone: str | None
) -> list[LineageMetric]:
    if period == "WEEK":
        rows = (await db.execute(_LINEAGE_WEEK_SQL, {"tz": timezone or "UTC"})).mappings()
    else:
        rows = (await db.execute(_LINEAGE_DAY_SQL)).mappings()
    return [LineageMetric(**r) for r in rows]


# ---------------------------------------------------------------------------
# Generic cumulative-count stats (jobs / datasets / sources)
# ---------------------------------------------------------------------------

def _day_sql(table: str) -> text:
    return text(
        f"""
        WITH hourly_series AS (
            SELECT generate_series(
                date_trunc('hour', now() - interval '23 hours'),
                date_trunc('hour', now()),
                '1 hour'
            ) AS start_interval
        ),
        before_count AS (
            SELECT count(*) AS n
            FROM {table}
            WHERE created_at < date_trunc('hour', now() - interval '23 hours')
        ),
        hourly AS (
            SELECT hs.start_interval, COUNT(t.uuid) AS in_hour
            FROM hourly_series hs
            LEFT JOIN {table} t
                   ON t.created_at >= hs.start_interval
                  AND t.created_at <  hs.start_interval + interval '1 hour'
            GROUP BY hs.start_interval
        )
        SELECT
            start_interval,
            start_interval + interval '1 hour' AS end_interval,
            SUM(in_hour) OVER (ORDER BY start_interval)
                + (SELECT n FROM before_count) AS count
        FROM hourly
        ORDER BY start_interval
        """
    )


def _week_sql(table: str) -> text:
    return text(
        f"""
        WITH daily_series AS (
            SELECT generate_series(
                date_trunc('day', now() AT TIME ZONE :tz) - interval '6 days',
                date_trunc('day', now() AT TIME ZONE :tz),
                '1 day'
            ) AS start_interval
        ),
        before_count AS (
            SELECT count(*) AS n
            FROM {table}
            WHERE (created_at AT TIME ZONE :tz) < date_trunc('day', now() AT TIME ZONE :tz - interval '6 days')
        ),
        daily AS (
            SELECT ds.start_interval, COUNT(t.uuid) AS in_day
            FROM daily_series ds
            LEFT JOIN {table} t
                   ON (t.created_at AT TIME ZONE :tz) >= ds.start_interval
                  AND (t.created_at AT TIME ZONE :tz) <  ds.start_interval + interval '1 day'
            GROUP BY ds.start_interval
        )
        SELECT
            start_interval,
            start_interval + interval '1 day' AS end_interval,
            SUM(in_day) OVER (ORDER BY start_interval)
                + (SELECT n FROM before_count) AS count
        FROM daily
        ORDER BY start_interval
        """
    )


async def _get_interval_stats(
    db: AsyncSession, table: str, period: str, timezone: str | None
) -> list[IntervalMetric]:
    if period == "WEEK":
        rows = (await db.execute(_week_sql(table), {"tz": timezone or "UTC"})).mappings()
    else:
        rows = (await db.execute(_day_sql(table))).mappings()
    return [IntervalMetric(**r) for r in rows]


async def get_job_stats(
    db: AsyncSession, period: str, timezone: str | None
) -> list[IntervalMetric]:
    return await _get_interval_stats(db, "jobs", period, timezone)


async def get_dataset_stats(
    db: AsyncSession, period: str, timezone: str | None
) -> list[IntervalMetric]:
    return await _get_interval_stats(db, "datasets", period, timezone)


async def get_source_stats(
    db: AsyncSession, period: str, timezone: str | None
) -> list[IntervalMetric]:
    return await _get_interval_stats(db, "sources", period, timezone)
