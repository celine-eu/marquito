"""
marquito CLI

Usage:
  marquito cleanup --retain=15d          # delete history older than 15 days
  marquito cleanup --retain=15d --dry-run  # preview without deleting
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import typer

app = typer.Typer(
    name="marquito",
    help="Marquito management CLI",
    add_completion=False,
)


@app.command()
def cleanup(
    retain: str = typer.Option(
        ...,
        "--retain",
        help="Retention window. Delete history older than this. Examples: 15d, 12h, 30m.",
        metavar="DURATION",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Print what would be deleted without making any changes.",
    ),
    db_url: str | None = typer.Option(
        None,
        "--db-url",
        envvar="MARQUITO_DATABASE_URL",
        help="Async database URL. Falls back to MARQUITO_* env vars.",
        show_default=False,
    ),
) -> None:
    """
    Remove history rows older than RETAIN from the database.

    Pruned tables: lineage_events, runs, run mappings,
    dataset_versions, job_versions.

    Reference data (namespaces, jobs, datasets) and the current
    version pointers are never touched.
    """
    from marquito.services.cleanup import parse_retain

    try:
        delta = parse_retain(retain)
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)

    asyncio.run(_cleanup_async(delta, dry_run=dry_run, db_url=db_url))


async def _cleanup_async(delta, *, dry_run: bool, db_url: str | None) -> None:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from marquito.services.cleanup import CleanupResult, count_stale, run_cleanup

    if db_url is None:
        from marquito.core.config import settings
        db_url = settings.database_url

    engine = create_async_engine(db_url, echo=False)
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    cutoff = datetime.now(timezone.utc) - delta

    typer.echo(
        f"{'[dry-run] ' if dry_run else ''}"
        f"Cutoff: {cutoff.strftime('%Y-%m-%d %H:%M:%S UTC')}  "
        f"(retain last {_fmt_delta(delta)})"
    )

    try:
        async with Session() as session:
            if dry_run:
                result = await count_stale(session, cutoff)
                _print_result(result, dry_run=True)
            else:
                async with session.begin():
                    result = await run_cleanup(session, cutoff)
                _print_result(result, dry_run=False)
    finally:
        await engine.dispose()


def _fmt_delta(delta) -> str:
    total = int(delta.total_seconds())
    if total % 86400 == 0:
        return f"{total // 86400}d"
    if total % 3600 == 0:
        return f"{total // 3600}h"
    if total % 60 == 0:
        return f"{total // 60}m"
    return f"{total}s"


def _print_result(result, *, dry_run: bool) -> None:
    prefix = "Would delete" if dry_run else "Deleted"
    typer.echo(f"\n{prefix}:")
    typer.echo(f"  lineage_events     {result.lineage_events:>8}")
    typer.echo(f"  runs               {result.runs:>8}")
    typer.echo(f"  run input mappings {result.run_input_mappings:>8}")
    typer.echo(f"  run output mappings{result.run_output_mappings:>8}")
    typer.echo(f"  dataset_versions   {result.dataset_versions:>8}")
    typer.echo(f"  job_versions       {result.job_versions:>8}")
    typer.echo(f"  {'─' * 29}")
    typer.echo(f"  total              {result.total:>8}")

    if dry_run:
        typer.echo("\nRun without --dry-run to apply.")
    else:
        typer.echo(
            typer.style(f"\nDone. {result.total} row(s) removed.", fg=typer.colors.GREEN)
        )
