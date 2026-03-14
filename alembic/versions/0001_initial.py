"""Initial schema

Revision ID: 0001_initial
Revises:
Create Date: 2024-01-01 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # namespaces
    op.create_table(
        "namespaces",
        sa.Column("uuid", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("name", sa.String(1024), unique=True, nullable=False),
        sa.Column("current_owner_name", sa.String(1024)),
        sa.Column("description", sa.Text),
        sa.Column("is_hidden", sa.Boolean, default=False, nullable=False),
    )

    # sources
    op.create_table(
        "sources",
        sa.Column("uuid", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("type", sa.String(64), nullable=False),
        sa.Column("name", sa.String(1024), unique=True, nullable=False),
        sa.Column("connection_url", sa.Text),
        sa.Column("description", sa.Text),
    )

    # datasets
    op.create_table(
        "datasets",
        sa.Column("uuid", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("namespace_uuid", postgresql.UUID(as_uuid=True), sa.ForeignKey("namespaces.uuid"), nullable=False),
        sa.Column("source_uuid", postgresql.UUID(as_uuid=True), sa.ForeignKey("sources.uuid")),
        sa.Column("name", sa.String(1024), nullable=False),
        sa.Column("type", sa.String(64), nullable=False, server_default="DB_TABLE"),
        sa.Column("physical_name", sa.String(1024)),
        sa.Column("description", sa.Text),
        sa.Column("is_hidden", sa.Boolean, default=False, nullable=False),
        sa.Column("last_modified_at", sa.DateTime(timezone=True)),
        sa.Column("current_version_uuid", postgresql.UUID(as_uuid=True)),
        sa.UniqueConstraint("namespace_uuid", "name"),
    )

    # dataset_fields
    op.create_table(
        "dataset_fields",
        sa.Column("uuid", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("dataset_uuid", postgresql.UUID(as_uuid=True), sa.ForeignKey("datasets.uuid"), nullable=False),
        sa.Column("name", sa.String(1024), nullable=False),
        sa.Column("type", sa.String(64)),
        sa.Column("description", sa.Text),
        sa.UniqueConstraint("dataset_uuid", "name"),
    )

    # dataset_tags
    op.create_table(
        "dataset_tags",
        sa.Column("dataset_uuid", postgresql.UUID(as_uuid=True), sa.ForeignKey("datasets.uuid"), primary_key=True),
        sa.Column("name", sa.String(1024), primary_key=True),
        sa.UniqueConstraint("dataset_uuid", "name"),
    )

    # dataset_field_tags
    op.create_table(
        "dataset_field_tags",
        sa.Column("field_uuid", postgresql.UUID(as_uuid=True), sa.ForeignKey("dataset_fields.uuid"), primary_key=True),
        sa.Column("name", sa.String(1024), primary_key=True),
        sa.UniqueConstraint("field_uuid", "name"),
    )

    # dataset_versions
    op.create_table(
        "dataset_versions",
        sa.Column("uuid", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("dataset_uuid", postgresql.UUID(as_uuid=True), sa.ForeignKey("datasets.uuid"), nullable=False),
        sa.Column("version", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("namespace_name", sa.String(1024)),
        sa.Column("dataset_name", sa.String(1024)),
        sa.Column("lifecycle_state", sa.String(64)),
        sa.Column("run_uuid", postgresql.UUID(as_uuid=True)),
        sa.Column("schema_version_uuid", postgresql.UUID(as_uuid=True)),
    )
    op.create_index("ix_dataset_versions_dataset_uuid", "dataset_versions", ["dataset_uuid"])

    # jobs
    op.create_table(
        "jobs",
        sa.Column("uuid", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("namespace_uuid", postgresql.UUID(as_uuid=True), sa.ForeignKey("namespaces.uuid"), nullable=False),
        sa.Column("name", sa.String(1024), nullable=False),
        sa.Column("type", sa.String(64), nullable=False, server_default="BATCH"),
        sa.Column("description", sa.Text),
        sa.Column("current_version_uuid", postgresql.UUID(as_uuid=True)),
        sa.Column("is_hidden", sa.Boolean, default=False, nullable=False),
        sa.UniqueConstraint("namespace_uuid", "name"),
    )

    # runs
    op.create_table(
        "runs",
        sa.Column("uuid", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("job_uuid", postgresql.UUID(as_uuid=True), sa.ForeignKey("jobs.uuid")),
        sa.Column("nominal_start_time", sa.DateTime(timezone=True)),
        sa.Column("nominal_end_time", sa.DateTime(timezone=True)),
        sa.Column("current_run_state", sa.String(64)),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("ended_at", sa.DateTime(timezone=True)),
        sa.Column("facets", postgresql.JSON),
    )

    # runs_input_mapping
    op.create_table(
        "runs_input_mapping",
        sa.Column("run_uuid", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.uuid"), primary_key=True),
        sa.Column("dataset_uuid", postgresql.UUID(as_uuid=True), sa.ForeignKey("datasets.uuid"), primary_key=True),
        sa.UniqueConstraint("run_uuid", "dataset_uuid"),
    )

    # runs_output_mapping
    op.create_table(
        "runs_output_mapping",
        sa.Column("run_uuid", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.uuid"), primary_key=True),
        sa.Column("dataset_uuid", postgresql.UUID(as_uuid=True), sa.ForeignKey("datasets.uuid"), primary_key=True),
        sa.UniqueConstraint("run_uuid", "dataset_uuid"),
    )

    # lineage_events (raw OpenLineage event store)
    op.create_table(
        "lineage_events",
        sa.Column("event_uuid", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("run_uuid", postgresql.UUID(as_uuid=True)),
        sa.Column("job_name", sa.String(1024)),
        sa.Column("job_namespace", sa.String(1024)),
        sa.Column("producer", sa.Text),
        sa.Column("payload", postgresql.JSON, nullable=False),
    )

    # Indexes for common query patterns
    op.create_index("ix_datasets_namespace_uuid", "datasets", ["namespace_uuid"])
    op.create_index("ix_jobs_namespace_uuid", "jobs", ["namespace_uuid"])
    op.create_index("ix_runs_job_uuid", "runs", ["job_uuid"])
    op.create_index("ix_runs_state", "runs", ["current_run_state"])
    op.create_index("ix_lineage_events_run_uuid", "lineage_events", ["run_uuid"])
    op.create_index("ix_lineage_events_event_time", "lineage_events", ["event_time"])


def downgrade() -> None:
    op.drop_table("lineage_events")
    op.drop_table("runs_output_mapping")
    op.drop_table("runs_input_mapping")
    op.drop_table("runs")
    op.drop_table("jobs")
    op.drop_table("dataset_versions")
    op.drop_table("dataset_field_tags")
    op.drop_table("dataset_tags")
    op.drop_table("dataset_fields")
    op.drop_table("datasets")
    op.drop_table("sources")
    op.drop_table("namespaces")
