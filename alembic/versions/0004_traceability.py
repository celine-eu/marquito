"""Add traceability / audit compliance columns

Revision ID: 0004_traceability
Revises: 0003_add_facets_columns
Create Date: 2026-03-14 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004_traceability"
down_revision: Union[str, None] = "0003_add_facets_columns"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add dataset_version_uuid to run mappings (nullable for backwards compat)
    op.add_column(
        "runs_input_mapping",
        sa.Column("dataset_version_uuid", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "runs_output_mapping",
        sa.Column("dataset_version_uuid", postgresql.UUID(as_uuid=True), nullable=True),
    )

    # Snapshot facets at each dataset version
    op.add_column(
        "dataset_versions",
        sa.Column("facets", postgresql.JSONB, server_default="{}", nullable=False),
    )

    # Job versions table
    op.create_table(
        "job_versions",
        sa.Column("uuid", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "job_uuid",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("jobs.uuid"),
            nullable=False,
        ),
        sa.Column("version", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("inputs", postgresql.JSONB, server_default="[]", nullable=False),
        sa.Column("outputs", postgresql.JSONB, server_default="[]", nullable=False),
        sa.Column("location", sa.Text, nullable=True),
        sa.Column("context", postgresql.JSONB, server_default="{}", nullable=False),
    )
    op.create_index("ix_job_versions_job_uuid", "job_versions", ["job_uuid"])


def downgrade() -> None:
    op.drop_index("ix_job_versions_job_uuid", table_name="job_versions")
    op.drop_table("job_versions")
    op.drop_column("dataset_versions", "facets")
    op.drop_column("runs_output_mapping", "dataset_version_uuid")
    op.drop_column("runs_input_mapping", "dataset_version_uuid")
