"""Add facets column to datasets and jobs

Revision ID: 0003_add_facets_columns
Revises: 0002_add_tags_table
Create Date: 2026-03-14 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003_add_facets_columns"
down_revision: Union[str, None] = "0002_add_tags_table"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("datasets", sa.Column("facets", postgresql.JSONB, server_default="{}", nullable=False))
    op.add_column("jobs", sa.Column("facets", postgresql.JSONB, server_default="{}", nullable=False))


def downgrade() -> None:
    op.drop_column("datasets", "facets")
    op.drop_column("jobs", "facets")
