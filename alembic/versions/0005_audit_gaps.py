"""Audit gaps — no schema changes needed (all columns exist)

Revision ID: 0005_audit_gaps
Revises: 0004_traceability
Create Date: 2026-03-14 00:00:00.000000
"""

from typing import Sequence, Union

revision: str = "0005_audit_gaps"
down_revision: Union[str, None] = "0004_traceability"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
