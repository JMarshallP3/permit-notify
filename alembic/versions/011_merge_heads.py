"""Merge conflicting migration heads

Revision ID: 011_merge_heads
Revises: 002, 010_schema_cleanup
Create Date: 2025-09-25 15:30:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '011_merge_heads'
down_revision = ('002', '010_schema_cleanup')
branch_labels = None
depends_on = None

def upgrade():
    # This is a merge migration - no changes needed
    pass

def downgrade():
    # This is a merge migration - no changes needed
    pass
