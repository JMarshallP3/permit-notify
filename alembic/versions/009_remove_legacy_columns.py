"""Remove legacy columns: operator, permit_no, lease_no

Revision ID: 009_remove_legacy_columns
Revises: 008_clear_incorrect_data
Create Date: 2025-09-23 19:55:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '009_remove_legacy_columns'
down_revision = '008_clear_incorrect_data'
branch_labels = None
depends_on = None

def upgrade():
    """Remove legacy columns: operator, permit_no, lease_no"""
    # Drop the columns
    op.drop_column('permits', 'operator')
    op.drop_column('permits', 'permit_no')
    op.drop_column('permits', 'lease_no')

def downgrade():
    """Re-add legacy columns for downgrade"""
    # Re-add columns with appropriate types and nullability
    op.add_column('permits', sa.Column('operator', sa.String(200), nullable=True))
    op.add_column('permits', sa.Column('permit_no', sa.String(50), nullable=True))
    op.add_column('permits', sa.Column('lease_no', sa.String(100), nullable=True))
