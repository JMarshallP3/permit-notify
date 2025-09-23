"""Remove well_name and field columns from permits table

Revision ID: 004_remove_well_name_and_field
Revises: 003_fix_rrc_w1_schema
Create Date: 2025-01-23 19:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '004_remove_well_name_and_field'
down_revision = '003_fix_rrc_w1_schema'
branch_labels = None
depends_on = None


def upgrade():
    """Remove well_name and field columns from permits table."""
    # Check if columns exist before dropping them
    with op.batch_alter_table('permits') as batch_op:
        # Drop well_name column if it exists
        try:
            batch_op.drop_column('well_name')
        except Exception:
            pass  # Column doesn't exist, skip
        
        # Drop field column if it exists
        try:
            batch_op.drop_column('field')
        except Exception:
            pass  # Column doesn't exist, skip


def downgrade():
    """Add back well_name and field columns to permits table."""
    with op.batch_alter_table('permits') as batch_op:
        # Add well_name column back
        batch_op.add_column(sa.Column('well_name', sa.String(255), nullable=True))
        
        # Add field column back
        batch_op.add_column(sa.Column('field', sa.String(255), nullable=True))
