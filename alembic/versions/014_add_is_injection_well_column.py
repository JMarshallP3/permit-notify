"""add is_injection_well column

Revision ID: 014_add_is_injection_well_column
Revises: 013_add_tenant_isolation_and_events
Create Date: 2025-09-30 14:30:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '014_add_is_injection_well_column'
down_revision = '013_add_tenant_isolation_and_events'
branch_labels = None
depends_on = None


def upgrade():
    # Add is_injection_well column to permits table
    op.add_column('permits', sa.Column('is_injection_well', sa.Boolean(), nullable=False, server_default='false'))
    
    # Remove server default after adding column
    op.alter_column('permits', 'is_injection_well', server_default=None)
    
    # Create index for efficient filtering
    op.create_index('idx_permit_injection_well', 'permits', ['is_injection_well'])


def downgrade():
    # Drop index
    op.drop_index('idx_permit_injection_well', table_name='permits')
    
    # Remove is_injection_well column
    op.drop_column('permits', 'is_injection_well')
