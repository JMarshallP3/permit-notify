"""add org_id to field_corrections table

Revision ID: 015_add_org_id_to_field_corrections
Revises: 014_add_is_injection_well_column
Create Date: 2025-09-30 16:40:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '015_add_org_id_to_field_corrections'
down_revision = '014_add_is_injection_well_column'
branch_labels = None
depends_on = None


def upgrade():
    # Add org_id column to field_corrections table if it exists
    try:
        op.add_column('field_corrections', sa.Column('org_id', sa.String(length=50), nullable=False, server_default='default_org'))
        
        # Remove server default after adding column
        op.alter_column('field_corrections', 'org_id', server_default=None)
        
        # Create index for efficient filtering
        op.create_index('ix_field_corrections_org_id', 'field_corrections', ['org_id'])
        
    except Exception as e:
        # Table might not exist yet, that's okay
        print(f"Note: Could not add org_id to field_corrections table: {e}")
        pass


def downgrade():
    # Drop index and column
    try:
        op.drop_index('ix_field_corrections_org_id', table_name='field_corrections')
        op.drop_column('field_corrections', 'org_id')
    except Exception:
        pass
